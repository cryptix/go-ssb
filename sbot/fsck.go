// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package sbot

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/machinebox/progress"
	refs "github.com/ssbc/go-ssb-refs"
	"github.com/ssbc/go-ssb-refs/tfk"
	margaret "github.com/ssbc/margaret/v2"
	mroaring "github.com/ssbc/margaret/v2/multilog/roaring"
	kitlog "go.mindeco.de/log"
	"go.mindeco.de/log/level"

	"github.com/ssbc/go-ssb"
	"github.com/ssbc/go-ssb/internal/storedrefs"
	"github.com/ssbc/go-ssb/message/multimsg"
	"github.com/ssbc/go-ssb/multilogs"
)

// FSCKMode is an enum for the sbot.FSCK function
type FSCKMode uint

const (
	_ FSCKMode = iota

	// FSCKModeLength just checks the feed lengths
	FSCKModeLength

	// FSCKModeSequences makes sure the sequence field of each message on a feed are increasing correctly
	FSCKModeSequences

	// FSCKModeVerify does a full signature and hash verification
	// FSCKModeVerify
)

type ErrConsistencyProblems struct {
	Errors    []ssb.ErrWrongSequence
	Sequences *roaring.Bitmap
}

func (e ErrConsistencyProblems) Error() string {
	if len(e.Errors) == 1 {
		return e.Errors[0].Error()
	}
	errStr := fmt.Sprintf("ssb: multiple consistency problems (%d) over %d messages", len(e.Errors), e.Sequences.GetCardinality())
	for i, err := range e.Errors {
		errStr += fmt.Sprintf("\n%02d: %s", i, err.Error())
	}
	errStr += "\n"
	return errStr
}

type fsckOpt struct {
	feedsIdx   *mroaring.MultiLog
	mode       FSCKMode
	progressFn FSCKUpdateFunc
}

type FSCKOption func(*fsckOpt) error

func FSCKWithFeedIndex(idx *mroaring.MultiLog) FSCKOption {
	return func(o *fsckOpt) error {
		o.feedsIdx = idx
		return nil
	}
}

func FSCKWithMode(m FSCKMode) FSCKOption {
	return func(o *fsckOpt) error {
		if m != FSCKModeLength && m != FSCKModeSequences {
			return fmt.Errorf("invalid fsck mode: %d", m)
		}

		o.mode = m
		return nil
	}
}

func FSCKWithProgress(fn FSCKUpdateFunc) FSCKOption {
	return func(o *fsckOpt) error {
		if fn == nil {
			return fmt.Errorf("warning: nil progress func")
		}
		o.progressFn = fn
		return nil
	}
}

// FSCKUpdateFunc is called with the a percentage float between 0 and 100
// and a durration who much time it should take, rounded to seconds.
type FSCKUpdateFunc func(percentage float64, timeLeft time.Duration)

// FSCK checks the consistency of the received messages and the indexes.
// progressFn offers a way to track the progress. It's okay to pass nil, the set sbot.info logger is used in that case.
func (s *Sbot) FSCK(opts ...FSCKOption) error {
	var opt fsckOpt

	for i, o := range opts {
		err := o(&opt)
		if err != nil {
			return fmt.Errorf("sbot/fsck: option #%d failed: %w", i, err)
		}
	}

	if opt.feedsIdx == nil {
		var ok bool
		opt.feedsIdx, ok = s.GetMultiLog(multilogs.IndexNameFeeds)
		if !ok {
			return errors.New("sbot: no users multilog")
		}
	}

	if opt.progressFn == nil {
		opt.progressFn = func(percentage float64, timeLeft time.Duration) {
			level.Info(s.info).Log("event", "fsck-progress", "done", percentage, "time-left", timeLeft.String())
		}
	}

	if opt.mode == 0 { // default to quick check
		opt.mode = FSCKModeLength
	}

	switch opt.mode {
	case FSCKModeLength:
		return lengthFSCK(opt.feedsIdx, s.ReceiveLog)

	case FSCKModeSequences:
		// sequences mode also runs the length check to catch index corruption
		if err := lengthFSCK(opt.feedsIdx, s.ReceiveLog); err != nil {
			return err
		}
		return sequenceFSCK(s.ReceiveLog, opt.progressFn)

	default:
		return errors.New("sbot: unknown fsck mode")
	}
}

// lengthFSCK checks the length of each stored feed and collects ALL broken feeds.
func lengthFSCK(authorMlog *mroaring.MultiLog, receiveLog margaret.Log[*multimsg.MultiMessage]) error {
	feeds, err := authorMlog.List()
	if err != nil {
		return fmt.Errorf("fsck/length: author listing failed: %w", err)
	}

	var problems []ssb.ErrWrongSequence

	for _, author := range feeds {
		var sr tfk.Feed
		err := sr.UnmarshalBinary([]byte(author))
		if err != nil {
			return fmt.Errorf("fsck/length: failed to unpack author %q: %w", author, err)
		}

		subLog, err := authorMlog.Get(author)
		if err != nil {
			return fmt.Errorf("fsck/length: failed to get sublog for %q: %w", author, err)
		}

		currentSeqFromIndex := subLog.Seq()

		if currentSeqFromIndex == margaret.SeqEmpty {
			continue
		}

		rxEntry, err := subLog.Get(currentSeqFromIndex)
		if err != nil {
			if margaret.IsErrNulled(err) {
				continue
			}
			return fmt.Errorf("fsck/length: failed to get rxlog entry for index entry %d for author %q: %w", currentSeqFromIndex, author, err)
		}

		rxSeq := int64(*rxEntry)
		mm, err := receiveLog.Get(rxSeq)
		if err != nil {
			if margaret.IsErrNulled(err) {
				continue
			}
			return fmt.Errorf("fsck/length: failed to load rxlog entry %d for %q: %w", rxSeq, author, err)
		}

		if mm.Message == nil {
			continue
		}
		msg := mm.Message

		// margaret indexes are 0-based, therefore +1
		if msg.Seq() != currentSeqFromIndex+1 {
			fr, err := sr.Feed()
			if err != nil {
				return fmt.Errorf("fsck/length: failed to feed reference for author (%q): %w", author, err)
			}
			problems = append(problems, ssb.ErrWrongSequence{
				Ref:     fr,
				Stored:  currentSeqFromIndex,
				Logical: msg.Seq(),
			})
		}
	}

	if len(problems) == 0 {
		return nil
	}

	// Build a bitmap of all rxlog sequences belonging to broken feeds
	// so the repair path can null them.
	nullMap := roaring.New()
	for _, p := range problems {
		feedAddr := storedrefs.Feed(p.Ref)
		subLog, err := authorMlog.Get(feedAddr)
		if err != nil {
			continue
		}
		qry := subLog.Query()
		for _, entry := range qry.Iter() {
			nullMap.Add(uint32(int64(*entry)))
		}
	}

	return ErrConsistencyProblems{
		Errors:    problems,
		Sequences: nullMap,
	}
}

// implements machinebox/progress.Counter
type processedCounter struct {
	mu sync.Mutex
	n  int64
}

func (p *processedCounter) Incr() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.n++
}

func (p *processedCounter) N() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.n
}

func (p *processedCounter) Err() error { return nil }

// sequenceFSCK goes through every message in the receiveLog
// and checks tha the sequence of a feed is correctly increasing by one each message
func sequenceFSCK(receiveLog margaret.Log[*multimsg.MultiMessage], progressFn FSCKUpdateFunc) error {
	// the last sequence number we saw of that author
	lastSequence := make(map[string]int64)

	// we need to keep track of _all_ the messages per feed
	// since we dont know in advance which ones we have to null
	allSeqsPerAuthor := make(map[string]*roaring.Bitmap)

	totalMessages := receiveLog.Seq()
	var pc processedCounter

	qry := receiveLog.Query()

	// which feeds have problems
	var consistencyErrors []ssb.ErrWrongSequence
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		p := progress.NewTicker(ctx, &pc, int64(totalMessages), 3*time.Second)
		for remaining := range p {
			estDone := remaining.Estimated()
			// how much time until it's done?
			timeLeft := estDone.Sub(time.Now()).Round(time.Second)
			progressFn(remaining.Percent(), timeLeft)
		}
	}()
	defer cancel()

	var nulled, checked int64
	for rxLogSeq, mm := range qry.Iter() {
		if mm.Message == nil {
			nulled++
			pc.Incr()
			continue
		}
		checked++
		msg := mm.Message

		msgSeq := msg.Seq()
		authorRef := msg.Author().String()

		seqMap, ok := allSeqsPerAuthor[authorRef]
		if !ok {
			seqMap = roaring.New()
			allSeqsPerAuthor[authorRef] = seqMap
		}
		seqMap.Add(uint32(rxLogSeq))

		currSeq, has := lastSequence[authorRef]

		if !has {
			if msgSeq != 1 { // not seen yet, so has to be the first
				seqErr := ssb.ErrWrongSequence{
					Ref:     msg.Author(),
					Stored:  rxLogSeq,
					Logical: int64(msg.Seq()),
				}
				consistencyErrors = append(consistencyErrors, seqErr)
				lastSequence[authorRef] = -1
				pc.Incr()
				continue
			}
			lastSequence[authorRef] = 1
			pc.Incr()
			continue
		}

		if currSeq < 0 { // feed broken, skipping
			pc.Incr()
			continue
		}

		if currSeq+1 != msgSeq { // correct next value?
			seqErr := ssb.ErrWrongSequence{
				Ref:     msg.Author(),
				Stored:  int64(currSeq + 1),
				Logical: int64(msg.Seq()),
			}
			consistencyErrors = append(consistencyErrors, seqErr)
			lastSequence[authorRef] = -1
			pc.Incr()
			continue
		}
		lastSequence[authorRef] = currSeq + 1

		// bench stats
		pc.Incr()
	}

	if err := qry.Err(); err != nil {
		return fmt.Errorf("fsck/seq: query error: %w", err)
	}

	skipped := int64(totalMessages) + 1 - checked - nulled // entries the iterator didn't return (storage-level nulled)
	fmt.Printf("fsck/sequences: checked %d messages across %d feeds (rxlog has %d entries, %d nulled at storage level)\n",
		checked, len(lastSequence), totalMessages+1, skipped)

	if len(consistencyErrors) == 0 {
		return nil
	}

	nullMap := roaring.New()
	for _, author := range consistencyErrors {
		if bmap, has := allSeqsPerAuthor[author.Ref.String()]; has {
			nullMap.Or(bmap)
		}
	}

	// error report
	return ErrConsistencyProblems{
		Errors:    consistencyErrors,
		Sequences: nullMap,
	}
}

// rxEntry holds an entry found in the rxlog for a specific feed during repair scanning.
type rxEntry struct {
	rxSeq  int64 // position in the global receive log
	msgSeq int64 // SSB message sequence number
}

// HealRepo repairs all broken feeds by scanning the ENTIRE rxlog to find
// ALL entries (including orphaned ones not referenced by any sublog), then
// truncating each feed to its last valid message chain and nulling everything else.
func (s *Sbot) HealRepo(report ErrConsistencyProblems) error {
	funcLog := kitlog.With(s.info, "event", "heal repo")
	brokenCount := len(report.Errors)
	if brokenCount == 0 {
		level.Warn(funcLog).Log("msg", "no errors to repair, run FSCK first.")
		return nil
	}

	level.Info(funcLog).Log("msg", "repairing broken feeds",
		"feeds", brokenCount,
	)

	// Build a set of broken feed author strings for fast lookup.
	brokenFeeds := make(map[string]refs.FeedRef, brokenCount)
	for _, e := range report.Errors {
		brokenFeeds[e.Ref.String()] = e.Ref
	}

	// Step 1: Scan the ENTIRE rxlog to find ALL entries belonging to broken feeds.
	// This catches orphaned entries that aren't referenced by any sublog.
	entriesPerFeed := make(map[string][]rxEntry)

	qry := s.ReceiveLog.Query()
	for rxLogSeq, mm := range qry.Iter() {
		if mm.Message == nil {
			continue // nulled entry
		}
		authorStr := mm.Message.Author().String()
		if _, broken := brokenFeeds[authorStr]; !broken {
			continue // not a broken feed, skip
		}
		entriesPerFeed[authorStr] = append(entriesPerFeed[authorStr], rxEntry{
			rxSeq:  rxLogSeq,
			msgSeq: int64(mm.Message.Seq()),
		})
	}
	if err := qry.Err(); err != nil {
		return fmt.Errorf("heal: rxlog scan error: %w", err)
	}

	// Step 2: For each broken feed, find the valid chain and null everything else.
	for authorStr, ref := range brokenFeeds {
		entries := entriesPerFeed[authorStr]
		err := s.repairFeedEntries(ref, entries, funcLog)
		if err != nil {
			return fmt.Errorf("heal: failed to repair feed %s: %w", ref.ShortSigil(), err)
		}
	}

	return nil
}

// repairFeedEntries repairs a broken feed given ALL its rxlog entries (found by
// scanning the entire rxlog). It finds the longest valid chain starting from
// sequence 1, nulls all other entries (including orphaned duplicates), and
// rebuilds the sublog with only the valid portion.
//
// entries must be in rxlog order (ascending), which is guaranteed because
// HealRepo scans the rxlog linearly.
func (s *Sbot) repairFeedEntries(ref refs.FeedRef, entries []rxEntry, log kitlog.Logger) error {
	feedAddr := storedrefs.Feed(ref)

	// Find the valid chain: seq 1, 2, 3, ... taking the FIRST occurrence of each.
	// Since entries are in rxlog order, the first occurrence of each sequence number
	// is the original entry; later occurrences are duplicates/orphans.
	var validRxSeqs []int64
	nextExpectedSeq := int64(1)
	for _, e := range entries {
		if e.msgSeq == nextExpectedSeq {
			validRxSeqs = append(validRxSeqs, e.rxSeq)
			nextExpectedSeq++
		}
	}

	// Null all entries that are NOT in the valid chain.
	validSet := make(map[int64]bool, len(validRxSeqs))
	for _, rxSeq := range validRxSeqs {
		validSet[rxSeq] = true
	}
	nulled := 0
	for _, e := range entries {
		if !validSet[e.rxSeq] {
			if err := s.ReceiveLog.Null(e.rxSeq); err != nil {
				level.Warn(log).Log("event", "null-failed", "rxSeq", e.rxSeq, "err", err)
			} else {
				nulled++
			}
		}
	}

	// Delete the old sublog.
	if err := s.Users.Delete(feedAddr); err != nil {
		return fmt.Errorf("failed to delete sublog: %w", err)
	}

	// Recreate the sublog with only valid entries.
	if len(validRxSeqs) > 0 {
		newSubLog, err := s.Users.Get(feedAddr)
		if err != nil {
			return fmt.Errorf("failed to recreate sublog: %w", err)
		}
		for _, rxSeq := range validRxSeqs {
			seq := mroaring.Seq(rxSeq)
			if _, err := newSubLog.Append(&seq); err != nil {
				return fmt.Errorf("failed to re-add valid entry: %w", err)
			}
		}
	}

	// Clean up other indexes for this feed.
	if err := s.GraphBuilder.DeleteAuthor(ref); err != nil {
		level.Warn(log).Log("event", "graph-delete-failed", "err", err)
	}

	// Remove EBT state for this feed.
	sfn, err := s.ebtState.StateFileName(s.KeyPair.ID())
	if err == nil {
		os.Remove(sfn)
	}

	if !s.disableNetwork {
		s.verifyRouter.CloseSink(ref)
	}

	level.Info(log).Log("event", "feed-repaired",
		"feed", ref.ShortSigil(),
		"valid", len(validRxSeqs),
		"nulled", nulled,
		"total-entries", len(entries),
	)

	return nil
}
