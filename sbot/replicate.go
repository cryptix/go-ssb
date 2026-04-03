// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package sbot

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.mindeco.de/log"
	"go.mindeco.de/log/level"

	"github.com/ssbc/go-ssb"
	refs "github.com/ssbc/go-ssb-refs"
	"github.com/ssbc/go-ssb/internal/statematrix"
	"github.com/ssbc/go-ssb/internal/storedrefs"
)

var _ ssb.Replicator = (*Sbot)(nil)

// Replicate mark a feed for replication and connection acceptance
func (sbot *Sbot) Replicate(r refs.FeedRef) error {
	slog, err := sbot.Users.Get(storedrefs.Feed(r))
	if err != nil {
		return fmt.Errorf("sbot/replicate: failed to get user feed: %w", err)
	}

	// convert margaret 0-indexed to SSB 1-indexed sequence
	// margaret: -1=empty, 0=one msg, 1=two msgs, ...
	// EBT:       0=want all, 1=have one,  2=have two,  ...
	seq := slog.Seq() + 1

	sbot.ebtState.Fill(sbot.KeyPair.ID(), []statematrix.ObservedFeed{
		{Feed: r, Note: ssb.Note{Seq: seq, Receive: true, Replicate: true}},
	})

	sbot.Replicator.Replicate(r)

	// push updated want-clock to active EBT sessions
	if sbot.ebtHandler != nil {
		sbot.ebtHandler.PushState()
	}
	return nil
}

func (sbot *Sbot) DontReplicate(r refs.FeedRef) error {
	slog, err := sbot.Users.Get(storedrefs.Feed(r))
	if err != nil {
		return fmt.Errorf("sbot/dontReplicate: failed to get user feed: %w", err)
	}

	// convert margaret 0-indexed to SSB 1-indexed sequence
	seq := slog.Seq() + 1

	sbot.ebtState.Fill(sbot.KeyPair.ID(), []statematrix.ObservedFeed{
		{Feed: r, Note: ssb.Note{Seq: seq, Receive: false, Replicate: true}},
	})

	sbot.Replicator.DontReplicate(r)

	// push updated want-clock to active EBT sessions
	if sbot.ebtHandler != nil {
		sbot.ebtHandler.PushState()
	}
	return nil
}

type graphReplicator struct {
	bot     *Sbot
	current *lister
}

func (s *Sbot) newGraphReplicator() (*graphReplicator, error) {
	var r graphReplicator
	r.bot = s
	r.current = newLister()

	replicateEvt := log.With(s.info, "event", "update-replicate")
	update := r.makeUpdater(replicateEvt, s.KeyPair.ID(), int(s.hopCount))

	// Populate the replication list once at startup from existing graph data.
	// Hops() and Build() both wait for index sync internally, so this is safe
	// to run concurrently with index loading.
	go update()

	// Re-run whenever the graph changes (new contact/metafeed messages).
	// Uses GraphBuilder.Seq() so that non-contact messages don't trigger
	// unnecessary hops recalculations.
	go debounce(s.rootCtx, 30*time.Second, s.GraphBuilder, update)

	return &r, nil
}

// makeUpdater returns a func that does the hop-walk and block checks, used together with debounce
func (r *graphReplicator) makeUpdater(log log.Logger, self refs.FeedRef, hopCount int) func() {
	return func() {
		start := time.Now()
		newWants := r.bot.GraphBuilder.Hops(self, hopCount)

		refs, err := newWants.List()
		if err != nil {
			level.Error(log).Log("msg", "want list failed", "err", err, "wants", newWants.Count())
			return
		}
		for _, ref := range refs {
			r.current.feedWants.AddRef(ref)
		}

		level.Debug(log).Log("feed-want-count", r.current.feedWants.Count(), "hops", hopCount, "took", time.Since(start))

		// make sure we dont fetch and allow blocked feeds
		g, err := r.bot.GraphBuilder.Build()
		if err != nil {
			level.Error(log).Log("msg", "failed to build blocks", "err", err)
			return
		}

		newBlocked := g.BlockedList(self)
		lst, err := newBlocked.List()
		if err == nil {
			for _, bf := range lst {
				r.current.blocked.AddRef(bf)
				r.current.feedWants.Delete(bf)
			}
		}
	}
}

// seqer is a minimal interface for checking the current sequence of a log.
type seqer interface {
	Seq() int64
}

// debounce watches rxlog.Seq() for changes and calls work() after interval of
// no further changes. The timer is not armed at startup; work() only fires when
// at least one change has been observed. This means rxlog controls what counts
// as a "change" — passing GraphBuilder instead of ReceiveLog limits work() to
// graph-relevant messages only.
func debounce(ctx context.Context, interval time.Duration, rxlog seqer, work func()) {
	var mu sync.Mutex
	var pending bool
	lastSeen := rxlog.Seq()

	// Start the timer in a stopped state; it is only armed when a change is seen.
	timer := time.NewTimer(interval)
	if !timer.Stop() {
		<-timer.C
	}

	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				newSeq := rxlog.Seq()
				mu.Lock()
				if newSeq != lastSeen {
					lastSeen = newSeq
					pending = true
					timer.Reset(interval)
				}
				mu.Unlock()
			}
		}
	}()

	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			mu.Lock()
			if pending {
				pending = false
				mu.Unlock()
				work()
			} else {
				mu.Unlock()
			}
		}
	}
}

func (r *graphReplicator) Block(ref refs.FeedRef)   { r.current.blocked.AddRef(ref) }
func (r *graphReplicator) Unblock(ref refs.FeedRef) { r.current.blocked.Delete(ref) }

func (r *graphReplicator) Replicate(ref refs.FeedRef) error {
	r.current.feedWants.AddRef(ref)
	return nil
}
func (r *graphReplicator) DontReplicate(ref refs.FeedRef) error {
	r.current.feedWants.Delete(ref)
	return nil
}

func (r *graphReplicator) Lister() ssb.ReplicationLister { return r.current }

type lister struct {
	feedWants *ssb.StrFeedSet
	blocked   *ssb.StrFeedSet
}

func newLister() *lister {
	return &lister{
		feedWants: ssb.NewFeedSet(0),
		blocked:   ssb.NewFeedSet(0),
	}
}

func (l lister) Authorize(remote refs.FeedRef) error {
	if l.blocked.Has(remote) {
		return errors.New("peer blocked")
	}

	if l.feedWants.Has(remote) {
		return nil
	}
	return errors.New("nope - access denied")
}

func (l lister) ReplicationList() *ssb.StrFeedSet { return l.feedWants }
func (l lister) BlockList() *ssb.StrFeedSet       { return l.blocked }
