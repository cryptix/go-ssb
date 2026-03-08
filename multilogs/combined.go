// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package multilogs

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/dgraph-io/sroar"
	"github.com/keks/persist"

	margaret "github.com/ssbc/margaret/v2"
	"github.com/ssbc/margaret/v2/multilog"
	"github.com/ssbc/margaret/v2/multilog/roaring"

	gabbygrove "github.com/ssbc/go-gabbygrove"
	"github.com/ssbc/go-ssb"
	refs "github.com/ssbc/go-ssb-refs"
	"github.com/ssbc/go-ssb/internal/statematrix"
	"github.com/ssbc/go-ssb/internal/storedrefs"
	"github.com/ssbc/go-ssb/message/multimsg"
	"github.com/ssbc/go-ssb/private"
	"github.com/ssbc/go-ssb/repo"
)

// NewCombinedIndex creates one big index which updates the multilogs users, byType, private and tangles.
// Compared to the "old" fatbot approach of just having 4 independent indexes,
// this one updates all 4 of them, resulting in less read-overhead
// while also being able to index private messages by tangle and type.
func NewCombinedIndex(
	repoPath string,
	box *private.Manager,
	self refs.FeedRef,
	rxlog margaret.Log[*multimsg.MultiMessage],
	u, p, bt, tan *roaring.MultiLog,
	channels, mentions *roaring.MultiLog,
	oh *roaring.MultiLog,
	sm *statematrix.StateMatrix,
) (*CombinedIndex, error) {
	r := repo.New(repoPath)
	statePath := r.GetPath(repo.PrefixMultiLog, "combined-state.json")
	mode := os.O_RDWR | os.O_EXCL
	if _, err := os.Stat(statePath); os.IsNotExist(err) {
		mode |= os.O_CREATE
	}
	os.MkdirAll(filepath.Dir(statePath), 0700)
	idxStateFile, err := os.OpenFile(statePath, mode, 0700)
	if err != nil {
		return nil, fmt.Errorf("error opening state file: %w", err)
	}

	idx := &CombinedIndex{
		self:  self,
		boxer: box,

		// application multilogs
		users:    u,
		private:  p,
		byType:   bt,
		tangles:  tan,
		channels: channels,
		mentions: mentions,

		ebtState: sm,

		// groups reindexing
		rxlog:        rxlog,
		orderdHelper: oh,

		file:      idxStateFile,
		l:         &sync.Mutex{},
		latestSeq: margaret.SeqEmpty,
	}

	// Load the persisted sequence so latestSeq matches what's on disk.
	var diskSeq int64
	if err := persist.Load(idx.file, &diskSeq); err == nil {
		idx.latestSeq = diskSeq
	}

	return idx, nil
}

type CombinedIndex struct {
	self  refs.FeedRef
	boxer *private.Manager

	rxlog margaret.Log[*multimsg.MultiMessage]

	users    *roaring.MultiLog
	private  *roaring.MultiLog
	byType   *roaring.MultiLog
	tangles  *roaring.MultiLog
	channels *roaring.MultiLog
	mentions *roaring.MultiLog

	orderdHelper *roaring.MultiLog

	ebtState *statematrix.StateMatrix

	file *os.File
	l    *sync.Mutex

	// latestSeq tracks the most recently processed rxlog sequence in memory.
	// It's only persisted to the state file AFTER flushing the roaring multilogs,
	// ensuring the state file never gets ahead of the actual bitmap data on disk.
	latestSeq int64

	// onEntry is called for each processed entry during Index(), if set.
	// Used by serveIndex to report progress.
	onEntry func()
}

// SetOnEntry sets a callback that is invoked for each entry processed by Index().
func (idx *CombinedIndex) SetOnEntry(fn func()) {
	idx.onEntry = fn
}

// appendSeq creates a *roaring.Seq from an int64 and appends it to the log.
func appendSeq(log margaret.Log[*roaring.Seq], seq int64) error {
	s := roaring.Seq(seq)
	_, err := log.Append(&s)
	return err
}

// Box2Reindex takes advantage of the other bitmap indexes to reindex just the messages
// from the passed author that are box2 but not yet readable by us.
//  1. taking private:meta:box2
//  2. ANDing it with the one of the author (intersection)
//  3. subtracting all the messages we _can_ read (private:box2:$ourFeed)
func (idx *CombinedIndex) Box2Reindex(author refs.FeedRef) error {
	idx.l.Lock()
	defer idx.l.Unlock()

	// (1) all messages in boxed2 format
	allBox2, err := idx.private.LoadInternalBitmap(multilog.Addr("meta:box2"))
	if err != nil {
		return fmt.Errorf("error getting all box2 messages: %w", err)
	}

	// (2) all messages by the author we should re-index
	fromAuthor, err := idx.users.LoadInternalBitmap(storedrefs.Feed(author))
	if err != nil {
		if !errors.Is(err, multilog.ErrNotFound) {
			return fmt.Errorf("error getting all from author: %w", err)
		}
		fromAuthor = sroar.NewBitmap()
	}

	// (3) intersection between the two
	fromAuthor.And(allBox2)

	if fromAuthor.GetCardinality() == 0 {
		fmt.Println("skipping empty set", allBox2.GetCardinality(), author.String())
		return nil
	}

	// (4) all messages we can already decrypt
	myReadableAddr := multilog.Addr("box2:") + storedrefs.Feed(idx.self)
	myReadable, err := idx.private.LoadInternalBitmap(myReadableAddr)
	if err != nil {
		return fmt.Errorf("error getting my readable: %w", err)
	}

	// (5) subtract those (4) from (3)
	readableIt := myReadable.NewIterator()
	for i := 0; i < myReadable.GetCardinality(); i++ {
		v := readableIt.Next()
		if fromAuthor.Contains(v) {
			fromAuthor.Remove(v)
		}
	}

	// (6) iterate over those and reindex them
	it := fromAuthor.NewIterator()
	for i := 0; i < fromAuthor.GetCardinality(); i++ {
		rxSeq := int64(it.Next())

		mm, err := idx.rxlog.Get(rxSeq)
		if err != nil {
			return err
		}

		err = idx.update(rxSeq, mm)
		if err != nil {
			return err
		}
	}

	return nil
}

// ProcessEntry processes a single log entry, updating all sublogs.
//
// The sequence number is tracked in memory (latestSeq) but NOT persisted
// to the state file here. State is only persisted after flushing the roaring
// bitmap data to disk (in Index() or FlushAndSave()), ensuring the state file
// never gets ahead of the actual bitmap data. Re-processing on restart is safe
// because sublog.Append() is idempotent (skips duplicates via bitmap Contains check).
func (idx *CombinedIndex) ProcessEntry(seq int64, mm *multimsg.MultiMessage) error {
	idx.l.Lock()
	defer idx.l.Unlock()

	if mm.Message != nil {
		err := idx.update(seq, mm)
		if err != nil {
			return err
		}
	}

	idx.latestSeq = seq
	return nil
}

// LastProcessedSeq returns the sequence number of the last processed message.
// This returns the in-memory value, which may be ahead of what's persisted on disk.
func (idx *CombinedIndex) LastProcessedSeq() int64 {
	idx.l.Lock()
	defer idx.l.Unlock()
	return idx.latestSeq
}

// Index processes all unprocessed entries from the given log.
func (idx *CombinedIndex) Index(log margaret.Log[*multimsg.MultiMessage]) error {
	lastSeq := idx.LastProcessedSeq()

	var opts []margaret.QueryOption
	if lastSeq >= 0 {
		opts = append(opts, margaret.Gt(lastSeq))
	}

	var count int
	qry := log.Query(opts...)
	for seq, mm := range qry.Iter() {
		if err := idx.ProcessEntry(seq, mm); err != nil {
			return err
		}
		if idx.onEntry != nil {
			idx.onEntry()
		}
		count++
		// Periodically flush the roaring bitmaps AND persist state during
		// bulk re-indexing. State is only saved AFTER flush, so the state
		// file can never reference bitmap data that isn't on disk yet.
		if count%1000 == 0 {
			if err := idx.users.Flush(); err != nil {
				return fmt.Errorf("error flushing user feeds during index: %w", err)
			}
			if err := persist.Save(idx.file, idx.latestSeq); err != nil {
				return fmt.Errorf("error saving state after flush: %w", err)
			}
		}
	}
	// Final flush + state save after processing all entries.
	if count > 0 {
		if err := idx.users.Flush(); err != nil {
			return fmt.Errorf("error flushing user feeds after index: %w", err)
		}
		if err := persist.Save(idx.file, idx.latestSeq); err != nil {
			return fmt.Errorf("error saving state after final flush: %w", err)
		}
	}
	return qry.Err()
}

// VerifyConsistency checks that the CombinedIndex's last-processed-seq is
// consistent with what's actually in the user-feeds multilog. If the state
// file is ahead of the multilog (e.g., due to a crash between state save
// and multilog flush), it rewinds the state to force a full re-index.
//
// The check works by comparing each feed's sublog length (number of indexed
// entries) with the message sequence of its latest entry. If a feed's sublog
// has 86 entries but the latest message has seq=97, then 11 entries were lost.
//
// Call this ONCE after opening but before the first Index() call.
func (idx *CombinedIndex) VerifyConsistency(rxlog margaret.Log[*multimsg.MultiMessage]) (rewound bool, err error) {
	idx.l.Lock()
	defer idx.l.Unlock()

	if idx.latestSeq < 0 {
		return false, nil // empty or not initialized, nothing to verify
	}

	// List all known feeds and check each one
	feeds, err := idx.users.List()
	if err != nil {
		return false, fmt.Errorf("consistency: failed to list feeds: %w", err)
	}

	var broken int
	for _, feedAddr := range feeds {
		userLog, err := idx.users.Get(feedAddr)
		if err != nil {
			continue
		}

		sublogLen := userLog.Seq() // 0-indexed: -1=empty, 0=one entry, etc.
		if sublogLen < 0 {
			continue
		}

		// Get the last entry in the sublog → resolve to the actual message
		rxSeqVal, err := userLog.Get(sublogLen)
		if err != nil {
			continue
		}
		mm, err := rxlog.Get(int64(*rxSeqVal))
		if err != nil {
			continue
		}
		if mm.Message == nil {
			continue // nulled
		}

		// The sublog should have exactly msg.Seq() entries (one per message).
		// margaret is 0-indexed, so sublogLen+1 should equal msg.Seq().
		msgSeq := mm.Message.Seq()
		if sublogLen+1 != msgSeq {
			broken++
		}
	}

	if broken == 0 {
		return false, nil
	}

	// Clear ALL sublogs before re-indexing to prevent duplicate entries.
	// Since we're rewinding the CombinedIndex state to -1, ALL rxlog entries
	// will be reprocessed. Without clearing, the reprocessing would append
	// to existing sublogs, creating duplicates that cause further corruption.
	for _, addr := range feeds {
		idx.users.Delete(addr)
	}

	// Also clear byType, tangles, and orderedHelper sublogs since they'll
	// also be rebuilt during re-indexing.
	if addrs, err := idx.byType.List(); err == nil {
		for _, addr := range addrs {
			idx.byType.Delete(addr)
		}
	}
	if addrs, err := idx.tangles.List(); err == nil {
		for _, addr := range addrs {
			idx.tangles.Delete(addr)
		}
	}
	if addrs, err := idx.orderdHelper.List(); err == nil {
		for _, addr := range addrs {
			idx.orderdHelper.Delete(addr)
		}
	}
	if addrs, err := idx.private.List(); err == nil {
		for _, addr := range addrs {
			idx.private.Delete(addr)
		}
	}

	// Reset the state to force a full re-index from the beginning.
	// A partial rewind isn't safe because we don't know how far back
	// the corruption extends.
	prevSeq := idx.latestSeq
	idx.latestSeq = margaret.SeqEmpty
	if err := persist.Save(idx.file, idx.latestSeq); err != nil {
		return false, fmt.Errorf("consistency: failed to rewind state: %w", err)
	}
	fmt.Printf("combined-index: consistency check found %d/%d feeds with missing index entries, forcing full re-index (was at seq %d)\n",
		broken, len(feeds), prevSeq)

	return true, nil
}

// update all the indexes with this new message which was stored as rxSeq
func (idx *CombinedIndex) update(rxSeq int64, mm *multimsg.MultiMessage) error {
	msg := mm.Message

	author := msg.Author()

	authorAddr := storedrefs.Feed(author)
	authorLog, err := idx.users.Get(authorAddr)
	if err != nil {
		return fmt.Errorf("error opening sublog: %w", err)
	}

	if err := appendSeq(authorLog, rxSeq); err != nil {
		return fmt.Errorf("error updating author sublog: %w", err)
	}

	// TODO: batch/debounce me
	err = idx.ebtState.Fill(idx.self, []statematrix.ObservedFeed{{
		Feed: author,
		Note: ssb.Note{
			Seq:       int64(msg.Seq()),
			Receive:   true,
			Replicate: true,
		},
	}})
	if err != nil {
		return fmt.Errorf("ebt update failed: %w", err)
	}

	// decrypt box 1 & 2
	content := msg.ContentBytes()
	// TODO: gabby grove
	if content[0] != '{' { // assuming all other content is json objects
		cleartext, err := idx.tryDecrypt(mm, rxSeq)
		if err != nil {
			if err == errSkip {
				// yes it's a boxed message but we can't read it (yet)
				return nil
			}
			// something went horribly wrong
			return err
		}
		content = cleartext
	}

	// by type:...  channels, mentions, and tangles (v1 & v2)
	var jsonContent struct {
		Type     string
		Root     *refs.MessageRef
		Tangles  refs.Tangles
		Channel  string           `json:"channel"`
		Mentions []mentionContent `json:"mentions"`
	}
	err = json.Unmarshal(content, &jsonContent)
	if err != nil {
		// broken messages stop all other indexing if we return an error
		// not much to do here but continue with the next
		return nil
	}

	typeStr := jsonContent.Type
	if typeStr == "" {
		return nil
	}
	typeIdxAddr := multilog.Addr("string:" + typeStr)

	// we need to keep the order intact for these
	if typeStr == "group/add-member" {
		sl, err := idx.orderdHelper.Get(typeIdxAddr)
		if err != nil {
			return err
		}
		if err := appendSeq(sl, rxSeq); err != nil {
			return err
		}
	}

	typedLog, err := idx.byType.Get(typeIdxAddr)
	if err != nil {
		return fmt.Errorf("error opening sublog: %w", err)
	}

	if err := appendSeq(typedLog, rxSeq); err != nil {
		return fmt.Errorf("error updating byType sublog: %w", err)
	}

	// root posts: messages without a content.root field
	if jsonContent.Root == nil {
		rootLog, err := idx.byType.Get(multilog.Addr("meta:root"))
		if err != nil {
			return fmt.Errorf("error opening root sublog: %w", err)
		}
		if err := appendSeq(rootLog, rxSeq); err != nil {
			return fmt.Errorf("error updating root sublog: %w", err)
		}
	}

	// channels
	if jsonContent.Channel != "" {
		channelLog, err := idx.channels.Get(multilog.Addr(jsonContent.Channel))
		if err != nil {
			return fmt.Errorf("error opening channel sublog: %w", err)
		}
		if err := appendSeq(channelLog, rxSeq); err != nil {
			return fmt.Errorf("error updating channel sublog: %w", err)
		}
	}

	// mentions: index each mentioned ref
	for _, mention := range jsonContent.Mentions {
		if mention.Link == "" {
			continue
		}
		mentionLog, err := idx.mentions.Get(multilog.Addr(mention.Link))
		if err != nil {
			return fmt.Errorf("error opening mention sublog: %w", err)
		}
		if err := appendSeq(mentionLog, rxSeq); err != nil {
			return fmt.Errorf("error updating mention sublog: %w", err)
		}
	}

	// tangles v1 and v2
	if jsonContent.Root != nil {
		addr := storedrefs.TangleV1(*jsonContent.Root)
		tangleLog, err := idx.tangles.Get(addr)
		if err != nil {
			return fmt.Errorf("error opening sublog: %w", err)
		}
		if err := appendSeq(tangleLog, rxSeq); err != nil {
			return fmt.Errorf("error updating v1 tangle sublog: %w", err)
		}
	}

	for tname, tip := range jsonContent.Tangles {
		if tname == "" {
			continue
		}
		if tip.Root == nil {
			continue
		}
		addr := storedrefs.TangleV2(tname, *tip.Root)
		tangleLog, err := idx.tangles.Get(addr)
		if err != nil {
			return fmt.Errorf("error opening sublog: %w", err)
		}
		if err := appendSeq(tangleLog, rxSeq); err != nil {
			return fmt.Errorf("error updating v2 tangle sublog: %w", err)
		}
	}

	return nil
}

// ResetState resets the CombinedIndex state to force a full re-index.
func (idx *CombinedIndex) ResetState() {
	idx.l.Lock()
	defer idx.l.Unlock()
	idx.latestSeq = margaret.SeqEmpty
	persist.Save(idx.file, idx.latestSeq)
}

// FlushAndSave flushes all roaring bitmap multilogs to disk and then persists
// the current sequence number. This must be called before the multilogs are
// closed to ensure the state file matches the flushed bitmap data.
//
// Call this from sbot.Close() BEFORE closing the roaring multilogs.
func (idx *CombinedIndex) FlushAndSave() error {
	idx.l.Lock()
	defer idx.l.Unlock()

	// Flush all roaring multilogs so bitmap data is on disk.
	for _, ml := range []*roaring.MultiLog{idx.users, idx.private, idx.byType, idx.tangles, idx.orderdHelper} {
		if ml == nil {
			continue
		}
		if err := ml.Flush(); err != nil {
			return fmt.Errorf("combined-index: flush error: %w", err)
		}
	}

	// Now save state — guaranteed to match what's on disk.
	if idx.latestSeq >= 0 {
		if err := persist.Save(idx.file, idx.latestSeq); err != nil {
			return fmt.Errorf("combined-index: state save error: %w", err)
		}
	}

	return nil
}

func (idx *CombinedIndex) Close() error {
	return idx.file.Close()
}

func (idx *CombinedIndex) tryDecrypt(mm *multimsg.MultiMessage, rxSeq int64) ([]byte, error) {
	msg := mm.Message
	box1, box2, err := getBoxedContent(mm)
	if err != nil {
		if err == errSkipBox1 || err == errSkipBox2 {
			return nil, errSkip
		}
		return nil, err
	}

	var (
		cleartext []byte
		idxAddr   multilog.Addr
	)

	// as a help for re-indexing, keep track of all box1 and box2 messages.
	if box1 != nil {
		idxAddr = multilog.Addr("meta:box1")
	} else {
		idxAddr = multilog.Addr("meta:box2")
	}

	boxTyped, err := idx.private.Get(idxAddr)
	if err != nil {
		return nil, err
	}
	if err := appendSeq(boxTyped, rxSeq); err != nil {
		return nil, fmt.Errorf("private: error marking type:box: %w", err)
	}

	// try decrypt and pass on the clear text
	if box1 != nil {
		content, err := idx.boxer.DecryptBox1(box1)
		if err != nil {
			return nil, errSkip
		}

		idxAddr = multilog.Addr("box1:") + storedrefs.Feed(idx.self)
		cleartext = content
	} else if box2 != nil {
		prev := refs.MessageRef{}
		if p := msg.Previous(); p != nil {
			prev = *p
		}
		content, err := idx.boxer.DecryptBox2(box2, msg.Author(), prev)
		if err != nil {
			return nil, errSkip
		}

		idxAddr = multilog.Addr("box2:") + storedrefs.Feed(idx.self)
		cleartext = content
	} else {
		return nil, fmt.Errorf("tryDecrypt: not skipped but also not valid content")
	}

	userPrivs, err := idx.private.Get(idxAddr)
	if err != nil {
		return nil, fmt.Errorf("combined/private: error opening priv sublog for: %w", err)
	}
	if err := appendSeq(userPrivs, rxSeq); err != nil {
		return nil, fmt.Errorf("combined/private: error appending PM: %w", err)
	}

	return cleartext, nil
}

// mentionContent represents a single entry in the content.mentions array
type mentionContent struct {
	Link string `json:"link"`
}

var (
	errSkip     = fmt.Errorf("ssb: skip - not for us")
	errSkipBox1 = fmt.Errorf("ssb: skip box1 message")
	errSkipBox2 = fmt.Errorf("ssb: skip box2 message")
)

// getBoxedContent returns either box1, box2 content or an error.
// if err == errSkip, this message couldn't be decrypted
func getBoxedContent(mm *multimsg.MultiMessage) ([]byte, []byte, error) {
	msg := mm.Message
	switch msg.Author().Algo() {

	// on the _crappy_ format, we need to base64 decode the data
	case refs.RefAlgoFeedSSB1:
		input := msg.ContentBytes()
		if !(input[0] == '"' && input[len(input)-1] == '"') {
			return nil, nil, errSkipBox1 // not a json string
		}

		if bytes.HasSuffix(input[1:], []byte(".box\"")) {
			b64data := bytes.TrimSuffix(input[1:], []byte(".box\""))
			boxedData := make([]byte, base64.StdEncoding.DecodedLen(len(input)-6))
			n, err := base64.StdEncoding.Decode(boxedData, b64data)
			if err != nil {
				return nil, nil, errSkipBox1
			}
			return boxedData[:n], nil, nil
		} else if bytes.HasSuffix(input[1:], []byte(".box2\"")) {
			b64data := bytes.TrimSuffix(input[1:], []byte(".box2\""))
			boxedData := make([]byte, base64.StdEncoding.DecodedLen(len(input)-7))
			n, err := base64.StdEncoding.Decode(boxedData, b64data)
			if err != nil {
				return nil, nil, errSkipBox1
			}
			return nil, boxedData[:n], nil
		} else {
			return nil, nil, fmt.Errorf("private/ssb1: unknown content type: %q", input[len(input)-10:])
		}

		// gg supports pure binary data
	case refs.RefAlgoFeedGabby:
		tr, ok := mm.AsGabby()
		if !ok {
			return nil, nil, fmt.Errorf("combined/private: error getting gabby msg")
		}

		evt, err := tr.UnmarshaledEvent()
		if err != nil {
			return nil, nil, fmt.Errorf("combined/private: error unpacking event from stored message: %w", err)
		}
		if evt.Content.Type != gabbygrove.ContentTypeArbitrary {
			return nil, nil, errSkipBox2
		}

		var (
			prefixBox1 = []byte("box1:")
			prefixBox2 = []byte("box2:")
		)
		switch {
		case bytes.HasPrefix(tr.Content, prefixBox1):
			return tr.Content[5:], nil, nil
		case bytes.HasPrefix(tr.Content, prefixBox2):
			return nil, tr.Content[5:], nil
		default:
			return nil, nil, fmt.Errorf("private/ssb1: unknown content type: %s", msg.Key().ShortSigil())
		}

	case refs.RefAlgoFeedBendyButt:
		// TODO: check first bytes and strip prefix
		return nil, nil, errSkipBox2

	default:
		err := fmt.Errorf("combined/private: unknown feed type: %s", msg.Author().Algo())
		return nil, nil, err
	}
}
