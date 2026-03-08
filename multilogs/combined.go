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
	"io"
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

		file: idxStateFile,
		l:    &sync.Mutex{},
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
func (idx *CombinedIndex) ProcessEntry(seq int64, mm *multimsg.MultiMessage) error {
	idx.l.Lock()
	defer idx.l.Unlock()

	// persist the current sequence number
	err := persist.Save(idx.file, seq)
	if err != nil {
		return fmt.Errorf("error saving current sequence number: %w", err)
	}

	if mm.Message == nil {
		return nil // nulled entry
	}

	return idx.update(seq, mm)
}

// LastProcessedSeq returns the sequence number of the last processed message.
func (idx *CombinedIndex) LastProcessedSeq() int64 {
	idx.l.Lock()
	defer idx.l.Unlock()

	var seq int64
	if err := persist.Load(idx.file, &seq); err != nil {
		if !errors.Is(err, io.EOF) {
			return margaret.SeqEmpty
		}
		return margaret.SeqEmpty
	}
	return seq
}

// Index processes all unprocessed entries from the given log.
func (idx *CombinedIndex) Index(log margaret.Log[*multimsg.MultiMessage]) error {
	lastSeq := idx.LastProcessedSeq()

	var opts []margaret.QueryOption
	if lastSeq >= 0 {
		opts = append(opts, margaret.Gt(lastSeq))
	}

	qry := log.Query(opts...)
	for seq, mm := range qry.Iter() {
		if err := idx.ProcessEntry(seq, mm); err != nil {
			return err
		}
	}
	return qry.Err()
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
