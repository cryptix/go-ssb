// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package graph

import (
	"fmt"
	"strings"
	"time"

	margaret "github.com/ssbc/margaret/v2"
	"github.com/zeebo/bencode"
	"go.mindeco.de/log"
	"go.mindeco.de/log/level"

	"github.com/ssbc/go-metafeed"
	"github.com/ssbc/go-metafeed/metamngmt"
	refs "github.com/ssbc/go-ssb-refs"
	"github.com/ssbc/go-ssb-refs/tfk"
	"github.com/ssbc/go-ssb/internal/storedrefs"
	"github.com/ssbc/go-ssb/message/legacy"
	"github.com/ssbc/go-ssb/message/multimsg"
)

type idxRelationState uint

const (
	idxRelValueNone idxRelationState = iota
	idxRelValueFollowing
	idxRelValueBlocking
	idxRelValueMetafeed
)

func (b *GraphBuilder) indexSyncStart() {
	b.idxInSync.Add(1)
}

func (b *GraphBuilder) indexSyncDone() {
	// this delay is here so that the WaitGroup is held while serveIndex processes the next entry
	time.AfterFunc(100*time.Millisecond, func() {
		b.idxInSync.Done()
	})
}

// WaitUntilIndexesAreSynced blocks until all the index processing is in sync with the rootlog
func (b *GraphBuilder) WaitUntilIndexesAreSynced() {
	b.idxInSync.Wait()
}

// graphLogIndexer implements a LogIndexer for the graph builder.
// It processes messages from a margaret log, calling the update function for each message.
type graphLogIndexer struct {
	name    string
	store   GraphStore
	seqKey  []byte
	builder *GraphBuilder
	update  func(seq int64, msg refs.Message) error

	// onEntry is called for each processed entry during Index(), if set.
	// Used by serveIndex to report progress.
	onEntry func()
}

// SetOnEntry sets a callback that is invoked for each entry processed by Index().
func (gi *graphLogIndexer) SetOnEntry(fn func()) {
	gi.onEntry = fn
}

func (gi *graphLogIndexer) lastProcessedSeq() int64 {
	val, _ := gi.store.SeqGet(gi.seqKey)
	return val
}

func (gi *graphLogIndexer) setLastProcessedSeq(seq int64) {
	_ = gi.store.SeqSet(gi.seqKey, seq)
}

// Index processes all unprocessed messages from the log.
func (gi *graphLogIndexer) Index(log margaret.Log[*multimsg.MultiMessage]) error {
	gi.builder.indexSyncStart()
	defer gi.builder.indexSyncDone()

	lastSeq := gi.lastProcessedSeq()
	var opts []margaret.QueryOption
	if lastSeq >= 0 {
		opts = append(opts, margaret.Gt(lastSeq))
	}
	qry := log.Query(opts...)
	for seq, mm := range qry.Iter() {
		if mm.Message == nil {
			gi.setLastProcessedSeq(seq)
			continue
		}
		if err := gi.update(seq, mm.Message); err != nil {
			return err
		}
		gi.setLastProcessedSeq(seq)

		if gi.onEntry != nil {
			gi.onEntry()
		}
	}
	return qry.Err()
}

func (gi *graphLogIndexer) Close() error { return nil }

// OpenContactsIndex returns a LogIndexer that processes contact messages.
func (b *GraphBuilder) OpenContactsIndex() *graphLogIndexer {
	b.indexSyncStart()
	defer b.indexSyncDone()

	return &graphLogIndexer{
		name:    "contacts",
		store:   b.store,
		seqKey:  []byte("trust-graph__seq:contacts"),
		builder: b,
		update:  b.updateContacts,
	}
}

func (b *GraphBuilder) updateContacts(_ int64, msg refs.Message) error {
	b.cacheLock.Lock()
	defer b.cacheLock.Unlock()

	var c refs.Contact
	err := c.UnmarshalJSON(msg.ContentBytes())
	if err != nil {
		// just ignore invalid messages
		return nil
	}

	addr := storedrefs.Feed(msg.Author())
	addr += storedrefs.Feed(c.Contact)
	switch {
	case c.Following:
		err = b.setRelation(addr, idxRelValueFollowing)
	case c.Blocking:
		err = b.setRelation(addr, idxRelValueBlocking)
	default:
		err = b.setRelation(addr, idxRelValueNone)
	}
	if err != nil {
		return fmt.Errorf("db/idx contacts: failed to update index. %+v: %w", c, err)
	}

	b.cachedGraph = nil
	return nil
}

// OpenAnnouncementIndex returns a LogIndexer that processes metafeed/announce messages.
func (b *GraphBuilder) OpenAnnouncementIndex() *graphLogIndexer {
	b.indexSyncStart()
	defer b.indexSyncDone()

	return &graphLogIndexer{
		name:    "announcements",
		store:   b.store,
		seqKey:  []byte("trust-graph__seq:announcements"),
		builder: b,
		update:  b.updateAnnouncement,
	}
}

func (b *GraphBuilder) updateAnnouncement(_ int64, msg refs.Message) error {
	b.cacheLock.Lock()
	defer b.cacheLock.Unlock()

	announceMsg, ok := legacy.VerifyMetafeedAnnounce(msg.ContentBytes(), msg.Author(), nil) // TODO: hmac support
	if !ok {
		return nil // skip invalid messages
	}

	addr := storedrefs.Feed(msg.Author())

	tfkRef, err := tfk.FeedFromRef(announceMsg.Metafeed)
	if err != nil {
		return fmt.Errorf("db/idx announcements: failed to turn metafeed value into binary: %w", err)
	}

	tfkBytes, err := tfkRef.MarshalBinary()
	if err != nil {
		return fmt.Errorf("db/idx announcements: failed to marshal tfk: %w", err)
	}

	err = b.setAnnouncement(addr, tfkBytes)
	if err != nil {
		return fmt.Errorf("db/idx announcements: failed to update index %+v: %w", announceMsg, err)
	}

	b.cachedGraph = nil
	return nil
}

// OpenMetafeedsIndex returns a LogIndexer that processes metafeed messages (add/existing, add/derived, tombstone).
func (b *GraphBuilder) OpenMetafeedsIndex() *graphLogIndexer {
	b.indexSyncStart()
	defer b.indexSyncDone()

	return &graphLogIndexer{
		name:    "metafeeds",
		store:   b.store,
		seqKey:  []byte("trust-graph__seq:metafeeds"),
		builder: b,
		update:  b.updateMetafeeds,
	}
}

func (b *GraphBuilder) updateMetafeeds(_ int64, msg refs.Message) error {
	b.cacheLock.Lock()
	defer b.cacheLock.Unlock()

	// skip invalid feeds
	if msg.Author().Algo() != refs.RefAlgoFeedBendyButt {
		return nil
	}

	msgLogger := log.With(b.log,
		"event", "metafeed update",
		"msg-key", msg.Key().ShortSigil(),
		"author", msg.Author().String(),
		"seq", msg.Seq(),
	)

	var bencoded []bencode.RawMessage
	err := bencode.DecodeBytes(msg.ContentBytes(), &bencoded)
	if err != nil {
		level.Warn(msgLogger).Log("warning", "content array unmarshal failed", "err", err)
		return nil
	}

	if n := len(bencoded); n != 2 {
		level.Warn(msgLogger).Log("warning", "index is not an array with length 2", "len", n)
		return nil
	}

	var justTheType metamngmt.Typed
	err = bencode.DecodeBytes(bencoded[0], &justTheType)
	if err != nil || justTheType.Type == "" {
		level.Warn(msgLogger).Log("warning", "content has no or broken type field", "err", err)
		return nil
	}

	level.Debug(msgLogger).Log("processing-rxseq", msg.Seq())

	addr := storedrefs.Feed(msg.Author())

	switch justTheType.Type {
	case "metafeed/add/existing":
		var addMsg metamngmt.AddExisting
		err = metafeed.VerifySubSignedContent(msg.ContentBytes(), &addMsg)
		if err != nil {
			level.Warn(msgLogger).Log("warning", "sub-signature is invalid", "err", err)
			return nil
		}

		if !addMsg.MetaFeed.Equal(msg.Author()) {
			level.Warn(msgLogger).Log("warning", "content is not about the author of the metafeed", "content feed", addMsg.MetaFeed.ShortSigil(), "meta author", msg.Author().ShortSigil())
			return nil
		}
		addr += storedrefs.Feed(addMsg.SubFeed)

		level.Info(msgLogger).Log("adding", addMsg.SubFeed.String())
		err = b.setRelation(addr, idxRelValueMetafeed)

	case "metafeed/add/derived":
		var addMsg metamngmt.AddDerived
		err = metafeed.VerifySubSignedContent(msg.ContentBytes(), &addMsg)
		if err != nil {
			level.Warn(msgLogger).Log("warning", "sub-signature is invalid", "err", err)
			return nil
		}

		if !addMsg.MetaFeed.Equal(msg.Author()) {
			level.Warn(msgLogger).Log("warning", "content is not about the author of the metafeed", "content feed", addMsg.MetaFeed.ShortSigil(), "meta author", msg.Author().ShortSigil())
			return nil
		}
		addr += storedrefs.Feed(addMsg.SubFeed)

		level.Info(msgLogger).Log("adding", addMsg.SubFeed.ShortSigil())
		err = b.setRelation(addr, idxRelValueMetafeed)

	case "metafeed/tombstone":
		var tMsg metamngmt.Tombstone
		err = metafeed.VerifySubSignedContent(msg.ContentBytes(), &tMsg)
		if err != nil {
			level.Warn(msgLogger).Log("warning", "sub-signature is invalid", "err", err)
			return nil
		}

		if !tMsg.MetaFeed.Equal(msg.Author()) {
			level.Warn(msgLogger).Log("warning", "content is not about the author of the metafeed", "content feed", tMsg.MetaFeed.ShortSigil(), "meta author", msg.Author().ShortSigil())
			return nil
		}
		addr += storedrefs.Feed(tMsg.SubFeed)

		level.Info(msgLogger).Log("removing", tMsg.SubFeed.ShortSigil())
		err = b.setRelation(addr, idxRelValueNone)

	default:
		level.Warn(msgLogger).Log("warning", "unhandeled message type", "type", justTheType.Type)
	}

	if err != nil {
		return fmt.Errorf("failed to update metafeed index with message %s: %w", msg.Key().String(), err)
	}

	return nil
}

// IsMetafeedMessage checks if a message has metafeed content type (for filtering).
func IsMetafeedMessage(msg refs.Message) bool {
	content := msg.ContentBytes()
	var signedContent []bencode.RawMessage
	err := bencode.DecodeBytes(content, &signedContent)
	if err != nil {
		return false
	}

	if len(signedContent) < 2 {
		return false
	}

	var justTheType metamngmt.Typed
	err = bencode.DecodeBytes(signedContent[0], &justTheType)
	if err != nil {
		return false
	}
	return strings.HasPrefix(justTheType.Type, "metafeed/")
}
