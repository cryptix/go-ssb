// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package sbot

import (
	"encoding/json"
	"fmt"

	"github.com/dgraph-io/badger/v3"
	refs "github.com/ssbc/go-ssb-refs"
	margaret "github.com/ssbc/margaret/v2"
	mindexes "github.com/ssbc/margaret/v2/indexes"
	"github.com/ssbc/margaret/v2/multilog/roaring"
	kitlog "go.mindeco.de/log"
	"go.mindeco.de/log/level"

	"github.com/ssbc/go-ssb"
	"github.com/ssbc/go-ssb/internal/mutil"
	"github.com/ssbc/go-ssb/internal/storedrefs"
	"github.com/ssbc/go-ssb/message/multimsg"
	"github.com/ssbc/go-ssb/multilogs"
	"github.com/ssbc/go-ssb/repo"
)

// NullContent drops the content portion of a gabbygrove transfer.
// seq is in the same base ase the feed (starting with 1).
func (s *Sbot) NullContent(fr refs.FeedRef, seq uint) error {
	if fr.Algo() != refs.RefAlgoFeedGabby {
		return ssb.ErrUnuspportedFormat
	}

	uf, ok := s.GetMultiLog(multilogs.IndexNameFeeds)
	if !ok {
		return fmt.Errorf("userFeeds mlog not present")
	}

	userLog, err := uf.Get(storedrefs.Feed(fr))
	if err != nil {
		return fmt.Errorf("nullContent: unable to load feed: %w", err)
	}

	// internal data strucutres are 0-indexed
	seqEntry, err := userLog.Get(int64(seq - 1))
	if err != nil {
		return fmt.Errorf("nullContent: unable to load feed: %w", err)
	}

	rootLogSeq := int64(*seqEntry)

	mm, err := s.ReceiveLog.Get(rootLogSeq)
	if err != nil {
		return fmt.Errorf("nullContent: failed to get message in rootLog: %w", err)
	}

	tr, ok := mm.AsGabby()
	if !ok {
		return fmt.Errorf("nullContent: expected gabbyGrove type MultiMessage")
	}

	tr.Content = nil

	nulled, err := mm.MarshalBinary()
	if err != nil {
		return fmt.Errorf("nullContent: unable to marshall nulled content transfer: %w", err)
	}

	err = s.ReceiveLog.Replace(rootLogSeq, nulled)
	if err != nil {
		return fmt.Errorf("nullContent: failed to execute replace operation: %w", err)
	}
	return nil
}

const FolderNameDelete = "drop-content-requests"

type dropContentTrigger struct {
	logger kitlog.Logger

	root  margaret.Log[*multimsg.MultiMessage]
	feeds *roaring.MultiLog

	nuller ssb.ContentNuller

	check chan *triggerEvent
}

type triggerEvent struct {
	author refs.FeedRef
	dcr    ssb.DropContentRequest
}

func (cdr *dropContentTrigger) consume() {
	evtLog := kitlog.With(cdr.logger, "event", "null content trigger")
	for evt := range cdr.check {

		feed, err := cdr.feeds.Get(storedrefs.Feed(evt.author))
		if err != nil {
			level.Warn(evtLog).Log("msg", "no such feed?", "err", err)
			continue
		}

		if !evt.dcr.Valid(mutil.Indirect(cdr.root, feed)) {
			level.Warn(evtLog).Log("msg", "invalid request")
			continue
		}

		err = cdr.nuller.NullContent(evt.author, evt.dcr.Sequence)
		if err != nil {
			level.Error(evtLog).Log("err", err)
			continue
		}

		level.Info(evtLog).Log("msg", "nulled successfully", "author", evt.author.ShortSigil(), "seq", evt.dcr.Sequence)
	}
}

func (dcr *dropContentTrigger) MakeSimpleIndex(db *badger.DB) (mindexes.Index[int64], *wrappedIndexSink, error) {

	// TODO: currently the locking of margaret/offset doesn't allow us to get previous messages while being in an index update
	dcr.check = make(chan *triggerEvent, 10)

	idx, snk, err := repo.OpenIndex(db, FolderNameDelete, dcr.idxupdate)
	if err != nil {
		return nil, nil, fmt.Errorf("error getting dcr trigger index: %w", err)
	}
	go dcr.consume()
	ws := &wrappedIndexSink{SinkIndex: snk, ch: dcr.check}
	return idx, ws, nil
}

type wrappedIndexSink struct {
	*mindexes.SinkIndex[*multimsg.MultiMessage, int64]

	ch chan *triggerEvent
}

func (snk *wrappedIndexSink) Close() error {
	close(snk.ch)
	return nil
}

func (dcr *dropContentTrigger) idxupdate(idx mindexes.SeqIndex) *mindexes.SinkIndex[*multimsg.MultiMessage, int64] {
	return mindexes.NewSinkIndex[*multimsg.MultiMessage, int64](idx, func(seq int64, mm *multimsg.MultiMessage) (mindexes.Addr, int64, bool) {
		if mm.Message == nil {
			return "", 0, false
		}

		msg := mm.Message
		author := msg.Author()
		if author.Algo() != refs.RefAlgoFeedGabby {
			return "", 0, false
		}

		var typed ssb.DropContentRequest
		err := json.Unmarshal(msg.ContentBytes(), &typed)
		if err == nil && typed.Type == ssb.DropContentRequestType {
			dcr.check <- &triggerEvent{
				author: author,
				dcr:    typed,
			}
		}

		return "", 0, false
	}).WithSeqTracking(idx)
}
