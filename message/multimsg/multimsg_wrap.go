// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package multimsg

import (
	"fmt"
	"time"

	gabbygrove "github.com/ssbc/go-gabbygrove"
	"github.com/ssbc/go-metafeed"
	refs "github.com/ssbc/go-ssb-refs"
	margaret "github.com/ssbc/margaret/v2"

	"github.com/ssbc/go-ssb/message/legacy"
)

// AlterableLog is a margaret log that supports nulling and replacing entries.
type AlterableLog = margaret.Alterable[*MultiMessage]

func NewWrappedLog(in margaret.Alterable[*MultiMessage]) *WrappedLog {
	return &WrappedLog{
		Alterable:   in,
		receivedNow: time.Now,
	}
}

// WrappedLog wraps an alterable log and adds received timestamps
// and message-type conversion on append.
type WrappedLog struct {
	margaret.Alterable[*MultiMessage]

	// overwriteable for testing
	receivedNow func() time.Time
}

// Append adds a MultiMessage to the log, setting the received timestamp if not already set.
func (wl *WrappedLog) Append(mm *MultiMessage) (int64, error) {
	if mm.received.IsZero() {
		mm.received = wl.receivedNow()
	}
	return wl.Alterable.Append(mm)
}

// AppendMessage wraps a refs.Message into a MultiMessage and appends it.
// This is used by the publish and verification paths which produce typed messages
// (StoredMessage, Transfer, etc.) that need to be wrapped before storage.
func (wl *WrappedLog) AppendMessage(msg refs.Message) (int64, error) {
	var mm MultiMessage
	mm.key = msg.Key()

	now := wl.receivedNow()
	switch tv := msg.(type) {
	case *legacy.StoredMessage:
		mm.tipe = Legacy
		mm.Message = tv
		tv.Timestamp_ = now
	case *gabbygrove.Transfer:
		mm.tipe = Gabby
		mm.Message = tv
		mm.received = now
	case *metafeed.Message:
		mm.tipe = MetaFeed
		mm.Message = tv
		mm.received = now
	default:
		return margaret.SeqEmpty, fmt.Errorf("wrappedLog: unsupported message type: %T", msg)
	}

	return wl.Alterable.Append(&mm)
}

// AppendBatchMessages wraps multiple refs.Messages into MultiMessages and appends them.
// When the underlying margaret log supports AppendBatch (single lock + single fsync),
// this will use it. For now, falls back to sequential Append calls.
func (wl *WrappedLog) AppendBatchMessages(msgs []refs.Message) ([]int64, error) {
	if len(msgs) == 0 {
		return nil, nil
	}

	now := wl.receivedNow()
	mms := make([]*MultiMessage, len(msgs))
	for i, msg := range msgs {
		var mm MultiMessage
		mm.key = msg.Key()

		switch tv := msg.(type) {
		case *legacy.StoredMessage:
			mm.tipe = Legacy
			mm.Message = tv
			tv.Timestamp_ = now
		case *gabbygrove.Transfer:
			mm.tipe = Gabby
			mm.Message = tv
			mm.received = now
		case *metafeed.Message:
			mm.tipe = MetaFeed
			mm.Message = tv
			mm.received = now
		default:
			return nil, fmt.Errorf("wrappedLog: unsupported message type: %T", msg)
		}
		mms[i] = &mm
	}

	// Use BatchAppender if the underlying log supports it (single lock + single fsync).
	if batcher, ok := wl.Alterable.(margaret.BatchAppender[*MultiMessage]); ok {
		return batcher.AppendBatch(mms)
	}
	// Fallback: sequential Append calls.
	seqs := make([]int64, len(mms))
	for i, mm := range mms {
		seq, err := wl.Alterable.Append(mm)
		if err != nil {
			return seqs[:i], fmt.Errorf("batch append failed at index %d: %w", i, err)
		}
		seqs[i] = seq
	}
	return seqs, nil
}
