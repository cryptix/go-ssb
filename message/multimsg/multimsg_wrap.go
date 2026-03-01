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
