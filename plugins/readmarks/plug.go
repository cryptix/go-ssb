// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

// Package readmarks provides muxrpc handlers for tracking read positions in
// streams (feeds, threads, channels, root log). Clients use these markers to
// resume where they left off when reconnecting.
//
// RPC methods:
//
//	readmarks.set   — mark a stream as read up to a given sequence
//	readmarks.get   — query the current read position for a stream
//	readmarks.list  — list all stored read markers (optionally filtered by type)
//	readmarks.delete — remove a read marker
package readmarks

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/dgraph-io/badger/v3"
	"github.com/ssbc/go-muxrpc/v3"
	"github.com/ssbc/go-muxrpc/v3/typemux"
	"github.com/ssbc/go-ssb"
	"go.mindeco.de/logging"
)

type plug struct {
	h muxrpc.Handler
}

func (p plug) Name() string            { return "readmarks" }
func (p plug) Method() muxrpc.Method   { return muxrpc.Method{"readmarks"} }
func (p plug) Handler() muxrpc.Handler { return p.h }

// New creates a readmarks plugin backed by the given badger database.
func New(log logging.Interface, db *badger.DB) ssb.Plugin {
	store := NewReadMarkStore(db)

	mux := typemux.New(log)

	mux.RegisterAsync(muxrpc.Method{"readmarks", "set"}, &setHandler{store: store})
	mux.RegisterAsync(muxrpc.Method{"readmarks", "get"}, &getHandler{store: store})
	mux.RegisterAsync(muxrpc.Method{"readmarks", "list"}, &listHandler{store: store})
	mux.RegisterAsync(muxrpc.Method{"readmarks", "delete"}, &deleteHandler{store: store})

	return plug{h: &mux}
}

// setArgs is the argument for readmarks.set.
type setArgs struct {
	StreamType string `json:"stream_type"` // "feed", "thread", "channel", or "log"
	StreamID   string `json:"stream_id"`   // feed ref, message ref, channel name, or "root"
	Sequence   int64  `json:"sequence"`    // sequence number read up to (inclusive)
}

type setHandler struct {
	store *ReadMarkStore
}

func (h *setHandler) HandleAsync(ctx context.Context, req *muxrpc.Request) (interface{}, error) {
	var args []setArgs
	if err := json.Unmarshal(req.RawArgs, &args); err != nil {
		return nil, fmt.Errorf("readmarks.set: bad arguments: %w", err)
	}
	if len(args) < 1 {
		return nil, fmt.Errorf("readmarks.set: expected 1 argument")
	}
	a := args[0]
	if err := h.store.Set(a.StreamType, a.StreamID, a.Sequence); err != nil {
		return nil, fmt.Errorf("readmarks.set: %w", err)
	}
	return true, nil
}

// getArgs is the argument for readmarks.get.
type getArgs struct {
	StreamType string `json:"stream_type"`
	StreamID   string `json:"stream_id"`
}

// getReply is the response from readmarks.get.
type getReply struct {
	StreamType string `json:"stream_type"`
	StreamID   string `json:"stream_id"`
	Sequence   int64  `json:"sequence"`
	Found      bool   `json:"found"`
}

type getHandler struct {
	store *ReadMarkStore
}

func (h *getHandler) HandleAsync(ctx context.Context, req *muxrpc.Request) (interface{}, error) {
	var args []getArgs
	if err := json.Unmarshal(req.RawArgs, &args); err != nil {
		return nil, fmt.Errorf("readmarks.get: bad arguments: %w", err)
	}
	if len(args) < 1 {
		return nil, fmt.Errorf("readmarks.get: expected 1 argument")
	}
	a := args[0]
	seq, found, err := h.store.Get(a.StreamType, a.StreamID)
	if err != nil {
		return nil, fmt.Errorf("readmarks.get: %w", err)
	}
	return getReply{
		StreamType: a.StreamType,
		StreamID:   a.StreamID,
		Sequence:   seq,
		Found:      found,
	}, nil
}

// listArgs is the optional argument for readmarks.list.
type listArgs struct {
	StreamType string `json:"stream_type"` // optional filter
}

type listHandler struct {
	store *ReadMarkStore
}

func (h *listHandler) HandleAsync(ctx context.Context, req *muxrpc.Request) (interface{}, error) {
	var streamType string
	if len(req.RawArgs) > 0 {
		var args []listArgs
		if err := json.Unmarshal(req.RawArgs, &args); err == nil && len(args) > 0 {
			streamType = args[0].StreamType
		}
	}
	marks, err := h.store.List(streamType)
	if err != nil {
		return nil, fmt.Errorf("readmarks.list: %w", err)
	}
	if marks == nil {
		marks = []ReadMark{}
	}
	return marks, nil
}

type deleteHandler struct {
	store *ReadMarkStore
}

func (h *deleteHandler) HandleAsync(ctx context.Context, req *muxrpc.Request) (interface{}, error) {
	var args []getArgs // same shape as get
	if err := json.Unmarshal(req.RawArgs, &args); err != nil {
		return nil, fmt.Errorf("readmarks.delete: bad arguments: %w", err)
	}
	if len(args) < 1 {
		return nil, fmt.Errorf("readmarks.delete: expected 1 argument")
	}
	a := args[0]
	if err := h.store.Delete(a.StreamType, a.StreamID); err != nil {
		return nil, fmt.Errorf("readmarks.delete: %w", err)
	}
	return true, nil
}
