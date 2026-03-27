// SPDX-FileCopyrightText: 2026 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

// Package muxrpctracing provides a muxrpc.HandlerWrapper that logs entry and
// exit of every RPC handler call with a unique request ID, duration, and panic
// recovery. It is designed to be passed to Network.Serve as a HandlerWrapper.
package muxrpctracing

import (
	"context"
	"fmt"
	"net"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/ssbc/go-muxrpc/v3"
	"github.com/ssbc/go-ssb"
	"go.mindeco.de/log"
	"go.mindeco.de/log/level"
)

// NewHandlerWrapper returns a muxrpc.HandlerWrapper that traces handler entry,
// exit, duration, and recovers from panics.
func NewHandlerWrapper(logger log.Logger) muxrpc.HandlerWrapper {
	var counter uint64
	return func(h muxrpc.Handler) muxrpc.Handler {
		return &tracingHandler{
			next:    h,
			log:     logger,
			counter: &counter,
		}
	}
}

type tracingHandler struct {
	next    muxrpc.Handler
	log     log.Logger
	counter *uint64
}

func (t *tracingHandler) Handled(m muxrpc.Method) bool {
	return t.next.Handled(m)
}

func (t *tracingHandler) HandleConnect(ctx context.Context, edp muxrpc.Endpoint) {
	remote := edp.Remote()
	peer := remoteSigil(remote)

	level.Info(t.log).Log("event", "handler.connect", "peer", peer)
	start := time.Now()

	t.next.HandleConnect(ctx, edp)

	level.Debug(t.log).Log("event", "handler.connect.done", "peer", peer, "dur", time.Since(start))
}

func (t *tracingHandler) HandleCall(ctx context.Context, req *muxrpc.Request) {
	rid := atomic.AddUint64(t.counter, 1)
	method := req.Method.String()
	rtype := string(req.Type)
	peer := remoteSigil(req.RemoteAddr())

	rlog := log.With(t.log, "req", rid, "method", method, "type", rtype, "peer", peer)
	level.Info(rlog).Log("event", "call.start")
	start := time.Now()

	defer func() {
		dur := time.Since(start)
		if r := recover(); r != nil {
			level.Error(rlog).Log("event", "call.panic", "dur", dur, "panic", fmt.Sprint(r), "stack", string(debug.Stack()))
			// Close the request stream so the remote gets an error rather than
			// hanging indefinitely.
			req.CloseWithError(fmt.Errorf("handler panic: %v", r))
			return
		}
		level.Info(rlog).Log("event", "call.done", "dur", dur)
	}()

	t.next.HandleCall(ctx, req)
}

// remoteSigil extracts a short peer identifier from a net.Addr. Returns the
// short sigil if the address wraps an SSB feed ref, otherwise the raw string.
func remoteSigil(addr net.Addr) string {
	if addr == nil {
		return "unknown"
	}
	ref, err := ssb.GetFeedRefFromAddr(addr)
	if err != nil {
		return addr.String()
	}
	return ref.ShortSigil()
}
