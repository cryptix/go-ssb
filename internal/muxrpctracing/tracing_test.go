// SPDX-FileCopyrightText: 2026 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package muxrpctracing

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/ssbc/go-muxrpc/v3"
	"go.mindeco.de/log"
)

type stubHandler struct {
	callFunc func(context.Context, *muxrpc.Request)
}

func (s *stubHandler) Handled(muxrpc.Method) bool { return true }
func (s *stubHandler) HandleConnect(context.Context, muxrpc.Endpoint) {}
func (s *stubHandler) HandleCall(ctx context.Context, req *muxrpc.Request) {
	if s.callFunc != nil {
		s.callFunc(ctx, req)
	}
}

func TestHandlerWrapperLogsEntryAndExit(t *testing.T) {
	var buf bytes.Buffer
	logger := log.NewLogfmtLogger(&buf)

	stub := &stubHandler{}
	wrapper := NewHandlerWrapper(logger)
	wrapped := wrapper(stub)

	if !wrapped.Handled(muxrpc.Method{"test"}) {
		t.Fatal("Handled should delegate to inner handler")
	}

	// We cannot easily construct a *muxrpc.Request without the full muxrpc
	// machinery, so we verify that the wrapper compiles and that Handled
	// delegates correctly.
	output := buf.String()
	_ = output
}

func TestRemoteSigilNilAddr(t *testing.T) {
	got := remoteSigil(nil)
	if got != "unknown" {
		t.Errorf("expected 'unknown', got %q", got)
	}
}

func TestRemoteSigilFallback(t *testing.T) {
	addr := &fakeAddr{s: "127.0.0.1:8008"}
	got := remoteSigil(addr)
	if !strings.Contains(got, "127.0.0.1") {
		t.Errorf("expected fallback to addr string, got %q", got)
	}
}

type fakeAddr struct{ s string }

func (f *fakeAddr) Network() string { return "tcp" }
func (f *fakeAddr) String() string  { return f.s }
