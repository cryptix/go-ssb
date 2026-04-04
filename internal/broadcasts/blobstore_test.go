// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package broadcasts

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ssbc/go-ssb"
)

// mockBlobEmitter records emitted notifications and can return errors.
type mockBlobEmitter struct {
	mu      sync.Mutex
	emitted []ssb.BlobStoreNotification
	err     error
	closed  bool
}

func (m *mockBlobEmitter) EmitBlob(n ssb.BlobStoreNotification) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.emitted = append(m.emitted, n)
	return nil
}

func (m *mockBlobEmitter) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func (m *mockBlobEmitter) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.emitted)
}

func (m *mockBlobEmitter) isClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

func TestBlobStoreBroadcast_EmitToMultipleSinks(t *testing.T) {
	t.Parallel()
	bcst := NewBlobStoreBroadcast()

	sink1 := &mockBlobEmitter{}
	sink2 := &mockBlobEmitter{}
	bcst.Register(sink1)
	bcst.Register(sink2)

	nf := ssb.BlobStoreNotification{Op: ssb.BlobStoreOpPut, Size: 42}
	if err := bcst.EmitBlob(nf); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if c := sink1.count(); c != 1 {
		t.Fatalf("sink1: expected 1 notification, got %d", c)
	}
	if c := sink2.count(); c != 1 {
		t.Fatalf("sink2: expected 1 notification, got %d", c)
	}
}

func TestBlobStoreBroadcast_CancelRemovesSink(t *testing.T) {
	t.Parallel()
	bcst := NewBlobStoreBroadcast()

	sink := &mockBlobEmitter{}
	cancel := bcst.Register(sink)

	cancel()

	if !sink.isClosed() {
		t.Fatal("expected sink to be closed after cancel")
	}

	nf := ssb.BlobStoreNotification{Op: ssb.BlobStoreOpPut}
	bcst.EmitBlob(nf)

	if c := sink.count(); c != 0 {
		t.Fatalf("expected 0 notifications after cancel, got %d", c)
	}
}

func TestBlobStoreBroadcast_FailingSinkRemoved(t *testing.T) {
	t.Parallel()
	bcst := NewBlobStoreBroadcast()

	failing := &mockBlobEmitter{err: errors.New("broken")}
	good := &mockBlobEmitter{}

	bcst.Register(failing)
	bcst.Register(good)

	nf := ssb.BlobStoreNotification{Op: ssb.BlobStoreOpPut}

	// First emit: failing sink should be removed
	bcst.EmitBlob(nf)

	if c := good.count(); c != 1 {
		t.Fatalf("good sink: expected 1, got %d", c)
	}

	// Second emit: only good sink should receive
	bcst.EmitBlob(nf)

	if c := good.count(); c != 2 {
		t.Fatalf("good sink: expected 2, got %d", c)
	}
}

func TestBlobStoreBroadcast_CloseAll(t *testing.T) {
	t.Parallel()
	bcst := NewBlobStoreBroadcast()

	sink1 := &mockBlobEmitter{}
	sink2 := &mockBlobEmitter{}
	bcst.Register(sink1)
	bcst.Register(sink2)

	if err := bcst.Close(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !sink1.isClosed() {
		t.Fatal("sink1 not closed")
	}
	if !sink2.isClosed() {
		t.Fatal("sink2 not closed")
	}
}

func TestBlobStoreBroadcast_ConcurrentEmit(t *testing.T) {
	t.Parallel()
	bcst := NewBlobStoreBroadcast()

	var totalEmitted atomic.Int32
	for range 5 {
		sink := &mockBlobEmitter{}
		bcst.Register(sink)
	}

	var wg sync.WaitGroup
	for i := range 20 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			nf := ssb.BlobStoreNotification{Op: ssb.BlobStoreOpPut, Size: int64(i)}
			bcst.EmitBlob(nf)
			totalEmitted.Add(1)
		}(i)
	}
	wg.Wait()

	if got := totalEmitted.Load(); got != 20 {
		t.Fatalf("expected 20 emits, got %d", got)
	}
}

func TestBlobStoreFuncEmitter(t *testing.T) {
	t.Parallel()
	var called bool
	fn := BlobStoreFuncEmitter(func(n ssb.BlobStoreNotification) error {
		called = true
		return nil
	})

	if err := fn.EmitBlob(ssb.BlobStoreNotification{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("function was not called")
	}

	// Close should be a no-op
	if err := fn.Close(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
}
