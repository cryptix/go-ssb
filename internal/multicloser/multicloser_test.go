// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package multicloser

import (
	"errors"
	"io"
	"sync/atomic"
	"testing"
)

// closerFunc adapts a function to io.Closer.
type closerFunc func() error

func (f closerFunc) Close() error { return f() }

func TestMultiCloser_Empty(t *testing.T) {
	t.Parallel()
	var mc MultiCloser
	if err := mc.Close(); err != nil {
		t.Fatalf("expected nil error from empty MultiCloser, got: %v", err)
	}
}

func TestMultiCloser_SingleCloser(t *testing.T) {
	t.Parallel()
	var called bool
	var mc MultiCloser
	mc.AddCloser(closerFunc(func() error {
		called = true
		return nil
	}))

	if err := mc.Close(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("closer was not called")
	}
}

func TestMultiCloser_LIFOOrder(t *testing.T) {
	t.Parallel()
	var order []int
	var mc MultiCloser

	for i := range 5 {
		i := i
		mc.AddCloser(closerFunc(func() error {
			order = append(order, i)
			return nil
		}))
	}

	if err := mc.Close(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// LIFO: last added (4) should be closed first
	expected := []int{4, 3, 2, 1, 0}
	if len(order) != len(expected) {
		t.Fatalf("expected %d closes, got %d", len(expected), len(order))
	}
	for i, v := range expected {
		if order[i] != v {
			t.Fatalf("position %d: expected %d, got %d (order=%v)", i, v, order[i], order)
		}
	}
}

func TestMultiCloser_ErrorAggregation(t *testing.T) {
	t.Parallel()
	errA := errors.New("error A")
	errB := errors.New("error B")

	var mc MultiCloser
	mc.AddCloser(closerFunc(func() error { return nil }))
	mc.AddCloser(closerFunc(func() error { return errA }))
	mc.AddCloser(closerFunc(func() error { return nil }))
	mc.AddCloser(closerFunc(func() error { return errB }))

	err := mc.Close()
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	// Both errors should be present (wrapped)
	if !errors.Is(err, errA) {
		t.Errorf("expected error to contain errA: %v", err)
	}
	if !errors.Is(err, errB) {
		t.Errorf("expected error to contain errB: %v", err)
	}
}

func TestMultiCloser_ClosesAllDespiteErrors(t *testing.T) {
	t.Parallel()
	var callCount atomic.Int32

	var mc MultiCloser
	for range 3 {
		mc.AddCloser(closerFunc(func() error {
			callCount.Add(1)
			return errors.New("fail")
		}))
	}

	mc.Close()

	if got := callCount.Load(); got != 3 {
		t.Fatalf("expected all 3 closers to be called, got %d", got)
	}
}

func TestMultiCloser_ConcurrentAdd(t *testing.T) {
	t.Parallel()
	var mc MultiCloser
	var callCount atomic.Int32

	done := make(chan struct{})
	for range 10 {
		go func() {
			mc.AddCloser(closerFunc(func() error {
				callCount.Add(1)
				return nil
			}))
			done <- struct{}{}
		}()
	}
	for range 10 {
		<-done
	}

	if err := mc.Close(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := callCount.Load(); got != 10 {
		t.Fatalf("expected 10 closers called, got %d", got)
	}
}

// Verify the io.Closer interface is satisfied.
var _ io.Closer = (*MultiCloser)(nil)
