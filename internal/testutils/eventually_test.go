// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package testutils

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestRequireEventually_ImmediateSuccess(t *testing.T) {
	t.Parallel()
	start := time.Now()
	RequireEventually(t, func() bool { return true }, 5*time.Second, "should pass immediately")
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("expected immediate return, took %s", elapsed)
	}
}

func TestRequireEventually_DelayedSuccess(t *testing.T) {
	t.Parallel()
	var ready atomic.Bool
	go func() {
		time.Sleep(200 * time.Millisecond)
		ready.Store(true)
	}()

	start := time.Now()
	RequireEventually(t, func() bool { return ready.Load() }, 5*time.Second, "should become ready")
	elapsed := time.Since(start)

	if elapsed < 150*time.Millisecond {
		t.Fatalf("returned too early: %s", elapsed)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("took too long: %s", elapsed)
	}
}

func TestRequireEventually_PollsUntilTrue(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	RequireEventually(t, func() bool {
		return calls.Add(1) >= 3
	}, 5*time.Second, "should poll multiple times")

	if c := calls.Load(); c < 3 {
		t.Fatalf("expected at least 3 calls, got %d", c)
	}
}
