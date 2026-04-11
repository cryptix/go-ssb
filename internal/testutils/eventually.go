// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package testutils

import (
	"testing"
	"time"
)

// RequireEventually polls condition until it returns true or timeout elapses.
// It replaces time.Sleep in tests with a deterministic polling approach that
// finishes as soon as the condition is met.
func RequireEventually(t testing.TB, condition func() bool, timeout time.Duration, msgAndArgs ...interface{}) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	interval := 50 * time.Millisecond
	if timeout > 10*time.Second {
		interval = 250 * time.Millisecond
	}

	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(interval)
	}

	if len(msgAndArgs) > 0 {
		t.Fatalf("condition not met within %s: %v", timeout, msgAndArgs[0])
	} else {
		t.Fatalf("condition not met within %s", timeout)
	}
}
