// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package testutils

import (
	"testing"

	"github.com/ssbc/go-ssb/internal/leakcheck"
)

// CheckGoroutineLeaks registers a cleanup function on t that checks for
// goroutine leaks after the test completes. Call this at the start of any
// test that creates goroutines (especially sbot instances).
//
//	func TestFoo(t *testing.T) {
//	    testutils.CheckGoroutineLeaks(t)
//	    // ... test code ...
//	}
func CheckGoroutineLeaks(t testing.TB) {
	t.Helper()
	t.Cleanup(func() {
		leakcheck.Check(t)
	})
}
