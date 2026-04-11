// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package testutils

import (
	"context"
	"testing"
	"time"

	margaret "github.com/ssbc/margaret/v2"
	"github.com/ssbc/margaret/v2/multilog/roaring"

	"github.com/ssbc/go-ssb/message/multimsg"
)

// WaitForSeq blocks until a roaring sublog reaches the target sequence number.
// It uses margaret's Live query support for true event-driven waiting -- no polling.
// The Live iterator blocks on an internal observable (WaitFor) until new entries
// arrive, so there is zero CPU overhead while waiting.
//
// If the log has already reached targetSeq, it returns immediately.
// If the timeout expires before the target is reached, it calls t.Fatalf.
func WaitForSeq(t testing.TB, log margaret.Log[*roaring.Seq], targetSeq int64, timeout time.Duration, msgAndArgs ...interface{}) {
	t.Helper()

	// Fast path: already reached target
	if log.Seq() >= targetSeq {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Open a live query starting from current position.
	// The iterator will yield existing entries then block for new ones.
	qry := log.Query(margaret.Live(ctx))

	for seq := range qry.Iter() {
		if seq >= targetSeq {
			return
		}
	}

	fatalf(t, "WaitForSeq", timeout, targetSeq, log.Seq(), qry.Err(), msgAndArgs)
}

// WaitForReceiveLogSeq blocks until a ReceiveLog reaches the target sequence.
// Like WaitForSeq, it uses margaret's Live query for event-driven waiting.
func WaitForReceiveLogSeq(t testing.TB, log margaret.Log[*multimsg.MultiMessage], targetSeq int64, timeout time.Duration, msgAndArgs ...interface{}) {
	t.Helper()

	if log.Seq() >= targetSeq {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	qry := log.Query(margaret.Live(ctx))

	for seq := range qry.Iter() {
		if seq >= targetSeq {
			return
		}
	}

	fatalf(t, "WaitForReceiveLogSeq", timeout, targetSeq, log.Seq(), qry.Err(), msgAndArgs)
}

func fatalf(t testing.TB, name string, timeout time.Duration, targetSeq, currentSeq int64, err error, msgAndArgs []interface{}) {
	t.Helper()
	if len(msgAndArgs) > 0 {
		t.Fatalf("%s: timed out after %s waiting for seq %d (at %d): %v", name, timeout, targetSeq, currentSeq, msgAndArgs[0])
	}
	t.Fatalf("%s: timed out after %s waiting for seq %d (at %d)", name, timeout, targetSeq, currentSeq)
}
