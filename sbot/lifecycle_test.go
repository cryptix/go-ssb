// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package sbot

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"go.mindeco.de/log"
)

func TestClose_CompletesWithinTimeout(t *testing.T) {
	t.Parallel()

	tPath := filepath.Join(t.TempDir(), t.Name())
	logger := log.NewLogfmtLogger(log.NewSyncWriter(testWriter{t}))

	bot, err := New(
		WithInfo(logger),
		WithRepoPath(tPath),
		DisableNetworkNode(),
	)
	if err != nil {
		t.Fatalf("failed to create sbot: %v", err)
	}

	// Close should complete well within 30s
	done := make(chan error, 1)
	go func() {
		done <- bot.Close()
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not complete within 10 seconds")
	}
}

func TestClose_Idempotent(t *testing.T) {
	t.Parallel()

	tPath := filepath.Join(t.TempDir(), t.Name())
	logger := log.NewLogfmtLogger(log.NewSyncWriter(testWriter{t}))

	bot, err := New(
		WithInfo(logger),
		WithRepoPath(tPath),
		DisableNetworkNode(),
	)
	if err != nil {
		t.Fatalf("failed to create sbot: %v", err)
	}

	// First close
	if err := bot.Close(); err != nil {
		t.Fatalf("first Close returned error: %v", err)
	}

	// Second close should not panic or error differently
	if err := bot.Close(); err != nil {
		t.Fatalf("second Close returned error: %v", err)
	}
}

func TestGoroutineCount_StableAfterClose(t *testing.T) {
	t.Parallel()

	// Take a baseline goroutine count before creating the bot
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	baseline := runtime.NumGoroutine()

	tPath := filepath.Join(t.TempDir(), t.Name())
	logger := log.NewLogfmtLogger(log.NewSyncWriter(testWriter{t}))

	bot, err := New(
		WithInfo(logger),
		WithRepoPath(tPath),
		DisableNetworkNode(),
	)
	if err != nil {
		t.Fatalf("failed to create sbot: %v", err)
	}

	if err := bot.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	// Allow goroutines to wind down
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		runtime.GC()
		time.Sleep(100 * time.Millisecond)
		current := runtime.NumGoroutine()
		// Allow a small margin (3) for runtime goroutines we can't control
		if current <= baseline+3 {
			return
		}
	}

	t.Fatalf("goroutine count did not stabilize after Close: baseline=%d, current=%d",
		baseline, runtime.NumGoroutine())
}

func TestShutdownThenClose(t *testing.T) {
	t.Parallel()

	tPath := filepath.Join(t.TempDir(), t.Name())
	logger := log.NewLogfmtLogger(log.NewSyncWriter(testWriter{t}))

	bot, err := New(
		WithInfo(logger),
		WithRepoPath(tPath),
		DisableNetworkNode(),
	)
	if err != nil {
		t.Fatalf("failed to create sbot: %v", err)
	}

	// Shutdown cancels the root context
	bot.Shutdown()

	// Close should still complete cleanly after Shutdown
	done := make(chan error, 1)
	go func() {
		done <- bot.Close()
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close after Shutdown returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not complete within 10 seconds after Shutdown")
	}
}

func TestNew_FailsGracefully_BadRepoPath(t *testing.T) {
	t.Parallel()

	// Use a path that can't be created (file as parent dir)
	tPath := filepath.Join(t.TempDir(), "file-not-dir")
	// Create a file where a directory is expected
	if err := writeFile(tPath); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	_, err := New(
		WithRepoPath(filepath.Join(tPath, "subdir", "repo")),
		DisableNetworkNode(),
	)
	if err == nil {
		t.Fatal("expected error for invalid repo path")
	}
}

// testWriter adapts testing.T to io.Writer for logging.
type testWriter struct {
	t testing.TB
}

func (tw testWriter) Write(p []byte) (n int, err error) {
	tw.t.Helper()
	tw.t.Log(string(p))
	return len(p), nil
}

func writeFile(path string) error {
	return os.WriteFile(path, []byte("not a directory"), 0644)
}
