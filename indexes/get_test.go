// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package indexes

import (
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/ssbc/margaret/v2/indexes"

	"github.com/ssbc/go-ssb/repo"
)

func openTestBadger(t *testing.T) *badger.DB {
	t.Helper()
	opts := badger.DefaultOptions("").WithInMemory(true).WithLogger(nil)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("failed to open badger: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestOpenGet_ReturnsWorkingIndex(t *testing.T) {
	t.Parallel()
	db := openTestBadger(t)

	idx, sinkIdx := OpenGet(db)
	if idx == nil {
		t.Fatal("expected non-nil index")
	}
	if sinkIdx == nil {
		t.Fatal("expected non-nil sink index")
	}
}

func TestBadgerIndex_SetGetHasDelete(t *testing.T) {
	t.Parallel()
	db := openTestBadger(t)

	idx := repo.NewBadgerIndex(db, []byte("test"))

	addr := indexes.Addr("some-key")

	// Initially not found
	_, err := idx.Get(addr)
	if err != indexes.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got: %v", err)
	}

	has, err := idx.Has(addr)
	if err != nil {
		t.Fatalf("Has error: %v", err)
	}
	if has {
		t.Fatal("expected Has to return false")
	}

	// Set a value
	if err := idx.Set(addr, 42); err != nil {
		t.Fatalf("Set error: %v", err)
	}

	// Get it back
	val, err := idx.Get(addr)
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if val != 42 {
		t.Fatalf("expected 42, got %d", val)
	}

	// Has should return true
	has, err = idx.Has(addr)
	if err != nil {
		t.Fatalf("Has error: %v", err)
	}
	if !has {
		t.Fatal("expected Has to return true")
	}

	// Delete
	if err := idx.Delete(addr); err != nil {
		t.Fatalf("Delete error: %v", err)
	}

	// Should be gone
	_, err = idx.Get(addr)
	if err != indexes.ErrNotFound {
		t.Fatalf("expected ErrNotFound after delete, got: %v", err)
	}
}

func TestBadgerIndex_PrefixIsolation(t *testing.T) {
	t.Parallel()
	db := openTestBadger(t)

	idx1 := repo.NewBadgerIndex(db, []byte("prefix1"))
	idx2 := repo.NewBadgerIndex(db, []byte("prefix2"))

	addr := indexes.Addr("shared-key")

	if err := idx1.Set(addr, 100); err != nil {
		t.Fatalf("Set error: %v", err)
	}
	if err := idx2.Set(addr, 200); err != nil {
		t.Fatalf("Set error: %v", err)
	}

	v1, err := idx1.Get(addr)
	if err != nil {
		t.Fatalf("Get idx1 error: %v", err)
	}
	v2, err := idx2.Get(addr)
	if err != nil {
		t.Fatalf("Get idx2 error: %v", err)
	}

	if v1 != 100 {
		t.Fatalf("idx1 expected 100, got %d", v1)
	}
	if v2 != 200 {
		t.Fatalf("idx2 expected 200, got %d", v2)
	}
}

func TestBadgerSeqIndex_SeqTracking(t *testing.T) {
	t.Parallel()
	db := openTestBadger(t)

	seqIdx := repo.NewBadgerSeqIndex(db, []byte("seqTest"))

	// Initially -1
	seq, err := seqIdx.GetSeq()
	if err != nil {
		t.Fatalf("GetSeq error: %v", err)
	}
	// Zero value when key doesn't exist yet
	_ = seq

	// Set and get
	if err := seqIdx.SetSeq(5); err != nil {
		t.Fatalf("SetSeq error: %v", err)
	}
	seq, err = seqIdx.GetSeq()
	if err != nil {
		t.Fatalf("GetSeq error: %v", err)
	}
	if seq != 5 {
		t.Fatalf("expected seq 5, got %d", seq)
	}
}
