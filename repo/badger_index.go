// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package repo

import (
	"encoding/binary"
	"fmt"

	"github.com/dgraph-io/badger/v3"
	margaret "github.com/ssbc/margaret/v2"
	"github.com/ssbc/margaret/v2/indexes"
)

// BadgerIndex implements indexes.Index[int64] backed by a badger database.
// It stores int64 values keyed by a prefix + addr.
type BadgerIndex struct {
	db     *badger.DB
	prefix []byte
}

var _ indexes.Index[int64] = (*BadgerIndex)(nil)

// NewBadgerIndex creates an index backed by badger with the given key prefix.
func NewBadgerIndex(db *badger.DB, prefix []byte) *BadgerIndex {
	return &BadgerIndex{db: db, prefix: prefix}
}

func (idx *BadgerIndex) key(addr indexes.Addr) []byte {
	k := make([]byte, len(idx.prefix)+len(addr))
	copy(k, idx.prefix)
	copy(k[len(idx.prefix):], addr)
	return k
}

func (idx *BadgerIndex) Get(addr indexes.Addr) (int64, error) {
	var val int64
	err := idx.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(idx.key(addr))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return indexes.ErrNotFound
			}
			return err
		}
		return item.Value(func(data []byte) error {
			if len(data) != 8 {
				return fmt.Errorf("badger index: expected 8 bytes, got %d", len(data))
			}
			val = int64(binary.BigEndian.Uint64(data))
			return nil
		})
	})
	return val, err
}

func (idx *BadgerIndex) Set(addr indexes.Addr, val int64) error {
	return idx.db.Update(func(txn *badger.Txn) error {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(val))
		return txn.Set(idx.key(addr), buf[:])
	})
}

func (idx *BadgerIndex) Delete(addr indexes.Addr) error {
	return idx.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(idx.key(addr))
	})
}

func (idx *BadgerIndex) Has(addr indexes.Addr) (bool, error) {
	var has bool
	err := idx.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(idx.key(addr))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		has = true
		return nil
	})
	return has, err
}

func (idx *BadgerIndex) Close() error { return nil }

// BadgerSeqIndex extends BadgerIndex with sequence tracking for incremental indexing.
type BadgerSeqIndex struct {
	*BadgerIndex
	seqKey []byte
}

var _ indexes.SeqIndex = (*BadgerSeqIndex)(nil)

// NewBadgerSeqIndex creates a seq-tracking index backed by badger.
func NewBadgerSeqIndex(db *badger.DB, prefix []byte) *BadgerSeqIndex {
	seqKey := make([]byte, len(prefix)+4)
	copy(seqKey, prefix)
	copy(seqKey[len(prefix):], []byte("_seq"))
	return &BadgerSeqIndex{
		BadgerIndex: NewBadgerIndex(db, prefix),
		seqKey:      seqKey,
	}
}

func (idx *BadgerSeqIndex) GetSeq() (int64, error) {
	var val int64 = margaret.SeqEmpty
	err := idx.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(idx.seqKey)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil // return SeqEmpty for not-yet-tracked
			}
			return err
		}
		return item.Value(func(data []byte) error {
			if len(data) != 8 {
				return fmt.Errorf("badger seq index: expected 8 bytes, got %d", len(data))
			}
			val = int64(binary.BigEndian.Uint64(data))
			return nil
		})
	})
	return val, err
}

func (idx *BadgerSeqIndex) SetSeq(seq int64) error {
	return idx.db.Update(func(txn *badger.Txn) error {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(seq))
		return txn.Set(idx.seqKey, buf[:])
	})
}
