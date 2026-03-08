// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package graph

import (
	"encoding/binary"
	"fmt"

	"github.com/dgraph-io/badger/v3"

	margaret "github.com/ssbc/margaret/v2"
)

// BadgerGraphStore implements GraphStore backed by a BadgerDB instance.
// All relation keys are stored with a "trust-graph" prefix.
// Sequence tracking keys use a separate prefix space.
type BadgerGraphStore struct {
	db *badger.DB
}

var _ GraphStore = (*BadgerGraphStore)(nil)

// NewBadgerGraphStore wraps an existing badger.DB for use as a GraphStore.
func NewBadgerGraphStore(db *badger.DB) *BadgerGraphStore {
	return &BadgerGraphStore{db: db}
}

func (s *BadgerGraphStore) Close() error {
	// Don't close the DB — it's shared with other indexes.
	return nil
}

func (s *BadgerGraphStore) prefixedKey(key []byte) []byte {
	out := make([]byte, 0, len(dbKeyPrefix)+len(key))
	out = append(out, dbKeyPrefix...)
	out = append(out, key...)
	return out
}

func (s *BadgerGraphStore) SetRelation(key []byte, value []byte) error {
	pk := s.prefixedKey(key)
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(pk, value)
	})
}

func (s *BadgerGraphStore) GetRelation(key []byte) ([]byte, error) {
	pk := s.prefixedKey(key)
	var result []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(pk)
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			result = make([]byte, len(v))
			copy(result, v)
			return nil
		})
	})
	return result, err
}

func (s *BadgerGraphStore) IterateAll(fn func(key, value []byte) error) error {
	return s.db.View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		for iter.Seek(dbKeyPrefix); iter.ValidForPrefix(dbKeyPrefix); iter.Next() {
			it := iter.Item()
			k := it.Key()[dbKeyPrefixLen:] // strip prefix

			var callErr error
			err := it.Value(func(v []byte) error {
				callErr = fn(k, v)
				return nil
			})
			if err != nil {
				return fmt.Errorf("badger graph store: failed to get value: %w", err)
			}
			if callErr != nil {
				return callErr
			}
		}
		return nil
	})
}

func (s *BadgerGraphStore) IteratePrefix(prefix []byte, fn func(key, value []byte) error) error {
	fullPrefix := s.prefixedKey(prefix)
	return s.db.View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		for iter.Seek(fullPrefix); iter.ValidForPrefix(fullPrefix); iter.Next() {
			it := iter.Item()
			k := it.Key()[dbKeyPrefixLen:] // strip prefix
			var callErr error
			err := it.Value(func(v []byte) error {
				callErr = fn(k, v)
				return nil
			})
			if err != nil {
				return fmt.Errorf("badger graph store: failed to get value: %w", err)
			}
			if callErr != nil {
				return callErr
			}
		}
		return nil
	})
}

func (s *BadgerGraphStore) DeletePrefix(prefix []byte) error {
	fullPrefix := s.prefixedKey(prefix)
	return s.db.Update(func(txn *badger.Txn) error {
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		for iter.Seek(fullPrefix); iter.ValidForPrefix(fullPrefix); iter.Next() {
			k := iter.Item().KeyCopy(nil)
			if err := txn.Delete(k); err != nil {
				return fmt.Errorf("badger graph store: delete %x: %w", k, err)
			}
		}
		return nil
	})
}

func (s *BadgerGraphStore) SeqGet(name []byte) (int64, error) {
	var val int64 = margaret.SeqEmpty
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(name)
		if err != nil {
			return err // key not found => SeqEmpty
		}
		return item.Value(func(data []byte) error {
			if len(data) == 8 {
				val = int64(binary.BigEndian.Uint64(data))
			}
			return nil
		})
	})
	if err != nil {
		// Key not found is not an error — just return SeqEmpty.
		return margaret.SeqEmpty, nil
	}
	return val, nil
}

func (s *BadgerGraphStore) SeqSet(name []byte, seq int64) error {
	return s.db.Update(func(txn *badger.Txn) error {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(seq))
		return txn.Set(name, buf[:])
	})
}
