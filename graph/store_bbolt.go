// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package graph

import (
	"bytes"
	"encoding/binary"
	"fmt"

	margaret "github.com/ssbc/margaret/v2"
	bolt "go.etcd.io/bbolt"
)

var (
	boltBucketRelations = []byte("graph-relations")
	boltBucketSeq       = []byte("graph-seq")
)

// BBoltGraphStore implements GraphStore backed by a shared bbolt database.
// It uses two buckets: "graph-relations" for edge data and "graph-seq" for
// sequence tracking. The bbolt DB is expected to be shared with other
// subsystems (each using their own buckets).
type BBoltGraphStore struct {
	db *bolt.DB
}

var _ GraphStore = (*BBoltGraphStore)(nil)

// NewBBoltGraphStore creates a GraphStore using the given bbolt database.
// It creates the required buckets if they don't exist.
func NewBBoltGraphStore(db *bolt.DB) (*BBoltGraphStore, error) {
	err := db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(boltBucketRelations); err != nil {
			return fmt.Errorf("bbolt graph store: create relations bucket: %w", err)
		}
		if _, err := tx.CreateBucketIfNotExists(boltBucketSeq); err != nil {
			return fmt.Errorf("bbolt graph store: create seq bucket: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &BBoltGraphStore{db: db}, nil
}

func (s *BBoltGraphStore) Close() error {
	// Don't close the DB — it's shared.
	return nil
}

func (s *BBoltGraphStore) SetRelation(key []byte, value []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(boltBucketRelations).Put(key, value)
	})
}

func (s *BBoltGraphStore) GetRelation(key []byte) ([]byte, error) {
	var result []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(boltBucketRelations).Get(key)
		if v == nil {
			return fmt.Errorf("bbolt graph store: key not found")
		}
		result = make([]byte, len(v))
		copy(result, v)
		return nil
	})
	return result, err
}

func (s *BBoltGraphStore) IterateAll(fn func(key, value []byte) error) error {
	return s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(boltBucketRelations).ForEach(fn)
	})
}

func (s *BBoltGraphStore) IteratePrefix(prefix []byte, fn func(key, value []byte) error) error {
	return s.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(boltBucketRelations).Cursor()
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			if err := fn(k, v); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *BBoltGraphStore) DeletePrefix(prefix []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(boltBucketRelations)
		c := b.Cursor()

		// Collect keys first — bbolt allows delete during cursor iteration
		// but the docs recommend caution; collecting is safest.
		var toDelete [][]byte
		for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
			keyCopy := make([]byte, len(k))
			copy(keyCopy, k)
			toDelete = append(toDelete, keyCopy)
		}

		for _, k := range toDelete {
			if err := b.Delete(k); err != nil {
				return fmt.Errorf("bbolt graph store: delete %x: %w", k, err)
			}
		}
		return nil
	})
}

func (s *BBoltGraphStore) SeqGet(name []byte) (int64, error) {
	var val int64 = margaret.SeqEmpty
	err := s.db.View(func(tx *bolt.Tx) error {
		data := tx.Bucket(boltBucketSeq).Get(name)
		if data == nil {
			return nil // not found => SeqEmpty
		}
		if len(data) == 8 {
			val = int64(binary.BigEndian.Uint64(data))
		}
		return nil
	})
	return val, err
}

func (s *BBoltGraphStore) SeqSet(name []byte, seq int64) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(seq))
		return tx.Bucket(boltBucketSeq).Put(name, buf[:])
	})
}
