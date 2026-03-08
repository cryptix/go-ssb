// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package readmarks

import (
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/dgraph-io/badger/v3"
)

var keyPrefix = []byte("readmark:")

// ReadMarkStore tracks read positions in streams (feeds, threads, channels, root log)
// using the shared BadgerDB instance.
//
// Keys have the format: readmark:<stream_type>:<stream_id>
// Values are int64 sequence numbers encoded as 8-byte big-endian.
type ReadMarkStore struct {
	db *badger.DB
}

// NewReadMarkStore creates a new read-marker store backed by the given badger database.
func NewReadMarkStore(db *badger.DB) *ReadMarkStore {
	return &ReadMarkStore{db: db}
}

func makeKey(streamType, streamID string) []byte {
	k := make([]byte, 0, len(keyPrefix)+len(streamType)+1+len(streamID))
	k = append(k, keyPrefix...)
	k = append(k, []byte(streamType)...)
	k = append(k, ':')
	k = append(k, []byte(streamID)...)
	return k
}

// Set stores the read position (sequence number) for a given stream.
func (s *ReadMarkStore) Set(streamType, streamID string, seq int64) error {
	if streamType == "" || streamID == "" {
		return fmt.Errorf("readmarks: stream type and ID must not be empty")
	}
	key := makeKey(streamType, streamID)
	return s.db.Update(func(txn *badger.Txn) error {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(seq))
		return txn.Set(key, buf[:])
	})
}

// Get retrieves the read position for a given stream.
// Returns the sequence and true if found, or -1 and false if not set.
func (s *ReadMarkStore) Get(streamType, streamID string) (int64, bool, error) {
	key := makeKey(streamType, streamID)
	var seq int64
	var found bool
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		found = true
		return item.Value(func(data []byte) error {
			if len(data) != 8 {
				return fmt.Errorf("readmarks: expected 8 bytes, got %d", len(data))
			}
			seq = int64(binary.BigEndian.Uint64(data))
			return nil
		})
	})
	if !found {
		seq = -1
	}
	return seq, found, err
}

// ReadMark represents a single read marker entry.
type ReadMark struct {
	StreamType string `json:"stream_type"`
	StreamID   string `json:"stream_id"`
	Sequence   int64  `json:"sequence"`
}

// List returns all stored read markers, optionally filtered by stream type.
// Pass "" for streamType to return all markers.
func (s *ReadMarkStore) List(streamType string) ([]ReadMark, error) {
	var marks []ReadMark
	prefix := keyPrefix
	if streamType != "" {
		prefix = makeKey(streamType, "")
	}
	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := item.Key()

			// Parse key: readmark:<type>:<id>
			rest := string(k[len(keyPrefix):])
			parts := strings.SplitN(rest, ":", 2)
			if len(parts) != 2 {
				continue
			}

			var mark ReadMark
			mark.StreamType = parts[0]
			mark.StreamID = parts[1]

			err := item.Value(func(data []byte) error {
				if len(data) != 8 {
					return fmt.Errorf("readmarks: expected 8 bytes, got %d", len(data))
				}
				mark.Sequence = int64(binary.BigEndian.Uint64(data))
				return nil
			})
			if err != nil {
				return err
			}
			marks = append(marks, mark)
		}
		return nil
	})
	return marks, err
}

// Delete removes the read marker for a given stream.
func (s *ReadMarkStore) Delete(streamType, streamID string) error {
	key := makeKey(streamType, streamID)
	return s.db.Update(func(txn *badger.Txn) error {
		err := txn.Delete(key)
		if err == badger.ErrKeyNotFound {
			return nil
		}
		return err
	})
}
