// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package graph

import "io"

// GraphStore abstracts the key-value storage used by the graph builder.
// Keys are raw byte slices (typically concatenated 34-byte TFK feed pairs).
// Implementations must support sorted prefix iteration.
type GraphStore interface {
	io.Closer

	// SetRelation stores a graph edge.
	// key is typically the concatenated from+to TFK pair (68 bytes).
	// value encodes the relation state (single byte for contacts,
	// or TFK bytes for announcements).
	SetRelation(key []byte, value []byte) error

	// GetRelation retrieves the value for a single relation key.
	// Returns ErrNotFound (or similar) if the key doesn't exist.
	GetRelation(key []byte) ([]byte, error)

	// IterateAll calls fn for every relation key/value pair.
	// Keys and values passed to fn are only valid for the duration of the call.
	IterateAll(fn func(key, value []byte) error) error

	// IteratePrefix calls fn for each relation key/value with the given prefix.
	// Keys and values passed to fn are only valid for the duration of the call.
	IteratePrefix(prefix []byte, fn func(key, value []byte) error) error

	// DeletePrefix deletes all relation keys matching the prefix.
	DeletePrefix(prefix []byte) error

	// SeqGet returns the stored int64 for a sequence tracking key.
	// Returns -2 (margaret.SeqEmpty) if the key doesn't exist.
	SeqGet(name []byte) (int64, error)

	// SeqSet stores an int64 for a sequence tracking key.
	SeqSet(name []byte, seq int64) error
}
