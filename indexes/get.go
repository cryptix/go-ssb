// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

// Package indexes contains functions to create indexing for 'get(%ref) -> message'.
// Also contains a utility to open the contact trust graph using the repo and graph packages.
package indexes

import (
	"github.com/dgraph-io/badger/v3"
	"github.com/ssbc/margaret/v2/indexes"

	"github.com/ssbc/go-ssb/internal/storedrefs"
	"github.com/ssbc/go-ssb/message/multimsg"
	"github.com/ssbc/go-ssb/repo"
)

// OpenGet supplies the get(msgRef) -> rootLogSeq index.
// Returns the lookup index and a SinkIndex that processes log entries.
func OpenGet(db *badger.DB) (indexes.Index[int64], *indexes.SinkIndex[*multimsg.MultiMessage, int64]) {
	seqIdx := repo.NewBadgerSeqIndex(db, []byte("byMsgRef"))
	idx := repo.NewBadgerIndex(db, []byte("byMsgRef"))

	sinkIdx := indexes.NewSinkIndex[*multimsg.MultiMessage](idx,
		func(seq int64, msg *multimsg.MultiMessage) (indexes.Addr, int64, bool) {
			if msg.Message == nil {
				return "", 0, false // skip nulled entries
			}
			return indexes.Addr(storedrefs.Message(msg.Key())), seq, true
		},
	).WithSeqTracking(seqIdx)

	return idx, sinkIdx
}
