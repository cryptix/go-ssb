// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

// Package mutil offers some margaret utilities.
package mutil

import (
	"errors"
	"fmt"

	margaret "github.com/ssbc/margaret/v2"
	"github.com/ssbc/margaret/v2/multilog/roaring"

	"github.com/ssbc/go-ssb/message/multimsg"
)

type indirectLog struct {
	root     margaret.Log[*multimsg.MultiMessage]
	indirect margaret.Log[*roaring.Seq]
}

// Indirect returns a new Log that uses the "indirect log" (a roaring bitmap sublog storing sequence numbers)
// to lookup entries in the "root log".
// This helps with the existing database abstraction where indexes just point to entries in the root log by sequence numbers.
func Indirect(root margaret.Log[*multimsg.MultiMessage], indirect margaret.Log[*roaring.Seq]) margaret.Log[*multimsg.MultiMessage] {
	return &indirectLog{
		root:     root,
		indirect: indirect,
	}
}

func (il *indirectLog) Seq() int64 {
	return il.indirect.Seq()
}

func (il *indirectLog) Get(seq int64) (*multimsg.MultiMessage, error) {
	v, err := il.indirect.Get(seq)
	if err != nil {
		return nil, fmt.Errorf("indirect: 1st lookup failed: %w", err)
	}

	rv, err := il.root.Get(int64(*v))
	if err != nil {
		return nil, fmt.Errorf("indirect: root lookup failed: %w", err)
	}
	return rv, nil
}

// Query returns an iterator that resolves indirect sequence references to actual messages.
func (il *indirectLog) Query(opts ...margaret.QueryOption) margaret.QueryIterator[*multimsg.MultiMessage] {
	innerQry := il.indirect.Query(opts...)

	var resolveErr error
	return margaret.NewLiveIterWrapper[*multimsg.MultiMessage](func(yield func(int64, *multimsg.MultiMessage) bool) {
		for seq, seqVal := range innerQry.Iter() {
			rootSeq := int64(*seqVal)
			msg, err := il.root.Get(rootSeq)
			if err != nil {
				if margaret.IsErrNulled(err) {
					continue
				}
				resolveErr = fmt.Errorf("indirect: resolve seq %d failed: %w", rootSeq, err)
				return
			}
			if !yield(seq, msg) {
				return
			}
		}
	}, func() error {
		if resolveErr != nil {
			return resolveErr
		}
		return innerQry.Err()
	})
}

// Append is not supported on an indirect log.
func (il *indirectLog) Append(*multimsg.MultiMessage) (int64, error) {
	return -2, errors.New("can't append to indirected log, sorry")
}

func (il *indirectLog) Close() error { return nil }
