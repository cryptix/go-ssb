// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package repo

import (
	"fmt"

	refs "github.com/ssbc/go-ssb-refs"
	margaret "github.com/ssbc/margaret/v2"

	"github.com/ssbc/go-ssb/message/multimsg"
)

// FilterFunc works on messages of a FilteredLog. If the func returns true, the message is included.
type FilterFunc func(refs.Message) bool

// NewFilteredLog wraps the passed log into a new one, using the FilterFunc to decide if a message is in the log.
func NewFilteredLog(b margaret.Log[*multimsg.MultiMessage], fn FilterFunc) margaret.Log[*multimsg.MultiMessage] {
	return &FilteredLog{
		backing: b,
		filter:  fn,
	}
}

// FilteredLog omits entries in the backing log as decided by the configured FilterFunc.
// It does so by claiming the entries are deleted (via returning margaret.ErrNulled instead)
type FilteredLog struct {
	backing margaret.Log[*multimsg.MultiMessage]
	filter  FilterFunc
}

func (fl *FilteredLog) Seq() int64 { return fl.backing.Seq() }

// Get retrieves the message, returning ErrNulled if the filter rejects it.
func (fl *FilteredLog) Get(s int64) (*multimsg.MultiMessage, error) {
	mm, err := fl.backing.Get(s)
	if err != nil {
		return nil, fmt.Errorf("filtered get: failed to retrieve entry: %w", err)
	}
	if mm.Message == nil {
		return nil, margaret.ErrNulled
	}
	if okay := fl.filter(mm.Message); !okay {
		return nil, margaret.ErrNulled
	}
	return mm, nil
}

// Query returns an iterator that skips entries rejected by the filter.
func (fl *FilteredLog) Query(opts ...margaret.QueryOption) margaret.QueryIterator[*multimsg.MultiMessage] {
	innerQry := fl.backing.Query(opts...)

	return margaret.NewLiveIterWrapper[*multimsg.MultiMessage](func(yield func(int64, *multimsg.MultiMessage) bool) {
		for seq, mm := range innerQry.Iter() {
			if mm.Message == nil {
				continue // skip nulled
			}
			if okay := fl.filter(mm.Message); !okay {
				continue // skip filtered
			}
			if !yield(seq, mm) {
				return
			}
		}
	}, innerQry.Err)
}

// Append is not supported on a filtered log.
func (fl *FilteredLog) Append(*multimsg.MultiMessage) (int64, error) {
	return -2, fmt.Errorf("FilteredLog is read-only")
}

func (fl *FilteredLog) Close() error { return nil }
