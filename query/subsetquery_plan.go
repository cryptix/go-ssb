// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package query

import (
	"fmt"

	"github.com/dgraph-io/sroar"
	refs "github.com/ssbc/go-ssb-refs"

	"github.com/ssbc/go-ssb/internal/storedrefs"
	"github.com/ssbc/go-ssb/message/multimsg"
	margaret "github.com/ssbc/margaret/v2"
	"github.com/ssbc/margaret/v2/multilog"
	"github.com/ssbc/margaret/v2/multilog/roaring"
)

// Searcher is implemented by full-text search indexes that return results as bitmaps.
type Searcher interface {
	Search(query string, limit int) (*sroar.Bitmap, error)
}

type SubsetPlaner struct {
	authors, bytype *roaring.MultiLog
	search          Searcher // optional, may be nil
}

func NewSubsetPlaner(authors, bytype *roaring.MultiLog) *SubsetPlaner {
	return &SubsetPlaner{
		authors: authors,
		bytype:  bytype,
	}
}

// WithSearch returns the planer configured with a full-text search index.
func (sp *SubsetPlaner) WithSearch(s Searcher) *SubsetPlaner {
	sp.search = s
	return sp
}

// QuerySubsetBitmap evaluates the passed SubsetOperation and returns a bitmap which maps to messages in the receive log.
func (sp *SubsetPlaner) QuerySubsetBitmap(qry SubsetOperation) (*sroar.Bitmap, error) {
	return combineBitmaps(sp, qry)
}

// QuerySubsetMessages evaluates the passed SubsetOperation and returns a slice of messages
func (sp *SubsetPlaner) QuerySubsetMessages(rxLog margaret.Log[*multimsg.MultiMessage], qry SubsetOperation) ([]refs.Message, error) {
	resulting, err := combineBitmaps(sp, qry)
	if err != nil {
		return nil, err
	}

	if resulting == nil {
		return nil, nil
	}

	// iterate over the combined set of bitmaps
	it := resulting.NewIterator()

	var msgs []refs.Message

	for i := 0; i < resulting.GetCardinality(); i++ {
		v := int64(it.Next())
		mm, err := rxLog.Get(v)
		if err != nil {
			return nil, err
		}

		if mm.Message == nil {
			continue
		}

		msgs = append(msgs, mm.Message)
	}

	return msgs, nil
}

func combineBitmaps(sp *SubsetPlaner, qry SubsetOperation) (*sroar.Bitmap, error) {
	switch qry.operation {

	case "author":
		return sp.authors.LoadInternalBitmap(storedrefs.Feed(*qry.feed))

	case "type":
		return sp.bytype.LoadInternalBitmap(multilog.Addr("string:" + qry.string))

	case "search":
		if sp.search == nil {
			return nil, fmt.Errorf("sbot: search index not configured")
		}
		return sp.search.Search(qry.string, 1000)

	case "or", "and":
		if len(qry.args) == 0 {
			return nil, nil
		}

		// run the first operation and use it's result as the workBitmap the rest will be applied to
		workBitmap, err := combineBitmaps(sp, qry.args[0])
		if err != nil {
			return nil, fmt.Errorf("boolean (%s) operation %d of %d failed: %w", qry.operation, 1, len(qry.args), err)
		}

		// choose the boolean operation that all arguments will use
		boolOp := workBitmap.Or
		if qry.operation == "and" {
			boolOp = workBitmap.And
		}

		for i, op := range qry.args[1:] {

			// get the bitmap for the current operation
			opsBitmap, err := combineBitmaps(sp, op)
			if err != nil {
				return nil, fmt.Errorf("boolean (%s) operation %d of %d failed: %w", qry.operation, i+1, len(qry.args)-1, err)
			}

			// apply the result to the workBitmap
			boolOp(opsBitmap)
		}
		return workBitmap, nil

	default:
		return nil, fmt.Errorf("sbot: invalid subset query: %s", qry.operation)
	}
}
