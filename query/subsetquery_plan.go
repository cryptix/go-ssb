// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package query

import (
	"fmt"

	"github.com/dgraph-io/sroar"
	refs "github.com/ssbc/go-ssb-refs"

	"github.com/ssbc/go-ssb"
	"github.com/ssbc/go-ssb/graph"
	"github.com/ssbc/go-ssb/internal/storedrefs"
	"github.com/ssbc/go-ssb/message/multimsg"
	"github.com/ssbc/go-ssb/multilogs"
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
	tangles         *roaring.MultiLog
	channels        *roaring.MultiLog
	mentions        *roaring.MultiLog

	// rxLog is needed for NOT operations (to compute the universe bitmap)
	// and for timestamp filtering
	rxLog margaret.Log[*multimsg.MultiMessage]

	// graphBuilder provides access to the social follow/block graph
	// for graph-aware query operations (followedBy, blockedBy, hops, friendsBlocks)
	graphBuilder graph.Builder
	search          Searcher // optional, may be nil
}

// NewSubsetPlaner creates a new SubsetPlaner with author and type indexes.
// For tangle query support, use NewSubsetPlanerWithTangles instead.
func NewSubsetPlaner(authors, bytype *roaring.MultiLog) *SubsetPlaner {
	return &SubsetPlaner{
		authors: authors,
		bytype:  bytype,
	}
}

// NewSubsetPlanerWithTangles creates a new SubsetPlaner with author, type, and tangle indexes.
func NewSubsetPlanerWithTangles(authors, bytype, tangles *roaring.MultiLog) *SubsetPlaner {
	return &SubsetPlaner{
		authors: authors,
		bytype:  bytype,
		tangles: tangles,
	}
}

// NewSubsetPlanerFull creates a SubsetPlaner with all available indexes.
func NewSubsetPlanerFull(
	authors, bytype, tangles *roaring.MultiLog,
	channels, mentions *roaring.MultiLog,
	rxLog margaret.Log[*multimsg.MultiMessage],
	gb graph.Builder,
) *SubsetPlaner {
	return &SubsetPlaner{
		authors:      authors,
		bytype:       bytype,
		tangles:      tangles,
		channels:     channels,
		mentions:     mentions,
		rxLog:        rxLog,
		graphBuilder: gb,
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

// universeMaxSeq returns the max sequence from the rxLog stored in the planer,
// falling back to the passed rxLog from QuerySubsetMessages if needed.
func (sp *SubsetPlaner) universeMaxSeq() int64 {
	if sp.rxLog != nil {
		return sp.rxLog.Seq()
	}
	return -1
}

// feedSetToBitmap converts a set of feeds into a combined bitmap by ORing
// each feed's author bitmap together. This is the bridge between graph queries
// (which return sets of feeds) and bitmap queries (which return message sets).
func feedSetToBitmap(sp *SubsetPlaner, feeds *ssb.StrFeedSet) (*sroar.Bitmap, error) {
	lst, err := feeds.List()
	if err != nil {
		return nil, fmt.Errorf("feedSetToBitmap: failed to list feeds: %w", err)
	}
	if len(lst) == 0 {
		return nil, nil
	}
	result := sroar.NewBitmap()
	for _, f := range lst {
		bm, err := sp.authors.LoadInternalBitmap(storedrefs.Feed(f))
		if err != nil {
			// feed might not have any messages in our log, skip it
			continue
		}
		result.Or(bm)
	}
	return result, nil
}

func combineBitmaps(sp *SubsetPlaner, qry SubsetOperation) (*sroar.Bitmap, error) {
	switch qry.operation {

	case "author":
		return sp.authors.LoadInternalBitmap(storedrefs.Feed(*qry.feed))

	case "type":
		return sp.bytype.LoadInternalBitmap(multilog.Addr("string:" + qry.string))

	case "tangle":
		if sp.tangles == nil {
			return nil, fmt.Errorf("sbot: tangle queries not supported (no tangle index)")
		}
		var addr multilog.Addr
		if qry.name == "" {
			addr = storedrefs.TangleV1(*qry.root)
		} else {
			addr = storedrefs.TangleV2(qry.name, *qry.root)
		}
		return sp.tangles.LoadInternalBitmap(addr)

	case "channel":
		if sp.channels == nil {
			return nil, fmt.Errorf("sbot: channel queries not supported (no channel index)")
		}
		return sp.channels.LoadInternalBitmap(multilogs.SanitizeChannelAddr(qry.string))

	case "mentions":
		if sp.mentions == nil {
			return nil, fmt.Errorf("sbot: mentions queries not supported (no mentions index)")
		}
		return sp.mentions.LoadInternalBitmap(multilog.Addr(qry.feed.String()))

	case "hasBlob":
		if sp.mentions == nil {
			return nil, fmt.Errorf("sbot: hasBlob queries not supported (no mentions index)")
		}
		return sp.mentions.LoadInternalBitmap(multilog.Addr(qry.ref))

	case "isRoot":
		// root posts are tracked in byType under the "meta:root" key
		return sp.bytype.LoadInternalBitmap(multilog.Addr("meta:root"))

	case "timestamp":
		// timestamp filtering requires the rxLog to scan messages
		if sp.rxLog == nil {
			return nil, fmt.Errorf("sbot: timestamp queries require rxLog on SubsetPlaner")
		}
		maxSeq := sp.rxLog.Seq()
		if maxSeq < 0 {
			return nil, nil
		}
		result := sroar.NewBitmap()
		for seq := int64(0); seq <= maxSeq; seq++ {
			mm, err := sp.rxLog.Get(seq)
			if err != nil {
				continue
			}
			if mm.Message == nil {
				continue
			}
			ts := mm.Message.Claimed().UnixMilli()
			if qry.timestampGt > 0 && ts <= qry.timestampGt {
				continue
			}
			if qry.timestampLt > 0 && ts >= qry.timestampLt {
				continue
			}
			result.Set(uint64(seq))
		}
		return result, nil

	case "not":
		if len(qry.args) != 1 {
			return nil, fmt.Errorf("sbot: not operation requires exactly one argument")
		}
		// get the child bitmap to exclude
		childBitmap, err := combineBitmaps(sp, qry.args[0])
		if err != nil {
			return nil, fmt.Errorf("not operation failed: %w", err)
		}
		if childBitmap == nil {
			// NOT(empty) = everything, return universe
			maxSeq := sp.universeMaxSeq()
			if maxSeq < 0 {
				return nil, nil
			}
			universe := sroar.NewBitmap()
			for i := uint64(0); i <= uint64(maxSeq); i++ {
				universe.Set(i)
			}
			return universe, nil
		}
		// build universe bitmap [0, maxSeq] and subtract child
		maxSeq := sp.universeMaxSeq()
		if maxSeq < 0 {
			return nil, nil
		}
		universe := sroar.NewBitmap()
		for i := uint64(0); i <= uint64(maxSeq); i++ {
			universe.Set(i)
		}
		universe.AndNot(childBitmap)
		return universe, nil

	case "followedBy":
		if sp.graphBuilder == nil {
			return nil, fmt.Errorf("sbot: followedBy queries require a graph builder")
		}
		follows, err := sp.graphBuilder.Follows(*qry.feed)
		if err != nil {
			return nil, fmt.Errorf("followedBy: failed to get follows: %w", err)
		}
		return feedSetToBitmap(sp, follows)

	case "blockedBy":
		if sp.graphBuilder == nil {
			return nil, fmt.Errorf("sbot: blockedBy queries require a graph builder")
		}
		g, err := sp.graphBuilder.Build()
		if err != nil {
			return nil, fmt.Errorf("blockedBy: failed to build graph: %w", err)
		}
		blocked := g.BlockedList(*qry.feed)
		return feedSetToBitmap(sp, blocked)

	case "hops":
		if sp.graphBuilder == nil {
			return nil, fmt.Errorf("sbot: hops queries require a graph builder")
		}
		hopSet := sp.graphBuilder.Hops(*qry.feed, qry.hops)
		return feedSetToBitmap(sp, hopSet)

	case "friendsBlocks":
		if sp.graphBuilder == nil {
			return nil, fmt.Errorf("sbot: friendsBlocks queries require a graph builder")
		}
		// Get the feeds that "who" follows
		follows, err := sp.graphBuilder.Follows(*qry.feed)
		if err != nil {
			return nil, fmt.Errorf("friendsBlocks: failed to get follows: %w", err)
		}
		friends, err := follows.List()
		if err != nil {
			return nil, fmt.Errorf("friendsBlocks: failed to list follows: %w", err)
		}
		// Build the graph once for all block lookups
		g, err := sp.graphBuilder.Build()
		if err != nil {
			return nil, fmt.Errorf("friendsBlocks: failed to build graph: %w", err)
		}
		// Collect all feeds blocked by any friend
		allBlocked := ssb.NewFeedSet(0)
		for _, friend := range friends {
			blocked := g.BlockedList(friend)
			blockedList, err := blocked.List()
			if err != nil {
				continue
			}
			for _, b := range blockedList {
				allBlocked.AddRef(b)
			}
		}
		return feedSetToBitmap(sp, allBlocked)

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
