// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

// Package query holds the first version of a generic query engine for go-ssb.
// The Subset operations are able to combine arbitrary boolen combinations of type:xzy and author:@foo filters into one result.
package query

import (
	"encoding/json"
	"fmt"

	"github.com/ssbc/go-ssb"
	refs "github.com/ssbc/go-ssb-refs"
)

// SubsetOptions defines additional options for the getSubset rpc call
type SubsetOptions struct {
	Keys       bool  `json:"keys"` // can't omit this falsy value, the JS-stack stack assumes true if it's not there
	Descending bool  `json:"descending,omitempty"`
	PageLimit  int   `json:"pageLimit,omitempty"`
	AfterSeq   int64 `json:"afterSeq,omitempty"` // cursor: resume after this receive-log sequence
}

// SubsetOperation encapsulates the recursive structure of operations for the QuerySubset*() methods
type SubsetOperation struct {
	operation string

	args   []SubsetOperation
	string string
	feed   *refs.FeedRef

	// for tangle queries
	root *refs.MessageRef
	name string // tangle name for v2 tangles (empty for v1)

	// for mentions / hasBlob
	ref string // generic ref string (feed, message, or blob ref)

	// for timestamp filtering
	timestampGt int64 // messages after this unix timestamp (milliseconds)
	timestampLt int64 // messages before this unix timestamp (milliseconds)

	// for graph operations (hops distance)
	hops int
}

// NewSubsetOpByType returns a single operation which filters messages by type
func NewSubsetOpByType(value string) SubsetOperation {
	return SubsetOperation{operation: "type", string: value}
}

// NewSubsetOpByAuthor returns a single operation which filters messages by author
func NewSubsetOpByAuthor(a refs.FeedRef) SubsetOperation {
	return SubsetOperation{operation: "author", feed: &a}
}

// NewSubsetOpByTangle returns a single operation which filters messages belonging to a v1 tangle (thread)
func NewSubsetOpByTangle(root refs.MessageRef) SubsetOperation {
	return SubsetOperation{operation: "tangle", root: &root}
}

// NewSubsetOpByTangleV2 returns a single operation which filters messages belonging to a named v2 tangle
func NewSubsetOpByTangleV2(name string, root refs.MessageRef) SubsetOperation {
	return SubsetOperation{operation: "tangle", root: &root, name: name}
}

// NewSubsetNotCombination returns a NOT operation that inverts/excludes the result of the inner operation.
// e.g. not(author(blocked)) excludes messages from a blocked author.
func NewSubsetNotCombination(op SubsetOperation) SubsetOperation {
	return SubsetOperation{operation: "not", args: []SubsetOperation{op}}
}

// NewSubsetOpByChannel returns a single operation which filters messages by channel (hashtag)
func NewSubsetOpByChannel(channel string) SubsetOperation {
	return SubsetOperation{operation: "channel", string: channel}
}

// NewSubsetOpIsRoot returns an operation that matches only root posts (messages without content.root)
func NewSubsetOpIsRoot() SubsetOperation {
	return SubsetOperation{operation: "isRoot"}
}

// NewSubsetOpByMention returns a single operation which filters messages that mention a specific feed
func NewSubsetOpByMention(a refs.FeedRef) SubsetOperation {
	return SubsetOperation{operation: "mentions", feed: &a}
}

// NewSubsetOpHasBlob returns a single operation which filters messages that reference a specific blob
func NewSubsetOpHasBlob(b refs.BlobRef) SubsetOperation {
	return SubsetOperation{operation: "hasBlob", ref: b.Sigil()}
}

// NewSubsetOpHasRoot returns a single operation which filters messages that have a specific root.
// This is equivalent to NewSubsetOpByTangle but named to match the oasis query builder pattern.
func NewSubsetOpHasRoot(root refs.MessageRef) SubsetOperation {
	return NewSubsetOpByTangle(root)
}

// NewSubsetOpByTimestamp returns an operation that filters messages within a time range.
// Both gt and lt are unix timestamps in milliseconds. Use 0 to leave a bound open.
func NewSubsetOpByTimestamp(gt, lt int64) SubsetOperation {
	return SubsetOperation{operation: "timestamp", timestampGt: gt, timestampLt: lt}
}

// --- Graph-aware operations ---
// These operations integrate with the social follow/block graph to filter
// messages based on trust relationships. They resolve a set of feeds from
// the graph and return the union of their message bitmaps.

// NewSubsetOpFollowedBy returns messages authored by feeds that the given feed follows.
// This is the core building block for "my timeline" queries.
func NewSubsetOpFollowedBy(who refs.FeedRef) SubsetOperation {
	return SubsetOperation{operation: "followedBy", feed: &who}
}

// NewSubsetOpBlockedBy returns messages authored by feeds that the given feed blocks.
// Typically used inside a NOT() for exclusion: not(blockedBy(me))
func NewSubsetOpBlockedBy(who refs.FeedRef) SubsetOperation {
	return SubsetOperation{operation: "blockedBy", feed: &who}
}

// NewSubsetOpInHops returns messages authored by feeds within N hops of the given feed.
// Hops 0 = direct follows, 1 = friends of friends (requires mutual follow), etc.
func NewSubsetOpInHops(who refs.FeedRef, hops int) SubsetOperation {
	return SubsetOperation{operation: "hops", feed: &who, hops: hops}
}

// NewSubsetOpFriendsBlocks returns messages authored by feeds blocked by any
// friend of the given feed. Used for social content moderation:
// not(friendsBlocks(me)) filters out content that your friends have blocked.
func NewSubsetOpFriendsBlocks(who refs.FeedRef) SubsetOperation {
	return SubsetOperation{operation: "friendsBlocks", feed: &who}
}

// NewSubsetOpByAuthors is a convenience builder that creates an OR combination
// of multiple author filters. If only one author is provided, it returns a simple
// author operation without wrapping it in an OR.
func NewSubsetOpByAuthors(authors ...refs.FeedRef) SubsetOperation {
	if len(authors) == 1 {
		return NewSubsetOpByAuthor(authors[0])
	}
	ops := make([]SubsetOperation, len(authors))
	for i, a := range authors {
		ops[i] = NewSubsetOpByAuthor(a)
	}
	return NewSubsetOrCombination(ops...)
}

// NewSubsetOpByTypes is a convenience builder that creates an OR combination
// of multiple type filters. If only one type is provided, it returns a simple
// type operation without wrapping it in an OR.
func NewSubsetOpByTypes(types ...string) SubsetOperation {
	if len(types) == 1 {
		return NewSubsetOpByType(types[0])
	}
	ops := make([]SubsetOperation, len(types))
	for i, t := range types {
		ops[i] = NewSubsetOpByType(t)
	}
	return NewSubsetOrCombination(ops...)
}

// NewSubsetOpByAuthorsAndTypes is a convenience builder matching the pattern from ssb-oasis query builder.
// It creates an AND combination of OR(authors...) and OR(types...).
// If either authors or types is empty, only the non-empty filter is applied.
func NewSubsetOpByAuthorsAndTypes(authors []refs.FeedRef, types []string) SubsetOperation {
	hasAuthors := len(authors) > 0
	hasTypes := len(types) > 0

	switch {
	case hasAuthors && hasTypes:
		return NewSubsetAndCombination(
			NewSubsetOpByAuthors(authors...),
			NewSubsetOpByTypes(types...),
		)
	case hasAuthors:
		return NewSubsetOpByAuthors(authors...)
	case hasTypes:
		return NewSubsetOpByTypes(types...)
	default:
		return SubsetOperation{}
	}
}

// NewSubsetAndCombination turns the list of passed operations into a logical combination where all of them need to apply
func NewSubsetAndCombination(ops ...SubsetOperation) SubsetOperation {
	return SubsetOperation{operation: "and", args: ops}
}

// NewSubsetOrCombination turns the list of passed operations into a logical combination where any of them needs to apply
func NewSubsetOrCombination(ops ...SubsetOperation) SubsetOperation {
	return SubsetOperation{operation: "or", args: ops}
}

// NewSubsetOpBySearch returns a single operation which performs full-text search
func NewSubsetOpBySearch(queryStr string) SubsetOperation {
	return SubsetOperation{operation: "search", string: queryStr}
}

// NewSubsetOpByBacklinks returns an operation that matches all messages linking
// to the given message ref via any indexed backlink selector (e.g. vote.link).
func NewSubsetOpByBacklinks(ref refs.MessageRef) SubsetOperation {
	return SubsetOperation{operation: "backlinks", root: &ref}
}

// MarshalJSON turns a SubsetOperation into JSON for remote calls.
func (so SubsetOperation) MarshalJSON() ([]byte, error) {
	var m subsetOperationJSONMarshaler

	m.Operation = so.operation
	m.String = so.string
	m.Feed = so.feed
	m.Args = so.args
	m.Root = so.root
	m.TangleName = so.name
	m.Ref = so.ref
	m.TimestampGt = so.timestampGt
	m.TimestampLt = so.timestampLt
	m.Hops = so.hops

	return json.Marshal(m)
}

// UnmarshalJSON turns JSON into a SubsetOperation and validates it.
func (so *SubsetOperation) UnmarshalJSON(input []byte) error {
	// TODO: restrict length to something reasonable
	if n := len(input); n > 4*1024 {
		return fmt.Errorf("subset query is too long (%d)", n)
	}

	var m subsetOperationJSONMarshaler
	err := json.Unmarshal(input, &m)
	if err != nil {
		return fmt.Errorf("subset query unmarshaling failed: %w", err)
	}

	switch m.Operation {
	case "and", "or":
		so.args = m.Args
	case "not":
		if len(m.Args) != 1 {
			return fmt.Errorf("subset: not operation requires exactly one argument, got %d", len(m.Args))
		}
		so.args = m.Args
	case "type":
		so.string = m.String
	case "channel":
		if m.String == "" {
			return fmt.Errorf("subset: channel can't be empty")
		}
		so.string = m.String
	case "search":
		so.string = m.String
	case "author":
		if m.Feed == nil {
			return fmt.Errorf("subset: author can't be empty")
		}
		if err := ssb.IsValidFeedFormat(*m.Feed); err != nil {
			return fmt.Errorf("subset: author is invalid feed format: %w", err)
		}
		so.feed = m.Feed
	case "mentions":
		if m.Feed == nil {
			return fmt.Errorf("subset: mentions feed can't be empty")
		}
		so.feed = m.Feed
	case "tangle":
		if m.Root == nil {
			return fmt.Errorf("subset: tangle root can't be empty")
		}
		so.root = m.Root
		so.name = m.TangleName
	case "backlinks":
		if m.Root == nil {
			return fmt.Errorf("subset: backlinks ref can't be empty")
		}
		so.root = m.Root
	case "isRoot":
		// no arguments needed
	case "hasBlob":
		if m.Ref == "" {
			return fmt.Errorf("subset: hasBlob ref can't be empty")
		}
		so.ref = m.Ref
	case "timestamp":
		so.timestampGt = m.TimestampGt
		so.timestampLt = m.TimestampLt
	case "followedBy", "blockedBy", "friendsBlocks":
		if m.Feed == nil {
			return fmt.Errorf("subset: %s requires a feed", m.Operation)
		}
		so.feed = m.Feed
	case "hops":
		if m.Feed == nil {
			return fmt.Errorf("subset: hops requires a feed")
		}
		so.feed = m.Feed
		so.hops = m.Hops
	default:
		return fmt.Errorf("unhandled subset operation: %q", m.Operation)
	}

	so.operation = m.Operation

	return nil
}

// a helper for converting a SubsetOperation to JSON
type subsetOperationJSONMarshaler struct {
	Operation string `json:"op"`

	Args   []SubsetOperation `json:"args,omitempty"`
	String string            `json:"string,omitempty"`
	Feed   *refs.FeedRef     `json:"feed,omitempty"`

	Root       *refs.MessageRef `json:"root,omitempty"`
	TangleName string           `json:"tangle_name,omitempty"`

	Ref         string `json:"ref,omitempty"`
	TimestampGt int64  `json:"gt,omitempty"`
	TimestampLt int64  `json:"lt,omitempty"`
	Hops        int    `json:"hops,omitempty"`
}
