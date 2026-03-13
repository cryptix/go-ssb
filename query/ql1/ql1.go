// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

// Package ql1 provides a fluent DSL for building QL-1 subset queries.
//
// Instead of the verbose constructor style:
//
//	query.NewSubsetAndCombination(
//	    query.NewSubsetOpByType("post"),
//	    query.NewSubsetOpByAuthor(feed),
//	)
//
// You can write:
//
//	ql.And(ql.Type("post"), ql.Author(feed))
//
// Queries carry options (descending, limit, keys) via fluent methods:
//
//	ql.Type("post").Descending().Limit(20)
//
// Query is a value type — option methods return new copies, so partial
// queries can be safely reused:
//
//	posts := ql.Type("post")
//	recent := posts.Descending().Limit(10)
//	byAlice := ql.And(posts, ql.Author(alice))
package ql1

import (
	refs "github.com/ssbc/go-ssb-refs"
	"github.com/ssbc/go-ssb/query"
)

// Query bundles a SubsetOperation with SubsetOptions.
type Query struct {
	op   query.SubsetOperation
	opts query.SubsetOptions
}

// --- leaf operations ---

// Type matches messages of the given type (e.g. "post", "about", "contact").
func Type(t string) Query {
	return Query{op: query.NewSubsetOpByType(t)}
}

// Author matches messages from a specific feed.
func Author(f refs.FeedRef) Query {
	return Query{op: query.NewSubsetOpByAuthor(f)}
}

// Channel matches messages posted to a channel (hashtag).
func Channel(ch string) Query {
	return Query{op: query.NewSubsetOpByChannel(ch)}
}

// Tangle matches messages belonging to a thread (v1 tangle).
func Tangle(root refs.MessageRef) Query {
	return Query{op: query.NewSubsetOpByTangle(root)}
}

// TangleV2 matches messages belonging to a named v2 tangle.
func TangleV2(name string, root refs.MessageRef) Query {
	return Query{op: query.NewSubsetOpByTangleV2(name, root)}
}

// Mention matches messages that mention a specific feed.
func Mention(f refs.FeedRef) Query {
	return Query{op: query.NewSubsetOpByMention(f)}
}

// HasBlob matches messages referencing a specific blob.
func HasBlob(b refs.BlobRef) Query {
	return Query{op: query.NewSubsetOpHasBlob(b)}
}

// IsRoot matches only root posts (no content.root field).
func IsRoot() Query {
	return Query{op: query.NewSubsetOpIsRoot()}
}

// Search performs a full-text search.
func Search(q string) Query {
	return Query{op: query.NewSubsetOpBySearch(q)}
}

// Timestamp matches messages within a time range (unix milliseconds).
// Use 0 to leave a bound open.
func Timestamp(gt, lt int64) Query {
	return Query{op: query.NewSubsetOpByTimestamp(gt, lt)}
}

// After matches messages after the given unix timestamp (milliseconds).
func After(gt int64) Query {
	return Timestamp(gt, 0)
}

// Before matches messages before the given unix timestamp (milliseconds).
func Before(lt int64) Query {
	return Timestamp(0, lt)
}

// --- graph operations ---

// FollowedBy matches messages from feeds followed by the given feed.
func FollowedBy(who refs.FeedRef) Query {
	return Query{op: query.NewSubsetOpFollowedBy(who)}
}

// BlockedBy matches messages from feeds blocked by the given feed.
func BlockedBy(who refs.FeedRef) Query {
	return Query{op: query.NewSubsetOpBlockedBy(who)}
}

// InHops matches messages from feeds within N hops of the given feed.
func InHops(who refs.FeedRef, hops int) Query {
	return Query{op: query.NewSubsetOpInHops(who, hops)}
}

// FriendsBlocks matches messages from feeds blocked by any friend of the given feed.
func FriendsBlocks(who refs.FeedRef) Query {
	return Query{op: query.NewSubsetOpFriendsBlocks(who)}
}

// --- multi-value convenience ---

// Authors matches messages from any of the given feeds.
func Authors(feeds ...refs.FeedRef) Query {
	return Query{op: query.NewSubsetOpByAuthors(feeds...)}
}

// Types matches messages of any of the given types.
func Types(types ...string) Query {
	return Query{op: query.NewSubsetOpByTypes(types...)}
}

// --- combinators ---

// And combines queries so all must match.
func And(qs ...Query) Query {
	ops := make([]query.SubsetOperation, len(qs))
	for i, q := range qs {
		ops[i] = q.op
	}
	return Query{op: query.NewSubsetAndCombination(ops...)}
}

// Or combines queries so any can match.
func Or(qs ...Query) Query {
	ops := make([]query.SubsetOperation, len(qs))
	for i, q := range qs {
		ops[i] = q.op
	}
	return Query{op: query.NewSubsetOrCombination(ops...)}
}

// Not inverts a query.
func Not(q Query) Query {
	return Query{op: query.NewSubsetNotCombination(q.op)}
}

// --- option methods (return new Query, safe to reuse originals) ---

// Descending returns results newest-first.
func (q Query) Descending() Query {
	q.opts.Descending = true
	return q
}

// Limit caps the number of results returned.
func (q Query) Limit(n int) Query {
	q.opts.PageLimit = n
	return q
}

// WithKeys includes message keys in results (default on the server when omitted).
func (q Query) WithKeys() Query {
	q.opts.Keys = true
	return q
}

// --- extraction ---

// Operation returns the underlying SubsetOperation for use with lower-level APIs.
func (q Query) Operation() query.SubsetOperation {
	return q.op
}

// Options returns a SubsetOptions pointer if any options were set, nil otherwise.
func (q Query) Options() *query.SubsetOptions {
	empty := query.SubsetOptions{}
	if q.opts == empty {
		return nil
	}
	return &q.opts
}
