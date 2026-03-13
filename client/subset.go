// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package client

import (
	"fmt"

	"github.com/ssbc/go-muxrpc/v3"
	refs "github.com/ssbc/go-ssb-refs"

	"github.com/ssbc/go-ssb/query"
	"github.com/ssbc/go-ssb/query/ql1"
)

// Query executes a ql1.Query against partialReplication.getSubset.
//
//	src, err := c.Query(ql.And(
//	    ql.Type("post"),
//	    ql.FollowedBy(me),
//	).Descending().Limit(50))
func (c Client) Query(q ql1.Query) (*muxrpc.ByteSource, error) {
	return c.GetSubset(q.Operation(), q.Options())
}

// GetSubset calls partialReplication.getSubset with the given query operation
// and options. If opts is nil, server defaults are used.
func (c Client) GetSubset(op query.SubsetOperation, opts *query.SubsetOptions) (*muxrpc.ByteSource, error) {
	var args []interface{}
	args = append(args, op)
	if opts != nil {
		args = append(args, *opts)
	}

	src, err := c.Source(c.rootCtx, muxrpc.TypeJSON, muxrpc.Method{"partialReplication", "getSubset"}, args...)
	if err != nil {
		return nil, fmt.Errorf("ssbClient: getSubset failed: %w", err)
	}
	return src, nil
}

// SubsetByType queries messages of the given type (e.g. "post", "about", "contact").
func (c Client) SubsetByType(msgType string, opts *query.SubsetOptions) (*muxrpc.ByteSource, error) {
	return c.GetSubset(query.NewSubsetOpByType(msgType), opts)
}

// SubsetByAuthor queries messages from a specific author.
func (c Client) SubsetByAuthor(author refs.FeedRef, opts *query.SubsetOptions) (*muxrpc.ByteSource, error) {
	return c.GetSubset(query.NewSubsetOpByAuthor(author), opts)
}

// SubsetByChannel queries messages posted to a specific channel (hashtag).
func (c Client) SubsetByChannel(channel string, opts *query.SubsetOptions) (*muxrpc.ByteSource, error) {
	return c.GetSubset(query.NewSubsetOpByChannel(channel), opts)
}

// SubsetByTangle queries messages belonging to a thread (v1 tangle).
func (c Client) SubsetByTangle(root refs.MessageRef, opts *query.SubsetOptions) (*muxrpc.ByteSource, error) {
	return c.GetSubset(query.NewSubsetOpByTangle(root), opts)
}

// SubsetByMention queries messages that mention a specific feed.
func (c Client) SubsetByMention(who refs.FeedRef, opts *query.SubsetOptions) (*muxrpc.ByteSource, error) {
	return c.GetSubset(query.NewSubsetOpByMention(who), opts)
}

// SubsetSearch performs a full-text search query.
func (c Client) SubsetSearch(queryStr string, opts *query.SubsetOptions) (*muxrpc.ByteSource, error) {
	return c.GetSubset(query.NewSubsetOpBySearch(queryStr), opts)
}

// SubsetFollowedBy queries messages from feeds followed by the given feed.
// This is useful for building a timeline view.
func (c Client) SubsetFollowedBy(who refs.FeedRef, opts *query.SubsetOptions) (*muxrpc.ByteSource, error) {
	return c.GetSubset(query.NewSubsetOpFollowedBy(who), opts)
}

// SubsetTimeline is a convenience method that returns posts from feeds followed
// by the given feed, excluding blocked feeds. Results are returned newest-first.
func (c Client) SubsetTimeline(who refs.FeedRef, limit int) (*muxrpc.ByteSource, error) {
	op := query.NewSubsetAndCombination(
		query.NewSubsetOpFollowedBy(who),
		query.NewSubsetOpByType("post"),
		query.NewSubsetNotCombination(query.NewSubsetOpBlockedBy(who)),
	)
	opts := &query.SubsetOptions{
		Keys:       true,
		Descending: true,
		PageLimit:  limit,
	}
	return c.GetSubset(op, opts)
}

// SubsetThreadReplies returns a thread's root post and all replies, oldest-first.
func (c Client) SubsetThreadReplies(root refs.MessageRef, limit int) (*muxrpc.ByteSource, error) {
	op := query.NewSubsetOpByTangle(root)
	opts := &query.SubsetOptions{
		Keys:      true,
		PageLimit: limit,
	}
	return c.GetSubset(op, opts)
}
