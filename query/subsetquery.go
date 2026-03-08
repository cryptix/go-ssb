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
	Keys       bool `json:"keys"` // can't omit this falsy value, the JS-stack stack assumes true if it's not there
	Descending bool `json:"descending,omitempty"`
	PageLimit  int  `json:"pageLimit,omitempty"`
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

// MarshalJSON turns a SubsetOperation into JSON for remote calls.
func (so SubsetOperation) MarshalJSON() ([]byte, error) {
	var m subsetOperationJSONMarshaler

	m.Operation = so.operation
	m.String = so.string
	m.Feed = so.feed
	m.Args = so.args
	m.Root = so.root
	m.TangleName = so.name

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
	case "type":
		so.string = m.String
	case "author":
		if m.Feed == nil {
			return fmt.Errorf("subset: author can't be empty")
		}
		if err := ssb.IsValidFeedFormat(*m.Feed); err != nil {
			return fmt.Errorf("subset: author is invalid feed format: %w", err)
		}
		so.feed = m.Feed
	case "tangle":
		if m.Root == nil {
			return fmt.Errorf("subset: tangle root can't be empty")
		}
		so.root = m.Root
		so.name = m.TangleName
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
}
