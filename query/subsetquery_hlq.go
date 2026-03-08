// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package query

import (
	"encoding/json"
	"fmt"

	refs "github.com/ssbc/go-ssb-refs"
)

// SubsetQueryMode defines how authors and types filters are combined
type SubsetQueryMode string

const (
	// SubsetQueryModeAuthorsAndTypes combines author and type filters with AND logic.
	// Matches messages that are by one of the listed authors AND of one of the listed types.
	SubsetQueryModeAuthorsAndTypes SubsetQueryMode = "authors-and-types"

	// SubsetQueryModeAuthorsOnly filters only by authors, ignoring types.
	SubsetQueryModeAuthorsOnly SubsetQueryMode = "authors-only"

	// SubsetQueryModeTypesOnly filters only by types, ignoring authors.
	SubsetQueryModeTypesOnly SubsetQueryMode = "types-only"
)

// SubsetQuery is a higher-level query format inspired by ssb-oasis.
// It provides a simplified interface for the common pattern of filtering
// by a set of authors and/or message types.
type SubsetQuery struct {
	Authors []refs.FeedRef  `json:"authors,omitempty"`
	Types   []string        `json:"types,omitempty"`
	Mode    SubsetQueryMode `json:"mode"`

	// Options for the query
	SubsetOptions
}

// ToOperation converts the high-level SubsetQuery into a SubsetOperation
// tree suitable for execution by the SubsetPlaner.
func (sq SubsetQuery) ToOperation() (SubsetOperation, error) {
	switch sq.Mode {
	case SubsetQueryModeAuthorsAndTypes:
		if len(sq.Authors) == 0 && len(sq.Types) == 0 {
			return SubsetOperation{}, fmt.Errorf("subset query: authors-and-types mode requires at least one author or type")
		}
		return NewSubsetOpByAuthorsAndTypes(sq.Authors, sq.Types), nil

	case SubsetQueryModeAuthorsOnly:
		if len(sq.Authors) == 0 {
			return SubsetOperation{}, fmt.Errorf("subset query: authors-only mode requires at least one author")
		}
		return NewSubsetOpByAuthors(sq.Authors...), nil

	case SubsetQueryModeTypesOnly:
		if len(sq.Types) == 0 {
			return SubsetOperation{}, fmt.Errorf("subset query: types-only mode requires at least one type")
		}
		return NewSubsetOpByTypes(sq.Types...), nil

	default:
		return SubsetOperation{}, fmt.Errorf("subset query: unknown mode %q", sq.Mode)
	}
}

// ParseSubsetQuery parses a JSON-encoded SubsetQuery from raw bytes.
func ParseSubsetQuery(data []byte) (*SubsetQuery, error) {
	var sq SubsetQuery
	if err := json.Unmarshal(data, &sq); err != nil {
		return nil, fmt.Errorf("subset query: failed to parse: %w", err)
	}
	return &sq, nil
}
