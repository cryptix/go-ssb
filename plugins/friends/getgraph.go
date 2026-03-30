// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package friends

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ssbc/go-muxrpc/v3"
	refs "github.com/ssbc/go-ssb-refs"
	"github.com/ssbc/go-ssb/graph"
	"go.mindeco.de/log"
)

// feedRelationship describes the relationship between a source feed and a target feed.
type feedRelationship struct {
	Following bool `json:"following"`
	Blocking  bool `json:"blocking"`
	FollowsMe bool `json:"followsMe"`
}

type getGraphH struct {
	self refs.FeedRef

	log log.Logger

	builder graph.Builder
}

func (h getGraphH) HandleAsync(ctx context.Context, req *muxrpc.Request) (interface{}, error) {
	type argT struct {
		Source refs.FeedRef `json:"source"`
	}
	var args []argT
	if err := json.Unmarshal(req.RawArgs, &args); err != nil {
		return nil, fmt.Errorf("invalid argument on getGraph call: %w", err)
	}

	var source refs.FeedRef
	if len(args) != 1 {
		source = h.self
	} else {
		source = args[0].Source
	}

	g, err := h.builder.Build()
	if err != nil {
		return nil, fmt.Errorf("getGraph: failed to build graph: %w", err)
	}

	followsSet, err := h.builder.Follows(source)
	if err != nil {
		return nil, fmt.Errorf("getGraph: failed to get follows: %w", err)
	}

	result := make(map[string]feedRelationship)

	// Add all followed feeds.
	followsList, err := followsSet.List()
	if err != nil {
		return nil, fmt.Errorf("getGraph: failed to list follows: %w", err)
	}
	for _, target := range followsList {
		result[target.String()] = feedRelationship{
			Following: true,
			Blocking:  g.Blocks(source, target),
			FollowsMe: g.Follows(target, source),
		}
	}

	// Add all blocked feeds that are not already in the result.
	blockedSet := g.BlockedList(source)
	blockedList, err := blockedSet.List()
	if err != nil {
		return nil, fmt.Errorf("getGraph: failed to list blocked: %w", err)
	}
	for _, target := range blockedList {
		key := target.String()
		if _, exists := result[key]; exists {
			continue
		}
		result[key] = feedRelationship{
			Following: false,
			Blocking:  true,
			FollowsMe: g.Follows(target, source),
		}
	}

	return result, nil
}
