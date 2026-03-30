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

type followsSrc struct {
	self refs.FeedRef

	log log.Logger

	builder graph.Builder
}

func (h followsSrc) HandleSource(ctx context.Context, req *muxrpc.Request, snk *muxrpc.ByteSink) error {
	type argT struct {
		Who refs.FeedRef `json:"who"`
	}
	var args []argT
	if err := json.Unmarshal(req.RawArgs, &args); err != nil {
		return fmt.Errorf("invalid argument on follows call: %w", err)
	}

	var who refs.FeedRef
	if len(args) != 1 {
		who = h.self
	} else {
		who = args[0].Who
	}

	set, err := h.builder.Follows(who)
	if err != nil {
		return fmt.Errorf("follows: failed to get follows set: %w", err)
	}

	lst, err := set.List()
	if err != nil {
		return fmt.Errorf("follows: failed to list follows set: %w", err)
	}

	snk.SetEncoding(muxrpc.TypeJSON)
	enc := json.NewEncoder(snk)

	for i, v := range lst {
		if err := enc.Encode(v); err != nil {
			return fmt.Errorf("follows: failed to send item %d: %w", i, err)
		}
	}

	return snk.Close()
}
