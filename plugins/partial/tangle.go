// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package partial

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ssbc/go-ssb"
	"github.com/ssbc/go-ssb/internal/mutil"
	"github.com/ssbc/go-ssb/internal/storedrefs"
	"github.com/ssbc/go-ssb/message/multimsg"

	"github.com/ssbc/go-muxrpc/v2"
	refs "github.com/ssbc/go-ssb-refs"
	margaret "github.com/ssbc/margaret/v2"
	"github.com/ssbc/margaret/v2/multilog/roaring"
)

type getTangleHandler struct {
	rxlog margaret.Log[*multimsg.MultiMessage]

	get   ssb.Getter
	roots *roaring.MultiLog
}

func (h getTangleHandler) HandleAsync(ctx context.Context, req *muxrpc.Request) (interface{}, error) {
	var mrs []refs.MessageRef
	err := json.Unmarshal(req.RawArgs, &mrs)
	if err != nil {
		return nil, err
	}

	if len(mrs) != 1 {
		return nil, fmt.Errorf("no args")
	}
	msg, err := h.get.Get(mrs[0])
	if err != nil {
		return nil, fmt.Errorf("getTangle: root message not found: %w", err)
	}

	vals := []interface{}{
		msg.ValueContentJSON(),
	}

	threadLog, err := h.roots.Get(storedrefs.Message(msg.Key()))
	if err != nil {
		return nil, fmt.Errorf("getTangle: failed to load thread: %w", err)
	}

	resolved := mutil.Indirect(h.rxlog, threadLog)
	qry := resolved.Query()

	for _, mm := range qry.Iter() {
		if mm.Message == nil {
			continue
		}
		vals = append(vals, mm.Message.ValueContentJSON())
	}
	if err := qry.Err(); err != nil {
		return nil, fmt.Errorf("getTangle: failed to read thread msgs: %w", err)
	}

	return vals, nil
}
