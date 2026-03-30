// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package get

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ssbc/go-muxrpc/v3"
	"github.com/ssbc/go-ssb"
	refs "github.com/ssbc/go-ssb-refs"
	"github.com/ssbc/go-ssb/private"
)

// ManyOption represents the arguments for a batch get call.
type ManyOption struct {
	IDs     []refs.MessageRef `json:"ids"`
	Private bool              `json:"private"`
}

// manyHandler handles the "get.many" muxrpc method for batch message retrieval.
type manyHandler struct {
	get     ssb.Getter
	unboxer *private.Manager
}

// HandleAsync retrieves multiple messages by their references in a single call.
// Args format: [{ids: ["%ref1", "%ref2", ...], private: bool}]
// Returns an array where each element is a KeyValueRaw message or null if not found.
func (h manyHandler) HandleAsync(ctx context.Context, req *muxrpc.Request) (interface{}, error) {
	opts, err := parseManyArgs(req.RawArgs)
	if err != nil {
		return nil, err
	}

	results := make([]*refs.KeyValueRaw, len(opts.IDs))
	for i, id := range opts.IDs {
		kv, err := h.getOne(id, opts.Private)
		if err != nil {
			// individual lookup failures yield null, not a full error
			continue
		}
		results[i] = kv
	}

	return results, nil
}

// parseManyArgs extracts ManyOption from the raw muxrpc arguments.
func parseManyArgs(raw json.RawMessage) (ManyOption, error) {
	var args []json.RawMessage
	if err := json.Unmarshal(raw, &args); err != nil {
		return ManyOption{}, fmt.Errorf("failed to parse arguments: %w", err)
	}

	if len(args) < 1 {
		return ManyOption{}, fmt.Errorf("invalid argument count: wanted 1 got %d", len(args))
	}

	var opts ManyOption
	if err := json.Unmarshal(args[0], &opts); err != nil {
		return ManyOption{}, fmt.Errorf("failed to parse options: %w", err)
	}

	if len(opts.IDs) == 0 {
		return ManyOption{}, fmt.Errorf("ids array must not be empty")
	}

	const maxIDs = 500
	if len(opts.IDs) > maxIDs {
		return ManyOption{}, fmt.Errorf("ids array exceeds maximum of %d", maxIDs)
	}

	return opts, nil
}

// getOne retrieves a single message, optionally decrypting it.
func (h manyHandler) getOne(id refs.MessageRef, priv bool) (*refs.KeyValueRaw, error) {
	msg, err := h.get.Get(id)
	if err != nil {
		return nil, fmt.Errorf("failed to load message %s: %w", id.String(), err)
	}

	kv := buildKeyValue(msg)

	if priv {
		if _, err := tryDecrypt(h.unboxer, msg, &kv); err != nil {
			return nil, err
		}
	}

	return &kv, nil
}
