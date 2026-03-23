// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

// Package get provides muxrpc handlers for message retrieval by reference.
// It supports single message lookup (get) and batch retrieval (get.many).
package get

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ssbc/go-muxrpc/v3"
	"github.com/ssbc/go-muxrpc/v3/typemux"
	"github.com/ssbc/go-ssb"
	refs "github.com/ssbc/go-ssb-refs"
	"github.com/ssbc/go-ssb/private"
	"go.mindeco.de/log"
)

type plugin struct {
	h muxrpc.Handler
}

func (p plugin) Name() string            { return "get" }
func (p plugin) Method() muxrpc.Method   { return muxrpc.Method{"get"} }
func (p plugin) Handler() muxrpc.Handler { return p.h }

// New creates a plugin that handles both "get" (single message) and
// "get.many" (batch message retrieval) muxrpc methods.
func New(g ssb.Getter, unboxer *private.Manager) ssb.Plugin {
	mux := typemux.New(log.NewNopLogger())

	mux.RegisterAsync(muxrpc.Method{"get"}, singleHandler{
		get:     g,
		unboxer: unboxer,
	})

	mux.RegisterAsync(muxrpc.Method{"get", "many"}, manyHandler{
		get:     g,
		unboxer: unboxer,
	})

	return plugin{h: &mux}
}

// Option represents the arguments for a single get call.
type Option struct {
	ID      refs.MessageRef `json:"id"`
	Private bool            `json:"private"`
}

// singleHandler handles the "get" muxrpc method for retrieving a single message.
type singleHandler struct {
	get     ssb.Getter
	unboxer *private.Manager
}

// HandleAsync retrieves a single message by its reference.
// It accepts either a bare message ref string or an object with id and private fields.
func (h singleHandler) HandleAsync(ctx context.Context, req *muxrpc.Request) (interface{}, error) {
	var args []json.RawMessage
	err := json.Unmarshal(req.RawArgs, &args)
	if err != nil {
		return nil, err
	}

	if n := len(args); n < 1 {
		return nil, fmt.Errorf("invalid argument count: wanted 1 got %d", n)
	}

	var o Option
	optErr := json.Unmarshal(args[0], &o)
	if optErr != nil {
		var asString refs.MessageRef
		strErr := json.Unmarshal(args[0], &asString)
		if strErr != nil {
			return nil, fmt.Errorf("failed to parse argument as object (%s) and as string (%s)", optErr, strErr)
		}
		o.ID = asString
	}

	msg, err := h.get.Get(o.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to load message: %w", err)
	}

	kv := buildKeyValue(msg)

	if o.Private {
		decrypted, err := tryDecrypt(h.unboxer, msg, &kv)
		if err != nil {
			return nil, err
		}
		_ = decrypted
	}

	return kv, nil
}

// buildKeyValue constructs a KeyValueRaw from a message.
func buildKeyValue(msg refs.Message) refs.KeyValueRaw {
	var kv refs.KeyValueRaw
	kv.Key_ = msg.Key()
	kv.Value = *msg.ValueContent()
	return kv
}

// tryDecrypt attempts to decrypt a message, updating the KeyValueRaw in place.
// Returns true if decryption succeeded, false if the message was not boxed.
// Returns an error only on actual decryption failures (not for unboxed messages).
func tryDecrypt(unboxer *private.Manager, msg refs.Message, kv *refs.KeyValueRaw) (bool, error) {
	cleartext, err := unboxer.DecryptMessage(msg)
	if err == nil {
		kv.Value.Meta = make(map[string]interface{}, 1)
		kv.Value.Meta["private"] = true
		kv.Value.Content = cleartext
		return true, nil
	}
	if !errors.Is(err, private.ErrNotBoxed) {
		return false, fmt.Errorf("failed to decrypt message: %w", err)
	}
	return false, nil
}
