// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package partial

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/ssbc/go-muxrpc/v3"
	refs "github.com/ssbc/go-ssb-refs"
	"github.com/ssbc/go-ssb/message/multimsg"
	"github.com/ssbc/go-ssb/query"
	margaret "github.com/ssbc/margaret/v2"
)

type getSubsetHandler struct {
	queryPlaner *query.SubsetPlaner

	rxLog margaret.Log[*multimsg.MultiMessage]
}

// kvWithSeq wraps KeyValueRaw with the receive log sequence for cursor-based pagination.
type kvWithSeq struct {
	refs.KeyValueRaw
	RxSeq int64 `json:"rxSeq"`
}

func (h getSubsetHandler) HandleSource(ctx context.Context, req *muxrpc.Request, sink *muxrpc.ByteSink) error {

	var (
		args []json.RawMessage
		arg  query.SubsetOperation
		opts query.SubsetOptions
	)

	err := json.Unmarshal(req.RawArgs, &args)
	if err != nil {
		return err
	}
	nArgs := len(args)
	if nArgs < 1 {
		return fmt.Errorf("expected one arguemnt got %d", nArgs)
	}

	err = json.Unmarshal(args[0], &arg)
	if err != nil {
		return err
	}

	if nArgs > 1 {
		err = json.Unmarshal(args[1], &opts)
		if err != nil {
			return err
		}
	} else { // set defaults
		opts.PageLimit = -1
		opts.Keys = true
	}

	resulting, err := h.queryPlaner.QuerySubsetBitmap(arg)
	if err != nil {
		return fmt.Errorf("failed to send query result to peer: %w", err)
	}

	if resulting == nil {
		sink.Close()
		return nil
	}

	sink.SetEncoding(muxrpc.TypeJSON)

	var (
		buf bytes.Buffer
		enc = json.NewEncoder(&buf)
	)

	pageLimit := opts.PageLimit
	afterSeq := opts.AfterSeq

	if opts.Descending {
		// Descending: materialize and walk from the end.
		// The iterator only goes low-to-high, so we reverse the array.
		vals := resulting.ToArray()
		count := 0
		for i := len(vals) - 1; i >= 0; i-- {
			v := vals[i]

			// Cursor: skip entries with seq >= afterSeq (descending)
			if afterSeq > 0 && int64(v) >= afterSeq {
				continue
			}

			if err := h.emitMessage(int64(v), opts.Keys, &buf, enc, sink); err != nil {
				return err
			}

			count++
			if pageLimit > 0 && count >= pageLimit {
				break
			}
		}
	} else {
		// Ascending: use bitmap iterator to avoid materializing the entire bitmap.
		it := resulting.NewIterator()
		count := 0
		for i := 0; i < resulting.GetCardinality(); i++ {
			v := it.Next()

			// Cursor: skip entries with seq <= afterSeq (ascending)
			if afterSeq > 0 && int64(v) <= afterSeq {
				continue
			}

			if err := h.emitMessage(int64(v), opts.Keys, &buf, enc, sink); err != nil {
				return err
			}

			count++
			if pageLimit > 0 && count >= pageLimit {
				break
			}
		}
	}

	sink.Close()
	return nil
}

// emitMessage fetches a message from the receive log at the given sequence and writes it to the sink.
func (h getSubsetHandler) emitMessage(rxSeq int64, keys bool, buf *bytes.Buffer, enc *json.Encoder, sink *muxrpc.ByteSink) error {
	mm, err := h.rxLog.Get(rxSeq)
	if err != nil {
		return fmt.Errorf("failed to get message at seq %d: %w", rxSeq, err)
	}

	if mm.Message == nil {
		return nil
	}
	msg := mm.Message

	if keys {
		buf.Reset()

		kv := kvWithSeq{
			RxSeq: rxSeq,
		}
		kv.Key_ = msg.Key()
		kv.Value = *msg.ValueContent()

		if err := enc.Encode(kv); err != nil {
			return fmt.Errorf("failed to encode json: %w", err)
		}

		if _, err = buf.WriteTo(sink); err != nil {
			return fmt.Errorf("failed to send json data: %w", err)
		}
	} else {
		_, err = sink.Write(msg.ValueContentJSON())
		if err != nil {
			return fmt.Errorf("failed to send json data: %w", err)
		}
	}

	return nil
}
