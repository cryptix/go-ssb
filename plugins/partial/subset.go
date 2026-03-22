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
	"github.com/ssbc/go-ssb/repo"
	margaret "github.com/ssbc/margaret/v2"
)

type getSubsetHandler struct {
	queryPlaner *query.SubsetPlaner

	rxLog       margaret.Log[*multimsg.MultiMessage]
	seqResolver *repo.SequenceResolver
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

	resulting, err := h.queryPlaner.QuerySubsetBitmap(ctx, arg)
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

	// When a SequenceResolver is available, sort results by claimed timestamp
	// instead of receive log sequence. This gives causal ordering (newest-authored
	// first) rather than receive ordering (last-replicated first).
	if h.seqResolver != nil {
		sorted, err := h.seqResolver.SortAndFilterBitmap(
			resulting,
			repo.SortByClaimed,
			func(int64) bool { return true }, // no timestamp filtering
			opts.Descending,
		)
		if err != nil {
			return fmt.Errorf("failed to sort results by claimed timestamp: %w", err)
		}

		// Cursor: afterSeq is an rxSeq from a previous page. Since results are
		// now sorted by claimed timestamp (not rxSeq), we find its position in
		// the sorted result and start emitting after that point.
		//
		// If the exact rxSeq isn't found (bitmap changed between pages), fall
		// back to using the claimed timestamp of afterSeq to find the closest
		// position: skip all entries whose timestamp is "before" the cursor in
		// the current sort direction.
		startIdx := 0
		if afterSeq > 0 {
			found := false
			for i, entry := range sorted {
				if entry.Seq == afterSeq {
					startIdx = i + 1
					found = true
					break
				}
			}
			if !found {
				// Fallback: use the claimed timestamp of afterSeq to find position.
				cursorTs, ok := h.seqResolver.GetClaimedMillis(afterSeq)
				if ok {
					// Convert to seconds to match the domain used by SortAndFilterBitmap.
					cursorTsSec := cursorTs / 1000
					for i, entry := range sorted {
						if opts.Descending {
							// Descending: skip entries with ts >= cursor
							if entry.By < cursorTsSec {
								startIdx = i
								break
							}
						} else {
							// Ascending: skip entries with ts <= cursor
							if entry.By > cursorTsSec {
								startIdx = i
								break
							}
						}
					}
				}
			}
		}

		count := 0
		for i := startIdx; i < len(sorted); i++ {
			if err := h.emitMessage(sorted[i].Seq, opts.Keys, &buf, enc, sink); err != nil {
				return err
			}
			count++
			if pageLimit > 0 && count >= pageLimit {
				break
			}
		}

		sink.Close()
		return nil
	}

	// Fallback: no SequenceResolver, walk bitmap by receive log sequence.
	if opts.Descending {
		vals := resulting.ToArray()
		count := 0
		for i := len(vals) - 1; i >= 0; i-- {
			v := vals[i]

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
		it := resulting.NewIterator()
		count := 0
		for i := 0; i < resulting.GetCardinality(); i++ {
			v := it.Next()

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
