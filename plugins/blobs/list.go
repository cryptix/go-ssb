// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package blobs

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ssbc/go-muxrpc/v3"
	"go.mindeco.de/logging"

	"github.com/ssbc/go-ssb"
)

type listHandler struct {
	bs  ssb.BlobStore
	log logging.Interface
}

func (h listHandler) HandleSource(ctx context.Context, req *muxrpc.Request, snk *muxrpc.ByteSink) error {
	snk.SetEncoding(muxrpc.TypeJSON)
	enc := json.NewEncoder(snk)

	for ref, err := range h.bs.List() {
		if err != nil {
			return fmt.Errorf("error listing blobs: %w", err)
		}
		if err := enc.Encode(ref); err != nil {
			return fmt.Errorf("error encoding blob ref: %w", err)
		}
	}

	return snk.Close()
}
