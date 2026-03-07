// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package blobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ssbc/go-muxrpc/v3"
	"go.mindeco.de/logging"

	refs "github.com/ssbc/go-ssb-refs"
)

type rmHandler struct {
	bs  interface{ Delete(refs.BlobRef) error }
	log logging.Interface
}

func (h rmHandler) HandleAsync(ctx context.Context, req *muxrpc.Request) (interface{}, error) {
	var blobRefs []refs.BlobRef

	err := json.Unmarshal(req.RawArgs, &blobRefs)
	if err != nil {
		return nil, fmt.Errorf("error parsing blob reference: %w", err)
	}
	if len(blobRefs) != 1 {
		return nil, errors.New("bad request - expected exactly one blob ref argument")
	}

	err = h.bs.Delete(blobRefs[0])
	if err != nil {
		return nil, fmt.Errorf("error deleting blob: %w", err)
	}

	return true, nil
}
