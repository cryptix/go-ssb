// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package names

import (
	"context"

	"github.com/ssbc/go-muxrpc/v3"
	"go.mindeco.de/logging"
)

type hGetAllWithImages struct {
	as  aboutStore
	log logging.Interface
}

// HandleAsync returns a map of feed sigil to ProfileEntry with name and image.
func (h hGetAllWithImages) HandleAsync(ctx context.Context, req *muxrpc.Request) (interface{}, error) {
	return h.as.AllWithImages()
}
