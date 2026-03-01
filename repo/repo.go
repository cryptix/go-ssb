// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

// Package repo contains utility modules to open offset logs and create different kinds of indexes.
package repo

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/dgraph-io/badger/v3"
	"github.com/ssbc/go-ssb"
	"github.com/ssbc/go-ssb/blobstore"
)

var _ Interface = repo{}

// New creates a new repository value, it opens the keypair and database from basePath if it is already existing
func New(basePath string) Interface {
	return repo{basePath: basePath}
}

type repo struct {
	basePath string
}

func (r repo) GetPath(rel ...string) string {
	return filepath.Join(append([]string{r.basePath}, rel...)...)
}

func OpenBadgerDB(path string) (*badger.DB, error) {
	err := os.MkdirAll(path, 0700)
	if err != nil {
		return nil, fmt.Errorf("OpenBadgerDB: failed to create directory: %w", err)
	}
	opts := badger.DefaultOptions(path)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("OpenBadgerDB: failed to open badger: %w", err)
	}
	return db, nil
}

func OpenBlobStore(r Interface) (ssb.BlobStore, error) {
	bs, err := blobstore.New(r.GetPath("blobs"))
	if err != nil {
		return nil, fmt.Errorf("error opening blob store: %w", err)
	}
	return bs, nil
}
