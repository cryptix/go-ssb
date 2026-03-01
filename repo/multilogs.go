// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package repo

import (
	"fmt"
	"os"

	"github.com/ssbc/margaret/v2/multilog/roaring"
	"github.com/ssbc/margaret/v2/multilog/roaring/fs"
)

const PrefixMultiLog = "sublogs"

// OpenFileSystemMultiLog opens a roaring bitmap multilog at the standard sublogs path.
// Sublogs store int64 sequence numbers referencing the root log.
func OpenFileSystemMultiLog(r Interface, name string) (*roaring.MultiLog, error) {
	dbPath := r.GetPath(PrefixMultiLog, name, "fs-bitmaps")
	err := os.MkdirAll(dbPath, 0700)
	if err != nil {
		return nil, fmt.Errorf("mlog/roaring: mkdir error for %q: %w", dbPath, err)
	}

	mlog := fs.NewMultiLog(dbPath)
	return mlog, nil
}

// OpenBitmapMultiLogAt opens a roaring bitmap multilog at an arbitrary path.
func OpenBitmapMultiLogAt(path string) (*roaring.MultiLog, error) {
	err := os.MkdirAll(path, 0700)
	if err != nil {
		return nil, fmt.Errorf("mlog/roaring: mkdir error for %q: %w", path, err)
	}
	return fs.NewMultiLog(path), nil
}
