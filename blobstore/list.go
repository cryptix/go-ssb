// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package blobstore

import (
	"encoding/hex"
	"fmt"
	"iter"
	"os"
	"path/filepath"

	refs "github.com/ssbc/go-ssb-refs"
)

func listBlobs(basePath string) iter.Seq2[refs.BlobRef, error] {
	return func(yield func(refs.BlobRef, error) bool) {
		root, err := os.Open(basePath)
		if err != nil {
			yield(refs.BlobRef{}, fmt.Errorf("error opening blobs directory: %w", err))
			return
		}
		defer root.Close()

		dirs, err := root.Readdir(0)
		if err != nil {
			yield(refs.BlobRef{}, fmt.Errorf("error reading blobs directory: %w", err))
			return
		}

		for _, d := range dirs {
			dir, err := os.Open(filepath.Join(basePath, d.Name()))
			if err != nil {
				if !yield(refs.BlobRef{}, fmt.Errorf("error opening subdirectory: %w", err)) {
					return
				}
				continue
			}

			blobs, err := dir.Readdir(0)
			dir.Close()
			if err != nil {
				if !yield(refs.BlobRef{}, fmt.Errorf("error reading blobs subdirectory: %w", err)) {
					return
				}
				continue
			}

			for _, b := range blobs {
				hexName := d.Name() + b.Name()
				raw, err := hex.DecodeString(hexName)
				if err != nil {
					if !yield(refs.BlobRef{}, fmt.Errorf("error decoding hex file name %q: %w", hexName, err)) {
						return
					}
					continue
				}

				ref, err := refs.NewBlobRefFromBytes(raw, refs.RefAlgoBlobSSB1)
				if err != nil {
					if !yield(refs.BlobRef{}, err) {
						return
					}
					continue
				}

				if !yield(ref, nil) {
					return
				}
			}
		}
	}
}
