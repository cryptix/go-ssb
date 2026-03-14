// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package blobs

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ssbc/go-muxrpc/v3"
	refs "github.com/ssbc/go-ssb-refs"
	"github.com/stretchr/testify/require"
	kitlog "go.mindeco.de/log"

	"github.com/ssbc/go-ssb/blobstore"
)

func TestHasHandlerSingle(t *testing.T) {
	r := require.New(t)

	// create a temporary blob store
	tDir := filepath.Join("testrun", t.Name())
	os.RemoveAll(tDir)
	os.MkdirAll(tDir, 0700)
	t.Cleanup(func() { os.RemoveAll(tDir) })

	bs, err := blobstore.New(tDir)
	r.NoError(err)

	log := kitlog.NewLogfmtLogger(os.Stderr)

	h := hasHandler{bs: bs, log: log}

	// Add a blob so we can test both cases
	existingRef, err := bs.Put(strings.NewReader("hello world"))
	r.NoError(err)

	// Test 1: existing blob should return true
	rawArgs, err := json.Marshal(existingRef)
	r.NoError(err)

	req := &muxrpc.Request{RawArgs: rawArgs}
	result, err := h.HandleAsync(nil, req)
	r.NoError(err)
	r.Equal(true, result, "existing blob should return true")

	// Test 2: non-existent blob should return false
	nonExistentRef, err := refs.ParseBlobRef("&0000000000000000000000000000000000000000000=.sha256")
	r.NoError(err)

	rawArgs2, err := json.Marshal(nonExistentRef)
	r.NoError(err)

	req2 := &muxrpc.Request{RawArgs: rawArgs2}
	result2, err := h.HandleAsync(nil, req2)
	r.NoError(err)
	r.Equal(false, result2, "non-existent blob should return false")
}

func TestHasHandlerMultiple(t *testing.T) {
	r := require.New(t)

	// create a temporary blob store
	tDir := filepath.Join("testrun", t.Name())
	os.RemoveAll(tDir)
	os.MkdirAll(tDir, 0700)
	t.Cleanup(func() { os.RemoveAll(tDir) })

	bs, err := blobstore.New(tDir)
	r.NoError(err)

	log := kitlog.NewLogfmtLogger(os.Stderr)

	h := hasHandler{bs: bs, log: log}

	// Add one blob
	existingRef, err := bs.Put(strings.NewReader("existing blob"))
	r.NoError(err)

	// Create a non-existent ref
	nonExistentRef, err := refs.ParseBlobRef("&0000000000000000000000000000000000000000000=.sha256")
	r.NoError(err)

	// Query with both refs: [existing, non-existent]
	rawArgs, err := json.Marshal([]refs.BlobRef{existingRef, nonExistentRef})
	r.NoError(err)

	req := &muxrpc.Request{RawArgs: rawArgs}
	result, err := h.HandleAsync(nil, req)
	r.NoError(err)

	has, ok := result.([]bool)
	r.True(ok, "expected []bool result")
	r.Len(has, 2)
	r.Equal(true, has[0], "existing blob should return true")
	r.Equal(false, has[1], "non-existent blob should return false")
}
