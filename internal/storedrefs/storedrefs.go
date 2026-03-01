// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

// Package storedrefs provides methods to encode certain types as bytes, as used by the internal storage system.
package storedrefs

import (
	"fmt"

	refs "github.com/ssbc/go-ssb-refs"
	"github.com/ssbc/go-ssb-refs/tfk"
	"github.com/ssbc/margaret/v2/multilog"
)

// Feed returns the key under which this ref is stored in the indexing system
func Feed(r refs.FeedRef) multilog.Addr {
	sr, err := tfk.FeedFromRef(r)
	if err != nil {
		panic(fmt.Errorf("failed to make stored feed ref: %w", err))
	}

	b, err := sr.MarshalBinary()
	if err != nil {
		panic(fmt.Errorf("error while marshalling stored feed ref: %w", err))
	}
	return multilog.Addr(b)
}

// Message returns the key under which this ref is stored in the indexing system
func Message(r refs.MessageRef) multilog.Addr {
	sr, err := tfk.MessageFromRef(r)
	if err != nil {
		panic(fmt.Errorf("failed to make stored message ref: %w", err))
	}

	b, err := sr.MarshalBinary()
	if err != nil {
		panic(fmt.Errorf("error while marshalling stored message ref: %w", err))
	}
	return multilog.Addr(b)
}

// TangleV1 show how we encode v1 (nameless) tangles for the storage layer
func TangleV1(r refs.MessageRef) multilog.Addr {
	var addr = make([]byte, 3+32)
	copy(addr[0:3], []byte("v1:"))
	r.CopyHashTo(addr[3:])
	return multilog.Addr(addr)
}

// TangleV2 show how we encode v2 (named) tangles for the storage layer
func TangleV2(name string, r refs.MessageRef) multilog.Addr {
	var addr = make([]byte, 4+32+len(name))
	copy(addr, []byte("v2:"+name+":"))
	r.CopyHashTo(addr[4+len(name):])
	return multilog.Addr(addr)
}
