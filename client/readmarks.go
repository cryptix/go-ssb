// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package client

import (
	"fmt"

	"github.com/ssbc/go-muxrpc/v3"
)

// ReadMark represents a read position marker for a stream.
type ReadMark struct {
	StreamType string `json:"stream_type"`
	StreamID   string `json:"stream_id"`
	Sequence   int64  `json:"sequence"`
}

// ReadMarkGetResult is returned by ReadMarkGet.
type ReadMarkGetResult struct {
	StreamType string `json:"stream_type"`
	StreamID   string `json:"stream_id"`
	Sequence   int64  `json:"sequence"`
	Found      bool   `json:"found"`
}

// ReadMarkSet stores a read position for a stream. streamType is one of
// "feed", "thread", "channel", or "log". streamID identifies the specific
// stream (a feed ref, message ref, channel name, or "root"). To track
// different sort orders, encode the order into the stream ID (e.g.
// "@ABC...ed25519:claimed"). sequence is the position read up to (inclusive).
func (c Client) ReadMarkSet(streamType, streamID string, sequence int64) error {
	arg := struct {
		StreamType string `json:"stream_type"`
		StreamID   string `json:"stream_id"`
		Sequence   int64  `json:"sequence"`
	}{streamType, streamID, sequence}

	var resp bool
	err := c.Async(c.rootCtx, &resp, muxrpc.TypeJSON, muxrpc.Method{"readmarks", "set"}, arg)
	if err != nil {
		return fmt.Errorf("ssbClient: readmarks.set failed: %w", err)
	}
	return nil
}

// ReadMarkGet retrieves the read position for a stream. Returns the result
// containing the sequence and whether a marker was found.
func (c Client) ReadMarkGet(streamType, streamID string) (ReadMarkGetResult, error) {
	arg := struct {
		StreamType string `json:"stream_type"`
		StreamID   string `json:"stream_id"`
	}{streamType, streamID}

	var resp ReadMarkGetResult
	err := c.Async(c.rootCtx, &resp, muxrpc.TypeJSON, muxrpc.Method{"readmarks", "get"}, arg)
	if err != nil {
		return ReadMarkGetResult{}, fmt.Errorf("ssbClient: readmarks.get failed: %w", err)
	}
	return resp, nil
}

// ReadMarkList returns all stored read markers, optionally filtered by stream
// type. Pass "" for streamType to get all markers.
func (c Client) ReadMarkList(streamType string) ([]ReadMark, error) {
	var resp []ReadMark

	if streamType != "" {
		arg := struct {
			StreamType string `json:"stream_type"`
		}{streamType}
		err := c.Async(c.rootCtx, &resp, muxrpc.TypeJSON, muxrpc.Method{"readmarks", "list"}, arg)
		if err != nil {
			return nil, fmt.Errorf("ssbClient: readmarks.list failed: %w", err)
		}
	} else {
		err := c.Async(c.rootCtx, &resp, muxrpc.TypeJSON, muxrpc.Method{"readmarks", "list"})
		if err != nil {
			return nil, fmt.Errorf("ssbClient: readmarks.list failed: %w", err)
		}
	}
	return resp, nil
}

// ReadMarkDelete removes the read marker for a stream.
func (c Client) ReadMarkDelete(streamType, streamID string) error {
	arg := struct {
		StreamType string `json:"stream_type"`
		StreamID   string `json:"stream_id"`
	}{streamType, streamID}

	var resp bool
	err := c.Async(c.rootCtx, &resp, muxrpc.TypeJSON, muxrpc.Method{"readmarks", "delete"}, arg)
	if err != nil {
		return fmt.Errorf("ssbClient: readmarks.delete failed: %w", err)
	}
	return nil
}
