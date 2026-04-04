// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package ssb

import (
	"encoding/json"
	"errors"
	"fmt"

	refs "github.com/ssbc/go-ssb-refs"
)

var ErrShuttingDown = fmt.Errorf("ssb: shutting down now") // this is fine

type ErrOutOfReach struct {
	Dist int
	Max  int
}

func (e ErrOutOfReach) Error() string {
	return fmt.Sprintf("ssb/graph: peer not in reach. d:%d, max:%d", e.Dist, e.Max)
}

// IsMessageUnusable returns true if the error indicates the message
// cannot be processed (wrong type, malformed, or invalid JSON).
func IsMessageUnusable(err error) bool {
	var ewt ErrWrongType
	if errors.As(err, &ewt) {
		return true
	}

	var emm ErrMalformedMsg
	if errors.As(err, &emm) {
		return true
	}

	var se *json.SyntaxError
	if errors.As(err, &se) {
		return true
	}

	return false
}

// ErrMalformedMsg indicates a message that could not be parsed.
type ErrMalformedMsg struct {
	reason string
	m      map[string]interface{}
}

func (emm ErrMalformedMsg) Error() string {
	s := "ErrMalformedMsg: " + emm.reason
	if emm.m != nil {
		s += fmt.Sprintf(" %+v", emm.m)
	}
	return s
}

// ErrMalfromedMsg is a deprecated alias for ErrMalformedMsg.
//
// Deprecated: Use ErrMalformedMsg instead.
type ErrMalfromedMsg = ErrMalformedMsg

type ErrWrongType struct {
	has, want string
}

func (ewt ErrWrongType) Error() string {
	return fmt.Sprintf("ErrWrongType: want: %s has: %s", ewt.want, ewt.has)
}

// ErrUnsupportedFormat indicates an unsupported message format.
var ErrUnsupportedFormat = fmt.Errorf("ssb: unsupported format")

// ErrUnuspportedFormat is a deprecated alias for ErrUnsupportedFormat.
//
// Deprecated: Use ErrUnsupportedFormat instead.
var ErrUnuspportedFormat = ErrUnsupportedFormat

// ErrWrongSequence is returned if there is a glitch on the current
// sequence number on the feed between in the offsetlog and the logical entry on the feed
type ErrWrongSequence struct {
	Ref             refs.FeedRef
	Logical, Stored int64
}

func (e ErrWrongSequence) Error() string {
	return fmt.Sprintf("ssb/consistency error: message sequence missmatch for feed %s Stored:%d Logical:%d",
		e.Ref.String(),
		e.Stored,
		e.Logical)
}
