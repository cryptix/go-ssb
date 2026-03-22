// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package ssb

import (
	"fmt"

	refs "github.com/ssbc/go-ssb-refs"
)

// ForkProof is evidence that a feed has forked: two different messages
// with the same author and sequence number but different keys.
//
// The Observed frontier establishes the worldview at the time of detection,
// allowing third parties to verify the context of the proof.
type ForkProof struct {
	// Observed is the frontier at the time the fork was detected.
	Observed Frontier

	// Left and Right are the two conflicting messages.
	// They must have the same author and sequence but different keys.
	Left  refs.Message
	Right refs.Message
}

// Validate checks that the fork proof is structurally valid:
// - Both messages have the same author
// - Both messages have the same sequence number
// - The messages have different keys (otherwise it's the same message, not a fork)
//
// Note: This does NOT verify message signatures. Callers should verify
// signatures independently before trusting a fork proof from an untrusted source.
func (fp ForkProof) Validate() error {
	if fp.Left == nil || fp.Right == nil {
		return fmt.Errorf("fork proof: both messages must be non-nil")
	}

	leftAuthor := fp.Left.Author()
	rightAuthor := fp.Right.Author()
	if !leftAuthor.Equal(rightAuthor) {
		return fmt.Errorf("fork proof: authors differ: %s vs %s",
			leftAuthor.String(), rightAuthor.String())
	}

	if fp.Left.Seq() != fp.Right.Seq() {
		return fmt.Errorf("fork proof: sequences differ: %d vs %d",
			fp.Left.Seq(), fp.Right.Seq())
	}

	if fp.Left.Key().Equal(fp.Right.Key()) {
		return fmt.Errorf("fork proof: messages have same key %s (not a fork)",
			fp.Left.Key().String())
	}

	return nil
}

// Author returns the feed that forked (from the Left message).
// Returns an error if the proof hasn't been validated.
func (fp ForkProof) Author() (refs.FeedRef, error) {
	if fp.Left == nil {
		return refs.FeedRef{}, fmt.Errorf("fork proof: left message is nil")
	}
	return fp.Left.Author(), nil
}

// ForkedSeq returns the sequence number at which the fork occurred.
func (fp ForkProof) ForkedSeq() (int64, error) {
	if fp.Left == nil {
		return 0, fmt.Errorf("fork proof: left message is nil")
	}
	return fp.Left.Seq(), nil
}

// String returns a human-readable summary of the fork proof.
func (fp ForkProof) String() string {
	if fp.Left == nil || fp.Right == nil {
		return "ForkProof{invalid: nil message}"
	}
	return fmt.Sprintf("ForkProof{author:%s seq:%d left:%s right:%s frontier:%s}",
		fp.Left.Author().ShortSigil(),
		fp.Left.Seq(),
		fp.Left.Key().ShortSigil(),
		fp.Right.Key().ShortSigil(),
		fp.Observed.String(),
	)
}
