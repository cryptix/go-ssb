// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package indexes

import (
	"fmt"
	"time"

	margaret "github.com/ssbc/margaret/v2"

	"github.com/ssbc/go-ssb/message/multimsg"
	"github.com/ssbc/go-ssb/repo"
)

type Timestamps struct {
	resolver *repo.SequenceResolver
}

func NewTimestampSorter(res *repo.SequenceResolver) *Timestamps {
	return &Timestamps{resolver: res}
}

func (idx *Timestamps) Close() error {
	return idx.resolver.Close()
}

// ProcessEntry indexes a single log entry into the sequence resolver.
func (idx *Timestamps) ProcessEntry(rxSeq int64, mm *multimsg.MultiMessage) error {
	if mm.Message == nil {
		// nulled entry
		err := idx.resolver.Append(rxSeq, 0, time.Now(), time.Now())
		if err != nil {
			return fmt.Errorf("error updating sequence resolver (nulled message): %w", err)
		}
		return nil
	}

	msg := mm.Message
	err := idx.resolver.Append(rxSeq, msg.Seq(), msg.Claimed(), msg.Received())
	if err != nil {
		return fmt.Errorf("error updating sequence resolver: %w", err)
	}
	return nil
}

// LastProcessedSeq returns the last sequence that was processed.
func (idx *Timestamps) LastProcessedSeq() int64 {
	return idx.resolver.Seq() - 1
}

// Index processes all entries from the given log, starting after LastProcessedSeq.
func (idx *Timestamps) Index(log margaret.Log[*multimsg.MultiMessage]) error {
	qry := log.Query(margaret.Gt(idx.LastProcessedSeq()))
	for seq, msg := range qry.Iter() {
		if err := idx.ProcessEntry(seq, msg); err != nil {
			return err
		}
	}
	return qry.Err()
}
