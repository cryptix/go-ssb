// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package sbot

import (
	"fmt"

	"github.com/ssbc/go-ssb"
	refs "github.com/ssbc/go-ssb-refs"
	"github.com/ssbc/go-ssb/internal/storedrefs"
	"github.com/ssbc/margaret/v2/indexes"
)

func (s *Sbot) Get(ref refs.MessageRef) (refs.Message, error) {
	getIdx, ok := s.idxMgr.GetSimpleIndex("get")
	if !ok {
		return nil, fmt.Errorf("sbot: get index disabled")
	}

	seq, err := getIdx.Get(indexes.Addr(storedrefs.Message(ref)))
	if err != nil {
		return nil, fmt.Errorf("sbot/get: failed to get seq val from index: %w", err)
	}

	if seq < 0 {
		return nil, fmt.Errorf("invalid sequence stored in index")
	}

	mm, err := s.ReceiveLog.Get(seq)
	if err != nil {
		return nil, fmt.Errorf("sbot/get: failed to load message: %w", err)
	}

	if mm.Message == nil {
		return nil, fmt.Errorf("sbot/get: stored message is nil")
	}

	return mm.Message, nil
}

func (s *Sbot) CurrentSequence(feed refs.FeedRef) (ssb.Note, error) {
	l, err := s.Users.Get(storedrefs.Feed(feed))
	if err != nil {
		return ssb.Note{}, fmt.Errorf("failed to get user log for %s: %w", feed.ShortSigil(), err)
	}

	currSeq := l.Seq()
	if currSeq != -1 {
		currSeq++
	}

	return ssb.Note{
		Seq:       currSeq,
		Replicate: true,
		Receive:   true, // TODO: not exactly... we might be getting this feed from somewhre else
	}, nil
}
