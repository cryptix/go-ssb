// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package multilogs

import (
	"fmt"

	"github.com/ssbc/margaret/v2/multilog/roaring"

	"github.com/ssbc/go-ssb/internal/storedrefs"
	"github.com/ssbc/go-ssb/message/multimsg"
)

const IndexNameFeeds = "userFeeds"

// UserFeedsUpdate indexes a single message into the per-author multilog.
// Index sync tracking is handled by the caller (e.g. Sbot.idxInSync).
func UserFeedsUpdate(seq int64, mm *multimsg.MultiMessage, mlog *roaring.MultiLog) error {

	if mm.Message == nil {
		return nil // nulled entry
	}

	author := mm.Message.Author()

	authorLog, err := mlog.Get(storedrefs.Feed(author))
	if err != nil {
		return fmt.Errorf("error opening sublog: %w", err)
	}

	if err := appendSeq(authorLog, seq); err != nil {
		return fmt.Errorf("error appending new author message: %w", err)
	}
	return nil
}
