// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package multilogs

import (
	"fmt"
	"sync"
	"time"

	"github.com/ssbc/margaret/v2/multilog/roaring"

	"github.com/ssbc/go-ssb/internal/storedrefs"
	"github.com/ssbc/go-ssb/message/multimsg"
)

const IndexNameFeeds = "userFeeds"

// TODO: idxInSync is a package-level WaitGroup shared across all sbot instances
// in the same process. This causes data races when multiple sbot instances run
// concurrently (e.g. in integration tests with makeTestBot). It should be moved
// to a struct field (e.g. on CombinedIndex or Sbot) so each instance has its own
// WaitGroup. This requires changing UserFeedsUpdate and WaitUntilUserFeedIndexIsSynced
// to be methods on that struct, and updating all callers.
var idxInSync sync.WaitGroup

func indexSyncStart() {
	idxInSync.Add(1)
}

func indexSyncDone() {
	time.AfterFunc(100*time.Millisecond, func() {
		idxInSync.Done()
	})
}

// WaitUntilUserFeedIndexIsSynced blocks until all the index processing is in sync with the rootlog
func WaitUntilUserFeedIndexIsSynced() {
	idxInSync.Wait()
}

func UserFeedsUpdate(seq int64, mm *multimsg.MultiMessage, mlog *roaring.MultiLog) error {
	indexSyncStart()
	defer indexSyncDone()

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
