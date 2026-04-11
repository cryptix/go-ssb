// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package sbot

import (
	mindexes "github.com/ssbc/margaret/v2/indexes"
	"github.com/ssbc/margaret/v2/multilog/roaring"

	"github.com/ssbc/go-ssb"
)

var _ ssb.Indexer = (*Sbot)(nil)

// GetSimpleIndex forwards to the IndexManager.
func (s *Sbot) GetSimpleIndex(name string) (mindexes.Index[int64], bool) {
	return s.idxMgr.GetSimpleIndex(name)
}

// GetMultiLog forwards to the IndexManager.
func (s *Sbot) GetMultiLog(name string) (*roaring.MultiLog, bool) {
	return s.idxMgr.GetMultiLog(name)
}

// GetIndexNamesSimple forwards to the IndexManager.
func (s *Sbot) GetIndexNamesSimple() []string {
	return s.idxMgr.GetIndexNamesSimple()
}

// GetIndexNamesMultiLog forwards to the IndexManager.
func (s *Sbot) GetIndexNamesMultiLog() []string {
	return s.idxMgr.GetIndexNamesMultiLog()
}

// WaitUntilIndexesAreSynced blocks until all the index processing is in sync with the rootlog.
func (s *Sbot) WaitUntilIndexesAreSynced() {
	s.idxMgr.WaitUntilIndexesAreSynced()
}

// AreIndexesSynced returns true when all indexes have caught up.
func (s *Sbot) AreIndexesSynced() bool {
	return s.idxMgr.AreIndexesSynced()
}
