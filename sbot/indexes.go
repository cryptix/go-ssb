// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package sbot

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/machinebox/progress"
	margaret "github.com/ssbc/margaret/v2"
	"github.com/ssbc/margaret/v2/indexes"
	"github.com/ssbc/margaret/v2/multilog/roaring"
	"go.mindeco.de/log"
	"go.mindeco.de/log/level"

	"github.com/ssbc/go-ssb"
	"github.com/ssbc/go-ssb/message/multimsg"
	"github.com/ssbc/go-ssb/multilogs"
)

// LogIndexer is implemented by all indexes that process messages from a margaret log.
type LogIndexer interface {
	Index(margaret.Log[*multimsg.MultiMessage]) error
}

func (s *Sbot) GetSimpleIndex(name string) (indexes.Index[int64], bool) {
	si, has := s.simpleIndex[name]
	return si, has
}

func (s *Sbot) GetMultiLog(name string) (*roaring.MultiLog, bool) {
	ml, has := s.mlogIndicies[name]
	return ml, has
}

func (s *Sbot) GetIndexNamesSimple() []string {
	var simple []string
	for name := range s.simpleIndex {
		simple = append(simple, name)
	}
	return simple
}

func (s *Sbot) GetIndexNamesMultiLog() []string {
	var mlogs []string
	for name := range s.mlogIndicies {
		mlogs = append(mlogs, name)
	}
	return mlogs
}

var _ ssb.Indexer = (*Sbot)(nil)

func (s *Sbot) indexSyncStart() {
	s.idxInSync.Add(1)
	atomic.AddInt64(&s.idxNumSyncing, 1)
}

func (s *Sbot) indexSyncDone() {
	atomic.AddInt64(&s.idxNumSyncing, -1)
	s.idxInSync.Done()
}

// WaitUntilIndexesAreSynced blocks until all the index processing is in sync with the rootlog
func (s *Sbot) WaitUntilIndexesAreSynced() {
	var wg sync.WaitGroup

	// wait for the indexes in parallel so we catch up as quickly as possible
	wg.Add(3)

	go func() {
		// wait for our internal indexes to catch up
		s.idxInSync.Wait()
		wg.Done()
	}()

	go func() {
		// wait for the multilogs to catch up
		multilogs.WaitUntilUserFeedIndexIsSynced()
		wg.Done()
	}()

	go func() {
		// wait for all of the graph builder's indexes to catch up
		s.GraphBuilder.WaitUntilIndexesAreSynced()
		wg.Done()
	}()

	// wait for all the concurrent waits to finish
	wg.Wait()
}

func (s *Sbot) AreIndexesSynced() bool {
	return s.idxNumSyncing == 0
}

// serveIndex fills an index with all messages from the receive log.
func (s *Sbot) serveIndex(name string, idx LogIndexer) {
	s.serveIndexFrom(name, idx, s.ReceiveLog)
}

// serveIndexFrom fills an index with messages from a specific log.
func (s *Sbot) serveIndexFrom(name string, idx LogIndexer, msgs margaret.Log[*multimsg.MultiMessage]) {
	s.indexSyncStart()

	s.indexStateMu.Lock()
	s.indexStates[name] = "pending"
	s.indexStateMu.Unlock()

	s.idxDone.Go(func() (retErr error) {
		logger := log.With(s.info, "index", name)

		defer func() {
			if r := recover(); r != nil {
				retErr = fmt.Errorf("sbot index(%s) panicked: %v", name, r)
				level.Error(logger).Log("event", "index panic", "err", retErr)
				s.indexStateMu.Lock()
				s.indexStates[name] = retErr.Error()
				s.indexStateMu.Unlock()
			}
		}()

		// Process backlog
		totalMessages := msgs.Seq()
		var ps progressCounter

		// If the index supports progress callbacks, wire it up
		if ci, ok := idx.(interface{ SetOnEntry(func()) }); ok {
			ci.SetOnEntry(func() { ps.Incr() })
		}

		ctx, cancel := context.WithCancel(s.rootCtx)
		go func() {
			p := progress.NewTicker(ctx, &ps, int64(totalMessages), 7*time.Second)
			pinfo := log.With(level.Info(logger), "event", "index-progress")
			for remaining := range p {
				estDone := remaining.Estimated()
				timeLeft := estDone.Sub(time.Now()).Round(time.Second)
				pinfo.Log("done", remaining.Percent(), "time-left", timeLeft)

				s.indexStateMu.Lock()
				s.indexStates[name] = fmt.Sprintf("%.2f%% (time left:%s)", remaining.Percent(), timeLeft)
				s.indexStateMu.Unlock()
			}
		}()

		err := idx.Index(msgs)
		cancel()
		s.indexSyncDone()
		if errors.Is(err, ssb.ErrShuttingDown) || errors.Is(err, context.Canceled) {
			return nil
		}
		if err != nil {
			s.indexStateMu.Lock()
			s.indexStates[name] = err.Error()
			s.indexStateMu.Unlock()
			level.Warn(logger).Log("event", "index stopped", "err", err)
			return fmt.Errorf("sbot index(%s) update of backlog failed: %w", name, err)
		}

		if !s.liveIndexUpdates {
			return nil
		}

		s.indexStateMu.Lock()
		s.indexStates[name] = "live"
		s.indexStateMu.Unlock()

		// Live updates: watch for new messages and re-index
		qry := msgs.Query(margaret.Live(s.rootCtx), margaret.Gt(msgs.Seq()))
		for range qry.Iter() {
			s.indexSyncStart()
			err := idx.Index(msgs)
			// delay the done to give a little time in case more messages arrive
			time.AfterFunc(100*time.Millisecond, func() {
				s.indexSyncDone()
			})
			if err != nil {
				if errors.Is(err, ssb.ErrShuttingDown) || errors.Is(err, context.Canceled) {
					return nil
				}
				s.indexStateMu.Lock()
				s.indexStates[name] = err.Error()
				s.indexStateMu.Unlock()
				level.Warn(logger).Log("event", "index stopped", "err", err)
				return fmt.Errorf("sbot index(%s) live update failed: %w", name, err)
			}
		}
		if err := qry.Err(); err != nil {
			if errors.Is(err, ssb.ErrShuttingDown) || errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("sbot index(%s) live query error: %w", name, err)
		}
		return nil
	})
}

type progressCounter struct {
	mu sync.Mutex
	n  uint
}

var _ progress.Counter = &progressCounter{}

func (p *progressCounter) N() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return int64(p.n)
}

func (p *progressCounter) Incr() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.n++
}

func (p *progressCounter) Err() error {
	return nil
}
