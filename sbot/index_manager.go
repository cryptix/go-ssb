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
	mindexes "github.com/ssbc/margaret/v2/indexes"
	"github.com/ssbc/margaret/v2/multilog/roaring"
	"go.mindeco.de/log"
	"go.mindeco.de/log/level"
	"golang.org/x/sync/errgroup"

	"github.com/ssbc/go-ssb"
	"github.com/ssbc/go-ssb/message/multimsg"
)

// IndexSyncer is implemented by components that have their own index sync tracking
// (e.g. the graph builder), allowing IndexManager to coordinate waiting for all
// indexes to be synced.
type IndexSyncer interface {
	WaitUntilIndexesAreSynced()
}

// indexTracker records a live index's last-processed sequence number so that
// WaitUntilIndexesAreSynced can verify every index has caught up to the log.
type indexTracker struct {
	lastProcessedSeq atomic.Int64
}

// IndexManager manages the lifecycle of all index goroutines: starting them,
// tracking their sync progress, and waiting for them to finish. It also provides
// lookup access to registered multilogs and simple indexes.
type IndexManager struct {
	info       log.Logger
	rootCtx    context.Context
	receiveLog margaret.Log[*multimsg.MultiMessage]

	idxDone       errgroup.Group
	idxSyncMu     sync.Mutex
	idxSyncCond   *sync.Cond
	idxNumSyncing int64

	liveIndexUpdates bool

	indexStateMu sync.Mutex
	indexStates  map[string]string

	// trackersMu guards trackers during registration (writes happen during init only).
	trackersMu sync.Mutex
	trackers   []*indexTracker

	mlogIndicies map[string]*roaring.MultiLog
	simpleIndex  map[string]mindexes.Index[int64]

	graphSyncer IndexSyncer
}

// NewIndexManager creates an IndexManager that serves indexes from the given receive log.
func NewIndexManager(
	logger log.Logger,
	rootCtx context.Context,
	receiveLog margaret.Log[*multimsg.MultiMessage],
	liveIndexUpdates bool,
) *IndexManager {
	im := &IndexManager{
		info:             logger,
		rootCtx:          rootCtx,
		receiveLog:       receiveLog,
		liveIndexUpdates: liveIndexUpdates,
		indexStates:      make(map[string]string),
		mlogIndicies:     make(map[string]*roaring.MultiLog),
		simpleIndex:      make(map[string]mindexes.Index[int64]),
	}
	im.idxSyncCond = sync.NewCond(&im.idxSyncMu)
	return im
}

// SetGraphSyncer sets the graph builder dependency used by WaitUntilIndexesAreSynced
// to also wait for graph indexes. This must be called before any sync waiting occurs.
func (im *IndexManager) SetGraphSyncer(gs IndexSyncer) {
	im.graphSyncer = gs
}

// RegisterMultiLog registers a named multilog for lookup via GetMultiLog.
func (im *IndexManager) RegisterMultiLog(name string, mlog *roaring.MultiLog) {
	im.mlogIndicies[name] = mlog
}

// RegisterSimpleIndex registers a named simple index for lookup via GetSimpleIndex.
func (im *IndexManager) RegisterSimpleIndex(name string, idx mindexes.Index[int64]) {
	im.simpleIndex[name] = idx
}

// GetSimpleIndex returns a simple index by name.
func (im *IndexManager) GetSimpleIndex(name string) (mindexes.Index[int64], bool) {
	si, has := im.simpleIndex[name]
	return si, has
}

// GetMultiLog returns a multilog by name.
func (im *IndexManager) GetMultiLog(name string) (*roaring.MultiLog, bool) {
	ml, has := im.mlogIndicies[name]
	return ml, has
}

// GetIndexNamesSimple returns the names of all registered simple indexes.
func (im *IndexManager) GetIndexNamesSimple() []string {
	var names []string
	for name := range im.simpleIndex {
		names = append(names, name)
	}
	return names
}

// GetIndexNamesMultiLog returns the names of all registered multilogs.
func (im *IndexManager) GetIndexNamesMultiLog() []string {
	var names []string
	for name := range im.mlogIndicies {
		names = append(names, name)
	}
	return names
}

// WaitUntilIndexesAreSynced blocks until all index processing is in sync with the rootlog.
// It first waits for any in-flight processing to complete, then verifies that every
// tracked index has processed at least up to the log's current sequence number.
// This closes the race where a live query notification is pending but the index
// goroutine hasn't woken up yet.
func (im *IndexManager) WaitUntilIndexesAreSynced() {
	// Phase 1: wait for any currently in-flight index processing.
	im.idxSyncMu.Lock()
	for atomic.LoadInt64(&im.idxNumSyncing) > 0 {
		im.idxSyncCond.Wait()
	}
	im.idxSyncMu.Unlock()

	// Phase 2: verify every tracked index has caught up to the log.
	// The live query notification may be pending but the index goroutine
	// hasn't woken up yet, so we poll briefly until all trackers report
	// a lastProcessedSeq >= the log's current seq.
	targetSeq := im.receiveLog.Seq()
	if targetSeq >= 0 && len(im.trackers) > 0 {
		const maxWait = 5 * time.Second
		deadline := time.Now().Add(maxWait)
		for time.Now().Before(deadline) {
			allCaughtUp := true
			for _, t := range im.trackers {
				if t.lastProcessedSeq.Load() < targetSeq {
					allCaughtUp = false
					break
				}
			}
			if allCaughtUp {
				break
			}
			// Brief sleep to let live query goroutines wake up and process.
			time.Sleep(1 * time.Millisecond)
		}
	}

	if im.graphSyncer != nil {
		im.graphSyncer.WaitUntilIndexesAreSynced()
	}
}

// AreIndexesSynced returns true if all indexes have caught up with the rootlog.
func (im *IndexManager) AreIndexesSynced() bool {
	if atomic.LoadInt64(&im.idxNumSyncing) != 0 {
		return false
	}
	targetSeq := im.receiveLog.Seq()
	for _, t := range im.trackers {
		if t.lastProcessedSeq.Load() < targetSeq {
			return false
		}
	}
	return true
}

// IndexStates returns a snapshot of the current state of all indexes.
func (im *IndexManager) IndexStates() map[string]string {
	im.indexStateMu.Lock()
	defer im.indexStateMu.Unlock()
	snapshot := make(map[string]string, len(im.indexStates))
	for k, v := range im.indexStates {
		snapshot[k] = v
	}
	return snapshot
}

// Wait waits for all index goroutines to finish and returns any error.
func (im *IndexManager) Wait() error {
	return im.idxDone.Wait()
}

func (im *IndexManager) syncStart() {
	atomic.AddInt64(&im.idxNumSyncing, 1)
}

func (im *IndexManager) syncDone() {
	if atomic.AddInt64(&im.idxNumSyncing, -1) == 0 {
		im.idxSyncCond.Broadcast()
	}
}

// ServeIndex fills an index with all messages from the receive log.
func (im *IndexManager) ServeIndex(name string, idx LogIndexer) {
	im.ServeIndexFrom(name, idx, im.receiveLog)
}

// ServeIndexFrom fills an index with messages from a specific log.
func (im *IndexManager) ServeIndexFrom(name string, idx LogIndexer, msgs margaret.Log[*multimsg.MultiMessage]) {
	im.syncStart()

	// Register a tracker so WaitUntilIndexesAreSynced can verify this index's progress.
	tracker := &indexTracker{}
	tracker.lastProcessedSeq.Store(-1)
	im.trackersMu.Lock()
	im.trackers = append(im.trackers, tracker)
	im.trackersMu.Unlock()

	im.indexStateMu.Lock()
	im.indexStates[name] = "pending"
	im.indexStateMu.Unlock()

	im.idxDone.Go(func() (retErr error) {
		logger := log.With(im.info, "index", name)

		defer func() {
			if r := recover(); r != nil {
				retErr = fmt.Errorf("sbot index(%s) panicked: %v", name, r)
				level.Error(logger).Log("event", "index panic", "err", retErr)
				im.indexStateMu.Lock()
				im.indexStates[name] = retErr.Error()
				im.indexStateMu.Unlock()
			}
		}()

		// Process backlog
		// msgs.Seq() is the last sequence number (0-indexed): N messages → Seq() = N-1.
		// The index resumes from its checkpoint, so only the remaining messages are processed.
		// We compute the actual remaining work so the ETA reflects real progress, not the
		// full log size.
		logSeq := msgs.Seq() // last seq (0-indexed); total messages = logSeq+1
		var lastProcessed int64 = -1
		if lp, ok := idx.(interface{ LastProcessedSeq() int64 }); ok {
			lastProcessed = lp.LastProcessedSeq()
		}
		// remaining = (logSeq) - lastProcessed  (number of messages still to process)
		// e.g. 1000 messages (seq 0..999), checkpoint at 799 → 200 remaining
		remaining := logSeq - lastProcessed
		if remaining < 0 {
			remaining = 0
		}
		var ps progressCounter

		// If the index supports progress callbacks, wire it up
		if ci, ok := idx.(interface{ SetOnEntry(func()) }); ok {
			ci.SetOnEntry(func() { ps.Incr() })
		}

		ctx, cancel := context.WithCancel(im.rootCtx)
		defer cancel()
		go func() {
			p := progress.NewTicker(ctx, &ps, remaining, 7*time.Second)
			pinfo := log.With(level.Info(logger), "event", "index-progress")
			for prog := range p {
				estDone := prog.Estimated()
				timeLeft := estDone.Sub(time.Now()).Round(time.Second)
				pinfo.Log("done", prog.Percent(), "time-left", timeLeft)

				im.indexStateMu.Lock()
				im.indexStates[name] = fmt.Sprintf("%.1f%% (time left:%s)", prog.Percent()*100, timeLeft)
				im.indexStateMu.Unlock()
			}
		}()

		err := idx.Index(msgs)
		tracker.lastProcessedSeq.Store(msgs.Seq())
		im.syncDone()
		if errors.Is(err, ssb.ErrShuttingDown) || errors.Is(err, context.Canceled) {
			return nil
		}
		if err != nil {
			im.indexStateMu.Lock()
			im.indexStates[name] = err.Error()
			im.indexStateMu.Unlock()
			level.Warn(logger).Log("event", "index stopped", "err", err)
			return fmt.Errorf("sbot index(%s) update of backlog failed: %w", name, err)
		}

		if !im.liveIndexUpdates {
			return nil
		}

		im.indexStateMu.Lock()
		im.indexStates[name] = "live"
		im.indexStateMu.Unlock()

		// Live updates: watch for new messages and re-index
		qry := msgs.Query(margaret.Live(im.rootCtx), margaret.Gt(msgs.Seq()))
		for range qry.Iter() {
			im.syncStart()
			err := idx.Index(msgs)
			tracker.lastProcessedSeq.Store(msgs.Seq())
			im.syncDone()
			if err != nil {
				if errors.Is(err, ssb.ErrShuttingDown) || errors.Is(err, context.Canceled) {
					return nil
				}
				im.indexStateMu.Lock()
				im.indexStates[name] = err.Error()
				im.indexStateMu.Unlock()
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

// LogIndexer is implemented by all indexes that process messages from a margaret log.
type LogIndexer interface {
	Index(margaret.Log[*multimsg.MultiMessage]) error
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
