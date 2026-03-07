# Design: Bulk Message Processing

## Problem Statement

The current message processing pipeline handles messages **one at a time**, from
verification through storage and indexing. This creates three categories of
problems:

1. **Write amplification** — Every single message triggers: 1 rxlog append,
   1 state file persist, 1 EBT state update, and N sublog appends (author,
   type, tangles, private). That's 5-10+ individual writes per message.

2. **Lock congestion** — Multiple hot mutexes serialize work that could be
   batched:
   - `generalVerifyDrain.mu` (per-feed verify+save, held across signature
     verification AND storage)
   - `CombinedIndex.l` (held for the entire index update of every message)
   - `VerificationRouter.mu` (held while looking up/creating sinks)
   - `publishLog.mu` (held during publish, which blocks on async index sync)

3. **Consistency gaps** — The async index design means there's a window between
   `rxlog.Append()` and `CombinedIndex.ProcessEntry()` where queries see stale
   data. The `publishLog.lastMsg` cache is a symptom-level fix for one case, but
   the general problem remains (e.g., back-to-back replication of the same feed
   from different peers).

## Current Architecture (Message Flow)

```
 Peer (EBT / Legacy Gossip)
  │
  │  raw bytes (one message at a time)
  ▼
 VerificationRouter.GetSink(author)     ◄── mutex: VerificationRouter.mu
  │
  ▼
 generalVerifyDrain.Verify(bytes)       ◄── mutex: generalVerifyDrain.mu
  │  1. Decode + signature verify
  │  2. ValidateNext (seq, prev hash)
  │  3. MargaretSaver.Save(msg)
  │     └── WrappedLog.AppendMessage()  ◄── mutex: inside offset2
  │         └── offset2.Append()        ← single write, fsync
  │
  ▼
 (async) serveIndex live query fires
  │
  ▼
 CombinedIndex.ProcessEntry(seq, mm)    ◄── mutex: CombinedIndex.l
  │  1. persist.Save(seq) to state file ← write
  │  2. users sublog append             ← write
  │  3. ebtState.Fill()                 ← write (TODO: batch/debounce)
  │  4. tryDecrypt (box1/box2)
  │  5. byType sublog append            ← write
  │  6. tangles sublog append(s)        ← write(s)
  │
  ▼
 (async) GraphBuilder index
  │  contact graph update               ← badger write
```

**Key observation**: In the EBT handler (`plugins/ebt/handler.go:177-219`),
messages arrive in a `for range rx.Iter(ctx)` loop and each one goes through
the full verify→save→(async)index pipeline individually. Legacy gossip
(`plugins/gossip/fetch.go:179-185`) does the same. There is zero batching
anywhere in the hot path.

## Proposed Design

### Core Idea: Batch at Three Levels

```
 Peer (EBT / Legacy Gossip)
  │
  │  raw bytes
  ▼
 ┌─────────────────────────────┐
 │  Level 1: Verify Batch      │  Feed-local, no global lock needed
 │  Accumulate N messages per   │
 │  feed, verify as a batch     │
 └─────────┬───────────────────┘
           │  []refs.Message (verified batch)
           ▼
 ┌─────────────────────────────┐
 │  Level 2: RxLog Bulk Append │  Single lock acquisition for N messages
 │  AppendBatch([]msg) → []seq │
 └─────────┬───────────────────┘
           │  []int64 (sequence numbers)
           ▼
 ┌─────────────────────────────┐
 │  Level 3: Index Batch       │  Process N entries per lock acquisition
 │  ProcessBatch([]entry)      │
 └─────────────────────────────┘
```

### Level 1: Batch Verification

**File**: `message/drains.go`

Currently `generalVerifyDrain.Verify()` holds `mu` for the entire
decode→verify→save path for one message. The proposed change:

```go
// New method on generalVerifyDrain
func (ld *generalVerifyDrain) VerifyBatch(msgs [][]byte) ([]refs.Message, error) {
    ld.mu.Lock()
    defer ld.mu.Unlock()

    verified := make([]refs.Message, 0, len(msgs))
    for _, raw := range msgs {
        next, err := ld.verify.Verify(raw)
        if err != nil {
            return verified, err  // stop at first bad message (feed is sequential)
        }
        err = ValidateNext(ld.latestMsg, next)
        if err != nil {
            if err == errSkip {
                continue
            }
            return verified, err
        }
        ld.latestSeq = int64(next.Seq())
        ld.latestMsg = next
        verified = append(verified, next)
    }
    // DO NOT save here — caller does bulk save
    return verified, nil
}
```

**Impact**: One lock acquisition per batch instead of per message. Signature
verification (the CPU-heavy part) still happens inside the lock because
`ValidateNext` depends on sequential state, but storage is deferred.

**Alternative considered**: Parallel signature verification with sequential
validation. This is more complex and signature verification isn't the primary
bottleneck (I/O is), so deferring this optimization.

### Level 2: Bulk RxLog Append

**File**: `message/multimsg/multimsg_wrap.go`, upstream `margaret/offset2`

```go
// New method on WrappedLog
func (wl *WrappedLog) AppendBatch(msgs []refs.Message) ([]int64, error) {
    mms := make([]*MultiMessage, len(msgs))
    for i, msg := range msgs {
        mm, err := wrapMessage(msg, wl.receivedNow())
        if err != nil {
            return nil, err
        }
        mms[i] = mm
    }
    return wl.Alterable.AppendBatch(mms)  // needs margaret support
}
```

**Margaret dependency**: `offset2` currently has `Append(value) (int64, error)`.
We need `AppendBatch(values) ([]int64, error)` that:
- Acquires the write lock once
- Writes all entries contiguously
- Calls `fsync` once at the end (not per entry)
- Returns all sequence numbers

This is the **highest-impact change** — it eliminates N-1 fsync calls and N-1
lock acquisitions per batch. If modifying margaret is not feasible short-term,
we can approximate this with a write-coalescing wrapper that buffers appends
and flushes periodically (e.g., every 50ms or N messages).

**File**: `message/verifier.go`

```go
// New method on MargaretSaver
func (ms MargaretSaver) SaveBatch(msgs []refs.Message) ([]int64, error) {
    return ms.WrappedLog.AppendBatch(msgs)
}

// New interface
type BatchSaveMessager interface {
    SaveMessager
    SaveBatch([]refs.Message) ([]int64, error)
}
```

### Level 3: Batch Indexing

**File**: `multilogs/combined.go`

```go
// New method on CombinedIndex
func (idx *CombinedIndex) ProcessBatch(entries []IndexEntry) error {
    idx.l.Lock()
    defer idx.l.Unlock()

    var lastSeq int64
    ebtUpdates := make([]statematrix.ObservedFeed, 0, len(entries))

    for _, e := range entries {
        if e.MM.Message == nil {
            continue
        }
        if err := idx.update(e.Seq, e.MM); err != nil {
            return err
        }
        lastSeq = e.Seq

        // Collect EBT updates instead of writing each one
        author := e.MM.Message.Author()
        ebtUpdates = append(ebtUpdates, statematrix.ObservedFeed{
            Feed: author,
            Note: ssb.Note{
                Seq:       int64(e.MM.Message.Seq()),
                Receive:   true,
                Replicate: true,
            },
        })
    }

    // Single state file persist for the whole batch
    if err := persist.Save(idx.file, lastSeq); err != nil {
        return err
    }

    // Single EBT state update for the whole batch
    if len(ebtUpdates) > 0 {
        if err := idx.ebtState.Fill(idx.self, ebtUpdates); err != nil {
            return err
        }
    }

    return nil
}

type IndexEntry struct {
    Seq int64
    MM  *multimsg.MultiMessage
}
```

**Key change**: `persist.Save()` and `ebtState.Fill()` are called once per
batch, not once per message. The `update()` method itself still does per-message
sublog appends, but those are bitmap operations (cheap).

**Current `update()` inlines EBT fill**: The existing `ProcessEntry` calls
`persist.Save` and `ebtState.Fill` per message. In `ProcessBatch`, we factor
those out. The inner `update()` method needs to be refactored to skip the EBT
fill (or a new `updateNoEBT()` variant).

### Batch Accumulation: Where and How

**Constraint**: Both `margaret.QueryIterator.Iter()` and `muxrpc.ByteSource.Iter()`
yield one item per iteration. There is no upstream "read N items" API. Batching
must be done by collecting items from the iterator into a slice before processing.

**Batch size**: 128 messages (count-based). Real-world replication data shows
feeds ranging from 8-1209 messages per sync. A batch of 128 gives 1-10 batches
for typical feeds, balancing throughput against memory and latency:

```
@3BLi: 286 msgs → 3 batches (128+128+30)
@plSv: 665 msgs → 6 batches
@kcsI: 1209 msgs → 10 batches
@71Ro: 12 msgs → 1 batch (flushed at end)
```

**File**: `plugins/ebt/handler.go` (EBT path)

EBT is trickier because the same stream carries both frontier updates and
messages from different authors. Messages must be accumulated and grouped
by author before batch processing.

```go
// In the Loop() method, replace the per-message processing:

const batchSize = 128

// Accumulate messages per-feed before processing
type pendingMsg struct {
    author refs.FeedRef
    raw    []byte
}
batch := make([]pendingMsg, 0, batchSize)

// Group by author, then verify+save each group as a batch
func (h *MUXRPCHandler) processBatch(batch []pendingMsg) {
    // Group by author
    byAuthor := map[string][]pendingMsg{}
    for _, m := range batch {
        key := m.author.String()
        byAuthor[key] = append(byAuthor[key], m)
    }

    for _, msgs := range byAuthor {
        sink, _ := h.verify.GetSink(msgs[0].author, true)
        raws := extractRawBytes(msgs)
        verified, _ := sink.VerifyBatch(raws)
        h.bulkSaver.SaveBatch(verified)
    }
}
```

When a frontier update arrives mid-batch, flush the pending message batch
first, then process the frontier update. This ensures messages verified
before state changes.

**File**: `plugins/gossip/fetch.go` (Legacy gossip path)

Legacy gossip is simpler: `fetchFeed()` processes a single feed at a time,
so all messages in the batch share the same author. Just collect from the
iterator and flush at batch size.

```go
// In fetchFeed(), replace the per-message loop:

const fetchBatchSize = 128

batch := make([][]byte, 0, fetchBatchSize)
for b := range src.Iter(ctx) {
    batch = append(batch, b)
    if len(batch) >= fetchBatchSize {
        verified, err := snk.VerifyBatch(batch)
        if err != nil { return err }
        if _, err := h.bulkSaver.SaveBatch(verified); err != nil {
            return err
        }
        latestSeq += len(verified)
        batch = batch[:0]
    }
}
// flush remainder
if len(batch) > 0 { /* same as above */ }
```

## Files Impacted

| File | Change | Risk |
|------|--------|------|
| `message/drains.go` | Add `VerifyBatch()`, `SequencedBatchVerificationSink` interface | Low — additive |
| `message/verifier.go` | Add `SaveBatch()`, `BatchSaveMessager` interface | Low — additive |
| `message/multimsg/multimsg_wrap.go` | Add `AppendBatch()` | Medium — core storage path |
| `multilogs/combined.go` | Add `ProcessBatch()`, refactor `update()` to not inline EBT | Medium — core indexing |
| `plugins/ebt/handler.go` | Batch accumulation in `Loop()` | Medium — replication path |
| `plugins/gossip/fetch.go` | Batch accumulation in `fetchFeed()` | Medium — replication path |
| `sbot/indexes.go` | Update `serveIndex` to use batch query | Low — orchestration |
| **margaret (upstream)** | `AppendBatch()` on offset2 | High — external dep |

## Migration / Compatibility Strategy

All changes are **additive**. Existing single-message interfaces remain:
- `SequencedVerificationSink.Verify([]byte)` stays, `VerifyBatch` is added
- `SaveMessager.Save()` stays, `BatchSaveMessager` is a supertype
- `CombinedIndex.ProcessEntry()` stays, `ProcessBatch` is added

This means we can land changes incrementally:
1. **Phase 1**: Batch indexing (`ProcessBatch`) — lowest risk, biggest
   write reduction
2. **Phase 2**: Batch verification (`VerifyBatch`) — decouple verify from save
3. **Phase 3**: Bulk rxlog append — requires margaret changes or a coalescing
   wrapper
4. **Phase 4**: Batch accumulation in EBT/gossip — ties it all together

## Expected Impact

| Metric | Current (per 1000 msgs) | After batching (batch=128) | Reduction |
|--------|------------------------|---------------------------|-----------|
| rxlog fsync calls | 1000 | ~8 | 99% |
| CombinedIndex lock acquisitions | 1000 | ~8 | 99% |
| persist.Save() calls | 1000 | ~8 | 99% |
| ebtState.Fill() calls | 1000 | ~8 | 99% |
| verify mutex acquisitions | 1000/feed | ~8/feed | 99% |

The primary bottleneck today is I/O (fsync per append) and lock contention.
Batching addresses both directly.

## Decisions Made

1. **Batch size**: 128, count-based. Real replication data shows feeds of
   8-1209 messages. 128 gives good amortization without excessive memory use.

2. **Margaret AppendBatch**: Separate task assigned to another agent. Will
   deliver a branch we can `replace` in go.mod. See
   `docs/task-margaret-batch-append.md`.

3. **Iterator constraint**: margaret and muxrpc iterators yield one item at a
   time. Batching is done by collecting items into a slice at the call site,
   not by changing the iterator API.

## Remaining Open Questions

1. **Error semantics in batch verify**: If message 47 of 128 fails verification,
   do we save the first 46 and return an error? This is the proposed behavior
   since feeds are sequential and a bad message invalidates everything after it.

2. **Interaction with live queries**: The `serveIndex` live path uses
   `margaret.Live()` which fires per-append. With bulk append, does it fire once
   per batch or once per entry? This affects the 100ms debounce logic. The
   margaret task document requests single-notification-per-batch as preferred
   behavior.

3. **Publish path**: No batching needed — publishes are infrequent and need
   immediate consistency. The async index race (`lastMsg` cache workaround)
   would be naturally solved if we had synchronous batch indexing after batch
   append.
