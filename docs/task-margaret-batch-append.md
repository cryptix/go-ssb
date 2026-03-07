# Task: Add Batch Append Support to Margaret offset2

## Context

go-ssb processes replicated messages one at a time. Each message triggers an
individual `offset2.Append()` call which acquires a lock and fsyncs. During
replication we routinely process hundreds to thousands of messages per feed
(e.g., 1209 messages for a single feed in ~90 seconds). Batching these appends
into single lock+fsync operations is the highest-impact performance improvement
we can make.

This task covers **only the margaret library changes**. The go-ssb integration
is handled separately.

## What We Need

### 1. `AppendBatch` on `offset2.Log`

```go
// AppendBatch appends multiple values to the log atomically.
// It acquires the write lock once, writes all entries contiguously,
// and calls fsync once at the end.
// Returns the sequence numbers of all appended entries.
func (log *offsetLog[T]) AppendBatch(values []T) ([]int64, error)
```

**Requirements:**
- Single lock acquisition for the entire batch
- All entries written contiguously to the offset file
- **One fsync** at the end, not per entry
- Returns `[]int64` with the sequence number of each appended entry
- If any write fails mid-batch, the log must remain consistent (truncate back
  to the state before the batch started)
- Empty batch (`len(values) == 0`) returns `nil, nil`

**Implementation notes:**
- The current `Append(value T) (int64, error)` encodes the value, writes a
  length-prefixed frame to the data file, updates the offset index, and fsyncs.
  `AppendBatch` should do the same but loop over values before the final fsync.
- The internal sequence counter should be updated atomically after all writes
  succeed.

### 2. Interface Update

The `margaret.Log[T]` interface (or a new `margaret.BatchLog[T]` interface)
should expose `AppendBatch`:

```go
// Option A: Extend the existing Log interface (breaking change)
type Log[T any] interface {
    // ... existing methods ...
    AppendBatch([]T) ([]int64, error)
}

// Option B: New interface (non-breaking, preferred)
type BatchAppender[T any] interface {
    AppendBatch([]T) ([]int64, error)
}
```

**Preference**: Option B (new interface) to avoid breaking existing consumers.
go-ssb will type-assert to `BatchAppender` where available.

### 3. Live Query Notification

When `AppendBatch` completes, the live query mechanism (`margaret.Live()`)
should be notified. Acceptable behaviors:

- **Preferred**: Fire once after the batch completes (not once per entry).
  This naturally batches downstream index processing.
- **Acceptable**: Fire once per entry. go-ssb already has a 100ms debounce on
  the index side that would coalesce these.

Document which behavior the implementation provides.

### 4. `Alterable[T]` Support

go-ssb wraps offset2 in `margaret.Alterable[*MultiMessage]`. If `Alterable`
is a separate wrapper, it also needs to expose `AppendBatch` (delegating to
the underlying log).

## What We Do NOT Need

- No changes to `Replace`, `Null`, `Get`, or `Query` methods
- No changes to the roaring multilog
- No changes to the index/sink infrastructure
- No new codec requirements — existing codecs work fine per-entry

## Testing

- Append a batch of N entries, verify all sequence numbers are contiguous
- Verify `Get(seq)` returns the correct value for each entry in the batch
- Verify `Log.Seq()` equals the last sequence after batch append
- Verify live queries see all entries after batch completes
- Verify empty batch is a no-op
- Verify partial write failure leaves log in consistent state (truncated to
  pre-batch state)
- Benchmark: compare `AppendBatch(100)` vs 100x `Append()` — expect
  significant improvement from single fsync

## Batch Sizes to Expect

Based on real replication data, typical feed sizes during initial sync:

| Messages | Frequency |
|----------|-----------|
| 1-15     | Common (small feeds) |
| 25-65    | Moderate |
| 200-700  | Large feeds |
| 1000+    | Heavy feeds |

go-ssb will batch in chunks of ~128 messages, so `AppendBatch` should handle
batches of 1-128 entries efficiently. Larger batches (up to 1000) should also
work but are not the common case.

## Deliverable

A branch of margaret that go-ssb can reference in `go.mod` via a `replace`
directive or a tagged pre-release version. The branch should be based on the
current margaret version used by go-ssb (`github.com/ssbc/margaret/v2`).
