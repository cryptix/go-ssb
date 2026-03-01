# Migrating from margaret v0.x to v2

This guide covers migrating `go-ssb` (and other consumers) from `github.com/ssbc/margaret` v0.4.x to `github.com/ssbc/margaret/v2`.

## Overview of Changes

| Aspect | v0.x | v2 |
|--------|------|-----|
| Module path | `github.com/ssbc/margaret` | `github.com/ssbc/margaret/v2` |
| Go version | 1.18+ | 1.25+ (generics + range-over-func) |
| Value types | `interface{}` everywhere | `Log[T Encodeable]` — fully generic |
| Queries | `Query(...) (luigi.Source, error)` | `Query(...) QueryIterator[T]` |
| Iteration | `luigi.Pump(ctx, sink, src)` | `for seq, val := range qry.Iter()` |
| Live mode | `margaret.Live(true)` | `margaret.Live(ctx)` |
| Codecs | External `margaret.Codec` | Types implement `Encodeable` (BinaryMarshaler+Unmarshaler) |
| SeqWrap | `margaret.SeqWrap(true)` | Removed — `Iter()` yields `(int64, T)` directly |
| Observables | `log.Changes() luigi.Observable` | Removed — use `Live(ctx)` queries |
| Indexes | `librarian.Index` / `librarian.SinkIndex` | `indexes.Index[V]` / `indexes.SinkIndex[T,V]` |
| MultiLog | `multilog.MultiLog` (untyped) | `multilog.MultiLog[T Encodeable]` |

## 1. Module Path

```go
// OLD
import "github.com/ssbc/margaret"
import librarian "github.com/ssbc/margaret/indexes"
import "github.com/ssbc/margaret/multilog"
import "github.com/ssbc/margaret/multilog/roaring"

// NEW
import margaret "github.com/ssbc/margaret/v2"
import "github.com/ssbc/margaret/v2/indexes"
import "github.com/ssbc/margaret/v2/multilog"
import "github.com/ssbc/margaret/v2/multilog/routed"  // replaces roaring
```

## 2. Log Interface — Generic Types

The core `Log` interface is now generic. All values flowing through the log must implement `Encodeable` (`encoding.BinaryMarshaler` + `encoding.BinaryUnmarshaler`).

**Important**: Always use pointer types as the type parameter (e.g. `*MultiMessage`, not `MultiMessage`) because `UnmarshalBinary` requires a pointer receiver.

```go
// OLD
var rxLog margaret.Log // stores interface{}
v, err := rxLog.Get(seq)
msg := v.(refs.Message) // type assertion at runtime

// NEW
var rxLog margaret.Log[*multimsg.MultiMessage] // stores typed values
msg, err := rxLog.Get(seq) // msg is already *multimsg.MultiMessage
```

### Making Types Encodeable

Types stored in a log must implement `MarshalBinary`/`UnmarshalBinary` instead of using an external codec.

```go
// OLD: external codec
import "github.com/ssbc/margaret/codec/msgpack"
log, err := offset2.Open(path, msgpack.New(&multimsg.MultiMessage{}))

// NEW: type implements Encodeable
func (mm *MultiMessage) MarshalBinary() ([]byte, error) { ... }
func (mm *MultiMessage) UnmarshalBinary(data []byte) error { ... }

log, err := offset2.Open[*MultiMessage](path)
```

## 3. Queries and Iteration

The biggest change: queries no longer return `(luigi.Source, error)` — they return a `QueryIterator[T]` directly. Errors in query construction are captured and returned via `qry.Err()`.

### Simple Query

```go
// OLD
src, err := rxLog.Query(margaret.Gte(0), margaret.Limit(10))
if err != nil { return err }
for {
    v, err := src.Next(ctx)
    if luigi.IsEOS(err) { break }
    if err != nil { return err }
    msg := v.(refs.Message)
    // use msg
}

// NEW
qry := rxLog.Query(margaret.Gte(0), margaret.Limit(10))
for seq, msg := range qry.Iter() {
    // msg is already the correct type
    // seq is the sequence number
}
if err := qry.Err(); err != nil {
    return err
}
```

### Luigi Pump → Range Loop

```go
// OLD
src, err := log.Query(margaret.Live(false), margaret.SeqWrap(true), snk.QuerySpec())
if err != nil { return err }
err = luigi.Pump(ctx, sink, src)

// NEW
qry := log.Query(margaret.Gte(startSeq))
for seq, val := range qry.Iter() {
    if err := process(seq, val); err != nil {
        return err
    }
}
if err := qry.Err(); err != nil {
    return err
}
```

## 4. Live Mode

`Live` now takes a `context.Context` instead of a boolean. The iterator blocks until new entries arrive or the context is cancelled.

```go
// OLD
// backlog pass
src, _ := log.Query(margaret.Live(false), margaret.SeqWrap(true))
luigi.Pump(ctx, sink, src)

// live pass
src, _ = log.Query(margaret.Live(true), margaret.SeqWrap(true))
luigi.Pump(ctx, sink, src)

// NEW — single pass handles both backlog and live
qry := log.Query(margaret.Live(ctx))
for seq, val := range qry.Iter() {
    process(seq, val) // drains existing entries, then blocks for new ones
}
// Iter() returns when ctx is cancelled
if err := qry.Err(); err != nil && err != context.Canceled {
    return err
}
```

## 5. SeqWrap — Removed

`SeqWrap` wrapped values with their sequence number. In v2, `Iter()` yields `(int64, T)` tuples directly — the sequence is always available.

```go
// OLD
src, _ := log.Query(margaret.SeqWrap(true))
for {
    v, err := src.Next(ctx)
    sw := v.(margaret.SeqWrapper)
    seq := sw.Seq()
    msg := sw.Value().(refs.Message)
}

// NEW
for seq, msg := range log.Query().Iter() {
    // seq and msg are directly available
}
```

## 6. QuerySpec → QueryOption

```go
// OLD: margaret.QuerySpec (function type)
margaret.Gt(seq)
margaret.Gte(seq)
margaret.Lt(seq)
margaret.Lte(seq)
margaret.Limit(n)
margaret.Live(true/false)
margaret.Reverse(true)
margaret.SeqWrap(true)
margaret.MergeQuerySpec(specs...)
margaret.ErrorQuerySpec(err)

// NEW: margaret.QueryOption (function type, similar names)
margaret.Gt(seq)      // same
margaret.Gte(seq)     // same
margaret.Lt(seq)      // same
margaret.Lte(seq)     // same
margaret.Limit(n)     // same
margaret.Live(ctx)    // takes context instead of bool
margaret.Reverse(yes) // same
// SeqWrap removed — not needed
// MergeQuerySpec removed — just pass multiple options
// ErrorQuerySpec removed — errors handled via QueryIterator.Err()
```

### Replacing MergeQuerySpec and SinkIndex.QuerySpec()

The old `librarian.SinkIndex` had a `QuerySpec()` method that returned where to resume indexing. In v2, this is handled by `indexes.SinkIndex` with `WithSeqTracking()`:

```go
// OLD (combined index pattern)
func (idx *CombinedIndex) QuerySpec() margaret.QuerySpec {
    seq := loadPersistedSeq()
    return margaret.MergeQuerySpec(
        margaret.Gt(seq),
        margaret.SeqWrap(true),
    )
}
// caller:
src, _ := log.Query(margaret.Live(false), snk.QuerySpec())
luigi.Pump(ctx, sink, src)

// NEW
sink := indexes.NewSinkIndex[*MultiMessage](idx, extractFn).
    WithSeqTracking(seqTracker)
sink.Index(log) // handles resume + iteration internally
```

## 7. Indexes

### Old: librarian.Index / librarian.SinkIndex

```go
// OLD
import librarian "github.com/ssbc/margaret/indexes"

type Index interface {
    Get(ctx context.Context, addr Addr) (luigi.Observable, error)
    // ...
}
type SinkIndex interface {
    luigi.Sink  // Pour(ctx, v) error
    QuerySpec() margaret.QuerySpec
    io.Closer
}
```

### New: indexes.Index[V] / indexes.SinkIndex[T, V]

```go
// NEW
import "github.com/ssbc/margaret/v2/indexes"

type Index[V any] interface {
    Get(Addr) (V, error)
    Set(Addr, V) error
    Delete(Addr) error
    Has(Addr) (bool, error)
    io.Closer
}

// For the "get by message key" index:
idx := indexes.NewMemory[int64]() // addr -> seq mapping
sink := indexes.NewSinkIndex[*MultiMessage](idx,
    func(seq int64, msg *MultiMessage) (indexes.Addr, int64, bool) {
        return indexes.Addr(msg.Key().String()), seq, true
    },
).WithSeqTracking(seqTracker)
sink.Index(rxLog) // processes all entries
```

### Replacing the Combined Index (Pour pattern)

The old `CombinedIndex` implements `luigi.Sink` — it receives `SeqWrapper` values via `Pour()`. In v2, use `multilog.Sink[T]` with a routing function:

```go
// OLD
type CombinedIndex struct { ... }
func (idx *CombinedIndex) Pour(ctx context.Context, swv interface{}) error {
    sw := swv.(margaret.SeqWrapper)
    seq := sw.Seq()
    msg := sw.Value().(refs.Message)
    // route to users, byType, tangles sublogs...
    authorLog.Append(seq)
    typedLog.Append(seq)
}
// Used via: luigi.Pump(ctx, combinedIdx, src)

// NEW
sink := multilog.NewSink[*MultiMessage](users, func(seq int64, msg *MultiMessage, mlog multilog.MultiLog[*MultiMessage]) error {
    // route to sublogs
    authorLog, _ := mlog.Get(multilog.Addr(msg.Author().String()))
    authorLog.Append(msg)
    return nil
})
sink.Index(rxLog)
```

## 8. MultiLog

### Interface

```go
// OLD
import "github.com/ssbc/margaret/multilog"

type MultiLog interface {
    Get(librarian.Addr) (margaret.Log, error)
    List() ([]librarian.Addr, error)
    // ...
}

subLog, err := userFeeds.Get(storedrefs.Feed(author))
v, err := subLog.Get(0) // returns interface{}

// NEW
import "github.com/ssbc/margaret/v2/multilog"

type MultiLog[T Encodeable] interface {
    Get(Addr) (margaret.Log[T], error)
    List() ([]Addr, error)
    Has(Addr) (bool, error)
    Delete(Addr) error
    io.Closer
}

subLog, err := userFeeds.Get(multilog.Addr(storedrefs.Feed(author)))
msg, err := subLog.Get(0) // returns *MultiMessage directly
```

### Roaring → Routed

The old `roaring.MultiLog` (bitmap-backed, stores global sequence numbers) is replaced by `routed.Routed[T]` (one offset2 log per address, stores full entries).

```go
// OLD
import "github.com/ssbc/margaret/multilog/roaring"
import rbadger "github.com/ssbc/margaret/multilog/roaring/badger"

mlog, err := rbadger.NewStandalone(path)
subLog, _ := mlog.Get(addr) // sublog stores int64 seq numbers
v, _ := subLog.Get(0)       // v is int64, not the actual message!
// Need indirection to get the actual message:
msg, _ := rxLog.Get(v.(int64))

// NEW
import "github.com/ssbc/margaret/v2/multilog/routed"

mlog, err := routed.NewRouted[*MultiMessage](path)
subLog, _ := mlog.Get(addr)
msg, _ := subLog.Get(0) // msg is *MultiMessage directly
```

**Note**: This is a fundamental design change. The roaring multilog was a compressed bitmap of sequence numbers — very space efficient but required a separate root log lookup. The routed multilog stores full copies of entries. If you need the old bitmap-index pattern, consider using `indexes.SinkIndex` to build an addr→[]int64 mapping, then look up entries in the root log manually.

## 9. mutil.Indirect — Simplified

The old `mutil.Indirect` wrapped a "root log" and an "index sublog" (which stored sequence numbers pointing back to the root log). In v2 with typed sublogs, this indirection may no longer be needed:

```go
// OLD: sublog stores int64 root-log offsets
userSubLog, _ := userFeeds.Get(addr)    // Log of int64
resolvedLog := mutil.Indirect(rxLog, userSubLog) // looks up int64 in rxLog
src, _ := resolvedLog.Query(...)

// NEW option A: routed sublogs store full entries
userSubLog, _ := userFeeds.Get(addr)    // Log[*MultiMessage]
for seq, msg := range userSubLog.Query().Iter() {
    // msg is already the full message, no indirection needed
}

// NEW option B: if you still need index→rootlog indirection
for _, rootSeq := range indexLog.Query().Iter() {
    msg, err := rxLog.Get(rootSeq)
    // ...
}
```

## 10. Changes() / luigi.Observable — Removed

The old API had `log.Changes()` returning a `luigi.Observable` for watching sequence updates. In v2, use live queries instead:

```go
// OLD
obs := log.Changes()
cancel := obs.Register(luigi.FuncSink(func(ctx context.Context, v interface{}, closeErr error) error {
    newSeq := v.(int64)
    // react to new sequence
}))

// NEW
go func() {
    for seq, val := range log.Query(margaret.Live(ctx)).Iter() {
        // react to new entries
    }
}()
```

## 11. Error Handling Changes

```go
// OLD
import "github.com/ssbc/go-luigi"

if luigi.IsEOS(err) { break }            // end of stream
if margaret.IsErrNulled(err) { continue } // nulled entry

// NEW
// EOS is implicit — the range loop ends naturally
// Nulled entries are skipped by the iterator automatically
// Check qry.Err() after the loop for actual errors:
qry := log.Query()
for seq, val := range qry.Iter() { ... }
if err := qry.Err(); err != nil {
    // handle real errors
}
```

## 12. offset2.Open

```go
// OLD
import "github.com/ssbc/margaret/offset2"
import "github.com/ssbc/margaret/codec/msgpack"

log, err := offset2.Open(path, msgpack.New(&multimsg.MultiMessage{}))

// NEW
import "github.com/ssbc/margaret/v2/offset2"

log, err := offset2.Open[*multimsg.MultiMessage](path)
```

## 13. Key Patterns in go-ssb

### Publisher

```go
// OLD
type Publisher interface {
    margaret.Log
    Publish(content interface{}) (refs.Message, error)
}

// NEW
type Publisher interface {
    margaret.Log[*MultiMessage]
    Publish(content interface{}) (refs.Message, error)
}
```

### Sbot Index Serving

```go
// OLD (sbot/indexes.go)
func (s *Sbot) serveIndexFrom(name string, snk librarian.SinkIndex, msgs margaret.Log) {
    src, _ := msgs.Query(margaret.Live(false), margaret.SeqWrap(true), snk.QuerySpec())
    luigi.Pump(s.rootCtx, progressSink, src)  // backlog

    src, _ = msgs.Query(margaret.Live(true), margaret.SeqWrap(true), snk.QuerySpec())
    luigi.PumpWithStatus(s.rootCtx, snk, src, ...) // live
}

// NEW
func (s *Sbot) serveIndexFrom(name string, sink *indexes.SinkIndex[*MultiMessage, int64], msgs margaret.Log[*MultiMessage]) {
    // SinkIndex.Index() handles resume + iteration internally
    sink.Index(msgs)

    // For live: use a Live query directly
    qry := msgs.Query(margaret.Live(s.rootCtx), margaret.Gt(sink.LastSeq()))
    for seq, val := range qry.Iter() {
        sink.Process(seq, val)
    }
}
```

### createLogStream Handler

```go
// OLD (plugins/rawread/rxlog.go)
src, err := g.root.Query(
    margaret.SeqWrap(false),
    margaret.Gte(qry.Seq),
    margaret.Limit(qry.Limit),
    margaret.Live(qry.Live),
    margaret.Reverse(qry.Reverse),
)
err = luigi.Pump(ctx, transform.NewKeyValueWrapper(snk, qry.Keys), src)

// NEW — with generic muxrpc v3 iterators
opts := []margaret.QueryOption{
    margaret.Gte(qry.Seq),
    margaret.Limit(qry.Limit),
    margaret.Reverse(qry.Reverse),
}
if qry.Live {
    opts = append(opts, margaret.Live(ctx))
}
qry := g.root.Query(opts...)
for seq, msg := range qry.Iter() {
    // send msg over muxrpc stream
    encoder.Encode(msg)
}
```

### FeedsWithSeqs

```go
// OLD (sbot.go)
subLog, err := feedIndex.Get(idxAddr)
seq := subLog.Seq() + 1

// NEW — same pattern, typed
subLog, err := feedIndex.Get(multilog.Addr(idxAddr))
seq := subLog.Seq() + 1
```

## Migration Order

Recommended order for migrating go-ssb:

1. **multimsg.MultiMessage** — add `MarshalBinary`/`UnmarshalBinary` methods (replaces codec)
2. **repo/log.go** — change `offset2.Open` to generic form
3. **sbot.go** — update `Publisher` and core interfaces to generic types
4. **sbot/new.go** — update `ReceiveLog` and multilog construction
5. **multilogs/** — replace roaring multilogs with routed or SinkIndex
6. **sbot/indexes.go** — replace `serveIndex`/`serveIndexFrom` with v2 patterns
7. **indexes/** — migrate simple indexes (get, timestamps)
8. **plugins/** — update muxrpc handlers to use iterators
9. **internal/mutil/** — simplify or remove `Indirect` if sublogs store full entries
10. **graph/** — update graph builder indexing
11. **Remove dependencies** — drop go-luigi, badger index backends, roaring backends
