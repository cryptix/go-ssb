# Design: OpenTelemetry Tracing for go-ssb

## Motivation

go-ssb currently has go-kit/prometheus metrics (counters, gauges, histograms)
exposed at `/metrics`. This gives aggregate numbers — bytes tx/rx, connection
counts, durations — but no **causal story**. You can see "replication is slow"
but not "replication of @alice's feed took 3s because index processing
blocked on graph builder."

Tracing gives you the causal chain: which peer triggered which work, how long
each stage took, where time was spent, and — with frontiers — what worldview
the work was based on.

### What We Want to Answer

1. **Sync**: "Why did replicating with peer X take 40 seconds?"
   - How many feeds? How many messages per feed?
   - Where was time spent: network, verification, storage, indexing?
   - Did the frontier advance? By how much?

2. **Queries**: "Why did this timeline query take 2 seconds?"
   - Which bitmap operations? How many results?
   - Did it hit the search index? How long did that take?
   - Was the graph builder involved? (followedBy, hops queries)
   - What frontier was the result based on?

3. **Indexing**: "What's the backlog processing throughput?"
   - Batch sizes, time per batch
   - Which index is the bottleneck?
   - How long until live?

## Current Observability Gaps

### Context Threading

The biggest gap is **context.Context not being passed** through critical paths.
OTel spans propagate via context, so no context = no tracing.

| Path | Context status |
|---|---|
| EBT `Loop()` → `flushPending()` → `VerifyBatch` → `SaveBatch` | ctx available but not passed to verify/save |
| Gossip `fetchFeed()` → `VerifyBatch` → `SaveBatch` | ctx available but not passed to verify/save |
| Query `HandleSource(ctx)` → `QuerySubsetBitmap(qry)` | **ctx received but never used** |
| Index `serveIndex` → `idx.Index(log)` | rootCtx available but `Index()` has no ctx param |
| `VerificationRouter.GetSink/SaveBatch` | No ctx parameter at all |
| `CombinedIndex.ProcessBatch` | No ctx parameter at all |

### Missing Metrics

- Query execution time (no histogram, no per-operation breakdown)
- Verification batch latency
- Index backlog depth and catch-up rate
- Per-peer replication throughput
- Frontier diff sizes (how much new data per sync)

## Approach: OTel API Only (No SDK in Library)

go-ssb is a **library** (not just a binary). The standard OTel practice:

- **Library code** (`go-ssb/`) depends only on `go.opentelemetry.io/otel` (the
  API package). This is zero-cost when no SDK is configured — all calls become
  no-ops.
- **Application code** (`cmd/go-sbot/`) wires up the OTel SDK, exporter, and
  sampler. This is where you choose stdout/OTLP/Jaeger export.

This means: importing the OTel API adds no overhead to users who don't enable
tracing. No SDK, no exporter, no goroutines — just interface calls that resolve
to no-ops.

## Span Hierarchy

### Sync Spans (EBT)

```
ssb.ebt.session                          ← per-peer session
  peer.id: @abc...
  direction: "duplex"
  │
  ├─ ssb.ebt.receive                    ← receiving messages from peer
  │    messages.total: 847
  │    feeds.count: 12
  │    │
  │    ├─ ssb.verify.batch              ← per-batch verification
  │    │    feed: @xyz...
  │    │    batch.size: 128
  │    │    batch.verified: 128
  │    │
  │    └─ ssb.rxlog.append              ← persist to receive log
  │         batch.size: 128
  │         seq.first: 94201
  │         seq.last: 94328
  │
  ├─ ssb.ebt.send                       ← sending messages to peer
  │    feeds.subscribed: 45
  │
  └─ ssb.ebt.frontier                   ← frontier exchange
       frontier.size: 67
       frontier.advanced: 5
       frontier.new_feeds: 1
```

### Sync Spans (Gossip)

```
ssb.gossip.session                       ← per-peer session
  peer.id: @abc...
  │
  └─ ssb.gossip.fetch                   ← per-feed fetch
       feed: @xyz...
       messages.fetched: 312
       │
       ├─ ssb.verify.batch
       └─ ssb.rxlog.append
```

### Query Spans

```
ssb.query.subset                         ← top-level query
  query.op: "and"
  result.count: 234
  frontier.size: 52
  │
  ├─ ssb.query.plan                     ← bitmap evaluation
  │    op: "followedBy"
  │    feeds.resolved: 47
  │    bitmap.cardinality: 12043
  │
  ├─ ssb.query.plan
  │    op: "type"
  │    type: "post"
  │    bitmap.cardinality: 89201
  │
  ├─ ssb.query.plan
  │    op: "and"
  │    bitmap.cardinality: 5821
  │
  ├─ ssb.query.search                   ← full-text search (if used)
  │    query: "hello world"
  │    results: 42
  │
  └─ ssb.query.emit                     ← result serialization
       messages.emitted: 234
       duration: 45ms
```

### Index Spans

```
ssb.index.backlog                        ← initial catch-up
  index.name: "combined"
  messages.total: 94000
  │
  └─ ssb.index.batch                    ← per-batch processing (repeated)
       batch.size: 128
       seq.first: 0
       seq.last: 127

ssb.index.live                           ← live update (repeated)
  index.name: "combined"
  trigger.seq: 94001
```

## Attribute Conventions

All SSB-specific attributes use the `ssb.` prefix:

| Attribute | Type | Description |
|---|---|---|
| `ssb.peer.id` | string | Remote peer feed ref (short sigil) |
| `ssb.feed` | string | Feed being processed (short sigil) |
| `ssb.feed.count` | int | Number of feeds involved |
| `ssb.message.count` | int | Number of messages in operation |
| `ssb.batch.size` | int | Batch size |
| `ssb.seq.first` | int | First sequence in range |
| `ssb.seq.last` | int | Last sequence in range |
| `ssb.frontier.size` | int | Number of feeds in frontier |
| `ssb.frontier.advanced` | int | Number of feeds that advanced (from Diff) |
| `ssb.query.op` | string | Query operation type |
| `ssb.query.type` | string | Message type filter |
| `ssb.bitmap.cardinality` | int | Bitmap result size |
| `ssb.index.name` | string | Index name |
| `ssb.search.query` | string | Full-text search query |

## Frontier Integration with Traces

Key insight: **a frontier is a natural trace correlation ID** for SSB.

When a query executes, the result depends on a specific frontier. When that
same frontier is seen in a sync trace, you can correlate: "this query result
was produced from data that arrived in *that* sync session."

Implementation:
1. After query bitmap evaluation, compute the frontier of feeds that
   contributed (from the author bitmaps)
2. Attach as span attribute: `ssb.frontier.size`, and record the frontier
   in a span event for detailed inspection
3. The same frontier (or one that `HappenedBefore` it) appears in EBT/gossip
   sync spans — giving you the end-to-end story

## Implementation Plan

### Phase 1: Foundation (this PR)

**Package `internal/tracing`** — thin wrapper around OTel API:

```go
package tracing

import "go.opentelemetry.io/otel"

var Tracer = otel.Tracer("go-ssb")
```

That's it. One line. All span creation uses `tracing.Tracer.Start(ctx, name)`.
When no SDK is configured, this is a no-op. When Jaeger/OTLP is configured
in `cmd/go-sbot`, spans flow automatically.

**Context threading** — add `context.Context` parameter where missing:

| Method | Change |
|---|---|
| `SubsetPlaner.QuerySubsetBitmap` | Add `ctx` first param |
| `SubsetPlaner.QuerySubsetMessages` | Add `ctx` first param |
| `Searcher.Search` | Add `ctx` first param |

These are the highest-value, lowest-risk changes. Query execution is the
first thing users want to understand.

### Phase 2: Sync tracing

Add `ctx` to verification/save path:

| Method | Change |
|---|---|
| `VerificationRouter.GetSink` | Add `ctx` |
| `VerificationRouter.SaveBatch` | Add `ctx` |
| `SequencedVerificationSink.VerifyBatch` | Add `ctx` |

Create spans in EBT `Loop()` and gossip `fetchFeed()`.

### Phase 3: Index tracing

| Method | Change |
|---|---|
| `LogIndexer.Index` | Add `ctx` first param |
| `CombinedIndex.ProcessBatch` | Add `ctx` |

Create spans in `serveIndex` for backlog and live processing.

### Phase 4: Frontier-enriched queries

After bitmap evaluation, compute the result's frontier and attach to span.
Return frontier alongside query results for self-describing responses.

## SDK Wiring (cmd/go-sbot)

The application configures the OTel SDK at startup, alongside existing
prometheus metrics:

```go
// cmd/go-sbot/tracing.go
func setupTracing(ctx context.Context) (func(), error) {
    // OTEL_EXPORTER_OTLP_ENDPOINT env var controls where traces go
    // Default: no-op (tracing disabled)
    // Set to "http://localhost:4318" for local Jaeger/Grafana Tempo
    exporter, err := otlptracehttp.New(ctx)
    if err != nil {
        return nil, err
    }
    tp := sdktrace.NewTracerProvider(
        sdktrace.WithBatcher(exporter),
        sdktrace.WithResource(resource.NewWithAttributes(
            semconv.SchemaURL,
            semconv.ServiceName("go-sbot"),
            attribute.String("ssb.feed", self.String()),
        )),
    )
    otel.SetTracerProvider(tp)
    return func() { tp.Shutdown(ctx) }, nil
}
```

The existing prometheus metrics continue working unchanged. OTel adds traces
alongside them. In the future, OTel metrics could replace go-kit/prometheus,
but that's a separate migration.

## Coexistence with go-kit/prometheus

- **Keep** existing go-kit metrics — they're working and cover aggregate stats
- **Add** OTel tracing for causal/structural insight
- **Future**: Migrate go-kit metrics to OTel Meter API for unified export
  (both prometheus and OTLP from one set of instruments)
- The go-kit `Counter`/`Gauge`/`Histogram` types and OTel `Meter` can
  coexist in the same binary without conflict

## Files to Create/Modify

### Phase 1 (this PR)
| File | Change |
|---|---|
| `internal/tracing/tracing.go` | Package with `Tracer` var |
| `query/subsetquery_plan.go` | Add `ctx` to `QuerySubsetBitmap`, `QuerySubsetMessages`, `combineBitmaps` |
| `query/subsetquery.go` | Add `ctx` to `Searcher.Search` interface |
| `plugins/partial/subset.go` | Pass `ctx` through to query planner |
| `multilogs/search_index.go` | Update `Search` to accept `ctx` |
| `go.mod` / `go.sum` | Add `go.opentelemetry.io/otel` dependency |

### Phase 2
| File | Change |
|---|---|
| `message/verifier.go` | Add `ctx` to `GetSink`, `SaveBatch` |
| `message/drains.go` | Add `ctx` to `VerifyBatch` |
| `plugins/ebt/handler.go` | Spans in `Loop()`, pass `ctx` to verify/save |
| `plugins/gossip/fetch.go` | Spans in `fetchFeed()`, pass `ctx` to verify/save |

### Phase 3
| File | Change |
|---|---|
| `sbot/indexes.go` | Add `ctx` to `LogIndexer.Index`, spans in `serveIndex` |
| `multilogs/combined.go` | Add `ctx` to `Index`, `ProcessBatch` |

### Phase 4
| File | Change |
|---|---|
| `query/subsetquery_plan.go` | Compute frontier from result bitmap, return alongside |
| `frontier.go` | Helper: `FrontierFromBitmapAndMultilog` |
