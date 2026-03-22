# Design: Frontier as Vector Clock

## Problem Statement

go-ssb needs a **comparable** representation of "what data has this node seen?"
that supports:

1. **Happened-before checks** — "is my cached query result still valid given
   the current state?" Without this, the only option is re-running queries or
   using hash-based invalidation (which gives you "same/different" but not
   "still valid / needs update").

2. **Fork proofs** — SSB has struggled with feed forks (same author, same
   sequence, different content). A frontier + message hash at a specific point
   constitutes a verifiable claim about what a node saw, enabling fork detection
   without trusting any single peer.

3. **Incremental pipeline inputs** — For shellshock-style DAG pipelines, a
   frontier tells a downstream node exactly which feeds advanced and by how
   much, enabling incremental recomputation instead of full re-runs.

## Why Not a Hash?

A hash (SHA256 of sorted `feed:seq` pairs) gives you equality comparison only:

```
hash(F1) == hash(F2)  → same worldview
hash(F1) != hash(F2)  → different, but HOW different? No idea.
```

A vector clock preserves the **partial order**. SSB feeds are append-only, so
each `(feed, seq)` pair is a Lamport-style counter. The frontier is the vector
of all of them.

## The Partial Order

```
F1 ≤ F2   iff   ∀ feed ∈ F1: seq₁[feed] ≤ seq₂[feed]
           and   F1.feeds ⊆ F2.feeds
```

| Comparison | Meaning |
|---|---|
| `F1 ≤ F2` | F2 saw everything F1 saw (plus maybe more). F1's result still holds within F2. |
| `F1 < F2` | Strictly later — F2 has strictly more data. |
| `F1 ∥ F2` | Concurrent — F1 saw feeds/seqs F2 didn't, and vice versa. Can't compare. |
| `F1 = F2` | Identical worldview. |

### Cache Validity Example

When a shellshock pipeline asks "is my cached result still good?":

- **F_cached ≤ F_now**: Yes — no new data in the feeds that matter. Reuse.
- **F_cached < F_now**: No — some feeds advanced. `Diff()` tells you which
  ones and by how much → incremental update.
- **F_cached ∥ F_now**: Concurrent worldviews (e.g., feed replicated from a
  different peer with a fork, or feeds were dropped). Flag it.

## Fork Proofs

A **fork** in SSB means: same author published two different messages at the
same sequence number. Classic SSB has no built-in mechanism to prove this to
third parties.

A `ForkProof` combines:
1. A `Frontier` — the worldview at the time of observation
2. Two conflicting messages (same author, same seq, different key/content)

The frontier establishes *context* — "I had this worldview when I observed
these two conflicting messages." This is important because:

- Without the frontier, a fork proof is just two messages. A malicious peer
  could fabricate them. With a frontier, the proof is tied to a verifiable
  state that other peers can cross-reference.
- The frontier's vector clock properties mean proofs compose: if peer A's
  frontier ≤ peer B's frontier, and A saw a fork, B should have seen it too
  (or something is wrong with B).

### ForkProof Structure

```go
type ForkProof struct {
    // The frontier at time of observation — establishes context
    Observed Frontier

    // The two conflicting messages (same author, same seq)
    Left  refs.Message
    Right refs.Message
}
```

Validation: `Left.Author() == Right.Author()`, `Left.Seq() == Right.Seq()`,
`Left.Key() != Right.Key()`, and both messages verify (valid signatures).

## Compact Binary Encoding

In practice, frontiers are scoped to a specific concern — not the whole
network. Three concrete shapes:

| Frontier scope | Typical feeds | Example |
|---|---|---|
| **Social (follows)** | ~50-100 | "Has my timeline changed since last render?" |
| **Thread (tangle)** | ~2-20 | "Has this conversation advanced?" |
| **git-ssb repo** | ~3-30 | "Any new commits/issues/PRs since last CI?" |

All well under 100 feeds. At 100 feeds with average seq ~5000:
- Uncompressed: `~3.4KB` — fits in a single SSB message
- Wire format is sorted `(pubkey, varint seq)` pairs
- Vector clock comparison is O(n) where n < 100 — essentially free

This means frontiers can be **published** as SSB messages: "this is the
worldview my computation used." A git-ssb CI bot can publish its frontier
alongside build results. A thread summary can declare which messages it saw.

### Wire Format

```
[count: varint]                           # number of feeds
for each feed (sorted by raw pubkey bytes):
    [algo + pubkey: varint-prefixed]      # feed identity
    [seq: varint]                         # feed sequence number
```

### Relationship to Existing NetworkFrontier

`ssb.NetworkFrontier` (in `ebt.go`) is a `map[string]Note` designed for EBT
replication protocol exchange. It includes replication control bits (`Replicate`,
`Receive`) that are protocol-level concerns.

The new `Frontier` type is a **data structure** — a pure vector clock with no
protocol semantics. The relationship:

- `NetworkFrontier` → EBT wire protocol, includes control bits
- `Frontier` → value type for comparison, caching, fork proofs
- Conversion: `Frontier.FromNetworkFrontier(nf)` strips control bits, keeps
  feed→seq mapping

They coexist because they serve different layers:
- `NetworkFrontier` is a protocol message ("here's what I have and what I want")
- `Frontier` is a logical clock ("here's what the world looked like when X happened")

## API Design

```go
// Frontier is a vector clock over SSB feeds.
// Each entry maps a feed to the highest sequence number observed.
// The zero value is an empty frontier.
type Frontier struct {
    feeds []FeedSeq // always sorted by feed ref bytes
}

type FeedSeq struct {
    Feed refs.FeedRef
    Seq  int64
}

// Comparison (the partial order)
func (f Frontier) HappenedBefore(other Frontier) bool  // f ≤ other (strict: f < other implied by !Equal)
func (f Frontier) Concurrent(other Frontier) bool       // f ∥ other
func (f Frontier) Equal(other Frontier) bool             // f = other

// Incremental pipeline support
func (f Frontier) Diff(other Frontier) []FeedAdvance     // what changed between f and other
func (f Frontier) Merge(other Frontier) Frontier          // element-wise max (join / LUB)

// Construction
func NewFrontier(entries ...FeedSeq) Frontier
func (f Frontier) Set(feed refs.FeedRef, seq int64) Frontier

// Encoding
func (f Frontier) Marshal() ([]byte, error)
func UnmarshalFrontier(data []byte) (Frontier, error)

// Interop
func FrontierFromNetworkFrontier(nf NetworkFrontier) (Frontier, error)
```

```go
type FeedAdvance struct {
    Feed   refs.FeedRef
    OldSeq int64  // -1 if feed is new in `other`
    NewSeq int64  // -1 if feed was dropped in `other`
}
```

## Criticisms and Open Questions

### 1. Scoping: Which Feeds Belong in a Frontier?

A frontier must be **scoped** — it tracks only the feeds relevant to a
specific concern. Including unrelated feeds wastes space and makes
`HappenedBefore` return false negatives (an unrelated feed advancing
doesn't invalidate a thread frontier).

Scoping strategies:
- **Social frontier**: `graph.Follows(me)` → all feeds I follow
- **Thread frontier**: Participants in a tangle (from tangle multilog)
- **Repo frontier**: Contributors listed in the git-ssb repo metadata
- **Query frontier**: The `authors` bitmap from a subset query defines
  which feeds the result depends on

The `SubsetPlaner` already knows which feeds a query touches (via author
bitmaps). A natural extension: after evaluating a query, also return the
frontier of feeds that contributed to the result. This makes query results
self-describing: "here's the data, and here's exactly when it becomes stale."

### 2. Feed Algorithm Support

Current `NetworkFrontier` only handles `RefAlgoFeedSSB1` (see `ebt.go:64`).
The new `Frontier` type should support all feed algorithms (Gabby Grove,
BendyButt) from the start, since metafeeds are increasingly important.

### 3. Fork Proof Propagation

How do fork proofs travel through the network? Options:
- **Gossip**: Peers exchange fork proofs like messages. Pro: automatic
  propagation. Con: spam vector (fabricated "proofs" with valid signatures
  from compromised keys).
- **On-demand**: Only share fork proofs when asked, or when replication of
  the forked feed is attempted. Lower overhead but slower propagation.
- **Published messages**: Author a `type:fork-proof` message on your own feed.
  Permanent, attributable, but increases feed size.

### 4. Monotonicity Guarantee

Because SSB feeds can't go backwards, `F1 ≤ F2` is monotonic — once a frontier
is satisfied, it stays satisfied forever. This is a CRDT-like property that
makes frontiers safe for caching without invalidation races.

**Exception**: Feed deletion / unfollow. If a node stops replicating a feed,
its local frontier for that feed freezes. A new frontier missing a feed that
was in an old one means `F_old ∥ F_new` (concurrent), not `F_old > F_new`.
This is the correct semantics — the worldviews are genuinely incomparable.

## Remarks: Shellshock / OTel Integration

### Merging URL and Path for OTel Spans

The current metrics stack uses go-kit/prometheus (`cmd/go-sbot/metrics.go`,
`network/conntracker.go`). There is no OpenTelemetry integration yet.

When adding OTel tracing to shellshock pipelines, the span naming convention
matters. The concern raised is about merging URL and path into span names:

- **Problem**: OTel recommends low-cardinality span names. If shellshock
  pipeline node names include dynamic content (feed refs, query parameters),
  every execution creates a unique span name, making traces unsearchable and
  blowing up storage.
- **Recommendation**: Use span **attributes** for high-cardinality data
  (feed refs, sequence numbers, frontier hashes), keep span **names** as
  static templates:
  ```
  span name:  "shellshock.pipeline.node"
  attributes:
    ssb.pipeline.name: "timeline-rebuild"
    ssb.pipeline.node: "filter-by-author"
    ssb.frontier.size: 342
    ssb.frontier.hash: "a3f2..."   (for quick equality check)
    ssb.feeds.advanced: 5          (from Diff)
  ```
- **Frontier as span context**: A frontier can serve as a natural "trace
  context" for SSB operations. Two operations with the same frontier are
  working from the same worldview. The frontier hash makes a good
  correlation ID.

### Pipeline Cache Keying

For shellshock DAG nodes:
- **Input**: `(query, frontier)` pair
- **Cache key**: `hash(query) + frontier` (not hash of frontier — we need
  the vector clock for comparison)
- **Cache hit**: If stored frontier `≤` current frontier and query hasn't
  changed → result is still valid
- **Incremental update**: `Diff(stored, current)` → only process the
  advanced feeds

This is where frontiers shine over hashes: a hash can only tell you
"invalidated." A frontier tells you "still valid" or "here's exactly what
changed."

## Files to Create/Modify

| File | Purpose |
|---|---|
| `frontier.go` | `Frontier` type, vector clock comparisons, `FeedSeq`, `FeedAdvance` |
| `frontier_encoding.go` | `Marshal`/`Unmarshal`, compact binary format |
| `frontier_test.go` | Tests for comparisons, encoding, edge cases |
| `fork_proof.go` | `ForkProof` type, validation |
| `fork_proof_test.go` | Fork proof validation tests |

## Implementation Order

1. `Frontier` type with vector clock operations (core value)
2. Binary encoding (needed for storage/wire)
3. `ForkProof` type (builds on Frontier)
4. `FrontierFromNetworkFrontier` bridge (connects to existing EBT)
5. (Future) Integration with query/SubsetPlaner — frontier-scoped queries
6. (Future) OTel span decoration with frontier attributes
