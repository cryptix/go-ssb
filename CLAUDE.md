# go-ssb Development Guide

## Project Overview

go-ssb is a Go implementation of the Secure Scuttlebutt (SSB) protocol. It provides peer-to-peer social networking with append-only message logs, cryptographic identities, and gossip-based replication.

## Build & Test

```bash
go build ./...           # Build all packages
go test ./...            # Run all tests
go test ./sbot/...       # Run sbot tests (integration-heavy, slower)
go test -race ./...      # Race detection
go vet ./...             # Lint
```

Tests frequently use `testutils` helpers and `testops` for filesystem setup. Many integration tests create temporary sbot instances with `makeTestBot()`.

## Key Commands

- `/plan` - Implementation planning
- `/learn-eval` - Extract and evaluate patterns from sessions
- `/skill-create` - Generate skills from git history

## Go Conventions

- Follow Effective Go and the Go Code Review Comments guide
- Use `errors.New` / `fmt.Errorf` with `%w` for wrapping — never string matching on errors
- No `init()` functions — explicit initialization in `main()` or constructors
- No global mutable state — pass dependencies via constructors
- Context must be the first parameter and propagated through all layers
- Return errors, don't panic — panics are only for truly unrecoverable situations
- Wrap errors with context: `fmt.Errorf("creating user: %w", err)`

## Code Style

- No emojis in code or comments
- Exported types and functions must have doc comments
- Comment the "why", not the "what". Keep comments short
- Keep functions under 50 lines — extract helpers
- Use table-driven tests for all logic with multiple cases
- Prefer `struct{}` for signal channels, not `bool`

## Architecture

### Core Data Flow

```
Message Arrives (network RPC / local publish)
  → Validate (signature, hash, format)
  → Append to ReceiveLog (root log, sequence N)
  → CombinedIndex.ProcessEntry(N, msg)
    → Update Users multilog (author → bitmap of seqs)
    → Update ByType multilog (msg type → bitmap of seqs)
    → Update Tangles multilog (thread root → bitmap of seqs)
    → Update Private multilog (box1/box2 tracking)
    → Update EBT state matrix
  → Graph builder processes contact messages
  → About/Names plugin processes profile messages
```

### Storage Layout

```
.ssb-go/
  log/                              # Root ReceiveLog (margaret append-only log)
  indexes/
    shared-badger/                  # BadgerDB for simple indexes + graph
  sublogs/
    userFeeds/fs-bitmaps/           # Roaring bitmaps: feed → {seq...}
    private/fs-bitmaps/             # Roaring bitmaps: box visibility
    msgTypes/fs-bitmaps/            # Roaring bitmaps: type → {seq...}
    tangles/fs-bitmaps/             # Roaring bitmaps: thread root → {seq...}
    combined-state.json             # Last processed sequence
```

### Key Dependencies

| Dependency | Purpose |
|---|---|
| `margaret/v2` | Log abstraction, index interfaces, roaring multilog |
| `badger/v3` | Key-value store for simple indexes and graph |
| `sroar` (dgraph-io) | Roaring bitmap operations (AND, OR, set ops) |
| `go-ssb-refs` | SSB reference types (FeedRef, MessageRef, etc.) |
| `go-muxrpc/v2` | RPC framework for SSB protocol |
| `secretstream` | Encrypted network transport (SHS) |

### Index Types

**Simple Indexes** (`indexes/`) — Map a single key to an int64 (sequence number):
- `byMsgRef` (Get index): message ref → receive log sequence
- Timestamps: sequence → claimed/received time
- Backed by BadgerDB via `repo.BadgerIndex`

**Multilogs** (`multilogs/`) — Map a key to a roaring bitmap of sequences:
- `userFeeds`: feed ref → all messages from that feed
- `msgTypes`: message type string → all messages of that type
- `tangles`: thread root ref → all messages in thread
- `private`: box1/box2 visibility tracking
- Backed by `margaret/v2/multilog/roaring`

**Graph Index** (`graph/`) — Trust/follow/block relationships from contact messages, stored in BadgerDB.

**About Index** (`plugins2/names/`) — Profile data (name, description, image) from about messages, stored in BadgerDB.

### Query System

`query/subsetquery_plan.go` implements bitmap-based query composition:
- `SubsetPlaner` combines `authors` and `bytype` multilogs
- Operations: `author`, `type`, `and`, `or`
- Returns `*sroar.Bitmap` of matching receive log sequences
- Results are resolved by reading from ReceiveLog at those sequences

### Key Patterns

- **Incremental indexing**: Each index tracks `LastProcessedSeq` to resume after restart
- **CombinedIndex**: Single index updates all 4 multilogs in one pass (less read overhead)
- **Bitmap set operations**: AND/OR on roaring bitmaps for efficient query composition
- **Box2Reindex**: When group keys are revealed, re-index previously-unreadable messages using bitmap intersection
- **serveIndex goroutines**: Each index runs in its own goroutine, processing backlog then live messages

### Key Source Files

| File | Purpose |
|---|---|
| `sbot/new.go` | Sbot initialization, index creation |
| `sbot/indexes.go` | Index lifecycle, sync, LogIndexer interface |
| `multilogs/combined.go` | CombinedIndex: main indexing logic |
| `multilogs/userfeeds.go` | User feeds multilog setup |
| `indexes/get.go` | Message ref → sequence lookup |
| `repo/badger_index.go` | BadgerIndex / BadgerSeqIndex implementations |
| `query/subsetquery.go` | Subset query AST (type, author, search, and, or) |
| `query/subsetquery_plan.go` | Bitmap query execution + Searcher interface |
| `graph/builder.go` | Trust graph construction |
| `plugins2/names/about.go` | About/profile indexing |
| `multilogs/search_index.go` | Bleve full-text search index |

### Full-Text Search (Bleve)

Enabled via `sbot.EnableSearch()` option. The `SearchIndex` in `multilogs/search_index.go`:
- Indexes `post` (text, channel) and `about` (name, description) messages
- Uses Bleve with scorch backend, stored at `.ssb-go/sublogs/search/`
- Follows the same `LogIndexer` pattern as `CombinedIndex` (incremental, batch processing)
- `Search(query, limit)` returns `*sroar.Bitmap` of matching receive log sequences
- Integrates with `SubsetPlaner` via `query.Searcher` interface
- Composable: `{"op":"and","args":[{"op":"search","string":"hello"},{"op":"type","string":"post"}]}`
- Skips encrypted messages (only indexes cleartext)
- Document IDs are stringified receive log sequence numbers

### Dependencies Note

`margaret/v2` and `go-muxrpc/v3` are unpublished v2/v3 modules available as branches on GitHub (`ssbc/margaret` branch `v2`, `ssbc/go-muxrpc` branch `v3`). The go.mod uses pseudo-versions pointing to these branch tips. After adding bleve, run `go mod tidy` with Go 1.25+ to update go.sum.

---

## Full Text Search Index Investigation (March 2026)

### Requirements

A full text search index for go-ssb should:
1. Be an **embedded Go library** (no external server)
2. Support **incremental indexing** (add documents as they arrive in the log)
3. Return results as **integer IDs** mappable to receive log sequence numbers
4. Support standard FTS features: tokenization, stemming, relevance ranking
5. Integrate with the existing **roaring bitmap query system** (results as bitmaps for AND/OR with other filters)
6. Handle SSB-scale data: tens of thousands to low millions of messages per node

### Candidates Evaluated

#### 1. Bleve (`blevesearch/bleve/v2`) — RECOMMENDED

- **Type**: Pure Go full text search library
- **GitHub**: ~10k stars, actively maintained by Couchbase
- **Scoring**: TF/IDF with query-time boosting (BM25 available via config)
- **Storage**: Scorch engine (segmented index with bolt/mmap backends)
- **Integration fit**:
  - Documents indexed with string IDs → use `strconv.FormatInt(seq, 10)` as document ID
  - Search returns `DocumentMatch` with ID field → parse back to int64 → feed into roaring bitmap
  - Supports `Batch()` for efficient bulk indexing
  - Incremental: call `Index(id, doc)` for each new message
  - Custom analyzers for SSB content (markdown-aware, mention-aware)
- **Pros**:
  - Pure Go, no CGo required (simpler builds, cross-compilation)
  - Battle-tested (used by Couchbase, Caddy, others)
  - Rich feature set: facets, highlighting, fuzzy matching, geo
  - Pluggable storage backends
  - Well-documented API
- **Cons**:
  - Slower than Tantivy/Lucene for raw indexing/search speed
  - Single-threaded indexing
  - Higher memory usage than SQLite FTS5
- **Integration sketch**:
  ```
  CombinedIndex.ProcessEntry(seq, msg):
    ... existing multilog updates ...
    if msg.Type == "post" || msg.Type == "about" || msg.Type == "blog":
      bleveIndex.Index(strconv.FormatInt(seq, 10), searchDoc)

  Query("hello world"):
    searchRequest := bleve.NewSearchRequest(bleve.NewQueryStringQuery("hello world"))
    result := bleveIndex.Search(searchRequest)
    bitmap := sroar.NewBitmap()
    for _, hit := range result.Hits:
      seq, _ := strconv.ParseInt(hit.ID, 10, 64)
      bitmap.Set(uint64(seq))
    // Can now AND/OR this bitmap with author/type filters
  ```

#### 2. Bluge (`blugelabs/bluge`) — VIABLE ALTERNATIVE

- **Type**: Pure Go, successor to Bleve by Bleve's original author
- **GitHub**: ~2k stars, low maintenance activity (issues from 2022+ unanswered)
- **Scoring**: BM25 by default
- **Integration fit**: Similar to Bleve but with cleaner API; supports external integer IDs natively
- **Pros**: Modern design, BM25 default, slightly better performance than Bleve
- **Cons**: Semi-abandoned maintenance, smaller community, less documentation
- **Verdict**: Good design but maintenance risk is too high for a core feature

#### 3. Tantivy via CGo (`anyproto/tantivy-go`)

- **Type**: Rust search engine with Go CGo bindings
- **GitHub**: tantivy has ~13k stars; tantivy-go bindings are newer (~200 stars)
- **Scoring**: BM25, block-max WAND
- **Performance**: ~2x faster than Lucene for search, multi-threaded indexing
- **Integration fit**:
  - `u64` fast fields can store receive log sequence numbers directly
  - High-performance search → bitmap conversion
  - Requires Rust toolchain for builds
- **Pros**: Fastest option by far, modern algorithms, excellent ranking
- **Cons**:
  - CGo boundary adds complexity and overhead per call
  - Requires Rust compiler in build chain
  - Cross-compilation becomes much harder
  - Bindings are young, API may change
  - go-ssb currently has zero CGo dependencies — adding one is a significant decision
- **Verdict**: Best performance but CGo/Rust dependency is a poor fit for a pure-Go project

#### 4. SQLite FTS5 (`mattn/go-sqlite3` or `modernc.org/sqlite`)

- **Type**: SQLite's built-in full text search extension
- **Two Go options**:
  - `mattn/go-sqlite3`: CGo wrapper (faster, requires C compiler)
  - `modernc.org/sqlite`: Pure Go transpilation (no CGo, slower)
- **Scoring**: BM25 built-in
- **Integration fit**:
  - Table: `CREATE VIRTUAL TABLE search USING fts5(content, type, author)`
  - Store `rowid` = receive log sequence number (natural mapping)
  - Query returns rowids → directly usable as bitmap entries
  - `modernc.org/sqlite` keeps the pure-Go build chain
- **Pros**:
  - FTS5 is extremely well-tested and mature
  - Low memory overhead (disk-oriented)
  - `rowid` maps perfectly to receive log sequences
  - `modernc.org/sqlite` avoids CGo entirely
  - Supports phrase queries, prefix queries, column filters, BM25 ranking
  - ACID transactions
- **Cons**:
  - SQLite is a relational DB; conceptual mismatch with log-oriented architecture
  - `modernc.org/sqlite` is ~2-3x slower than native SQLite
  - Less flexibility for custom tokenizers/analyzers compared to Bleve
  - Adding SQLite as a dependency when the project already uses Badger
- **Verdict**: Pragmatic choice if simplicity and low memory are priorities; `modernc.org/sqlite` variant keeps pure-Go

#### 5. ZincSearch — NOT SUITABLE

- **Type**: Standalone search server (like lightweight Elasticsearch)
- **Status**: Uses Bluge internally, ~17k stars
- **Verdict**: Not embeddable as a library; it's a separate server process. Not suitable for go-ssb's embedded architecture.

### Recommendation

**Primary: Bleve** — Best balance of features, maintenance, pure-Go compatibility, and integration with the existing architecture. The document ID → sequence number mapping is straightforward, and search results can be converted to roaring bitmaps for composition with existing subset queries.

**Alternative: SQLite FTS5 via `modernc.org/sqlite`** — If disk/memory efficiency is paramount and the feature set of FTS5 (phrase queries, BM25, prefix matching) is sufficient. The rowid-to-sequence mapping is the most natural of all options.

### Integration Architecture (Bleve)

```
New components:
  multilogs/search.go     — SearchIndex wrapping bleve.Index
                            Implements same incremental pattern as CombinedIndex
                            Indexes: post text, about name/description, channel names, blog content

  query/subsetquery.go    — Add "search" operation to SubsetOperation
                            Returns bitmap from Bleve results
                            Composes with existing author/type/and/or operations

  plugins2/search/        — MAPI plugin exposing search over muxrpc
                            search.query({query: "hello", limit: 20})

Storage:
  .ssb-go/indexes/search-bleve/   — Bleve index files

Indexing:
  CombinedIndex gains optional *SearchIndex field
  ProcessEntry extracts text content and calls SearchIndex.Index()
  Tracks own LastProcessedSeq for incremental startup
```

### What to Index

| Message Type | Fields to Index |
|---|---|
| `post` | `text` (main content, markdown) |
| `about` | `name`, `description` |
| `blog` | `title`, `summary`, blob content (if available) |
| `channel` | channel name |
| `contact` | (not indexed — no text content) |
| `vote` | (not indexed — no text content) |
| `pub` | (not indexed — no text content) |

### Privacy Considerations

- Private messages (box1/box2): Only index if successfully decrypted by this node
- The search index itself must be treated as sensitive (contains plaintext of private messages)
- On reindex (Box2Reindex), newly-decryptable messages should be added to the search index
- Search results must be filtered through the same visibility rules as normal queries
