# go-ssb Renovation Plan

## Context

go-ssb is a mature Go implementation of the SSB protocol that has accumulated significant technical debt. The three primary painpoints identified by the maintainer are:

1. **Giant sbot object/package** - The `Sbot` struct has 40+ fields and `sbot/new.go` is 1070 lines. It's a God Object mixing state, networking, plugins, and indexes.
2. **Klunky tests** - 34 packages have zero tests, existing tests use hardcoded sleeps, lack `t.Parallel()`, and are fragile.
3. **Memory leaks for long-running nodes** - Goroutine leaks, missing context propagation, unbuffered channels that block, and no shutdown timeouts.

Additional issues found: broken error checking, public API typos, 44 panics in non-test code, race conditions in broadcasts, and unsafe shutdown ordering.

---

## Phase 1: Stop the Bleeding (Memory Leaks & Crash Risks)

Priority: **Critical** | Risk: High if left unfixed | Scope: Surgical fixes, no refactoring

### 1.1 Fix goroutine leaks

- **sbot/indexes.go:146-161**: Progress ticker goroutine - move `cancel()` to `defer cancel()` immediately after `context.WithCancel()`. Currently only called on success path.
- **plugins/gossip/feed_manager.go:70**: `go fm.serveLiveFeeds()` - add WaitGroup tracking and ensure `Close()` waits for goroutine exit.
- **network/new.go:418-440**: Unbuffered `newConn` channel can block Accept loop. Add buffer: `make(chan net.Conn, 8)`.
- **network/new.go:189-199**: WebSocket HTTP server goroutine - use `httpServer.Shutdown(ctx)` in Close() instead of `httpServer.Close()`.

### 1.2 Fix shutdown ordering and timeouts

- **sbot/new.go:998-1059**: Reorder shutdown: wait for `idxDone` errgroup BEFORE closing network node. Add `context.WithTimeout` (30s) around `idxDone.Wait()` to prevent infinite hang.
- **cmd/go-sbot/main.go:428-442**: Replace `time.Sleep(2*time.Second)` with channel-based synchronization. Wait for `sbot.Close()` to complete, then exit.
- **sbot/indexes.go:188, graph/builder_indexing.go:41, plugins2/names/about.go:55**: Replace `time.AfterFunc(100ms)` debouncing with proper channel signaling.

### 1.3 Fix lock contention

- **sbot/new.go:592-594**: `mkHandler` holds `closedMu` during all auth checks and I/O. Minimize lock scope to only protect the `s.closed` boolean check, then release before doing network/graph operations.

### 1.4 Fix race condition in broadcasts

- **internal/broadcasts/blobstore.go:44,54**: `emit()` deletes from `bcst.sinks` map in goroutine while main loop iterates. Collect deletions and apply after iteration, or copy the sinks slice before iterating.

### 1.5 Fix missing context propagation

- **network/tunnel_dial.go:36**: Accept context parameter instead of `context.TODO()`.
- **client/client.go:58**: Accept context in constructor or require it via option.
- **blobstore/wants.go:36**: Accept context from caller instead of `context.Background()`.

### 1.6 Fix resource leak on partial initialization

- **sbot/new.go:498-499**: Move `s.closers.AddCloser(s.indexStore)` and `s.closers.AddCloser(s.boltDB)` to immediately after opening the databases (~line 260-270), not after all index setup. This prevents DB leak if index initialization fails.

### Verification
- `go test -race ./...` passes
- `go vet ./...` clean
- Run sbot for 24h+ under load, monitor goroutine count with pprof (`/debug/pprof/goroutine`). Count should stabilize, not grow.
- Kill sbot with SIGTERM, verify clean shutdown within 30s (no hang).

---

## Phase 2: Fix Correctness Issues (Error Handling & API)

Priority: **High** | Risk: Silent data loss, broken error paths | Scope: Focused fixes

### 2.1 Fix broken errors.Is() checks

- **errors.go:27-39**: `errors.Is(err, ErrWrongType{})` always returns false because struct types without `Is()` method use `==` comparison. Fix by adding `Is()` methods to `ErrWrongType` and `ErrMalfromedMsg` that compare by type only:
  ```go
  func (e ErrWrongType) Is(target error) bool {
      _, ok := target.(ErrWrongType)
      return ok
  }
  ```
- **errors.go:35**: `errors.Is(err, &json.SyntaxError{})` - replace with `errors.As(err, &json.SyntaxError{})`.

### 2.2 Fix public API typos (with deprecation aliases)

- **errors.go:42**: Rename `ErrMalfromedMsg` to `ErrMalformedMsg`. Add deprecated alias: `type ErrMalfromedMsg = ErrMalformedMsg`.
- **errors.go:63**: Rename `ErrUnuspportedFormat` to `ErrUnsupportedFormat`. Add deprecated alias: `var ErrUnuspportedFormat = ErrUnsupportedFormat`.

### 2.3 Audit and fix swallowed errors

- **plugins2/names/about.go**: Replace `_ = err` with proper error handling or explicit comment.
- **multilogs/membership_index.go**: Same.
- **network/network_advertiser.go**: `_ = .Close()` - at minimum log the error.

### 2.4 Replace panics with error returns where possible

Audit the 44 panic sites. Priority targets (non-test, non-init):
- **internal/storedrefs/storedrefs.go** (4 panics) - these are in serialization code, should return errors
- **blobstore/wants.go** (2 panics)
- **graph/builder.go** (2 panics)
- **cmd/go-sbot/main.go** (6 panics) - many are in flag parsing, acceptable but review

### 2.5 Remove init() functions

Per project conventions, migrate to explicit initialization:
- **message/legacy/encode.go:280**: Move init logic to package-level var with sync.Once or explicit init call.
- **network/network.go:15**: Same.
- **sbot/manifest.go:39**: Same.
- **cmd/sbotcli/main.go:61**, **cmd/ssb-keygen/keygen.go:36**: Flag registration in init() is standard Go - leave these.

### Verification
- `go build ./...` still compiles
- `go test ./...` passes
- Grep for `errors.Is.*ErrWrongType` and `errors.Is.*ErrMalfromedMsg` - all callers updated
- Grep for remaining `_ =` patterns in non-test code

---

## Phase 3: Decompose the Sbot God Object

Priority: **High** | Risk: Medium (refactoring) | Scope: Large but incremental

### 3.1 Extract IndexManager

Create `sbot/index_manager.go` (~200 lines):
- Move index-related fields from Sbot: `combIdx`, `indexStore`, `boltDB`, index sync state, `serveIndex` goroutine management
- Move `WaitUntilIndexesAreSynced()`, `indexSyncDone()`, `serveIndex()`, index creation logic from `New()`
- Sbot holds `*IndexManager` instead of 10+ individual index fields
- Key files: `sbot/new.go:386-500` (index setup), `sbot/indexes.go` (full file)

### 3.2 Extract NetworkManager

Create `sbot/network_manager.go` (~200 lines):
- Move network-related fields: `Network`, `node`, connection tracking, `connTracker`
- Move `mkHandler` closure (sbot/new.go:591-665) into a proper `HandlerFactory` type with explicit dependencies
- Move network setup from `New()` (~lines 530-580)
- Key files: `sbot/new.go:530-665`, `network/new.go`

### 3.3 Extract PluginRegistry

Create `sbot/plugin_registry.go` (~150 lines):
- Move plugin registration and lifecycle
- Add optional `Close()` to plugin interface or use type assertion
- Move `initPlugins()` logic from `New()`
- Key files: `sbot/new.go:670-930`, `plugin.go`

### 3.4 Slim down New()

After extractions, `New()` should be ~200 lines:
1. Parse options
2. Open receive log
3. Create IndexManager
4. Create NetworkManager
5. Register plugins via PluginRegistry
6. Return Sbot

### 3.5 Split cmd/go-sbot/main.go

Extract from the 635-line main.go:
- `cmd/go-sbot/config.go` - flag definitions and config loading
- `cmd/go-sbot/serve.go` - the serve loop and signal handling
- `cmd/go-sbot/fsck.go` - FSCK and repair logic (if not already separate)

### Verification
- `go build ./...` compiles
- `go test ./sbot/...` passes (all existing tests still work)
- No new public API surface - extracting internal types only
- `sbot/new.go` under 400 lines

---

## Phase 4: Test Infrastructure Overhaul

Priority: **Medium** | Risk: Low | Scope: Incremental, package by package

### 4.1 Create test helpers package improvements

- **internal/testutils/**: Add `RequireEventually(t, func() bool, timeout, msg)` to replace `time.Sleep` in tests
- Add goroutine leak checker using `internal/leakcheck/` (already exists!) - wire it into test helpers
- Add `MakeTestBot()` helper that returns cleanup function and registers leak check

### 4.2 Fix existing flaky tests

- Replace all `time.Sleep` in tests with `RequireEventually` or channel-based sync
- Add `t.Parallel()` to tests that don't share state
- Use buffered error channels: `make(chan error, 1)` instead of unbuffered in test goroutines
- Key files: `cmd/sbotcli/simple_test.go`, `plugins2/names/about_test.go`, `sbot/*_test.go`

### 4.3 Add tests for critical untested packages (priority order)

1. **indexes/** - Core index code, 2 files, 0 tests
2. **internal/broadcasts/** - Event broadcasting with known race condition
3. **internal/multicloser/** - Resource lifecycle management
4. **plugins/ebt/** - Core replication protocol, 3 files, 0 tests
5. **plugins/publish/** - Message publishing, 2 files, 0 tests
6. **plugins/friends/** - Social graph queries, 5 files, 0 tests

### 4.4 Add shutdown/lifecycle tests

- Test that `sbot.Close()` completes within 30s
- Test that goroutine count returns to baseline after Close()
- Test partial initialization failure cleanup

### Verification
- `go test -race -count=3 ./...` passes consistently (no flakes in 3 runs)
- Test coverage for newly-tested packages > 60%
- `go test -run TestShutdown -timeout 60s ./sbot/...` passes

---

## Phase 5: Modernization & Polish

Priority: **Low** | Risk: Low | Scope: Opportunistic

### 5.1 Dependency updates

- Evaluate badger/v3 -> v4 migration (v3 is maintenance-only)
- Pin margaret/v2 and go-muxrpc/v3 to tagged releases if/when available
- Run `govulncheck ./...` and address any findings

### 5.2 Structured logging audit

- Ensure consistent use of go-kit/log throughout (already the primary approach)
- Add missing context to log messages in network and index paths
- Ensure error logs include enough context to debug without source code

### 5.3 Add pprof-based memory monitoring

- Add optional memory stats logging on interval for long-running nodes
- Document pprof endpoints for operators
- Add goroutine count metric to existing metrics system

### 5.4 Documentation

- Update CLAUDE.md architecture section to reflect new IndexManager/NetworkManager/PluginRegistry structure
- Document shutdown sequence and expected behavior
- Add troubleshooting guide for memory issues

### Verification
- `govulncheck ./...` clean
- Long-running node (72h) shows stable memory profile
- All existing integration tests pass

---

## Packages With Zero Tests (Full List)

For reference, these 34 packages have no test files:

| Package | Files | Priority |
|---------|-------|----------|
| `indexes/` | 2 | HIGH - core index code |
| `internal/broadcasts/` | 2 | HIGH - known race condition |
| `internal/multicloser/` | 1 | HIGH - resource lifecycle |
| `plugins/ebt/` | 3 | HIGH - core replication |
| `plugins/publish/` | 2 | HIGH - message publishing |
| `plugins/friends/` | 5 | HIGH - social graph |
| `plugins/partial/` | 3 | MEDIUM - partial replication |
| `plugins/rawread/` | 3 | MEDIUM - log reading |
| `plugins/conn/` | 2 | MEDIUM |
| `plugins/groups/` | 2 | MEDIUM |
| `plugins/legacyinvites/` | 3 | MEDIUM |
| `plugins/private/` | 2 | MEDIUM |
| `plugins/status/` | 1 | LOW |
| `plugins/whoami/` | 1 | LOW |
| `plugins/test/` | 1 | LOW |
| `plugins2/` | 1 | LOW |
| `private/box/` | 1 | LOW |
| `internal/config-reader/` | 1 | LOW |
| `internal/extra25519/` | 1 | LOW |
| `internal/lo25519/` | 1 | LOW |
| `internal/multierror/` | 1 | LOW |
| `internal/mutil/` | 1 | LOW |
| `internal/neterr/` | 1 | LOW |
| `internal/refactors/` | 4 | LOW |
| `internal/testutils/` | 4 | LOW |
| `internal/tools/` | 2 | LOW |
| `internal/tracing/` | 1 | LOW |
| `cmd/gossb-migrate-mf/` | 1 | LOW |
| `cmd/gossb-null-entry/` | 1 | LOW |
| `cmd/ssb-drop-feed/` | 1 | LOW |
| `cmd/ssb-keygen/` | 1 | LOW |
| `cmd/ssb-logcat/` | 1 | LOW |
| `cmd/ssb-offset-converter/` | 1 | LOW |
| `cmd/ssb-truncate-log/` | 1 | LOW |

---

## Files Modified Per Phase

| Phase | Key Files |
|-------|-----------|
| 1 | `sbot/indexes.go`, `sbot/new.go`, `network/new.go`, `network/tunnel_dial.go`, `cmd/go-sbot/main.go`, `internal/broadcasts/blobstore.go`, `plugins/gossip/feed_manager.go`, `client/client.go`, `blobstore/wants.go` |
| 2 | `errors.go`, `plugins2/names/about.go`, `multilogs/membership_index.go`, `internal/storedrefs/storedrefs.go`, `message/legacy/encode.go`, `network/network.go`, `sbot/manifest.go` |
| 3 | `sbot/new.go` (major split), new: `sbot/index_manager.go`, `sbot/network_manager.go`, `sbot/plugin_registry.go`, `cmd/go-sbot/config.go`, `cmd/go-sbot/serve.go` |
| 4 | `internal/testutils/`, new test files in `indexes/`, `internal/broadcasts/`, `plugins/ebt/`, `plugins/publish/`, `plugins/friends/`, `sbot/*_test.go` |
| 5 | `go.mod`, various logging improvements, monitoring docs |
