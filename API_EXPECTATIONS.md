# go-ssb API Behavioral Expectations

Derived from the ssb-glcrab GUI client. Each section describes what the client
expects from the sbot (go-ssb) when making a particular RPC call. These can be
turned into integration tests against a running go-ssb instance.

---

## 1. Connection & Identity

### 1.1 Unix Socket Connection
- **Call:** `client.NewUnix(path)`
- **Expects:** A unix socket at `<repo>/socket` accepts connections and
  performs secret-handshake authentication.
- **Failure mode:** Client exits immediately if socket is unavailable.

### 1.2 Whoami
- **Call:** `client.Whoami()`
- **Expects:** Returns the sbot's own `refs.FeedRef`.
- **Invariant:** The returned feed ref must be a valid ed25519 SSB feed
  identity (sigil starts with `@`, suffix `.ed25519`).

---

## 2. Names Index

### 2.1 NamesGet
- **Call:** `client.NamesGet()`
- **Expects:** Returns a `client.NamesGetResult` mapping feed refs to their
  self-assigned "about" names.
- **Invariant:** For any feed that has published an `about` message with a
  `name` field targeting itself, `NamesGetResult.GetCommonName(feed)` must
  return that name.
- **Used at:** startup and on-demand refresh.

### 2.2 NamesGetCommonName
- **Call:** `ssbNames.GetCommonName(feedRef)`
- **Expects:** `(name string, ok bool)`. Returns `ok=true` with the display
  name if the feed has one, `ok=false` otherwise.

### 2.3 NamesImageFor
- **Call:** `client.NamesImageFor(feedRef)`
- **Expects:** Returns a `refs.BlobRef` pointing to the avatar image blob
  declared in the feed's most recent self-about message with an `image` field.
- **Failure:** Returns error if no avatar is set. Client falls back to
  generated hexagen placeholder.

---

## 3. Feed Discovery

### 3.1 ReplicateUpTo
- **Call:** `client.ReplicateUpTo()`
- **Expects:** A `muxrpc.ByteSource` stream of `ssb.ReplicateUpToResponse`
  objects, one per known/replicated feed.
- **Each entry must contain:**
  - `ID`: valid `refs.FeedRef`
  - `Sequence`: `int64` >= 0, the latest known sequence number for that feed.
- **Invariant:** Every feed the sbot is replicating must appear exactly once in
  the stream.
- **Stream termination:** Source closes cleanly after all feeds are emitted.

---

## 4. Subset Queries (partialReplication.getSubset)

All subset queries go through `client.GetSubset(op, opts)` and return a
`*muxrpc.ByteSource` streaming `refs.KeyValueRaw` messages.

### 4.1 Common Options (`query.SubsetOptions`)
- `Keys: true` — every response object must include the message key
  (`refs.MessageRef`).
- `Descending: true` — messages should arrive roughly newest-first by receive
  sequence (the client re-sorts by claimed timestamp as a safety net, but the
  server should respect this).
- `PageLimit: N` — the stream must close after emitting at most N messages.
- `AfterSeq: seq` — only return messages with `rxSeq < seq` (for cursor-based
  pagination). When `AfterSeq` is 0, return from the latest.

### 4.2 rxSeq Pagination Field
- **Critical:** Each streamed message must include an `rxSeq` JSON field
  (receive-log sequence number) alongside the `KeyValueRaw` fields.
- The client deserializes this via a wrapper struct:
  ```go
  type kvrWithSeq struct {
      refs.KeyValueRaw
      RxSeq int64 `json:"rxSeq"`
  }
  ```
- The minimum `rxSeq` in a batch becomes the cursor for the next page request.
- **Invariant:** `rxSeq` values must be monotonically assigned and stable
  (i.e., a given message always has the same `rxSeq`).

### 4.3 KeyValueRaw Response Shape
Each streamed message must be deserializable as `refs.KeyValueRaw` and provide:
- `Key` — `refs.MessageRef` (the `%...sha256` hash)
- `Author()` — returns `refs.FeedRef` of the message author
- `ContentBytes()` — returns the raw JSON content as `[]byte`
- `Claimed()` — returns `time.Time` of the claimed timestamp

The content JSON must have a `"type"` field matching the message type.

---

## 5. Query Compositions

### 5.1 Timeline Query
- **Composition:** `AND(FollowedBy(who), Type("post"), NOT(BlockedBy(who)))`
- **Expects:** Only messages of type `"post"`, authored by feeds that `who`
  follows, excluding authors that `who` has blocked.
- **Invariants:**
  - No messages from feeds not followed by `who`.
  - No messages from feeds blocked by `who`.
  - All returned messages have `type: "post"` in content.
  - Results are paginated by `rxSeq`.

### 5.2 Profile Query
- **Composition:** `AND(ByAuthor(author), Type("post"))`
- **Expects:** Only `"post"` messages authored by the given feed.
- **Invariant:** Every returned message's `Author()` equals `author`.

### 5.3 Channel Query
- **Composition:** `ByChannel(channel)`
- **Expects:** Messages posted to the given channel (content has
  `"channel": "<name>"`).
- **Invariant:** All returned messages reference the requested channel.

### 5.4 Search Query
- **Composition:** `BySearch(term)`
- **Expects:** Messages whose content text matches the search term.
- **Fallback:** If server-side search is unavailable, the client fetches
  timeline posts and filters locally with case-insensitive substring match.
  This means the server may return an error or empty results for search, and
  the client will cope.

### 5.5 Contacts Query
- **Composition:** `AND(ByAuthor(author), Type("contact"))`
- **Expects:** Contact messages by the given author.
- **Content shape:**
  ```json
  {
    "type": "contact",
    "contact": "@<feedref>",
    "following": true|false
  }
  ```

### 5.6 About-Self Query
- **Composition:** `AND(ByAuthor(who), Type("about"))`
- **Expects:** About messages authored by `who`.
- **Note:** The client filters for self-about (where `about == author`) on the
  client side. The server returns all about messages by that author.
- **Content shape:**
  ```json
  {
    "type": "about",
    "about": "@<feedref>",
    "name": "display name",
    "description": "bio text",
    "image": "&<blobref>"
  }
  ```
  All fields except `type` and `about` are optional.

### 5.7 Mentions Query
- **Call:** `client.SubsetByMention(who, opts)`
- **Expects:** Messages that contain `who` in their `mentions` array.
- **Invariant:** Every returned message must have a mention entry whose `link`
  matches `who`.

---

## 6. Threads

### 6.1 TanglesThread
- **Call:** `client.TanglesThread(args)` where `args` has:
  - `Root`: `refs.MessageRef` — the thread root message
  - `Limit`: `int64` — max messages to return
  - `Keys`: `true`
- **Expects:** A stream containing the root message itself plus all replies
  (messages with `root` pointing to the given ref), ordered oldest-first
  (chronological).
- **Invariants:**
  - The root message MUST be included in the response.
  - Reply messages must have `root` matching the requested root ref.
  - Order should be chronological (ascending by sequence/timestamp).

---

## 7. Publishing

### 7.1 Publish Post
- **Call:** `client.Publish(post)` where `post` is `refs.Post`:
  ```go
  refs.Post{
      Type: "post",
      Text: "message body",
  }
  ```
- **Expects:** Returns `refs.MessageRef` — the hash of the newly created
  message.
- **Invariant:** After publishing, the message must be retrievable via subset
  queries (e.g., `ByAuthor(self) + Type("post")`).

### 7.2 Publish Reply
- Same as above, but with additional fields:
  ```go
  refs.Post{
      Type:   "post",
      Text:   "reply body",
      Root:   &rootRef,         // thread root
      Branch: refs.MessageRefs{branchRef}, // parent message
  }
  ```
- **Invariants:**
  - The published reply must appear in `TanglesThread(root)` results.
  - `Root` must be set to the original thread root, not the immediate parent.
  - `Branch` must contain the message being directly replied to.

---

## 8. Blobs

### 8.1 BlobsWant
- **Call:** `client.BlobsWant(blobRef)`
- **Expects:** Tells the sbot to fetch the blob from the network. Returns
  error on failure.
- **Behavior:** After a successful `BlobsWant`, the blob should become
  available in the local blob store within a reasonable time. The client
  currently waits 2 seconds and retries `blobs.Get`.

### 8.2 Local Blob Store (blobs.Get)
- **Call:** `blobstore.New(path).Get(blobRef)`
- **Expects:** Returns an `io.ReadCloser` with the blob data if available
  locally, error otherwise.
- **Path:** Blobs are stored at `<repo>/blobs/` in the sbot's repo directory.
- **Invariant:** Any blob that has been successfully fetched via the network
  (after `BlobsWant`) must be readable from the local store.

---

## 9. Message Content Decoding Expectations

The client decodes message content JSON into typed structs. The sbot must store
and return content that matches these shapes:

### 9.1 Post
```json
{
  "type": "post",
  "text": "markdown body",
  "root": "%<msgref>",           // optional, for replies
  "branch": ["%<msgref>", ...],  // optional, for replies
  "mentions": [                  // optional
    {"link": "@<feedref>", "name": "display name"},
    {"link": "&<blobref>", "name": "filename.jpg"}
  ]
}
```

### 9.2 About
```json
{
  "type": "about",
  "about": "@<feedref>",
  "name": "display name",        // optional
  "description": "bio",          // optional
  "image": "&<blobref>"          // optional
}
```

### 9.3 Contact
```json
{
  "type": "contact",
  "contact": "@<feedref>",
  "following": true
}
```

---

## 10. Suggested Test Scenarios

### Smoke Tests
1. Connect via unix socket and call `Whoami` — verify valid feed ref returned.
2. Call `NamesGet` — verify it returns a map, and feeds with self-about name
   messages appear.
3. Call `ReplicateUpTo` — verify stream completes and all entries have valid
   feed refs and non-negative sequences.

### Subset Query Tests
4. Publish 3 posts as user A, query `ByAuthor(A) + Type("post")` — verify
   exactly 3 results, all by A, all type "post".
5. Have user A follow user B, user B publishes a post. Query timeline for A —
   verify B's post appears.
6. Have user A block user C, user C publishes a post. Query timeline for A —
   verify C's post does NOT appear.
7. Publish a post in channel "test", query `ByChannel("test")` — verify it
   appears.
8. Query with `PageLimit: 2` on a feed with 5 posts — verify exactly 2
   results. Use the minimum `rxSeq` as cursor for next page — verify next 2
   results, then final 1.
9. Verify `rxSeq` is present and monotonic across paginated queries.

### Thread Tests
10. Publish a root post, then 2 replies. Call `TanglesThread(root)` — verify
    all 3 messages returned, root included, in chronological order.
11. Verify reply messages have correct `root` and `branch` fields.

### Publish Tests
12. Publish a post via `client.Publish` — verify returned `MessageRef` is
    valid. Query back via `GetSubset` — verify it appears.
13. Publish a reply — verify it appears in `TanglesThread` for the root.

### Blob Tests
14. Publish an about message with an image blob ref. Call `NamesImageFor` —
    verify it returns the correct blob ref.
15. Call `BlobsWant` for a known blob, then `blobs.Get` — verify the blob data
    is retrievable.

### Names Tests
16. Publish self-about with name "Alice". Call `NamesGet`, then
    `GetCommonName` — verify "Alice" returned.
17. Call `NamesImageFor` for a feed with no avatar — verify error returned.

### Edge Cases
18. Query timeline for a user following nobody — verify empty result, no error.
19. Query `TanglesThread` for a non-existent root — verify graceful empty
    result or clear error.
20. Publish post with mentions (feed ref + blob ref) — verify mentions
    preserved in round-trip query.
21. Search for a term that appears in a post — verify it is returned by
    `BySearch`.
22. `AfterSeq: 0` should return the most recent page; `AfterSeq: 1` should
    return nothing (no messages with rxSeq < 1).

---

## 11. Subsystem Improvements Needed

Derived from auditing ssb-oasis (the web frontend). These additions let
clients drop local caches and N+1 RPC loops, keeping the client thin and
pushing work to the sbot where indexes already exist.

### 11.1 `names.getAll` — Bulk Name + Image Map

**Current state:** `names.get` returns `map[feed][author]→name` but no images.
Clients must call `names.getImageFor` individually per feed, creating an N+1
loop (e.g. 200 RPCs to render a contacts page).

**Need:** Extend `names.get` (or add `names.getAll`) to return:
```json
{
  "@feed1.ed25519": { "name": "Alice", "image": "&blob1.sha256" },
  "@feed2.ed25519": { "name": "Bob",   "image": "&blob2.sha256" }
}
```
One RPC replaces O(2N) calls. The sbot already has both name and image in the
about index (`AboutInfo` struct has `Name`, `Description`, `Image` fields) —
this just exposes them together.

**Test scenario:** Publish self-about with name and image for 3 feeds. Call
`names.getAll` — verify all 3 appear with correct name and image blob ref.

### 11.2 `names.getFor` — Single-Feed Profile Bundle

**Current state:** Rendering an author page requires 3 separate `getAbout`
calls (name, description, image), each doing a subset query + stream scan.

**Need:** `names.getFor(feedRef)` → `{name, description, image}` in one call.
The about index already stores all three fields per feed.

**Call:** `ssb.names.getFor(feedRef)`
**Returns:**
```json
{
  "name": "Alice",
  "description": "I like cats",
  "image": "&blobref.sha256"
}
```
Missing fields should be omitted or null, not error.

**Test scenario:** Publish self-about with name + description + image. Call
`names.getFor` — verify all 3 fields. Call for feed with no about — verify
empty/null response, no error.

### 11.3 `friends.follows` — Direct Follows for a Feed

**Current state:** `friends.hops({start, max:1})` returns feeds at distance 1
but includes the feed itself and doesn't distinguish follows from followers.
Clients must filter and guess.

**Need:** `friends.follows(feedRef)` → list of feeds that `feedRef` follows.
Clean, unambiguous. The graph builder already has `Follows(ref)` which returns
exactly this.

**Call:** `ssb.friends.follows({who: feedRef})`
**Returns:** Stream of `refs.FeedRef` — each feed that `who` follows.

**Test scenario:** A follows B and C, A blocks D. Call `friends.follows(A)` —
verify B and C appear, D does not, A does not.

### 11.4 `friends.getGraph` — Bulk Relationship Map

**Current state:** Getting the relationship with one feed requires 3 RPCs
(`isFollowing` twice + `isBlocking`). Rendering a contacts page does this
per-feed, creating O(3N) calls.

**Need:** `friends.getGraph({source})` → `map[feed]→{following, blocking,
followsMe}`. One call replaces O(3N).

**Call:** `ssb.friends.getGraph({source: feedRef})`
**Returns:**
```json
{
  "@feedB.ed25519": { "following": true,  "blocking": false, "followsMe": true },
  "@feedC.ed25519": { "following": false, "blocking": true,  "followsMe": false }
}
```
Only include feeds where at least one relationship exists (sparse map).

**Test scenario:** A follows B, B follows A, A blocks C. Call
`friends.getGraph({source: A})` — verify B shows `following+followsMe`, C
shows `blocking`, unknown feeds are absent.

### 11.5 Batch Message Get — `getMany`

**Current state:** Resolving liked posts requires fetching each message
individually by key. The popular page scans all votes then calls `get(key)`
in a loop — O(N) RPCs.

**Need:** `getMany([key1, key2, ...])` → `[msg1, msg2, ...]`. One RPC for
a batch of message keys.

**Call:** `ssb.get({ids: ["%ref1", "%ref2", ...]})`
**Returns:** Array of `KeyValueRaw` messages in the same order as input. Missing
keys should return null in their position, not error the whole call.

**Test scenario:** Publish 5 messages. Call `getMany` with their 5 keys plus
one non-existent key — verify 5 valid messages + 1 null, in order.

### 11.6 Subset Query: Use `mentions` Operator in Clients

**Current state:** ssb-oasis scans 35,000 messages via `messagesByType("post")`
then filters client-side for mentions. The `mentions` operator already exists
in go-ssb's subset planner and works.

**Need:** No go-ssb changes needed. This is a client-side migration:
```json
{"op": "and", "args": [
  {"op": "type", "string": "post"},
  {"op": "mentions", "feed": "@me.ed25519"}
]}
```
Replaces the 35k message scan with a bitmap intersection.

**Test scenario:** Already covered by test scenario 20 (mentions round-trip).

---

## 12. Sorting Expectations

### 12.1 Claimed Timestamp Sorting

When a `SequenceResolver` is available (the normal case for a fully-indexed
sbot), subset query results are sorted by **claimed timestamp** (the
`value.timestamp` field set by the message author), not by receive log
sequence.

- `Descending: true` — newest-authored messages first (highest claimed
  timestamp).
- `Descending: false` (default) — oldest-authored messages first.

This is critical for timeline views: a message authored recently but
replicated late should still appear at the top in descending mode.

**Test scenario:** Two authors publish at different times. Replicate the
older author's message AFTER the newer one. Query descending — verify the
newer-authored message appears first despite having a higher rxSeq.

### 12.2 Hops Query with Timestamp Sort

- **Composition:** `AND(Type("post"), Hops(who, 2))`
- **With options:** `{descending: true, keys: true, pageLimit: 64}`
- **Expects:** Posts from feeds within 2 hops of `who`, sorted by claimed
  timestamp descending. This is the "extended timeline" view.

**Test scenario:** A follows B, B follows C. C publishes a post. Query
`AND(type:post, hops(A, 2))` — verify C's post appears.
