// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package client_test

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ssbc/go-muxrpc/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ssbc/go-ssb"
	refs "github.com/ssbc/go-ssb-refs"
	"github.com/ssbc/go-ssb/client"
	"github.com/ssbc/go-ssb/internal/testutils"
	"github.com/ssbc/go-ssb/query"
	"github.com/ssbc/go-ssb/repo"
	"github.com/ssbc/go-ssb/sbot"
)

// kvWithRxSeq mirrors the server-side kvWithSeq wrapper so tests can decode
// the rxSeq field that the handler attaches to each subset response message.
type kvWithRxSeq struct {
	refs.KeyValueRaw
	RxSeq int64 `json:"rxSeq"`
}

// subsetTestEnv holds the common state for subset muxrpc tests.
type subsetTestEnv struct {
	srv    *sbot.Sbot
	c      *client.Client
	srvErr chan error
	kps    map[string]ssb.KeyPair // nick -> keypair
}

// setupSubsetTest creates a sbot with HMAC signing, three identities (arny, bert, cloe),
// starts the network, and returns a connected TCP client. The caller must call
// env.cleanup() when done.
func setupSubsetTest(t *testing.T) *subsetTestEnv {
	t.Helper()
	r := require.New(t)

	hk := make([]byte, 32)
	_, err := rand.Read(hk)
	r.NoError(err)

	srvRepo := filepath.Join("testrun", t.Name(), "serv")
	os.RemoveAll(srvRepo)
	srvLog := testutils.NewRelativeTimeLogger(nil)

	srv, err := sbot.New(
		sbot.WithInfo(srvLog),
		sbot.WithRepoPath(srvRepo),
		sbot.WithHMACSigning(hk),
		sbot.WithListenAddr(":0"),
	)
	r.NoError(err, "sbot srv init failed")

	tRepo := repo.New(srvRepo)

	kps := make(map[string]ssb.KeyPair)
	for _, nick := range []string{"arny", "bert", "cloe"} {
		kp, err := repo.NewKeyPair(tRepo, nick, refs.RefAlgoFeedSSB1)
		r.NoError(err)
		kps[nick] = kp
	}

	srvErrc := make(chan error, 1)
	go func() {
		err := srv.Network.Serve(context.TODO())
		if err != nil {
			srvErrc <- fmt.Errorf("serve exited: %w", err)
		}
		close(srvErrc)
	}()

	kp, err := ssb.LoadKeyPair(filepath.Join(srvRepo, "secret"))
	r.NoError(err)
	srvAddr := srv.Network.GetListenAddr()

	c, err := client.NewTCP(kp, srvAddr)
	r.NoError(err, "failed to make client connection")

	return &subsetTestEnv{
		srv:    srv,
		c:      c,
		srvErr: srvErrc,
		kps:    kps,
	}
}

func (e *subsetTestEnv) cleanup(t *testing.T) {
	t.Helper()
	r := require.New(t)
	r.NoError(e.c.Close())
	e.srv.Shutdown()
	r.NoError(e.srv.Close())
	r.NoError(<-e.srvErr)
}

// collectSubsetRaw drains a ByteSource into kvWithRxSeq entries. This decodes
// the full JSON including the rxSeq field that KeyValueRaw would ignore.
func collectSubsetRaw(ctx context.Context, src *muxrpc.ByteSource) ([]kvWithRxSeq, error) {
	var results []kvWithRxSeq
	for msg := range muxrpc.SourceAs[json.RawMessage](ctx, src) {
		var kv kvWithRxSeq
		if err := json.Unmarshal(msg, &kv); err != nil {
			return nil, fmt.Errorf("unmarshal kvWithRxSeq: %w", err)
		}
		results = append(results, kv)
	}
	return results, nil
}

// collectSubsetKV drains a ByteSource into KeyValueRaw entries (ignoring rxSeq).
func collectSubsetKV(ctx context.Context, src *muxrpc.ByteSource) []refs.KeyValueRaw {
	var results []refs.KeyValueRaw
	for msg := range muxrpc.SourceAs[refs.KeyValueRaw](ctx, src) {
		results = append(results, msg)
	}
	return results
}

func TestSubsetPageLimit(t *testing.T) {
	r, a := require.New(t), assert.New(t)
	env := setupSubsetTest(t)
	defer env.cleanup(t)

	// Publish 5 posts as arny
	for i := 0; i < 5; i++ {
		_, err := env.srv.PublishAs("arny", refs.NewPost(fmt.Sprintf("post %d", i)))
		r.NoError(err)
	}
	env.srv.WaitUntilIndexesAreSynced()

	ctx := context.TODO()
	src, err := env.c.SubsetByAuthor(env.kps["arny"].ID(), &query.SubsetOptions{
		Keys:      true,
		PageLimit: 2,
	})
	r.NoError(err)

	msgs := collectSubsetKV(ctx, src)
	a.Len(msgs, 2, "PageLimit:2 should return exactly 2 messages")
}

func TestSubsetRxSeqField(t *testing.T) {
	r, a := require.New(t), assert.New(t)
	env := setupSubsetTest(t)
	defer env.cleanup(t)

	// Publish 3 posts
	for i := 0; i < 3; i++ {
		_, err := env.srv.PublishAs("arny", refs.NewPost(fmt.Sprintf("rxseq post %d", i)))
		r.NoError(err)
	}
	env.srv.WaitUntilIndexesAreSynced()

	ctx := context.TODO()
	src, err := env.c.SubsetByType("post", &query.SubsetOptions{Keys: true})
	r.NoError(err)

	results, err := collectSubsetRaw(ctx, src)
	r.NoError(err)
	a.Len(results, 3)

	for i, kv := range results {
		// rxSeq should be a non-negative value (receive log sequences start at 0)
		a.GreaterOrEqual(kv.RxSeq, int64(0), "message %d: rxSeq should be non-negative", i)
		// Key should not be empty
		a.NotNil(kv.Key(), "message %d: key should be present", i)
	}
}

func TestSubsetAfterSeqPagination(t *testing.T) {
	r, a := require.New(t), assert.New(t)
	env := setupSubsetTest(t)
	defer env.cleanup(t)

	// Publish 5 posts as arny
	for i := 0; i < 5; i++ {
		_, err := env.srv.PublishAs("arny", refs.NewPost(fmt.Sprintf("pagination post %d", i)))
		r.NoError(err)
	}
	env.srv.WaitUntilIndexesAreSynced()

	ctx := context.TODO()
	authorOp := query.NewSubsetOpByAuthor(env.kps["arny"].ID())

	// Page 1: get first 2 results (descending so newest first)
	src, err := env.c.GetSubset(authorOp, &query.SubsetOptions{
		Keys:       true,
		Descending: true,
		PageLimit:  2,
	})
	r.NoError(err)

	page1, err := collectSubsetRaw(ctx, src)
	r.NoError(err)
	a.Len(page1, 2, "page 1 should have 2 results")

	// Use the last rxSeq from page 1 as cursor for page 2
	cursor := page1[len(page1)-1].RxSeq
	a.Greater(cursor, int64(0), "cursor rxSeq should be positive")

	src2, err := env.c.GetSubset(authorOp, &query.SubsetOptions{
		Keys:       true,
		Descending: true,
		PageLimit:  2,
		AfterSeq:   cursor,
	})
	r.NoError(err)

	page2, err := collectSubsetRaw(ctx, src2)
	r.NoError(err)
	a.Len(page2, 2, "page 2 should have 2 results")

	// Verify no overlap: collect all rxSeqs from both pages
	seen := make(map[int64]bool)
	for _, kv := range page1 {
		seen[kv.RxSeq] = true
	}
	for _, kv := range page2 {
		a.False(seen[kv.RxSeq], "page 2 rxSeq %d overlaps with page 1", kv.RxSeq)
	}
}

func TestSubsetAfterSeqEdgeCases(t *testing.T) {
	r, a := require.New(t), assert.New(t)
	env := setupSubsetTest(t)
	defer env.cleanup(t)

	// Publish 3 posts
	for i := 0; i < 3; i++ {
		_, err := env.srv.PublishAs("arny", refs.NewPost(fmt.Sprintf("edge post %d", i)))
		r.NoError(err)
	}
	env.srv.WaitUntilIndexesAreSynced()

	ctx := context.TODO()
	authorOp := query.NewSubsetOpByAuthor(env.kps["arny"].ID())

	// AfterSeq:0 should return results (0 is "no cursor" since the field is omitempty
	// and the server treats 0 as "start from beginning")
	src, err := env.c.GetSubset(authorOp, &query.SubsetOptions{
		Keys:       true,
		Descending: true,
		PageLimit:  2,
		AfterSeq:   0,
	})
	r.NoError(err)
	page, err := collectSubsetRaw(ctx, src)
	r.NoError(err)
	a.Len(page, 2, "AfterSeq:0 should return first page of results")

	// AfterSeq:1 with ascending order should skip the first two messages (seq 0 and 1)
	// and return only seq 2
	src2, err := env.c.GetSubset(authorOp, &query.SubsetOptions{
		Keys:     true,
		AfterSeq: 1,
	})
	r.NoError(err)
	page2, err := collectSubsetRaw(ctx, src2)
	r.NoError(err)
	a.Len(page2, 1, "AfterSeq:1 ascending should skip seqs 0 and 1, returning only seq 2")
	if len(page2) > 0 {
		a.Greater(page2[0].RxSeq, int64(1), "all results should have rxSeq > 1")
	}
}

// TestSubsetSortOrder verifies that ascending and descending options produce
// correctly ordered results over muxrpc. The handler sorts by claimed timestamp
// (Unix seconds), so we create two groups of messages with a >1s gap between
// them to get distinct timestamp buckets.
func TestSubsetSortOrder(t *testing.T) {
	r := require.New(t)
	env := setupSubsetTest(t)
	defer env.cleanup(t)

	// Group 1: arny publishes 2 posts (timestamp T)
	for i := 0; i < 2; i++ {
		_, err := env.srv.PublishAs("arny", refs.NewPost(fmt.Sprintf("early post %d", i)))
		r.NoError(err)
	}

	// Sleep >1s so the next group gets a different Unix second
	time.Sleep(1500 * time.Millisecond)

	// Group 2: bert publishes 2 posts (timestamp T+1 or later)
	for i := 0; i < 2; i++ {
		_, err := env.srv.PublishAs("bert", refs.NewPost(fmt.Sprintf("late post %d", i)))
		r.NoError(err)
	}
	env.srv.WaitUntilIndexesAreSynced()

	ctx := context.TODO()
	postOp := query.NewSubsetOpByType("post")

	t.Run("ascending returns earlier messages first", func(t *testing.T) {
		a := assert.New(t)
		src, err := env.c.GetSubset(postOp, &query.SubsetOptions{Keys: true})
		r.NoError(err)
		results, err := collectSubsetRaw(ctx, src)
		r.NoError(err)
		a.Len(results, 4, "should get all 4 posts")

		// First message should be from the early group (arny)
		a.Equal(env.kps["arny"].ID().String(), results[0].Author().String(),
			"ascending: first message should be from the earlier group")
		// Last message should be from the late group (bert)
		a.Equal(env.kps["bert"].ID().String(), results[len(results)-1].Author().String(),
			"ascending: last message should be from the later group")
	})

	t.Run("descending returns later messages first", func(t *testing.T) {
		a := assert.New(t)
		src, err := env.c.GetSubset(postOp, &query.SubsetOptions{
			Keys:       true,
			Descending: true,
		})
		r.NoError(err)
		results, err := collectSubsetRaw(ctx, src)
		r.NoError(err)
		a.Len(results, 4, "should get all 4 posts")

		// First message should be from the late group (bert)
		a.Equal(env.kps["bert"].ID().String(), results[0].Author().String(),
			"descending: first message should be from the later group")
		// Last message should be from the early group (arny)
		a.Equal(env.kps["arny"].ID().String(), results[len(results)-1].Author().String(),
			"descending: last message should be from the earlier group")
	})
}

func TestSubsetByChannel(t *testing.T) {
	r, a := require.New(t), assert.New(t)
	env := setupSubsetTest(t)
	defer env.cleanup(t)

	// Publish posts in different channels
	channelPosts := []struct {
		nick    string
		text    string
		channel string
	}{
		{"arny", "hello ssb-dev", "ssb-dev"},
		{"bert", "ssb-dev from bert", "ssb-dev"},
		{"cloe", "offtopic post", "random"},
		{"arny", "more dev talk", "ssb-dev"},
	}

	for _, p := range channelPosts {
		_, err := env.srv.PublishAs(p.nick, map[string]interface{}{
			"type":    "post",
			"text":    p.text,
			"channel": p.channel,
		})
		r.NoError(err)
	}
	env.srv.WaitUntilIndexesAreSynced()

	ctx := context.TODO()
	src, err := env.c.SubsetByChannel("ssb-dev", &query.SubsetOptions{Keys: true})
	r.NoError(err)

	msgs := collectSubsetKV(ctx, src)
	a.Len(msgs, 3, "should get 3 posts in ssb-dev channel")
}

func TestSubsetByMention(t *testing.T) {
	r, a := require.New(t), assert.New(t)
	env := setupSubsetTest(t)
	defer env.cleanup(t)

	arnyID := env.kps["arny"].ID()

	// Publish a post mentioning arny
	_, err := env.srv.PublishAs("bert", map[string]interface{}{
		"type":     "post",
		"text":     "hey arny check this out",
		"mentions": []map[string]string{{"link": arnyID.String()}},
	})
	r.NoError(err)

	// Publish a post NOT mentioning arny
	_, err = env.srv.PublishAs("cloe", refs.NewPost("just a normal post"))
	r.NoError(err)

	// Publish another post mentioning arny
	_, err = env.srv.PublishAs("cloe", map[string]interface{}{
		"type":     "post",
		"text":     "arny you there?",
		"mentions": []map[string]string{{"link": arnyID.String()}},
	})
	r.NoError(err)

	env.srv.WaitUntilIndexesAreSynced()

	ctx := context.TODO()
	src, err := env.c.SubsetByMention(arnyID, &query.SubsetOptions{Keys: true})
	r.NoError(err)

	msgs := collectSubsetKV(ctx, src)
	a.Len(msgs, 2, "should get 2 posts mentioning arny")
}

func TestSubsetBlockExclusion(t *testing.T) {
	r, a := require.New(t), assert.New(t)
	env := setupSubsetTest(t)
	defer env.cleanup(t)

	arnyID := env.kps["arny"].ID()
	cloeID := env.kps["cloe"].ID()

	// arny follows bert
	_, err := env.srv.PublishAs("arny", refs.NewContactFollow(env.kps["bert"].ID()))
	r.NoError(err)

	// arny blocks cloe
	_, err = env.srv.PublishAs("arny", refs.NewContactBlock(cloeID))
	r.NoError(err)

	// bert publishes a post (should appear in arny's timeline)
	_, err = env.srv.PublishAs("bert", refs.NewPost("bert says hello"))
	r.NoError(err)

	// cloe publishes a post (should be excluded from arny's timeline)
	_, err = env.srv.PublishAs("cloe", refs.NewPost("cloe says hello"))
	r.NoError(err)

	env.srv.WaitUntilIndexesAreSynced()

	ctx := context.TODO()

	// Query arny's timeline: posts from followed feeds, excluding blocked feeds
	src, err := env.c.SubsetTimeline(arnyID, 10, 0)
	r.NoError(err)

	msgs := collectSubsetKV(ctx, src)
	a.Len(msgs, 1, "arny's timeline should have 1 post (bert's), cloe's should be excluded")

	// Verify the message is from bert, not cloe
	if len(msgs) > 0 {
		a.Equal(env.kps["bert"].ID().String(), msgs[0].Author().String(),
			"the timeline post should be from bert")
	}
}

func TestSubsetTimelineEmptyFollows(t *testing.T) {
	r, a := require.New(t), assert.New(t)
	env := setupSubsetTest(t)
	defer env.cleanup(t)

	// Publish some posts but arny follows nobody
	_, err := env.srv.PublishAs("bert", refs.NewPost("bert post"))
	r.NoError(err)
	_, err = env.srv.PublishAs("cloe", refs.NewPost("cloe post"))
	r.NoError(err)

	env.srv.WaitUntilIndexesAreSynced()

	ctx := context.TODO()

	// arny follows nobody, so timeline should be empty
	src, err := env.c.SubsetTimeline(env.kps["arny"].ID(), 10, 0)
	r.NoError(err)

	msgs := collectSubsetKV(ctx, src)
	a.Len(msgs, 0, "timeline for user following nobody should be empty")
}

func TestSubsetThreadReplies(t *testing.T) {
	r, a := require.New(t), assert.New(t)
	env := setupSubsetTest(t)
	defer env.cleanup(t)

	// Publish a root post
	rootMsg, err := env.srv.PublishAs("arny", refs.NewPost("thread root"))
	r.NoError(err)
	rootRef := rootMsg.Key()

	// Publish replies with root and branch fields
	reply1, err := env.srv.PublishAs("bert", map[string]interface{}{
		"type":   "post",
		"text":   "reply 1",
		"root":   rootRef.String(),
		"branch": rootRef.String(),
	})
	r.NoError(err)

	_, err = env.srv.PublishAs("cloe", map[string]interface{}{
		"type":   "post",
		"text":   "reply 2",
		"root":   rootRef.String(),
		"branch": reply1.Key().String(),
	})
	r.NoError(err)

	env.srv.WaitUntilIndexesAreSynced()

	ctx := context.TODO()
	src, err := env.c.SubsetThreadReplies(rootRef, 10)
	r.NoError(err)

	results, err := collectSubsetRaw(ctx, src)
	r.NoError(err)
	// The tangle index includes the root post and all replies
	a.GreaterOrEqual(len(results), 2, "thread should have at least 2 messages (replies)")

	// Verify the responses have valid content by checking each message has a key
	for i, kv := range results {
		a.NotNil(kv.Key(), "message %d should have a key", i)
	}

	// Verify root and branch fields in reply content by decoding the raw JSON
	for _, kv := range results {
		var content map[string]interface{}
		if err := json.Unmarshal(kv.ContentBytes(), &content); err != nil {
			continue // root post content may not have root/branch
		}
		if rootField, ok := content["root"]; ok {
			a.Equal(rootRef.String(), rootField,
				"reply root field should match the thread root")
		}
	}
}

// TestSubsetSortOrderSameKeys verifies that ascending and descending return the
// same set of messages regardless of sort direction.
func TestSubsetSortOrderSameKeys(t *testing.T) {
	r, a := require.New(t), assert.New(t)
	env := setupSubsetTest(t)
	defer env.cleanup(t)

	for i := 0; i < 4; i++ {
		_, err := env.srv.PublishAs("arny", refs.NewPost(fmt.Sprintf("order post %d", i)))
		r.NoError(err)
	}
	env.srv.WaitUntilIndexesAreSynced()

	ctx := context.TODO()
	authorOp := query.NewSubsetOpByAuthor(env.kps["arny"].ID())

	srcAsc, err := env.c.GetSubset(authorOp, &query.SubsetOptions{Keys: true})
	r.NoError(err)
	ascending, err := collectSubsetRaw(ctx, srcAsc)
	r.NoError(err)

	srcDesc, err := env.c.GetSubset(authorOp, &query.SubsetOptions{
		Keys:       true,
		Descending: true,
	})
	r.NoError(err)
	descending, err := collectSubsetRaw(ctx, srcDesc)
	r.NoError(err)

	a.Len(ascending, 4)
	a.Len(descending, 4)

	ascKeys := make(map[string]bool)
	for _, kv := range ascending {
		ascKeys[kv.Key().String()] = true
	}
	for _, kv := range descending {
		a.True(ascKeys[kv.Key().String()],
			"descending result %s should also appear in ascending results", kv.Key().String())
	}
}
