// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package query_test

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mindeco.de/log"

	"github.com/ssbc/go-ssb"
	refs "github.com/ssbc/go-ssb-refs"
	"github.com/ssbc/go-ssb/query"
	"github.com/ssbc/go-ssb/repo"
	"github.com/ssbc/go-ssb/sbot"
)

func TestSubsetQuerySerializing(t *testing.T) {

	testRef, err := refs.NewFeedRefFromBytes(bytes.Repeat([]byte{1}, 32), refs.RefAlgoFeedSSB1)
	if err != nil {
		t.Fatal(err)
	}

	cases := []tcaseSerialized{
		{
			name:      "simple type",
			query:     query.NewSubsetOpByType("foo"),
			jsonInput: `{"op":"type","string":"foo"}`,
		},

		{
			name:      "simple author",
			query:     query.NewSubsetOpByAuthor(testRef),
			jsonInput: `{"op":"author","feed":"@AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=.ed25519"}`,
		},

		{
			name: "simple and",
			query: query.NewSubsetAndCombination(
				query.NewSubsetOpByAuthor(testRef),
				query.NewSubsetOpByType("foo"),
			),
			jsonInput: `{"op":"and","args":[{"op":"author","feed":"@AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=.ed25519"},{"op":"type","string":"foo"}]}`,
		},

		{
			name: "simple or",
			query: query.NewSubsetOrCombination(
				query.NewSubsetOpByType("foo"),
				query.NewSubsetOpByType("bar"),
			),
			jsonInput: `{"op":"or","args":[{"op":"type","string":"foo"},{"op":"type","string":"bar"}]}`,
		},

		{
			name: "author and two types",
			query: query.NewSubsetAndCombination(
				query.NewSubsetOpByAuthor(testRef),
				query.NewSubsetOrCombination(
					query.NewSubsetOpByType("foo"),
					query.NewSubsetOpByType("bar"),
				),
			),
			jsonInput: `{"op":"and","args":[{"op":"author","feed":"@AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=.ed25519"},{"op":"or","args":[{"op":"type","string":"foo"},{"op":"type","string":"bar"}]}]}`,
		},

		{
			name:      "invalid operation",
			jsonInput: `{"op":"stuff","times":"over 9000"}`,
			invalid:   true,
		},

		{
			name:      "invalid feed",
			jsonInput: `{"op":"author","feed":"over 9000"}`,
			invalid:   true,
		},

		{
			name:      "empty feed",
			jsonInput: `{"op":"author","feed":""}`,
			invalid:   true,
		},

		// tangle operations
		{
			name:      "empty tangle root",
			jsonInput: `{"op":"tangle"}`,
			invalid:   true,
		},

		// new operations
		{
			name:      "simple channel",
			query:     query.NewSubsetOpByChannel("ssb-dev"),
			jsonInput: `{"op":"channel","string":"ssb-dev"}`,
		},

		{
			name:      "empty channel",
			jsonInput: `{"op":"channel"}`,
			invalid:   true,
		},

		{
			name:  "not operation",
			query: query.NewSubsetNotCombination(query.NewSubsetOpByType("post")),
			jsonInput: `{"op":"not","args":[{"op":"type","string":"post"}]}`,
		},

		{
			name:      "isRoot",
			query:     query.NewSubsetOpIsRoot(),
			jsonInput: `{"op":"isRoot"}`,
		},

		{
			name:      "mentions feed",
			query:     query.NewSubsetOpByMention(testRef),
			jsonInput: `{"op":"mentions","feed":"@AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=.ed25519"}`,
		},

		{
			name:      "empty mentions",
			jsonInput: `{"op":"mentions"}`,
			invalid:   true,
		},

		{
			name:      "hasBlob",
			jsonInput: `{"op":"hasBlob","ref":"&abc123.sha256"}`,
		},

		{
			name:      "empty hasBlob",
			jsonInput: `{"op":"hasBlob"}`,
			invalid:   true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, tc.run)
	}
}

type tcaseSerialized struct {
	name string

	jsonInput string
	query     query.SubsetOperation

	invalid bool
}

func (tc tcaseSerialized) run(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)

	var parsed query.SubsetOperation
	err := json.Unmarshal([]byte(tc.jsonInput), &parsed)
	if tc.invalid {
		r.Error(err, "expected error parsing invalid input")
		return
	}

	r.NoError(err, "error while parsing json input")

	a.Equal(tc.query, parsed, "generated output does not map back to input")

	out, err := json.Marshal(tc.query)
	r.NoError(err, "failed to create json from input query")

	a.Equal(tc.jsonInput, string(out), "failed to create the wanted output")
}

func TestSubsetQueryPlanExecution(t *testing.T) {
	r := require.New(t)

	hk := make([]byte, 32)
	n, err := rand.Read(hk)
	r.NoError(err)
	r.Equal(32, n)

	tRepoPath := filepath.Join("testrun", t.Name())
	os.RemoveAll(tRepoPath)

	tRepo := repo.New(tRepoPath)

	// make three new keypairs with nicknames
	n2kp := make(map[string]ssb.KeyPair)

	kpArny, err := repo.NewKeyPair(tRepo, "arny", refs.RefAlgoFeedSSB1)
	r.NoError(err)
	n2kp["arny"] = kpArny

	kpBert, err := repo.NewKeyPair(tRepo, "bert", refs.RefAlgoFeedGabby)
	r.NoError(err)
	n2kp["bert"] = kpBert

	kpCloe, err := repo.NewKeyPair(tRepo, "cloe", refs.RefAlgoFeedSSB1)
	r.NoError(err)
	n2kp["cloe"] = kpCloe

	kps, err := repo.AllKeyPairs(tRepo)
	r.NoError(err)
	r.Len(kps, 3)

	// make the bot
	logger := log.NewLogfmtLogger(os.Stderr)
	mainbot, err := sbot.New(
		sbot.WithInfo(logger),
		sbot.WithRepoPath(tRepoPath),
		sbot.WithHMACSigning(hk),
		sbot.DisableNetworkNode(),
	)
	r.NoError(err)

	// create some messages
	var testRefs []refs.Message
	testMsgs := []struct {
		as string      // nick name
		c  interface{} // content
	}{
		{"arny", refs.NewAboutName(kpArny.ID(), "i'm arny!")},     // 0: about
		{"arny", refs.NewContactFollow(kpBert.ID())},              // 1: contact
		{"bert", refs.NewAboutName(kpBert.ID(), "i'm bert!")},     // 2: about
		{"bert", refs.NewContactFollow(kpArny.ID())},              // 3: contact
		{"bert", refs.NewAboutName(kpCloe.ID(), "that cloe")},    // 4: about
		{"cloe", refs.NewAboutName(kpBert.ID(), "iditot")},       // 5: about
		{"cloe", refs.NewPost("hello, world!")},                   // 6: post (root)
		{"cloe", refs.NewAboutName(kpCloe.ID(), "i'm cloe!")},    // 7: about
	}

	for idx, intro := range testMsgs {
		ref, err := mainbot.PublishAs(intro.as, intro.c)
		r.NoError(err, "publish %d failed", idx)
		r.NotNil(ref)
		testRefs = append(testRefs, ref)
	}

	// second batch: messages that reference earlier ones or use channels/mentions
	moreMsgs := []struct {
		as string
		c  interface{}
	}{
		{"arny", map[string]interface{}{ // 8: post with channel (root)
			"type":    "post",
			"text":    "in #ssb-dev channel",
			"channel": "ssb-dev",
		}},
		{"bert", map[string]interface{}{ // 9: post mentioning arny (root)
			"type":     "post",
			"text":     "mentioning arny",
			"mentions": []map[string]string{{"link": kpArny.ID().String()}},
		}},
		{"cloe", map[string]interface{}{ // 10: reply in channel (not root)
			"type":    "post",
			"text":    "#ssb-dev reply",
			"channel": "ssb-dev",
			"root":    testRefs[6].Key().String(),
		}},
	}

	for idx, intro := range moreMsgs {
		ref, err := mainbot.PublishAs(intro.as, intro.c)
		r.NoError(err, "publish more %d failed", idx)
		r.NotNil(ref)
		testRefs = append(testRefs, ref)
	}

	totalMsgs := len(testMsgs) + len(moreMsgs)
	r.EqualValues(totalMsgs-1, mainbot.ReceiveLog.Seq(), "did not get all the messages")

	// wait for indexes to catch up, since the tests rely on them being up-to-date to be able to ask for messages by author or type
	mainbot.WaitUntilIndexesAreSynced()

	sp := query.NewSubsetPlanerFull(mainbot.Users, mainbot.ByType, mainbot.Tangles, mainbot.Channels, mainbot.Mentions, mainbot.ReceiveLog, mainbot.GraphBuilder, mainbot.SeqResolver)

	t.Run("by author", func(t *testing.T) {
		r := require.New(t)

		msgs, err := sp.QuerySubsetMessages(mainbot.ReceiveLog, query.NewSubsetOpByAuthor(kpArny.ID()))
		r.NoError(err)
		r.Len(msgs, 2, "wrong number of resulting messages")
		r.Equal(testRefs[0], msgs[0])
		r.Equal(testRefs[1], msgs[1])
	})

	t.Run("by type", func(t *testing.T) {
		r := require.New(t)

		msgs, err := sp.QuerySubsetMessages(mainbot.ReceiveLog, query.NewSubsetOpByType("post"))
		r.NoError(err)
		r.Len(msgs, 4, "wrong number of resulting messages (6,8,9,10)")
	})

	t.Run("OR two types (contact and post)", func(t *testing.T) {
		r := require.New(t)

		qry := query.NewSubsetOrCombination(query.NewSubsetOpByType("contact"), query.NewSubsetOpByType("post"))
		res, err := sp.QuerySubsetMessages(mainbot.ReceiveLog, qry)
		r.NoError(err)
		r.Len(res, 6, "wrong number of resulting messages (1,3,6,8,9,10)")
	})

	// convenience builder tests

	t.Run("byTypes convenience (contact and post)", func(t *testing.T) {
		r := require.New(t)

		qry := query.NewSubsetOpByTypes("contact", "post")
		res, err := sp.QuerySubsetMessages(mainbot.ReceiveLog, qry)
		r.NoError(err)
		r.Len(res, 6, "wrong number of resulting messages (1,3,6,8,9,10)")
	})

	t.Run("byTypes convenience single type", func(t *testing.T) {
		r := require.New(t)

		qry := query.NewSubsetOpByTypes("post")
		res, err := sp.QuerySubsetMessages(mainbot.ReceiveLog, qry)
		r.NoError(err)
		r.Len(res, 4, "wrong number of resulting messages (6,8,9,10)")
	})

	t.Run("byAuthors convenience (arny and cloe)", func(t *testing.T) {
		r := require.New(t)

		qry := query.NewSubsetOpByAuthors(kpArny.ID(), kpCloe.ID())
		res, err := sp.QuerySubsetMessages(mainbot.ReceiveLog, qry)
		r.NoError(err)
		r.Len(res, 7, "wrong number of resulting messages (arny: 0,1,8; cloe: 5,6,7,10)")
	})

	t.Run("byAuthorsAndTypes (bert's contacts)", func(t *testing.T) {
		r := require.New(t)

		qry := query.NewSubsetOpByAuthorsAndTypes(
			[]refs.FeedRef{kpBert.ID()},
			[]string{"contact"},
		)
		res, err := sp.QuerySubsetMessages(mainbot.ReceiveLog, qry)
		r.NoError(err)
		r.Len(res, 1, "wrong number of resulting messages")
		r.Equal(testRefs[3], res[0])
	})

	t.Run("byAuthorsAndTypes (multiple authors, multiple types)", func(t *testing.T) {
		r := require.New(t)

		qry := query.NewSubsetOpByAuthorsAndTypes(
			[]refs.FeedRef{kpArny.ID(), kpCloe.ID()},
			[]string{"about", "post"},
		)
		res, err := sp.QuerySubsetMessages(mainbot.ReceiveLog, qry)
		r.NoError(err)
		// arny: about(0), post(8); cloe: about(5), post(6), about(7), post(10) = 6 msgs
		r.Len(res, 6, "wrong number of resulting messages")
	})

	t.Run("byAuthorsAndTypes authors-only fallback", func(t *testing.T) {
		r := require.New(t)

		qry := query.NewSubsetOpByAuthorsAndTypes(
			[]refs.FeedRef{kpArny.ID()},
			nil,
		)
		res, err := sp.QuerySubsetMessages(mainbot.ReceiveLog, qry)
		r.NoError(err)
		r.Len(res, 3, "wrong number of resulting messages (0,1,8)")
	})

	t.Run("byAuthorsAndTypes types-only fallback", func(t *testing.T) {
		r := require.New(t)

		qry := query.NewSubsetOpByAuthorsAndTypes(
			nil,
			[]string{"post"},
		)
		res, err := sp.QuerySubsetMessages(mainbot.ReceiveLog, qry)
		r.NoError(err)
		r.Len(res, 4, "wrong number of resulting messages (6,8,9,10)")
	})

	// SubsetQuery high-level API tests

	t.Run("SubsetQuery authors-and-types mode", func(t *testing.T) {
		r := require.New(t)

		sq := query.SubsetQuery{
			Authors: []refs.FeedRef{kpBert.ID()},
			Types:   []string{"about"},
			Mode:    query.SubsetQueryModeAuthorsAndTypes,
		}
		op, err := sq.ToOperation()
		r.NoError(err)

		res, err := sp.QuerySubsetMessages(mainbot.ReceiveLog, op)
		r.NoError(err)
		r.Len(res, 2, "bert has 2 about messages")
	})

	t.Run("SubsetQuery authors-only mode", func(t *testing.T) {
		r := require.New(t)

		sq := query.SubsetQuery{
			Authors: []refs.FeedRef{kpCloe.ID()},
			Mode:    query.SubsetQueryModeAuthorsOnly,
		}
		op, err := sq.ToOperation()
		r.NoError(err)

		res, err := sp.QuerySubsetMessages(mainbot.ReceiveLog, op)
		r.NoError(err)
		r.Len(res, 4, "cloe has 4 messages total (5,6,7,10)")
	})

	t.Run("SubsetQuery types-only mode", func(t *testing.T) {
		r := require.New(t)

		sq := query.SubsetQuery{
			Types: []string{"contact"},
			Mode:  query.SubsetQueryModeTypesOnly,
		}
		op, err := sq.ToOperation()
		r.NoError(err)

		res, err := sp.QuerySubsetMessages(mainbot.ReceiveLog, op)
		r.NoError(err)
		r.Len(res, 2, "2 contact messages total")
	})

	t.Run("SubsetQuery invalid mode", func(t *testing.T) {
		r := require.New(t)

		sq := query.SubsetQuery{
			Mode: "invalid",
		}
		_, err := sq.ToOperation()
		r.Error(err)
	})

	// new operation tests

	t.Run("by channel", func(t *testing.T) {
		r := require.New(t)

		qry := query.NewSubsetOpByChannel("ssb-dev")
		res, err := sp.QuerySubsetMessages(mainbot.ReceiveLog, qry)
		r.NoError(err)
		r.Len(res, 2, "two messages in ssb-dev channel (8, 10)")
	})

	t.Run("by mention", func(t *testing.T) {
		r := require.New(t)

		qry := query.NewSubsetOpByMention(kpArny.ID())
		res, err := sp.QuerySubsetMessages(mainbot.ReceiveLog, qry)
		r.NoError(err)
		r.Len(res, 1, "one message mentions arny (9)")
	})

	t.Run("isRoot", func(t *testing.T) {
		r := require.New(t)

		// isRoot should return all messages that are NOT replies (no content.root)
		// That's: 0,1,2,3,4,5,6,7,8,9 = 10 messages (all except 10 which has root)
		qry := query.NewSubsetOpIsRoot()
		res, err := sp.QuerySubsetMessages(mainbot.ReceiveLog, qry)
		r.NoError(err)
		r.Len(res, 10, "10 root messages (all except the reply at index 10)")
	})

	t.Run("isRoot AND type post", func(t *testing.T) {
		r := require.New(t)

		// root posts only (no replies): posts at 6, 8, 9 (10 is a reply)
		qry := query.NewSubsetAndCombination(
			query.NewSubsetOpIsRoot(),
			query.NewSubsetOpByType("post"),
		)
		res, err := sp.QuerySubsetMessages(mainbot.ReceiveLog, qry)
		r.NoError(err)
		r.Len(res, 3, "3 root posts (6, 8, 9)")
	})

	t.Run("NOT author (exclude arny)", func(t *testing.T) {
		r := require.New(t)

		qry := query.NewSubsetNotCombination(query.NewSubsetOpByAuthor(kpArny.ID()))
		res, err := sp.QuerySubsetMessages(mainbot.ReceiveLog, qry)
		r.NoError(err)
		// total 11 msgs minus arny's 3 (0,1,8) = 8
		r.Len(res, 8, "8 messages not by arny")
	})

	t.Run("posts NOT by arny", func(t *testing.T) {
		r := require.New(t)

		qry := query.NewSubsetAndCombination(
			query.NewSubsetOpByType("post"),
			query.NewSubsetNotCombination(query.NewSubsetOpByAuthor(kpArny.ID())),
		)
		res, err := sp.QuerySubsetMessages(mainbot.ReceiveLog, qry)
		r.NoError(err)
		// posts: 6,8,9,10 minus arny's post 8 = 3
		r.Len(res, 3, "3 posts not by arny (6, 9, 10)")
	})

	t.Run("channel AND author", func(t *testing.T) {
		r := require.New(t)

		qry := query.NewSubsetAndCombination(
			query.NewSubsetOpByChannel("ssb-dev"),
			query.NewSubsetOpByAuthor(kpCloe.ID()),
		)
		res, err := sp.QuerySubsetMessages(mainbot.ReceiveLog, qry)
		r.NoError(err)
		r.Len(res, 1, "cloe's ssb-dev message (10)")
	})

	// graph-aware query tests
	// The contact messages establish: arny follows bert (msg 1), bert follows arny (msg 3)

	t.Run("followedBy arny", func(t *testing.T) {
		r := require.New(t)

		// arny follows bert, so followedBy(arny) = bert's messages
		qry := query.NewSubsetOpFollowedBy(kpArny.ID())
		res, err := sp.QuerySubsetMessages(mainbot.ReceiveLog, qry)
		r.NoError(err)
		// bert has messages: 2(about), 3(contact), 4(about), 9(post) = 4 messages
		r.Len(res, 4, "bert's messages (followed by arny)")
	})

	t.Run("followedBy bert", func(t *testing.T) {
		r := require.New(t)

		// bert follows arny, so followedBy(bert) = arny's messages
		qry := query.NewSubsetOpFollowedBy(kpBert.ID())
		res, err := sp.QuerySubsetMessages(mainbot.ReceiveLog, qry)
		r.NoError(err)
		// arny has messages: 0(about), 1(contact), 8(post) = 3 messages
		r.Len(res, 3, "arny's messages (followed by bert)")
	})

	t.Run("followedBy AND type post (timeline)", func(t *testing.T) {
		r := require.New(t)

		// arny's timeline: posts by people arny follows (bert)
		qry := query.NewSubsetAndCombination(
			query.NewSubsetOpFollowedBy(kpArny.ID()),
			query.NewSubsetOpByType("post"),
		)
		res, err := sp.QuerySubsetMessages(mainbot.ReceiveLog, qry)
		r.NoError(err)
		// bert's posts: 9 = 1 post
		r.Len(res, 1, "bert's posts (arny's timeline)")
	})

	t.Run("NOT blockedBy (content moderation)", func(t *testing.T) {
		r := require.New(t)

		// nobody has blocked anyone in our test data, so blockedBy returns empty
		// NOT(empty) = everything
		qry := query.NewSubsetNotCombination(
			query.NewSubsetOpBlockedBy(kpArny.ID()),
		)
		res, err := sp.QuerySubsetMessages(mainbot.ReceiveLog, qry)
		r.NoError(err)
		r.Len(res, 11, "no one is blocked, so all 11 messages returned")
	})

	t.Run("friendsBlocks (empty case)", func(t *testing.T) {
		r := require.New(t)

		// arny's friends (bert) haven't blocked anyone
		qry := query.NewSubsetOpFriendsBlocks(kpArny.ID())
		res, err := sp.QuerySubsetMessages(mainbot.ReceiveLog, qry)
		r.NoError(err)
		r.Nil(res, "no friends blocks means nil result")
	})

	// --- Timestamp-sorted query tests ---
	// These verify that subset query results can be sorted by claimed timestamp
	// (causal order) using the SequenceResolver, and that pagination works on
	// the sorted results.

	t.Run("timestamp-sorted descending (posts)", func(t *testing.T) {
		r := require.New(t)
		a := assert.New(t)

		// Get the bitmap for all posts
		bitmap, err := sp.QuerySubsetBitmap(query.NewSubsetOpByType("post"))
		r.NoError(err)
		r.NotNil(bitmap)

		// Sort by claimed timestamp descending
		sorted, err := mainbot.SeqResolver.SortAndFilterBitmap(
			bitmap,
			repo.SortByClaimed,
			func(int64) bool { return true },
			true, // descending
		)
		r.NoError(err)
		a.Len(sorted, 4, "4 post messages")

		// Verify descending order: each entry's timestamp should be >= the next
		for i := 0; i < len(sorted)-1; i++ {
			a.GreaterOrEqual(sorted[i].By, sorted[i+1].By,
				"entry %d (ts=%d) should be >= entry %d (ts=%d)",
				i, sorted[i].By, i+1, sorted[i+1].By)
		}
	})

	t.Run("timestamp-sorted ascending (posts)", func(t *testing.T) {
		r := require.New(t)
		a := assert.New(t)

		bitmap, err := sp.QuerySubsetBitmap(query.NewSubsetOpByType("post"))
		r.NoError(err)

		sorted, err := mainbot.SeqResolver.SortAndFilterBitmap(
			bitmap,
			repo.SortByClaimed,
			func(int64) bool { return true },
			false, // ascending
		)
		r.NoError(err)
		a.Len(sorted, 4)

		for i := 0; i < len(sorted)-1; i++ {
			a.LessOrEqual(sorted[i].By, sorted[i+1].By,
				"entry %d (ts=%d) should be <= entry %d (ts=%d)",
				i, sorted[i].By, i+1, sorted[i+1].By)
		}
	})

	t.Run("timestamp-sorted timeline (followedBy + type + NOT blocked)", func(t *testing.T) {
		r := require.New(t)
		a := assert.New(t)

		// arny's timeline: posts from feeds arny follows (bert), not blocked
		timelineOp := query.NewSubsetAndCombination(
			query.NewSubsetOpFollowedBy(kpArny.ID()),
			query.NewSubsetOpByType("post"),
			query.NewSubsetNotCombination(query.NewSubsetOpBlockedBy(kpArny.ID())),
		)
		bitmap, err := sp.QuerySubsetBitmap(timelineOp)
		r.NoError(err)
		r.NotNil(bitmap)

		sorted, err := mainbot.SeqResolver.SortAndFilterBitmap(
			bitmap,
			repo.SortByClaimed,
			func(int64) bool { return true },
			true, // descending = newest first
		)
		r.NoError(err)
		// bert has 1 post (msg 9)
		a.Len(sorted, 1, "bert's posts on arny's timeline")

		// Verify the rxSeq matches expected
		a.EqualValues(9, sorted[0].Seq, "should be bert's post at rxSeq 9")
	})

	t.Run("pagination on timestamp-sorted results", func(t *testing.T) {
		r := require.New(t)
		a := assert.New(t)

		// Get all messages sorted by claimed timestamp descending
		bitmap, err := sp.QuerySubsetBitmap(query.NewSubsetOpByType("about"))
		r.NoError(err)
		r.NotNil(bitmap)

		sorted, err := mainbot.SeqResolver.SortAndFilterBitmap(
			bitmap,
			repo.SortByClaimed,
			func(int64) bool { return true },
			true, // descending
		)
		r.NoError(err)
		// about messages: 0, 2, 4, 5, 7 = 5 messages
		a.Len(sorted, 5, "5 about messages total")

		// Page 1: first 2 messages
		page1 := sorted[:2]
		a.Len(page1, 2)

		// Page 2: use rxSeq of last message in page 1 as cursor
		cursorSeq := page1[1].Seq
		startIdx := 0
		for i, entry := range sorted {
			if entry.Seq == cursorSeq {
				startIdx = i + 1
				break
			}
		}

		page2End := startIdx + 2
		if page2End > len(sorted) {
			page2End = len(sorted)
		}
		page2 := sorted[startIdx:page2End]
		a.Len(page2, 2, "page 2 has 2 messages")

		// Verify no overlap between pages
		for _, p1 := range page1 {
			for _, p2 := range page2 {
				a.NotEqual(p1.Seq, p2.Seq, "pages should not overlap")
			}
		}

		// Verify page 2 timestamps are <= page 1's last timestamp
		a.GreaterOrEqual(page1[1].By, page2[0].By,
			"page 2 should continue where page 1 left off")

		// Page 3: remaining messages
		cursor2 := page2[1].Seq
		startIdx2 := 0
		for i, entry := range sorted {
			if entry.Seq == cursor2 {
				startIdx2 = i + 1
				break
			}
		}
		page3 := sorted[startIdx2:]
		a.Len(page3, 1, "page 3 has remaining 1 message")

		// All 5 messages accounted for across 3 pages
		allSeqs := make(map[int64]bool)
		for _, p := range page1 {
			allSeqs[p.Seq] = true
		}
		for _, p := range page2 {
			allSeqs[p.Seq] = true
		}
		for _, p := range page3 {
			allSeqs[p.Seq] = true
		}
		a.Len(allSeqs, 5, "all 5 about messages covered across 3 pages")
	})

	// shutdown bot
	mainbot.Shutdown()
	r.NoError(mainbot.Close())
}
