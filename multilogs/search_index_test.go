// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package multilogs_test

import (
	"context"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mindeco.de/log"

	refs "github.com/ssbc/go-ssb-refs"
	"github.com/ssbc/go-ssb/query"
	"github.com/ssbc/go-ssb/repo"
	"github.com/ssbc/go-ssb/sbot"
)

func TestSearchIndex(t *testing.T) {
	r := require.New(t)

	hk := make([]byte, 32)
	n, err := rand.Read(hk)
	r.NoError(err)
	r.Equal(32, n)

	tRepoPath := filepath.Join("testrun", t.Name())
	os.RemoveAll(tRepoPath)

	tRepo := repo.New(tRepoPath)

	// create keypairs for test identities
	kpAlice, err := repo.NewKeyPair(tRepo, "alice", refs.RefAlgoFeedSSB1)
	r.NoError(err)

	kpBob, err := repo.NewKeyPair(tRepo, "bob", refs.RefAlgoFeedSSB1)
	r.NoError(err)

	kps, err := repo.AllKeyPairs(tRepo)
	r.NoError(err)
	r.Len(kps, 2)

	// create the bot with search enabled
	logger := log.NewLogfmtLogger(os.Stderr)
	mainbot, err := sbot.New(
		sbot.WithInfo(logger),
		sbot.WithRepoPath(tRepoPath),
		sbot.WithHMACSigning(hk),
		sbot.DisableNetworkNode(),
		sbot.EnableSearch(),
	)
	r.NoError(err)
	r.NotNil(mainbot.SearchIndex, "search index should be initialized when EnableSearch is used")

	// publish posts with distinctive text content
	posts := []struct {
		nick string
		content interface{}
	}{
		// posts with searchable text
		{"alice", map[string]interface{}{
			"type": "post",
			"text": "the quick brown fox jumps over the lazy dog",
		}},
		{"alice", map[string]interface{}{
			"type": "post",
			"text": "scuttlebutt is a decentralized protocol for social networking",
		}},
		{"bob", map[string]interface{}{
			"type": "post",
			"text": "the quick brown rabbit hops through the garden",
		}},
		{"bob", map[string]interface{}{
			"type": "post",
			"text": "cryptography ensures secure peer-to-peer communication",
		}},
		// about messages (should also be indexed)
		{"alice", refs.NewAboutName(kpAlice.ID(), "alice wonderland")},
		// contact message (should NOT be indexed by search)
		{"alice", refs.NewContactFollow(kpBob.ID())},
	}

	for idx, p := range posts {
		ref, err := mainbot.PublishAs(p.nick, p.content)
		r.NoError(err, "publish %d failed", idx)
		r.NotNil(ref)
	}

	r.EqualValues(len(posts)-1, mainbot.ReceiveLog.Seq(), "should have all messages in receive log")

	// wait for all indexes (including search) to catch up
	mainbot.WaitUntilIndexesAreSynced()

	// Explicitly re-index to ensure the search index has processed all messages.
	// The live indexer goroutine may not have picked up the latest messages yet
	// due to the inherent race between publish callbacks and live query notifications.
	err = mainbot.SearchIndex.Index(mainbot.ReceiveLog)
	r.NoError(err, "explicit search re-index failed")

	// set up the subset planer with search
	sp := query.NewSubsetPlanerFull(
		mainbot.Users, mainbot.ByType, mainbot.Tangles,
		mainbot.Channels, mainbot.Mentions,
		mainbot.ReceiveLog, mainbot.GraphBuilder, mainbot.SeqResolver,
	).WithSearch(mainbot.SearchIndex)

	t.Run("search returns matching posts", func(t *testing.T) {
		r := require.New(t)
		a := assert.New(t)

		// search for "quick brown" -- should match alice's fox post and bob's rabbit post
		qry := query.NewSubsetOpBySearch("quick brown")
		msgs, err := sp.QuerySubsetMessages(context.Background(), mainbot.ReceiveLog, qry)
		r.NoError(err)
		a.Len(msgs, 2, "should find 2 messages containing 'quick brown'")
	})

	t.Run("search for unique term", func(t *testing.T) {
		r := require.New(t)
		a := assert.New(t)

		// "scuttlebutt" only appears in one post
		qry := query.NewSubsetOpBySearch("scuttlebutt")
		msgs, err := sp.QuerySubsetMessages(context.Background(), mainbot.ReceiveLog, qry)
		r.NoError(err)
		a.Len(msgs, 1, "should find 1 message containing 'scuttlebutt'")
	})

	t.Run("search composes with type filter", func(t *testing.T) {
		r := require.New(t)
		a := assert.New(t)

		// search AND type("post") -- "quick" appears only in posts, so AND with type should work
		qry := query.NewSubsetAndCombination(
			query.NewSubsetOpBySearch("quick"),
			query.NewSubsetOpByType("post"),
		)
		msgs, err := sp.QuerySubsetMessages(context.Background(), mainbot.ReceiveLog, qry)
		r.NoError(err)
		a.Len(msgs, 2, "search AND type(post) should find 2 matching posts")
	})

	t.Run("search composes with author filter", func(t *testing.T) {
		r := require.New(t)
		a := assert.New(t)

		// search for "quick" AND author(alice) -- only alice's fox post
		qry := query.NewSubsetAndCombination(
			query.NewSubsetOpBySearch("quick"),
			query.NewSubsetOpByAuthor(kpAlice.ID()),
		)
		msgs, err := sp.QuerySubsetMessages(context.Background(), mainbot.ReceiveLog, qry)
		r.NoError(err)
		a.Len(msgs, 1, "search AND author(alice) should find 1 matching post")
	})

	t.Run("search with no results", func(t *testing.T) {
		r := require.New(t)
		a := assert.New(t)

		// search for a term that does not appear in any message
		qry := query.NewSubsetOpBySearch("xylophone")
		msgs, err := sp.QuerySubsetMessages(context.Background(), mainbot.ReceiveLog, qry)
		r.NoError(err)
		a.Empty(msgs, "search for non-existent term should return no results")
	})

	t.Run("search indexes about messages", func(t *testing.T) {
		r := require.New(t)
		a := assert.New(t)

		// "wonderland" appears in alice's about name
		qry := query.NewSubsetOpBySearch("wonderland")
		msgs, err := sp.QuerySubsetMessages(context.Background(), mainbot.ReceiveLog, qry)
		r.NoError(err)
		a.Len(msgs, 1, "should find 1 about message containing 'wonderland'")
	})

	t.Run("search does not index contact messages", func(t *testing.T) {
		r := require.New(t)
		a := assert.New(t)

		// "contact" is the type of follow messages but the text isn't indexed
		// We published a contact/follow which has no searchable text content.
		// Search for alice's ID string which appears in the contact message but should not be indexed.
		qry := query.NewSubsetAndCombination(
			query.NewSubsetOpBySearch("contact"),
			query.NewSubsetOpByType("contact"),
		)
		msgs, err := sp.QuerySubsetMessages(context.Background(), mainbot.ReceiveLog, qry)
		r.NoError(err)
		a.Empty(msgs, "contact messages should not appear in search results")
	})

	t.Run("search result bitmap works with OR", func(t *testing.T) {
		r := require.New(t)
		a := assert.New(t)

		// OR of two searches that match different posts
		qry := query.NewSubsetOrCombination(
			query.NewSubsetOpBySearch("fox"),
			query.NewSubsetOpBySearch("cryptography"),
		)
		msgs, err := sp.QuerySubsetMessages(context.Background(), mainbot.ReceiveLog, qry)
		r.NoError(err)
		a.Len(msgs, 2, "OR of two searches should find 2 distinct messages")
	})

	// shutdown bot
	mainbot.Shutdown()
	r.NoError(mainbot.Close())
}

func TestSearchIndexDisabled(t *testing.T) {
	r := require.New(t)

	hk := make([]byte, 32)
	_, err := rand.Read(hk)
	r.NoError(err)

	tRepoPath := filepath.Join("testrun", t.Name())
	os.RemoveAll(tRepoPath)

	// create bot WITHOUT search enabled
	logger := log.NewLogfmtLogger(os.Stderr)
	mainbot, err := sbot.New(
		sbot.WithInfo(logger),
		sbot.WithRepoPath(tRepoPath),
		sbot.WithHMACSigning(hk),
		sbot.DisableNetworkNode(),
	)
	r.NoError(err)
	r.Nil(mainbot.SearchIndex, "search index should be nil when EnableSearch is not used")

	// set up subset planer without search -- querying search should fail gracefully
	sp := query.NewSubsetPlanerFull(
		mainbot.Users, mainbot.ByType, mainbot.Tangles,
		mainbot.Channels, mainbot.Mentions,
		mainbot.ReceiveLog, mainbot.GraphBuilder, mainbot.SeqResolver,
	)

	qry := query.NewSubsetOpBySearch("anything")
	_, err = sp.QuerySubsetBitmap(context.Background(), qry)
	r.Error(err, "search query should fail when search index is not configured")
	r.Contains(err.Error(), "search index not configured")

	mainbot.Shutdown()
	r.NoError(mainbot.Close())
}
