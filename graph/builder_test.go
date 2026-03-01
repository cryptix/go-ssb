// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package graph

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/dgraph-io/badger/v3"
	margaret "github.com/ssbc/margaret/v2"
	"github.com/ssbc/margaret/v2/multilog/roaring"
	roaringfs "github.com/ssbc/margaret/v2/multilog/roaring/fs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ssbc/go-ssb"
	"github.com/ssbc/go-ssb/internal/testutils"
	"github.com/ssbc/go-ssb/message/multimsg"
	"github.com/ssbc/go-ssb/multilogs"
	"github.com/ssbc/go-ssb/repo"
)

func makeBadger(t *testing.T) testStore {
	r := require.New(t)
	info := testutils.NewRelativeTimeLogger(nil)

	tRepoPath := filepath.Join("testrun", t.Name())
	os.RemoveAll(tRepoPath)
	os.MkdirAll(tRepoPath, 0700)

	tRepo := repo.New(tRepoPath)
	tRootLog, err := repo.OpenLog(tRepo)
	r.NoError(err)

	// Create user feeds multilog
	uf := roaringfs.NewMultiLog(filepath.Join(tRepoPath, "userFeeds"))

	var builder *BadgerBuilder

	var tc testStore

	pth := tRepo.GetPath("contacts", "db")
	err = os.MkdirAll(pth, 0700)
	r.NoError(err, "error making index directory")

	badgerOpts := badger.DefaultOptions(pth).WithLoggingLevel(badger.ERROR)
	badgerDB, err := badger.Open(badgerOpts)
	r.NoError(err, "db/idx: badger failed to open")

	builder = NewBuilder(info, badgerDB, nil)

	contactsIdx := builder.OpenContactsIndex()
	metafeedsIdx := builder.OpenMetafeedsIndex()
	announcementIdx := builder.OpenAnnouncementIndex()

	tc.root = tRootLog
	tc.gbuilder = builder
	tc.userLogs = uf
	tc.indexers = []testLogIndexer{contactsIdx, metafeedsIdx, announcementIdx}

	t.Cleanup(func() {
		r.NoError(uf.Close())
		r.NoError(contactsIdx.Close())
		r.NoError(metafeedsIdx.Close())
		r.NoError(announcementIdx.Close())
		r.NoError(badgerDB.Close())
		r.NoError(tRootLog.Close())
		t.Log("closed scenario")
	})
	return tc
}

func TestBadger(t *testing.T) {
	tc := makeBadger(t)
	t.Run("scene1", tc.theScenario)
}

type testLogIndexer interface {
	Index(margaret.Log[*multimsg.MultiMessage]) error
	Close() error
}

type testStore struct {
	root     *multimsg.WrappedLog
	userLogs *roaring.MultiLog

	gbuilder Builder
	indexers []testLogIndexer
}

func (tc testStore) reindex(t *testing.T) {
	r := require.New(t)
	// Update user feeds index
	for seq := int64(0); seq <= tc.root.Seq(); seq++ {
		mm, err := tc.root.Get(seq)
		if err != nil {
			continue
		}
		err = multilogs.UserFeedsUpdate(seq, mm, tc.userLogs)
		r.NoError(err)
	}
	// Run all graph indexes
	for _, idx := range tc.indexers {
		err := idx.Index(margaret.Log[*multimsg.MultiMessage](tc.root))
		r.NoError(err)
	}
}

func (tc testStore) newPublisher(t *testing.T) *publisher {
	return newPublisher(t, tc.root, tc.userLogs)
}

func (tc testStore) theScenario(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)

	// some new people
	myself := tc.newPublisher(t)

	alice := tc.newPublisher(t)
	bob := tc.newPublisher(t)
	claire := tc.newPublisher(t)
	debby := tc.newPublisher(t)

	g, err := tc.gbuilder.Build()
	r.NoError(err)
	r.Equal(0, g.NodeCount())

	auth := tc.gbuilder.Authorizer(myself.key.ID(), 0)

	// > create contacts
	myself.follow(alice.key.ID())
	myself.block(bob.key.ID())
	tc.reindex(t)

	// this should result in 3 nodes - alice, bob, and myself
	g, err = tc.gbuilder.Build()
	r.NoError(err)
	if !a.Equal(3, g.NodeCount(), "wrong count for number of nodes in the graph") {
		return
	}

	// not followed
	err = auth.Authorize(claire.key.ID())
	r.NotNil(err, "unknown ID")
	hopsErr, ok := err.(*ssb.ErrOutOfReach)
	r.True(ok, "acutal err: %T\n%+v", err, err)
	r.True(hopsErr.Dist < 0)

	// following
	err = auth.Authorize(alice.key.ID())
	r.Nil(err)

	// blocked
	err = auth.Authorize(bob.key.ID())
	r.NotNil(err, "no error for blocked peer")
	hopsErr, ok = err.(*ssb.ErrOutOfReach)
	r.True(ok, "acutal err: %T\n%+v", err, err)
	r.True(hopsErr.Dist < 0)

	// alice follows claire
	alice.follow(claire.key.ID())
	tc.reindex(t)

	if os.Getenv("LIBRARIAN_WRITEALL") != "0" {
		t.Fatal("please 'export LIBRARIAN_WRITEALL=0' for this test to pass")
		// TODO: expose index flushing
	}

	g, err = tc.gbuilder.Build()
	r.NoError(err)
	r.Equal(4, g.NodeCount())

	// now allowed. zero hops and not friends
	err = auth.Authorize(claire.key.ID())
	r.NotNil(err, "authorized wrong person (claire)")
	hopsErr, ok = err.(*ssb.ErrOutOfReach)
	r.True(ok, "acutal err: %T\n%+v", err, err)
	r.Equal(1, hopsErr.Dist)
	r.Equal(0, hopsErr.Max)

	// alice follows me
	alice.follow(myself.key.ID())
	tc.reindex(t)

	g, err = tc.gbuilder.Build()
	r.NoError(err)
	r.Equal(4, g.NodeCount()) // same nodes more edges

	// now allowed. friends with alice but still 0 hops
	err = auth.Authorize(claire.key.ID())
	r.NotNil(err)
	hopsErr, ok = err.(*ssb.ErrOutOfReach)
	r.True(ok, "acutal err: %T\n%+v", err, err)
	r.Equal(1, hopsErr.Dist)
	r.Equal(0, hopsErr.Max)

	// works for 1 hop
	h1 := tc.gbuilder.Authorizer(myself.key.ID(), 1)
	err = h1.Authorize(claire.key.ID())
	r.NoError(err)

	// claire follows debby
	claire.follow(debby.key.ID())
	tc.reindex(t)

	g, err = tc.gbuilder.Build()
	r.NoError(err)
	r.Equal(5, g.NodeCount()) // same nodes more edges

	err = h1.Authorize(debby.key.ID())
	r.NotNil(err)
	hopsErr, ok = err.(*ssb.ErrOutOfReach)
	r.True(ok, "acutal err: %T\n%+v", err, err)
	r.Equal(2, hopsErr.Dist)
	r.Equal(1, hopsErr.Max)

	h2 := tc.gbuilder.Authorizer(myself.key.ID(), 2)
	err = h2.Authorize(debby.key.ID())
	r.Nil(err)
}
