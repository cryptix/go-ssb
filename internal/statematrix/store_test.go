// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package statematrix

import (
	"bytes"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ssbc/go-ssb"
	refs "github.com/ssbc/go-ssb-refs"
)

func TestNew(t *testing.T) {
	r := require.New(t)
	os.RemoveAll("testrun")
	os.Mkdir("testrun", 0700)
	m, err := New("testrun/new", testFeed(0))
	r.NoError(err)

	feeds := []ObservedFeed{
		{Feed: testFeed(1), Note: ssb.Note{Replicate: true, Receive: true, Seq: 5}},
		{Feed: testFeed(2), Note: ssb.Note{Replicate: true, Receive: true, Seq: 499}},
		{Feed: testFeed(5), Note: ssb.Note{Replicate: true, Receive: true, Seq: 3000}},
	}
	r.NoError(m.Fill(testFeed(0), feeds))

	start := time.Now()
	feeds = []ObservedFeed{
		{Feed: testFeed(1), Note: ssb.Note{Replicate: true, Receive: true, Seq: 2}},
		{Feed: testFeed(2), Note: ssb.Note{Replicate: true, Receive: true, Seq: 20}},
		{Feed: testFeed(3), Note: ssb.Note{Replicate: true, Receive: true, Seq: 200}},
		{Feed: testFeed(4), Note: ssb.Note{Replicate: true, Receive: true, Seq: 2000}},
	}
	r.NoError(m.Fill(testFeed(23), feeds))
	t.Logf("%v (took: %s)", m, time.Since(start))

	feeds = []ObservedFeed{
		{Feed: testFeed(1), Note: ssb.Note{Replicate: true, Receive: true, Seq: 4}},
		{Feed: testFeed(2), Note: ssb.Note{Replicate: true, Receive: true, Seq: 500}},
		{Feed: testFeed(5), Note: ssb.Note{Replicate: true, Receive: true, Seq: 9000}},
	}
	r.NoError(m.Fill(testFeed(23), feeds))

	feeds = []ObservedFeed{
		{Feed: testFeed(1), Note: ssb.Note{Replicate: true, Receive: true, Seq: 3}},
		{Feed: testFeed(2), Note: ssb.Note{Replicate: true, Receive: true, Seq: 499}},
		{Feed: testFeed(5), Note: ssb.Note{Replicate: true, Receive: true, Seq: 1000}},
	}
	r.NoError(m.Fill(testFeed(13), feeds))

	feeds = []ObservedFeed{
		{Feed: testFeed(1), Note: ssb.Note{Replicate: true, Receive: true, Seq: 5}},
		{Feed: testFeed(2), Note: ssb.Note{Replicate: true, Receive: true, Seq: 750}},
		{Feed: testFeed(5), Note: ssb.Note{Replicate: true, Receive: true, Seq: 1000}},
	}
	r.NoError(m.Fill(testFeed(9), feeds))

	has, err := m.HasLonger()
	r.NoError(err)
	r.Len(has, 3)
	t.Logf("%+v", has)

	feeds = []ObservedFeed{
		{Feed: testFeed(2), Note: ssb.Note{Replicate: true, Receive: true, Seq: 750}},
		{Feed: testFeed(5), Note: ssb.Note{Replicate: true, Receive: true, Seq: 1000}},
	}
	r.NoError(m.Fill(testFeed(0), feeds))

	has, err = m.HasLonger()
	r.NoError(err)
	r.Len(has, 1)
	t.Logf("%+v", has)

	feeds = []ObservedFeed{
		{Feed: testFeed(5), Note: ssb.Note{Replicate: true, Receive: true, Seq: 9000}},
	}
	r.NoError(m.Fill(testFeed(0), feeds))

	has, err = m.HasLonger()
	r.NoError(err)
	r.Len(has, 0)

	feeds = []ObservedFeed{
		{Feed: testFeed(1), Note: ssb.Note{Replicate: true, Receive: true, Seq: 0}},
	}
	r.NoError(m.Fill(testFeed(0), feeds))

	has, err = m.HasLonger()
	r.NoError(err)
	r.Len(has, 3)
	t.Logf("%+v", has)

	feeds = []ObservedFeed{
		{Feed: testFeed(1), Note: ssb.Note{Replicate: true, Receive: true, Seq: 10}},
	}
	r.NoError(m.Fill(testFeed(0), feeds))

	has, err = m.HasLonger()
	r.NoError(err)
	r.Len(has, 0)

	r.NoError(m.Close())
}

func testFeed(i int) refs.FeedRef {
	k := bytes.Repeat([]byte(strconv.Itoa(i)), 32)
	if len(k) > 32 {
		k = k[:32]
	}

	ref, err := refs.NewFeedRefFromBytes(k, refs.RefAlgoFeedSSB1)
	if err != nil {
		panic(err)
	}

	return ref
}

func TestChanged(t *testing.T) {
	r := require.New(t)
	os.RemoveAll("testrun")
	os.Mkdir("testrun", 0700)
	m, err := New("testrun/new", testFeed(0))
	r.NoError(err)

	// 0 has seen feed(1) up until 2
	feeds := []ObservedFeed{
		{Feed: testFeed(1), Note: ssb.Note{Replicate: true, Receive: true, Seq: 2}},
	}
	r.NoError(m.Fill(testFeed(0), feeds))

	// feed(1) already has 25 tho
	feeds = []ObservedFeed{
		{Feed: testFeed(1), Note: ssb.Note{Replicate: true, Receive: true, Seq: 25}},
	}
	r.NoError(m.Fill(testFeed(1), feeds))

	changed, err := m.Changed(testFeed(0), testFeed(1))
	r.NoError(err)

	// changed should have 1 as two still (to get just 3)
	note, has := changed[testFeed(1).String()]
	r.True(has, "changed doesnt have feed(1) (has %d entries)", len(changed))
	r.Equal(int64(2), note.Seq)
	r.True(note.Replicate)
	r.True(note.Receive)
}

func TestUpdate(t *testing.T) {
	r := require.New(t)
	a := require.New(t)
	os.RemoveAll("testrun")
	os.Mkdir("testrun", 0700)
	m, err := New("testrun/update", testFeed(0))
	r.NoError(err)
	defer m.Close()

	peer := testFeed(1)

	// initial update from peer: they know about feeds 2 and 3
	update := ssb.NetworkFrontier{
		testFeed(2).String(): ssb.Note{Seq: 10, Replicate: true, Receive: true},
		testFeed(3).String(): ssb.Note{Seq: 20, Replicate: true, Receive: false},
	}
	merged, err := m.Update(peer, update)
	r.NoError(err)
	a.Equal(int64(10), merged[testFeed(2).String()].Seq)
	a.Equal(int64(20), merged[testFeed(3).String()].Seq)
	a.True(merged[testFeed(2).String()].Receive)
	a.False(merged[testFeed(3).String()].Receive)

	// incremental update: peer now has more of feed 2, and adds feed 4
	update2 := ssb.NetworkFrontier{
		testFeed(2).String(): ssb.Note{Seq: 15, Replicate: true, Receive: true},
		testFeed(4).String(): ssb.Note{Seq: 5, Replicate: true, Receive: true},
	}
	merged, err = m.Update(peer, update2)
	r.NoError(err)

	// feed 2 should be updated
	a.Equal(int64(15), merged[testFeed(2).String()].Seq)
	// feed 3 should still be there from previous update
	a.Equal(int64(20), merged[testFeed(3).String()].Seq)
	// feed 4 should be new
	a.Equal(int64(5), merged[testFeed(4).String()].Seq)
}

func TestWantsList(t *testing.T) {
	r := require.New(t)
	os.RemoveAll("testrun")
	os.Mkdir("testrun", 0700)
	m, err := New("testrun/wantslist", testFeed(0))
	r.NoError(err)
	defer m.Close()

	peer := testFeed(1)

	feeds := []ObservedFeed{
		{Feed: testFeed(2), Note: ssb.Note{Replicate: true, Receive: true, Seq: 5}},
		{Feed: testFeed(3), Note: ssb.Note{Replicate: true, Receive: false, Seq: 10}},
		{Feed: testFeed(4), Note: ssb.Note{Replicate: true, Receive: true, Seq: 15}},
		{Feed: testFeed(5), Note: ssb.Note{Replicate: false, Receive: false, Seq: 0}},
	}
	r.NoError(m.Fill(peer, feeds))

	wants, err := m.WantsList(peer)
	r.NoError(err)

	// only feeds with Receive=true should be in the list
	r.Len(wants, 2)

	wantStrs := make(map[string]bool)
	for _, w := range wants {
		wantStrs[w.String()] = true
	}
	r.True(wantStrs[testFeed(2).String()], "feed 2 should be wanted")
	r.True(wantStrs[testFeed(4).String()], "feed 4 should be wanted")
}

func TestWantsFeed(t *testing.T) {
	r := require.New(t)
	os.RemoveAll("testrun")
	os.Mkdir("testrun", 0700)
	m, err := New("testrun/wantsfeed", testFeed(0))
	r.NoError(err)
	defer m.Close()

	peer := testFeed(1)

	feeds := []ObservedFeed{
		{Feed: testFeed(2), Note: ssb.Note{Replicate: true, Receive: true, Seq: 5}},
		{Feed: testFeed(3), Note: ssb.Note{Replicate: true, Receive: false, Seq: 10}},
	}
	r.NoError(m.Fill(peer, feeds))

	wants, err := m.WantsFeed(peer, testFeed(2))
	r.NoError(err)
	r.True(wants, "peer should want feed 2")

	wants, err = m.WantsFeed(peer, testFeed(3))
	r.NoError(err)
	r.False(wants, "peer should not want feed 3 (receive=false)")

	wants, err = m.WantsFeed(peer, testFeed(99))
	r.NoError(err)
	r.False(wants, "peer should not want unknown feed")
}

func TestInspect(t *testing.T) {
	r := require.New(t)
	os.RemoveAll("testrun")
	os.Mkdir("testrun", 0700)
	m, err := New("testrun/inspect", testFeed(0))
	r.NoError(err)
	defer m.Close()

	// inspect empty peer
	nf, err := m.Inspect(testFeed(1))
	r.NoError(err)
	r.Len(nf, 0)

	// fill and inspect
	feeds := []ObservedFeed{
		{Feed: testFeed(2), Note: ssb.Note{Replicate: true, Receive: true, Seq: 42}},
	}
	r.NoError(m.Fill(testFeed(1), feeds))

	nf, err = m.Inspect(testFeed(1))
	r.NoError(err)
	r.Len(nf, 1)
	r.Equal(int64(42), nf[testFeed(2).String()].Seq)
}

func TestSaveAndReload(t *testing.T) {
	r := require.New(t)
	os.RemoveAll("testrun")
	os.Mkdir("testrun", 0700)

	self := testFeed(0)
	peer := testFeed(1)

	// create, fill, save, close
	m, err := New("testrun/persist", self)
	r.NoError(err)

	feeds := []ObservedFeed{
		{Feed: testFeed(2), Note: ssb.Note{Replicate: true, Receive: true, Seq: 100}},
		{Feed: testFeed(3), Note: ssb.Note{Replicate: true, Receive: false, Seq: 200}},
	}
	r.NoError(m.Fill(peer, feeds))
	r.NoError(m.SaveAndClose(peer))
	r.NoError(m.Close())

	// reopen and verify data persisted
	m2, err := New("testrun/persist", self)
	r.NoError(err)
	defer m2.Close()

	nf, err := m2.Inspect(peer)
	r.NoError(err)
	r.Len(nf, 2)
	r.Equal(int64(100), nf[testFeed(2).String()].Seq)
	r.Equal(int64(200), nf[testFeed(3).String()].Seq)
	r.True(nf[testFeed(2).String()].Receive)
	r.False(nf[testFeed(3).String()].Receive)
}

func TestFillDeletesOnNoReplicate(t *testing.T) {
	r := require.New(t)
	os.RemoveAll("testrun")
	os.Mkdir("testrun", 0700)
	m, err := New("testrun/filldelete", testFeed(0))
	r.NoError(err)
	defer m.Close()

	peer := testFeed(1)

	// add a feed
	feeds := []ObservedFeed{
		{Feed: testFeed(2), Note: ssb.Note{Replicate: true, Receive: true, Seq: 10}},
	}
	r.NoError(m.Fill(peer, feeds))

	nf, err := m.Inspect(peer)
	r.NoError(err)
	r.Len(nf, 1)

	// remove it by setting Replicate=false
	feeds = []ObservedFeed{
		{Feed: testFeed(2), Note: ssb.Note{Replicate: false}},
	}
	r.NoError(m.Fill(peer, feeds))

	nf, err = m.Inspect(peer)
	r.NoError(err)
	r.Len(nf, 0, "feed should be deleted when Replicate=false")
}
