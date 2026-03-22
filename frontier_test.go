// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package ssb

import (
	"bytes"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	refs "github.com/ssbc/go-ssb-refs"
)

func makeFeedRef(i int) refs.FeedRef {
	k := bytes.Repeat([]byte{byte(i)}, 32)
	ref, err := refs.NewFeedRefFromBytes(k, refs.RefAlgoFeedSSB1)
	if err != nil {
		panic(err)
	}
	return ref
}

func TestFrontierNewAndLen(t *testing.T) {
	f := NewFrontier()
	assert.Equal(t, 0, f.Len())

	f = NewFrontier(
		FeedSeq{Feed: makeFeedRef(1), Seq: 10},
		FeedSeq{Feed: makeFeedRef(2), Seq: 20},
	)
	assert.Equal(t, 2, f.Len())
}

func TestFrontierDeduplication(t *testing.T) {
	// Same feed twice — should keep the higher seq
	f := NewFrontier(
		FeedSeq{Feed: makeFeedRef(1), Seq: 10},
		FeedSeq{Feed: makeFeedRef(1), Seq: 50},
	)
	assert.Equal(t, 1, f.Len())
	assert.Equal(t, int64(50), f.Seq(makeFeedRef(1)))
}

func TestFrontierSet(t *testing.T) {
	f := NewFrontier(FeedSeq{Feed: makeFeedRef(1), Seq: 10})

	// Add new feed
	f2 := f.Set(makeFeedRef(2), 20)
	assert.Equal(t, 2, f2.Len())
	assert.Equal(t, int64(20), f2.Seq(makeFeedRef(2)))

	// Original unchanged (immutability)
	assert.Equal(t, 1, f.Len())

	// Update existing feed
	f3 := f2.Set(makeFeedRef(1), 99)
	assert.Equal(t, 2, f3.Len())
	assert.Equal(t, int64(99), f3.Seq(makeFeedRef(1)))
}

func TestFrontierSeqNotFound(t *testing.T) {
	f := NewFrontier(FeedSeq{Feed: makeFeedRef(1), Seq: 10})
	assert.Equal(t, int64(-1), f.Seq(makeFeedRef(99)))
}

func TestFrontierEqual(t *testing.T) {
	a := NewFrontier(
		FeedSeq{Feed: makeFeedRef(1), Seq: 10},
		FeedSeq{Feed: makeFeedRef(2), Seq: 20},
	)
	b := NewFrontier(
		FeedSeq{Feed: makeFeedRef(2), Seq: 20},
		FeedSeq{Feed: makeFeedRef(1), Seq: 10},
	)
	assert.True(t, a.Equal(b))

	c := NewFrontier(
		FeedSeq{Feed: makeFeedRef(1), Seq: 10},
		FeedSeq{Feed: makeFeedRef(2), Seq: 21},
	)
	assert.False(t, a.Equal(c))
}

func TestFrontierHappenedBefore(t *testing.T) {
	r := require.New(t)

	empty := NewFrontier()
	a := NewFrontier(
		FeedSeq{Feed: makeFeedRef(1), Seq: 10},
		FeedSeq{Feed: makeFeedRef(2), Seq: 20},
	)
	b := NewFrontier(
		FeedSeq{Feed: makeFeedRef(1), Seq: 15},
		FeedSeq{Feed: makeFeedRef(2), Seq: 25},
	)

	// Empty happened before everything
	r.True(empty.HappenedBefore(a))
	r.True(empty.HappenedBefore(empty))

	// a ≤ b (all seqs in a are ≤ corresponding seqs in b)
	r.True(a.HappenedBefore(b))

	// b did not happen before a
	r.False(b.HappenedBefore(a))

	// Equal frontiers: a ≤ a
	r.True(a.HappenedBefore(a))

	// Superset: b has extra feeds → a still ≤ b
	bPlus := b.Set(makeFeedRef(3), 100)
	r.True(a.HappenedBefore(bPlus))

	// But bPlus did not happen before a (has feed 3 that a doesn't)
	r.False(bPlus.HappenedBefore(a))
}

func TestFrontierConcurrent(t *testing.T) {
	// a has feed 1 higher, b has feed 2 higher → concurrent
	a := NewFrontier(
		FeedSeq{Feed: makeFeedRef(1), Seq: 20},
		FeedSeq{Feed: makeFeedRef(2), Seq: 10},
	)
	b := NewFrontier(
		FeedSeq{Feed: makeFeedRef(1), Seq: 10},
		FeedSeq{Feed: makeFeedRef(2), Seq: 20},
	)
	assert.True(t, a.Concurrent(b))
	assert.True(t, b.Concurrent(a))

	// a ≤ c → not concurrent
	c := NewFrontier(
		FeedSeq{Feed: makeFeedRef(1), Seq: 20},
		FeedSeq{Feed: makeFeedRef(2), Seq: 20},
	)
	assert.False(t, a.Concurrent(c))
}

func TestFrontierDiff(t *testing.T) {
	a := NewFrontier(
		FeedSeq{Feed: makeFeedRef(1), Seq: 10},
		FeedSeq{Feed: makeFeedRef(2), Seq: 20},
	)
	b := NewFrontier(
		FeedSeq{Feed: makeFeedRef(1), Seq: 15},
		FeedSeq{Feed: makeFeedRef(3), Seq: 5},
	)

	diff := a.Diff(b)
	assert.Len(t, diff, 3)

	// Build a map for easier assertions
	byFeed := make(map[string]FeedAdvance)
	for _, d := range diff {
		byFeed[d.Feed.String()] = d
	}

	// Feed 1: advanced from 10 to 15
	d1 := byFeed[makeFeedRef(1).String()]
	assert.Equal(t, int64(10), d1.OldSeq)
	assert.Equal(t, int64(15), d1.NewSeq)

	// Feed 2: dropped (in a but not b)
	d2 := byFeed[makeFeedRef(2).String()]
	assert.Equal(t, int64(20), d2.OldSeq)
	assert.Equal(t, int64(-1), d2.NewSeq)

	// Feed 3: new (in b but not a)
	d3 := byFeed[makeFeedRef(3).String()]
	assert.Equal(t, int64(-1), d3.OldSeq)
	assert.Equal(t, int64(5), d3.NewSeq)
}

func TestFrontierDiffEmpty(t *testing.T) {
	a := NewFrontier(FeedSeq{Feed: makeFeedRef(1), Seq: 10})
	diff := a.Diff(a)
	assert.Len(t, diff, 0, "diff of identical frontiers should be empty")
}

func TestFrontierMerge(t *testing.T) {
	a := NewFrontier(
		FeedSeq{Feed: makeFeedRef(1), Seq: 10},
		FeedSeq{Feed: makeFeedRef(2), Seq: 20},
	)
	b := NewFrontier(
		FeedSeq{Feed: makeFeedRef(1), Seq: 15},
		FeedSeq{Feed: makeFeedRef(3), Seq: 5},
	)

	merged := a.Merge(b)
	assert.Equal(t, 3, merged.Len())
	assert.Equal(t, int64(15), merged.Seq(makeFeedRef(1))) // max(10, 15)
	assert.Equal(t, int64(20), merged.Seq(makeFeedRef(2))) // only in a
	assert.Equal(t, int64(5), merged.Seq(makeFeedRef(3)))  // only in b
}

func TestFrontierMarshalRoundtrip(t *testing.T) {
	r := require.New(t)

	original := NewFrontier(
		FeedSeq{Feed: makeFeedRef(1), Seq: 42},
		FeedSeq{Feed: makeFeedRef(2), Seq: 9999},
		FeedSeq{Feed: makeFeedRef(3), Seq: 1},
	)

	data, err := original.Marshal()
	r.NoError(err)

	decoded, err := UnmarshalFrontier(data)
	r.NoError(err)

	r.True(original.Equal(decoded), "roundtrip should preserve equality")
	r.Equal(original.Len(), decoded.Len())
	for _, e := range original.Feeds() {
		r.Equal(e.Seq, decoded.Seq(e.Feed))
	}
}

func TestFrontierMarshalEmpty(t *testing.T) {
	r := require.New(t)

	empty := NewFrontier()
	data, err := empty.Marshal()
	r.NoError(err)

	decoded, err := UnmarshalFrontier(data)
	r.NoError(err)
	r.Equal(0, decoded.Len())
}

func TestFrontierFromNetworkFrontier(t *testing.T) {
	r := require.New(t)

	feed1 := makeFeedRef(1)
	feed2 := makeFeedRef(2)
	feed3 := makeFeedRef(3)

	nf := NetworkFrontier{
		feed1.String(): Note{Seq: 10, Replicate: true, Receive: true},
		feed2.String(): Note{Seq: 20, Replicate: true, Receive: false},
		feed3.String(): Note{Seq: -1, Replicate: false, Receive: false}, // don't want
	}

	f, err := FrontierFromNetworkFrontier(nf)
	r.NoError(err)

	r.Equal(2, f.Len(), "should skip non-replicated feeds")
	r.Equal(int64(10), f.Seq(feed1))
	r.Equal(int64(20), f.Seq(feed2))
	r.Equal(int64(-1), f.Seq(feed3), "non-replicated feed should not be in frontier")
}

func TestFrontierString(t *testing.T) {
	f := NewFrontier()
	assert.Equal(t, "Frontier{}", f.String())

	f = NewFrontier(FeedSeq{Feed: makeFeedRef(1), Seq: 10})
	s := f.String()
	assert.Contains(t, s, "1 feeds")
}

// --- Fork Proof Tests ---

// fakeMessage implements refs.Message for testing fork proofs.
type fakeMessage struct {
	author refs.FeedRef
	seq    int64
	key    refs.MessageRef
}

func (m *fakeMessage) Seq() int64                       { return m.seq }
func (m *fakeMessage) Author() refs.FeedRef             { return m.author }
func (m *fakeMessage) Key() refs.MessageRef              { return m.key }
func (m *fakeMessage) Previous() *refs.MessageRef        { return nil }
func (m *fakeMessage) Claimed() time.Time                { return time.Now() }
func (m *fakeMessage) Received() time.Time               { return time.Now() }
func (m *fakeMessage) ContentBytes() []byte              { return nil }
func (m *fakeMessage) ValueContent() *refs.Value         { return nil }
func (m *fakeMessage) ValueContentJSON() json.RawMessage { return nil }

func testMsgRef(i int) refs.MessageRef {
	k := bytes.Repeat([]byte(strconv.Itoa(i)), 32)[:32]
	ref, err := refs.NewMessageRefFromBytes(k, refs.RefAlgoMessageSSB1)
	if err != nil {
		panic(err)
	}
	return ref
}

func TestForkProofValidate(t *testing.T) {
	r := require.New(t)

	author := makeFeedRef(1)
	left := &fakeMessage{author: author, seq: 5, key: testMsgRef(1)}
	right := &fakeMessage{author: author, seq: 5, key: testMsgRef(2)}

	fp := ForkProof{
		Observed: NewFrontier(FeedSeq{Feed: author, Seq: 10}),
		Left:     left,
		Right:    right,
	}
	r.NoError(fp.Validate())

	// Author should match
	a, err := fp.Author()
	r.NoError(err)
	r.True(author.Equal(a))

	// Seq should be 5
	seq, err := fp.ForkedSeq()
	r.NoError(err)
	r.Equal(int64(5), seq)
}

func TestForkProofValidateDifferentAuthors(t *testing.T) {
	left := &fakeMessage{author: makeFeedRef(1), seq: 5, key: testMsgRef(1)}
	right := &fakeMessage{author: makeFeedRef(2), seq: 5, key: testMsgRef(2)}

	fp := ForkProof{Left: left, Right: right}
	err := fp.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "authors differ")
}

func TestForkProofValidateDifferentSeqs(t *testing.T) {
	author := makeFeedRef(1)
	left := &fakeMessage{author: author, seq: 5, key: testMsgRef(1)}
	right := &fakeMessage{author: author, seq: 6, key: testMsgRef(2)}

	fp := ForkProof{Left: left, Right: right}
	err := fp.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "sequences differ")
}

func TestForkProofValidateSameKey(t *testing.T) {
	author := makeFeedRef(1)
	key := testMsgRef(1)
	left := &fakeMessage{author: author, seq: 5, key: key}
	right := &fakeMessage{author: author, seq: 5, key: key}

	fp := ForkProof{Left: left, Right: right}
	err := fp.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "same key")
}

func TestForkProofValidateNilMessages(t *testing.T) {
	fp := ForkProof{}
	err := fp.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "non-nil")
}

func TestForkProofString(t *testing.T) {
	author := makeFeedRef(1)
	fp := ForkProof{
		Observed: NewFrontier(FeedSeq{Feed: author, Seq: 10}),
		Left:     &fakeMessage{author: author, seq: 5, key: testMsgRef(1)},
		Right:    &fakeMessage{author: author, seq: 5, key: testMsgRef(2)},
	}
	s := fp.String()
	assert.Contains(t, s, "ForkProof")
	assert.Contains(t, s, "seq:5")
}
