// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package ssb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	refs "github.com/ssbc/go-ssb-refs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNoteMarshalRoundtrip(t *testing.T) {
	cases := []struct {
		name string
		note Note
		want string // expected JSON integer value
	}{
		{
			name: "not replicated",
			note: Note{Seq: 0, Replicate: false, Receive: false},
			want: "-1",
		},
		{
			name: "empty feed, receiving",
			note: Note{Seq: -1, Replicate: true, Receive: true},
			want: "0", // -1 maps to 0 (no messages)
		},
		{
			name: "empty feed, not receiving",
			note: Note{Seq: -1, Replicate: true, Receive: false},
			want: "1", // 0 << 1 | 1
		},
		{
			name: "seq 1, receiving",
			note: Note{Seq: 1, Replicate: true, Receive: true},
			want: "2", // 1 << 1 | 0
		},
		{
			name: "seq 1, not receiving",
			note: Note{Seq: 1, Replicate: true, Receive: false},
			want: "3", // 1 << 1 | 1
		},
		{
			name: "seq 5, receiving",
			note: Note{Seq: 5, Replicate: true, Receive: true},
			want: "10", // 5 << 1 | 0
		},
		{
			name: "seq 5, not receiving",
			note: Note{Seq: 5, Replicate: true, Receive: false},
			want: "11", // 5 << 1 | 1
		},
		{
			name: "seq 0, receiving (no messages in SSB terms)",
			note: Note{Seq: 0, Replicate: true, Receive: true},
			want: "0", // 0 << 1 | 0
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			a := assert.New(t)

			data, err := tc.note.MarshalJSON()
			r.NoError(err)
			a.Equal(tc.want, string(data), "marshal mismatch")
		})
	}
}

func TestNoteMarshalUnmarshalRoundtrip(t *testing.T) {
	// Notes where Seq >= 0 and Replicate=true should roundtrip exactly
	cases := []Note{
		{Seq: 1, Replicate: true, Receive: true},
		{Seq: 1, Replicate: true, Receive: false},
		{Seq: 5, Replicate: true, Receive: true},
		{Seq: 5, Replicate: true, Receive: false},
		{Seq: 100, Replicate: true, Receive: true},
		{Seq: 100, Replicate: true, Receive: false},
	}

	for _, original := range cases {
		name := fmt.Sprintf("seq%d_recv%v", original.Seq, original.Receive)
		t.Run(name, func(t *testing.T) {
			r := require.New(t)
			a := assert.New(t)

			// marshal the note
			data, err := original.MarshalJSON()
			r.NoError(err)

			// wrap in a NetworkFrontier for unmarshaling
			feedRef := testFeedRef(t, 1)
			nfJSON := fmt.Sprintf(`{%q:%s}`, feedRef.String(), string(data))

			var nf NetworkFrontier
			err = json.Unmarshal([]byte(nfJSON), &nf)
			r.NoError(err)

			decoded, has := nf[feedRef.String()]
			r.True(has, "feed not found in decoded frontier")
			a.Equal(original.Seq, decoded.Seq, "Seq mismatch")
			a.Equal(original.Replicate, decoded.Replicate, "Replicate mismatch")
			a.Equal(original.Receive, decoded.Receive, "Receive mismatch")
		})
	}
}

func TestNoteNotReplicateRoundtrip(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)

	// Replicate=false encodes as -1
	note := Note{Seq: 42, Replicate: false, Receive: true}
	data, err := note.MarshalJSON()
	r.NoError(err)
	a.Equal("-1", string(data))

	// Unmarshal -1 back
	feedRef := testFeedRef(t, 1)
	nfJSON := fmt.Sprintf(`{%q:-1}`, feedRef.String())
	var nf NetworkFrontier
	err = json.Unmarshal([]byte(nfJSON), &nf)
	r.NoError(err)

	decoded, has := nf[feedRef.String()]
	r.True(has)
	a.False(decoded.Replicate, "should not replicate")
	// when Replicate is false, Seq and Receive are undefined
}

func TestNoteEmptyFeedMarshal(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)

	// margaret -1 (empty feed) should marshal as 0 (no messages in protocol terms)
	note := Note{Seq: -1, Replicate: true, Receive: true}
	data, err := note.MarshalJSON()
	r.NoError(err)
	a.Equal("0", string(data))

	// 0 with receive=false should be 1
	note.Receive = false
	data, err = note.MarshalJSON()
	r.NoError(err)
	a.Equal("1", string(data))
}

func TestNetworkFrontierRoundtrip(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)

	f1 := testFeedRef(t, 1)
	f2 := testFeedRef(t, 2)
	f3 := testFeedRef(t, 3)

	original := NetworkFrontier{
		f1.String(): Note{Seq: 5, Replicate: true, Receive: true},
		f2.String(): Note{Seq: 10, Replicate: true, Receive: false},
		f3.String(): Note{Seq: 0, Replicate: false, Receive: false},
	}

	data, err := json.Marshal(original)
	r.NoError(err)

	var decoded NetworkFrontier
	err = json.Unmarshal(data, &decoded)
	r.NoError(err)

	// f1: seq 5, replicate, receive
	n1, has := decoded[f1.String()]
	r.True(has)
	a.Equal(int64(5), n1.Seq)
	a.True(n1.Replicate)
	a.True(n1.Receive)

	// f2: seq 10, replicate, no receive
	n2, has := decoded[f2.String()]
	r.True(has)
	a.Equal(int64(10), n2.Seq)
	a.True(n2.Replicate)
	a.False(n2.Receive)

	// f3: not replicated - decoded as -1, so it has Replicate=false
	n3, has := decoded[f3.String()]
	r.True(has)
	a.False(n3.Replicate)
}

func TestNetworkFrontierSkipsInvalidFeeds(t *testing.T) {
	r := require.New(t)

	// invalid feed ref should be skipped
	nfJSON := `{"not-a-feed":10, "also-bad":-1}`
	var nf NetworkFrontier
	err := json.Unmarshal([]byte(nfJSON), &nf)
	r.NoError(err)
	r.Len(nf, 0)
}

func TestNetworkFrontierSkipsNonSSB1(t *testing.T) {
	r := require.New(t)

	// only SSB1 feeds are supported
	feed := testFeedRef(t, 1)
	nfJSON := fmt.Sprintf(`{%q:10}`, feed.String())

	var nf NetworkFrontier
	err := json.Unmarshal([]byte(nfJSON), &nf)
	r.NoError(err)
	r.Len(nf, 1, "SSB1 feed should be included")
}

// testFeedRef creates a deterministic test feed reference
func testFeedRef(t *testing.T, i int) refs.FeedRef {
	t.Helper()
	k := bytes.Repeat([]byte{byte(i)}, 32)
	ref, err := refs.NewFeedRefFromBytes(k, refs.RefAlgoFeedSSB1)
	if err != nil {
		t.Fatal(err)
	}
	return ref
}
