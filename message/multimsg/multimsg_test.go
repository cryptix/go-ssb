// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package multimsg

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"io"
	"testing"

	gabbygrove "github.com/ssbc/go-gabbygrove"
	refs "github.com/ssbc/go-ssb-refs"
	"github.com/stretchr/testify/require"

	"github.com/ssbc/go-ssb/internal/storedrefs"
	"github.com/ssbc/go-ssb/message/legacy"
)

func newTestKeyPair(r io.Reader, algo refs.RefAlgo) (refs.FeedRef, error) {
	pubKey, _, err := ed25519.GenerateKey(r)
	if err != nil {
		return refs.FeedRef{}, err
	}
	return refs.NewFeedRefFromBytes(pubKey, algo)
}

func TestMultiMsgLegacy(t *testing.T) {
	r := require.New(t)

	kpSeed := bytes.Repeat([]byte("feed"), 8)
	feedRef, err := newTestKeyPair(bytes.NewReader(kpSeed), refs.RefAlgoFeedSSB1)
	r.NoError(err)

	msgKey, err := refs.NewMessageRefFromBytes(bytes.Repeat([]byte("acab"), 8), refs.RefAlgoMessageSSB1)
	r.NoError(err)

	// craft legacy testmessage
	testContent := []byte(`{Hello: world}`)
	var lm legacy.StoredMessage
	lm.Author_ = storedrefs.SerialzedFeed{FeedRef: feedRef}
	lm.Sequence_ = 123
	lm.Key_ = storedrefs.SerialzedMessage{MessageRef: msgKey}
	lm.Raw_ = testContent

	var mm MultiMessage
	mm.tipe = Legacy
	mm.Message = &lm

	b, err := mm.MarshalBinary()
	r.NoError(err)

	t.Log("\n", hex.Dump(b))

	r.Equal(Legacy, MessageType(b[0]))

	var mm2 MultiMessage
	err = mm2.UnmarshalBinary(b)
	r.NoError(err)
	r.NotNil(mm2.Message)
	r.Equal(Legacy, mm2.tipe)
	r.True(mm2.key.Equal(msgKey))
	legacy, ok := mm2.AsLegacy()
	r.True(ok)
	r.Equal(testContent, legacy.Raw_)
	r.EqualValues(123, legacy.Seq())
}

func TestMultiMsgGabby(t *testing.T) {
	r := require.New(t)

	kpSeed := bytes.Repeat([]byte("bee4"), 8)
	feedRef, err := newTestKeyPair(bytes.NewReader(kpSeed), refs.RefAlgoFeedGabby)
	r.NoError(err)

	authorRef, err := gabbygrove.NewBinaryRef(feedRef)
	r.NoError(err)

	cref, err := gabbygrove.NewContentRefFromBytes(kpSeed)
	r.NoError(err)

	payloadRef, err := gabbygrove.NewBinaryRef(cref)
	r.NoError(err)

	var evt = &gabbygrove.Event{
		Author:   authorRef,
		Sequence: 123,
		Content: gabbygrove.Content{
			Hash: payloadRef,
			Size: 23,
			Type: gabbygrove.ContentTypeJSON,
		},
	}

	evtBytes, err := evt.MarshalCBOR()
	r.NoError(err)

	testContent := []byte("someContent")
	tr := &gabbygrove.Transfer{
		Event:     evtBytes,
		Signature: []byte("none"),
		Content:   testContent,
	}

	var mm MultiMessage
	mm.tipe = Gabby
	mm.Message = tr

	b, err := mm.MarshalBinary()
	r.NoError(err)
	r.Equal(Gabby, MessageType(b[0]))

	var mm2 MultiMessage
	err = mm2.UnmarshalBinary(b)
	r.NoError(err)
	r.Equal(Gabby, mm.tipe)
	gabby, ok := mm2.AsGabby()
	r.True(ok)
	r.NotNil(gabby)
	r.Equal(testContent, gabby.Content)
	r.Equal([]byte("none"), gabby.Signature)
	evt2, err := gabby.UnmarshaledEvent()
	r.NoError(err)
	r.EqualValues(123, evt2.Sequence)
}
