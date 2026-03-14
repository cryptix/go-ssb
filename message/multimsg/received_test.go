// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package multimsg

import (
	"bytes"
	"crypto/ed25519"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ssbc/go-ssb/internal/storedrefs"
	"github.com/ssbc/go-ssb/message/legacy"
	"github.com/ssbc/margaret/v2/offset2"
	"github.com/stretchr/testify/require"

	gabbygrove "github.com/ssbc/go-gabbygrove"
	refs "github.com/ssbc/go-ssb-refs"
)

// fakeNow can be used as a stub for time.Now()
// it unsets itself each call so set the next field before calling it
type fakeNow struct{ next time.Time }

func (fn *fakeNow) Now() time.Time {
	nxt := fn.next
	fn.next = time.Unix(13*60*60+37*60, 0)
	return nxt
}

func TestReceivedSet(t *testing.T) {
	r := require.New(t)

	tPath := filepath.Join("testrun", t.Name())
	os.RemoveAll(tPath)

	oLog, err := offset2.Open[*MultiMessage](tPath)
	r.NoError(err)

	wl := NewWrappedLog(oLog)
	fn := fakeNow{}
	wl.receivedNow = fn.Now

	// generate a keypair for testing without importing go-ssb root
	pubKey, privKey, err := ed25519.GenerateKey(nil)
	r.NoError(err)
	bobsFeed, err := refs.NewFeedRefFromBytes(pubKey, refs.RefAlgoFeedGabby)
	r.NoError(err)
	// quirky way to make a refs.Message

	msgKey, err := refs.NewMessageRefFromBytes(bytes.Repeat([]byte("acab"), 8), refs.RefAlgoMessageSSB1)
	r.NoError(err)

	var lm legacy.LegacyMessage
	lm.Hash = "sha256"
	lm.Author = bobsFeed.String()
	lm.Previous = nil
	lm.Sequence = 666

	newMsg := &legacy.StoredMessage{
		Key_:      storedrefs.SerialzedMessage{MessageRef: msgKey},
		Author_:   storedrefs.SerialzedFeed{FeedRef: bobsFeed},
		Previous_: nil,
		Sequence_: int64(lm.Sequence),
		Raw_:      []byte(`"fakemsg"`),
	}

	fn.next = time.Unix(23, 0)
	seq, err := wl.AppendMessage(newMsg)
	r.NoError(err)
	r.NotNil(seq)

	// retreive it
	gotMsg, err := wl.Get(seq)
	r.NoError(err)

	// check the received
	rxt := gotMsg.Received()
	r.NotNil(rxt)
	r.EqualValues(23, rxt.Unix(), "time: %s", rxt)

	// a gabby message

	enc := gabbygrove.NewEncoder(ed25519.PrivateKey(privKey))

	tr, ref, err := enc.Encode(1, gabbygrove.BinaryRef{}, "hello, world")
	r.NoError(err)
	r.NotNil(ref)

	fn.next = time.Unix(42, 0)
	ggSeq, err := wl.AppendMessage(tr)
	r.NoError(err)
	r.NotNil(ggSeq)

	gotMsg, err = wl.Get(ggSeq)
	r.NoError(err)

	rxt = gotMsg.Received()
	r.NotNil(rxt)
	r.EqualValues(42, rxt.Unix(), "time: %s", rxt)

	r.NoError(oLog.Close())
}
