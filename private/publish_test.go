// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package private_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ssbc/margaret/v2/multilog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	kitlog "go.mindeco.de/log"

	"github.com/ssbc/go-ssb"
	refs "github.com/ssbc/go-ssb-refs"
	"github.com/ssbc/go-ssb/client"
	"github.com/ssbc/go-ssb/internal/storedrefs"
	"github.com/ssbc/go-ssb/multilogs"
	"github.com/ssbc/go-ssb/private"
	"github.com/ssbc/go-ssb/sbot"
)

func TestPrivatePublish(t *testing.T) {
	t.Run("classic", testPublishPerAlgo(refs.RefAlgoFeedSSB1))
	t.Run("gabby", testPublishPerAlgo(refs.RefAlgoFeedGabby))
}

func testPublishPerAlgo(algo refs.RefAlgo) func(t *testing.T) {
	return func(t *testing.T) {
		r, a := require.New(t), assert.New(t)

		srvRepo := filepath.Join("testrun", t.Name(), "serv")
		os.RemoveAll(srvRepo)

		alice, err := ssb.NewKeyPair(bytes.NewReader(bytes.Repeat([]byte("alice"), 8)), algo)
		r.NoError(err)

		srvLog := kitlog.NewNopLogger()
		if testing.Verbose() {
			srvLog = kitlog.NewJSONLogger(os.Stderr)
		}

		srv, err := sbot.New(
			sbot.WithKeyPair(alice),
			sbot.WithInfo(srvLog),
			sbot.WithRepoPath(srvRepo),
			sbot.WithListenAddr(":0"),
			sbot.LateOption(sbot.WithUNIXSocket()),
		)
		r.NoError(err, "failed to init sbot")

		const n = 32
		for i := n; i > 0; i-- {
			_, err := srv.PublishLog.Publish(struct {
				Type string `json:"type"`
				Text string
				I    int
			}{"test", "clear text!", i})
			r.NoError(err)
		}

		r.NoError(err, "sbot srv init failed")

		c, err := client.NewUnix(filepath.Join(srvRepo, "socket"))
		r.NoError(err, "failed to make client connection")

		type msg struct {
			Type string `json:"type"`
			Msg  string
		}
		ref, err := c.PrivatePublish(msg{"test", "hello, world"}, alice.ID())
		r.NoError(err, "failed to publish")
		r.NotNil(ref)

		// wait for the private index to catch up
		time.Sleep(500 * time.Millisecond)

		src, err := c.PrivateRead()
		r.NoError(err, "failed to open private stream")

		count := 0
		var savedMsg refs.KeyValueRaw
		for rawMsg := range src.Iter(context.TODO()) {
			err = json.Unmarshal(rawMsg, &savedMsg)
			r.NoError(err, "failed to unpack msg")
			count++
		}
		r.Equal(1, count, "expected exactly one private message from stream")

		if !a.True(savedMsg.Key().Equal(ref)) {
			whoops, err := srv.Get(ref)
			r.NoError(err)
			t.Log(string(whoops.ContentBytes()))
		}

		// try with v2 query (SeqWrap removed; iterator yields (seq, value) tuples)
		pl, ok := srv.GetMultiLog(multilogs.IndexNamePrivates)
		r.True(ok)

		userPrivs, err := pl.Get(multilog.Addr("box1:") + storedrefs.Feed(srv.KeyPair.ID()))
		r.NoError(err)

		unboxlog := private.NewUnboxerLog(srv.ReceiveLog, userPrivs, srv.KeyPair)

		qry := unboxlog.Query()
		count = 0
		for _, wrappedMsg := range qry.Iter() {
			r.Equal(wrappedMsg.Key().String(), ref.String())
			count++
		}
		r.NoError(qry.Err())
		r.Equal(1, count, "expected exactly one private message")

		// shutdown
		a.NoError(c.Close())
		srv.Shutdown()
		r.NoError(srv.Close())
	}
}
