// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package client_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/ssbc/go-muxrpc/v3"
	margaret "github.com/ssbc/margaret/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ssbc/go-ssb"
	refs "github.com/ssbc/go-ssb-refs"
	"github.com/ssbc/go-ssb/client"
	"github.com/ssbc/go-ssb/internal/testutils"
	"github.com/ssbc/go-ssb/message"
	"github.com/ssbc/go-ssb/sbot"
)

func TestReadStreamAsInterfaceMessage(t *testing.T) {
	r, a := require.New(t), assert.New(t)

	srvRepo := filepath.Join("testrun", t.Name(), "serv")
	os.RemoveAll(srvRepo)

	srvLog := testutils.NewRelativeTimeLogger(nil)

	srv, err := sbot.New(
		sbot.WithInfo(srvLog),
		sbot.WithRepoPath(srvRepo),
		sbot.WithListenAddr(":0"))
	r.NoError(err, "sbot srv init failed")

	var srvErrc = make(chan error, 1)
	go func() {
		err := srv.Network.Serve(context.TODO())
		if err != nil {
			srvErrc <- fmt.Errorf("ali serve exited: %w", err)
		}
		close(srvErrc)
	}()

	kp, err := ssb.LoadKeyPair(filepath.Join(srvRepo, "secret"))
	r.NoError(err, "failed to load servers keypair")
	srvAddr := srv.Network.GetListenAddr()
	r.NotNil(srvAddr, "listener not ready")

	c, err := client.NewTCP(kp, srvAddr)
	r.NoError(err, "failed to make client connection")
	// end test boilerplate

	// no messages yet
	r.Equal(margaret.SeqEmpty, srv.ReceiveLog.Seq())

	var wantRefs []string
	for i := 0; i < 10; i++ {
		msg := testMsg{"test", "hello", 23}
		ref, err := c.Publish(msg)
		r.NoError(err, "failed to call publish")
		r.NotNil(ref)

		// get stored message from the log
		wantSeq := int64(i)
		a.Equal(wantSeq, srv.ReceiveLog.Seq())
		mm, err := srv.ReceiveLog.Get(wantSeq)
		r.NoError(err)
		r.NotNil(mm.Message)
		newMsg := mm.Message
		r.Equal(newMsg.Key(), ref)
		wantRefs = append(wantRefs, ref.String())

		opts := message.CreateLogArgs{}
		opts.Keys = true
		opts.Limit = 1
		opts.Seq = int64(i)

		src, err := c.CreateLogStream(opts)
		r.NoError(err)

		ctx := context.TODO()
		count := 0
		var streamMsg refs.KeyValueRaw
		for streamMsg = range muxrpc.SourceAs[refs.KeyValueRaw](ctx, src) {
			count++
		}
		r.Equal(1, count, "expected exactly 1 message")

		a.Equal(newMsg.Author().String(), streamMsg.Author().String())
		a.EqualValues(newMsg.Seq(), streamMsg.Seq())
	}

	opts := message.CreateLogArgs{}
	opts.Keys = true
	opts.Limit = 10

	src, err := c.CreateLogStream(opts)
	r.NoError(err)

	ctx := context.TODO()
	i := 0
	for msg := range muxrpc.SourceAs[refs.KeyValueRaw](ctx, src) {
		a.Equal(wantRefs[i], msg.Key().String())
		i++
	}
	r.Equal(10, i, "expected 10 messages")

	a.NoError(c.Close())

	srv.Shutdown()
	r.NoError(srv.Close())
	r.NoError(<-srvErrc)
}
