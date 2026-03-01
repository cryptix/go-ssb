// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

// Package gossip implements the createHistoryStream muxrpc call. Legacy (non-EBT) Replication of fetching and verifying the selected feeds is found here.
package gossip

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/kit/metrics"
	"github.com/ssbc/go-muxrpc/v2"
	margaret "github.com/ssbc/margaret/v2"
	"github.com/ssbc/margaret/v2/multilog/roaring"
	"go.mindeco.de/log"
	"go.mindeco.de/log/level"
	"go.mindeco.de/logging"

	"github.com/ssbc/go-ssb"
	refs "github.com/ssbc/go-ssb-refs"
	"github.com/ssbc/go-ssb/internal/storedrefs"
	"github.com/ssbc/go-ssb/message"
	"github.com/ssbc/go-ssb/message/multimsg"
	"github.com/ssbc/go-ssb/repo"
)

// LegacyGossip implements incoming and outgoing createHistoryStream calls.
// Either register this plugin's HandleConnect for fetching feeds
// or the sbot.Negotiate plugin to get EBT opertunistlicly and fallback to this
// don't register both!
//
// TODO: add feature flag for live streaming
type LegacyGossip struct {
	repo repo.Interface

	Id         refs.FeedRef
	ReceiveLog margaret.Log[*multimsg.MultiMessage]
	UserFeeds  *roaring.MultiLog
	WantList   ssb.ReplicationLister
	Info       logging.Interface

	hmacSec HMACSecret

	promisc bool // ask for remote feed even if it's not on owns fetch list

	enableLiveStreaming bool

	activeLock  *sync.Mutex
	activeFetch map[string]struct{}

	sysGauge metrics.Gauge
	sysCtr   metrics.Counter

	feedManager *FeedManager

	verifyRouter *message.VerificationRouter

	rootCtx context.Context

	numberOfConcurrentReplicationsPerPeer int
	tokenPool                             *TokenPool
}

func (LegacyGossip) Handled(m muxrpc.Method) bool { return m.String() == "createHistoryStream" }

// HandleConnect on this handler triggers legacy createHistoryStream replication.
func (g *LegacyGossip) HandleConnect(ctx context.Context, e muxrpc.Endpoint) {
	g.StartLegacyFetching(ctx, e)
}

func (g *LegacyGossip) StartLegacyFetching(ctx context.Context, e muxrpc.Endpoint) {
	remote := e.Remote()
	remoteRef, err := ssb.GetFeedRefFromAddr(remote)
	if err != nil {
		return
	}

	// dont want to fetch messages from yourself
	if remoteRef.Equal(g.Id) {
		return
	}

	info := log.With(g.Info, "remote", remoteRef.ShortSigil(), "event", "gossiprx", "live", g.enableLiveStreaming)

	if g.promisc {
		hasCallee, err := g.UserFeeds.Has(storedrefs.Feed(remoteRef))
		if err != nil {
			info.Log("handleConnect", "UserFeeds.Has(callee)", "err", err)
			return
		}

		if !hasCallee {
			info.Log("handleConnect", "oops - dont have feed of remote peer. requesting...")
			if err := g.fetchFeed(ctx, remoteRef, e, time.Now(), g.enableLiveStreaming); err != nil {
				info.Log("handleConnect", "fetchFeed callee failed", "err", err)
				return
			}
			info.Log("msg", "done fetching callee")
		}
	}

	//level.Debug(info).Log("msg", "hops count", "count", feeds.Count())
	err = g.FetchAll(ctx, e, g.enableLiveStreaming)
	if err != nil && !muxrpc.IsSinkClosed(err) {
		level.Warn(info).Log("msg", "hops failed", "err", err)
		return
	}

	if !g.enableLiveStreaming {
		// start polling
		tick := time.NewTicker(5 * time.Minute)

		for {
			select {
			case <-ctx.Done():
				return

			case <-tick.C:
				err = g.FetchAll(ctx, e, g.enableLiveStreaming)
				if err != nil && !muxrpc.IsSinkClosed(err) {
					level.Warn(info).Log("msg", "hops failed", "err", err)
					return
				}
			}
		}
	}
}

func (g *LegacyGossip) HandleCall(
	ctx context.Context,
	req *muxrpc.Request,
) {
	if req.Type == "" {
		req.Type = "async"
	}

	hlog := log.With(g.Info, "event", "gossiptx")
	errLog := level.Error(hlog)

	closeIfErr := func(err error) {
		if err != nil {
			errLog.Log("err", err)
			req.CloseWithError(err)
			return
		}
		req.Stream.Close()
	}

	snk, err := req.ResponseSink()
	if err != nil {
		errLog.Log("err", err)
		req.CloseWithError(err)
		return
	}

	switch req.Method.String() {

	//  https://ssbc.github.io/scuttlebutt-protocol-guide/#createHistoryStream
	case "createHistoryStream":

		var args []json.RawMessage
		err := json.Unmarshal(req.RawArgs, &args)
		if err != nil {
			closeIfErr(fmt.Errorf("bad argumentss: %w", err))
			return
		}
		if len(args) < 1 {
			err := errors.New("ssb/message: not enough arguments, expecting feed id")
			closeIfErr(err)
			return
		}

		var query = message.NewCreateHistoryStreamArgs()
		err = json.Unmarshal(args[0], &query)
		if err != nil {
			closeIfErr(fmt.Errorf("bad request: %w", err))
			return
		}

		remote, err := ssb.GetFeedRefFromAddr(req.RemoteAddr())
		if err != nil {
			closeIfErr(fmt.Errorf("bad remote: %w", err))
			return
		}

		hlog = log.With(hlog, "fr", query.ID.ShortSigil(), "remote", remote.ShortSigil())
		// dbgLog = level.Warn(hlog)

		// skip this check for self/master or in promisc mode (talk to everyone)
		if !(g.Id.Equal(remote) || g.promisc) {
			blocks := g.WantList.BlockList()

			if blocks.Has(query.ID) {
				// dbgLog.Log("msg", "feed blocked")
				req.Stream.Close()
				return
			}
		}

		err = g.feedManager.CreateStreamHistory(ctx, snk, query)
		if err != nil {
			err = fmt.Errorf("createHistoryStream failed: %w", err)
			errLog.Log("err", err)
			req.CloseWithError(err)
			return
		}
		// don't close stream (feedManager will pass it on to live processing or close it itself)

	// TODO: move gossip.ping to it's own handler
	case "gossip.ping":
		snk.SetEncoding(muxrpc.TypeJSON)
		ts := []byte(strconv.FormatInt(time.Now().UnixNano()/1000000, 10))
		_, err = snk.Write(ts)
		if err != nil {
			closeIfErr(fmt.Errorf("pour failed to pong: %w", err))
			return
		}
		// just leave this stream open.
		// some versions of ssb-gossip don't like if the stream is closed without an error

	default:
		closeIfErr(fmt.Errorf("unknown command: %q", req.Method.String()))
	}
}
