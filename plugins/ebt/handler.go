// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package ebt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"

	"go.mindeco.de/log"

	"github.com/ssbc/go-muxrpc/v3"
	margaret "github.com/ssbc/margaret/v2"
	"github.com/ssbc/margaret/v2/multilog/roaring"
	"go.mindeco.de/log/level"
	"go.mindeco.de/logging"

	"github.com/ssbc/go-ssb"
	refs "github.com/ssbc/go-ssb-refs"
	"github.com/ssbc/go-ssb/internal/statematrix"
	"github.com/ssbc/go-ssb/internal/storedrefs"
	"github.com/ssbc/go-ssb/message"
	"github.com/ssbc/go-ssb/message/multimsg"
	"github.com/ssbc/go-ssb/plugins/gossip"
)

type MUXRPCHandler struct {
	info logging.Interface

	self      refs.FeedRef
	rootLog   margaret.Log[*multimsg.MultiMessage]
	userFeeds *roaring.MultiLog

	livefeeds *gossip.FeedManager

	// wantList ssb.ReplicationLister

	stateMatrix *statematrix.StateMatrix

	verify *message.VerificationRouter

	Sessions Sessions
}

func (h *MUXRPCHandler) check(err error) {
	if err != nil && !muxrpc.IsSinkClosed(err) {
		level.Error(h.info).Log("error", err)
	}
}

func (MUXRPCHandler) Handled(m muxrpc.Method) bool { return m.String() == "ebt.replicate" }

// HandleConnect does nothing. Feature negotiation is done by sbot
func (h *MUXRPCHandler) HandleConnect(ctx context.Context, e muxrpc.Endpoint) {}

// HandleCall handles the server side (getting called by client)
func (h *MUXRPCHandler) HandleCall(ctx context.Context, req *muxrpc.Request) {
	checkAndClose := func(err error) {
		h.check(err)
		if err != nil {
			closeErr := req.CloseWithError(err)
			h.check(fmt.Errorf("error closeing request %q: %w", req.Method, closeErr))
		}
	}

	if req.Type != "duplex" {
		checkAndClose(fmt.Errorf("invalid type: %s", req.Type))
		return
	}

	var args []struct{ Version int }
	err := json.Unmarshal(req.RawArgs, &args)
	if err != nil {
		checkAndClose(err)
		return
	}

	if n := len(args); n != 1 {
		checkAndClose(fmt.Errorf("expected one argument but got %d", n))
		return
	}

	if args[0].Version != 3 {
		checkAndClose(errors.New("go-ssb only support ebt v3"))
		return
	}
	level.Debug(h.info).Log("event", "replicating", "version", args[0].Version)

	// get writer and reader from duplex call
	snk, err := req.ResponseSink()
	if err != nil {
		checkAndClose(err)
		return
	}

	src, err := req.ResponseSource()
	if err != nil {
		checkAndClose(err)
		return
	}

	h.Loop(ctx, snk, src, req.RemoteAddr())
}

func (h *MUXRPCHandler) sendState(ctx context.Context, tx *muxrpc.ByteSink, remote refs.FeedRef) error {
	// send our full frontier - the peer will decide what to act on.
	// over-sending is safe; under-sending (filtering based on stale peer data) can cause missed feeds.
	selfNf, err := h.stateMatrix.Inspect(h.self)
	if err != nil {
		return fmt.Errorf("failed to get own frontier: %w", err)
	}

	// make a copy so we don't modify the stored frontier
	currState := make(ssb.NetworkFrontier, len(selfNf))
	for k, v := range selfNf {
		currState[k] = v
	}

	selfRef := h.self.String()

	// don't receive your own feed
	if myNote, has := currState[selfRef]; has {
		myNote.Receive = false
		currState[selfRef] = myNote
	}

	tx.SetEncoding(muxrpc.TypeJSON)
	err = json.NewEncoder(tx).Encode(currState)
	if err != nil {
		return fmt.Errorf("failed to send currState: %d: %w", len(currState), err)
	}

	return nil
}

// PushState sends the current frontier state to all active EBT sessions.
// Called when the local want-clock changes (e.g. via Replicate()).
func (h *MUXRPCHandler) PushState() {
	h.Sessions.ForEach(func(sess *session) {
		if err := h.sendState(context.TODO(), sess.tx, sess.peer); err != nil {
			h.check(err)
		}
	})
}

// Loop executes the ebt logic loop, reading from the peer and sending state and messages as requests
func (h *MUXRPCHandler) Loop(ctx context.Context, tx *muxrpc.ByteSink, rx *muxrpc.ByteSource, remoteAddr net.Addr) {
	peer, err := ssb.GetFeedRefFromAddr(remoteAddr)
	if err != nil {
		h.check(err)
		return
	}

	session := h.Sessions.Started(remoteAddr, peer, tx)

	peerLogger := log.With(h.info, "r", peer.ShortSigil())

	defer func() {
		h.Sessions.Ended(remoteAddr)

		level.Debug(peerLogger).Log("event", "loop exited", "rx-err", rx.Err(), "ctx-err", ctx.Err())
		err := h.stateMatrix.SaveAndClose(peer)
		if err != nil {
			level.Warn(h.info).Log("event", "failed to save state matrix for peer", "err", err)
		}
	}()

	if err := h.sendState(ctx, tx, peer); err != nil {
		h.check(err)
		return
	}

	for jsonBody := range rx.Iter(ctx) {

		var frontierUpdate ssb.NetworkFrontier
		err = json.Unmarshal(jsonBody, &frontierUpdate)
		if err != nil { // assume it's a message

			// redundant pass of finding out the author
			// would be rad to get this from the pretty-printed version
			// and just pass that to verify
			var msgWithAuthor struct {
				Author refs.FeedRef
			}

			err := json.Unmarshal(jsonBody, &msgWithAuthor)
			if err != nil {
				h.check(err)
				continue
			}

			// only accept messages for feeds we actually want
			wanted, werr := h.stateMatrix.WantsFeed(h.self, msgWithAuthor.Author)
			if werr != nil {
				h.check(werr)
				continue
			}
			if !wanted {
				level.Debug(peerLogger).Log("event", "skipping unwanted feed", "author", msgWithAuthor.Author.ShortSigil())
				continue
			}

			vsnk, err := h.verify.GetSink(msgWithAuthor.Author, true)
			if err != nil {
				h.check(err)
				continue
			}

			err = vsnk.Verify(jsonBody)
			if err != nil {
				// TODO: mark feed as bad
				h.check(err)
			}

			continue
		}

		// update our network perception with the full merge
		_, err = h.stateMatrix.Update(peer, frontierUpdate)
		if err != nil {
			h.check(err)
			return
		}

		// TODO: partition wants across the open connections
		// one peer might be closer to a feed
		// for this we also need timing and other heuristics

		// only process the feeds that actually changed in this update,
		// not the full merged frontier (which would tear down and recreate
		// all existing subscriptions)
		for feedStr, their := range frontierUpdate {
			// these were already validated by the .UnmarshalJSON() method
			// but we need the refs.Feed for the createHistArgs
			feed, err := refs.ParseFeedRef(feedStr)
			if err != nil {
				h.check(err)
				return
			}

			if !their.Replicate {
				continue
			}

			if !their.Receive {
				session.Unsubscribe(feed)
				continue
			}

			// check our local sequence for this feed
			// only create a history stream if we have messages the peer doesn't
			userLog, err := h.userFeeds.Get(storedrefs.Feed(feed))
			if err != nil {
				level.Debug(peerLogger).Log("event", "no local data for feed", "feed", feed.ShortSigil())
				continue
			}
			ourSeq := userLog.Seq() + 1 // margaret 0-indexed to SSB 1-indexed
			level.Debug(peerLogger).Log("event", "note-rx", "feed", feed.ShortSigil(), "their-seq", their.Seq, "our-seq", ourSeq, "receive", their.Receive)
			if ourSeq < their.Seq {
				// peer has more than us - they are the source, not us.
				// skip sending; when we later receive messages, PushState
				// will trigger a new frontier exchange
				continue
			}

			arg := message.CreateHistArgs{
				ID:  feed,
				Seq: int64(their.Seq + 1),
			}
			arg.Limit = -1
			arg.Live = true

			// IMPORTANT: use a new variable name (feedCtx) to avoid shadowing
			// the outer ctx. If we shadow ctx, each iteration chains contexts:
			//   outerCtx → ctx_a → ctx_b → ctx_c
			// Then canceling cancel_a (via Unsubscribe or re-Subscribe) would
			// cascade and cancel ctx_b and ctx_c, killing the entire EBT loop.
			feedCtx, cancel := context.WithCancel(ctx)

			err = h.livefeeds.CreateStreamHistory(feedCtx, tx, arg)
			if err != nil {
				cancel()
				level.Debug(peerLogger).Log("event", "CreateStreamHistory failed", "feed", feed.ShortSigil(), "err", err)
				h.check(err)
				return
			}
			level.Debug(peerLogger).Log("event", "live-stream-created", "feed", feed.ShortSigil(), "from-seq", arg.Seq)
			session.Subscribed(feed, cancel)
		}
	}

	h.check(rx.Err())
}
