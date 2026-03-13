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
	"time"

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

// Close cancels all active EBT sessions and their feed subscriptions.
func (h *MUXRPCHandler) Close() error {
	h.Sessions.CloseAll()
	return nil
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
		if err := h.sendState(sess.ctx, sess.tx, sess.peer); err != nil {
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

	session := h.Sessions.Started(ctx, remoteAddr, peer, tx)

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

	// ebtItem is either a message (pendingMsg) or a frontier update.
	type pendingMsg struct {
		author refs.FeedRef
		raw    []byte
	}
	type ebtItem struct {
		msg      *pendingMsg
		frontier ssb.NetworkFrontier
	}

	// Receiver goroutine: reads from the muxrpc stream and classifies
	// each item as a message or frontier update. This decouples the
	// network read from the batch processing below.
	items := make(chan ebtItem, 128)
	go func() {
		defer close(items)
		for jsonBody := range rx.Iter(ctx) {
			raw := make([]byte, len(jsonBody))
			copy(raw, jsonBody)

			var frontierUpdate ssb.NetworkFrontier
			if json.Unmarshal(raw, &frontierUpdate) == nil {
				items <- ebtItem{frontier: frontierUpdate}
				continue
			}

			var msgWithAuthor struct {
				Author refs.FeedRef
			}
			if err := json.Unmarshal(raw, &msgWithAuthor); err != nil {
				h.check(err)
				continue
			}
			items <- ebtItem{msg: &pendingMsg{author: msgWithAuthor.Author, raw: raw}}
		}
	}()

	// Batch processing state.
	const ebtBatchSize = 128
	const ebtFlushInterval = 50 * time.Millisecond
	pending := make([]pendingMsg, 0, ebtBatchSize)

	flushTimer := time.NewTimer(ebtFlushInterval)
	flushTimer.Stop()
	defer flushTimer.Stop()

	// flushPending verifies, persists, and ACKs all accumulated messages.
	// After successful persist, ebtState.Fill is called directly — this is
	// the ACK. The peer will see the updated frontier on the next exchange.
	flushPending := func() {
		if len(pending) == 0 {
			return
		}

		// Group by author for per-feed verification.
		byAuthor := make(map[string][]int)
		authorOrder := make([]string, 0)
		for i, pm := range pending {
			key := pm.author.String()
			if _, exists := byAuthor[key]; !exists {
				authorOrder = append(authorOrder, key)
			}
			byAuthor[key] = append(byAuthor[key], i)
		}

		ebtUpdates := make([]statematrix.ObservedFeed, 0, len(pending))

		for _, authorKey := range authorOrder {
			indices := byAuthor[authorKey]
			author := pending[indices[0]].author

			vsnk, sinkErr := h.verify.GetSink(author, true)
			if sinkErr != nil {
				h.check(sinkErr)
				continue
			}

			raws := make([][]byte, len(indices))
			for j, idx := range indices {
				raws[j] = pending[idx].raw
			}

			verified, verifyErr := vsnk.VerifyBatch(raws)
			if len(verified) > 0 {
				if _, saveErr := h.verify.SaveBatch(verified); saveErr != nil {
					h.check(saveErr)
					continue
				}

				// Collect EBT state updates for the ACK.
				lastMsg := verified[len(verified)-1]
				ebtUpdates = append(ebtUpdates, statematrix.ObservedFeed{
					Feed: author,
					Note: ssb.Note{
						Seq:       int64(lastMsg.Seq()),
						Receive:   true,
						Replicate: true,
					},
				})
			}
			if verifyErr != nil {
				h.check(verifyErr)
			}
		}

		// ACK: update our frontier directly after persist.
		// CombinedIndex will also call Fill (idempotent), but we
		// don't wait for the async index — the persist IS the commit point.
		if len(ebtUpdates) > 0 {
			if err := h.stateMatrix.Fill(h.self, ebtUpdates); err != nil {
				h.check(err)
			}
		}

		pending = pending[:0]
	}

	// Processing loop: select on incoming items and the flush timer.
	// Messages accumulate until the batch is full or the timer fires.
	for {
		select {
		case item, ok := <-items:
			if !ok {
				// Stream closed — flush remaining messages and exit.
				flushPending()
				h.check(rx.Err())
				return
			}

			if item.msg != nil {
				wanted, werr := h.stateMatrix.WantsFeed(h.self, item.msg.author)
				if werr != nil {
					h.check(werr)
					continue
				}
				if !wanted {
					level.Debug(peerLogger).Log("event", "skipping unwanted feed", "author", item.msg.author.ShortSigil())
					continue
				}

				pending = append(pending, *item.msg)
				if len(pending) >= ebtBatchSize {
					flushPending()
					flushTimer.Stop()
				} else if len(pending) == 1 {
					// First message in a new batch — start the flush timer.
					flushTimer.Reset(ebtFlushInterval)
				}
				continue
			}

			// Frontier update — flush pending messages first so any
			// state changes happen after messages are persisted.
			flushPending()
			flushTimer.Stop()

			_, err = h.stateMatrix.Update(peer, item.frontier)
			if err != nil {
				h.check(err)
				return
			}

			// only process the feeds that actually changed in this update,
			// not the full merged frontier (which would tear down and recreate
			// all existing subscriptions)
			for feedStr, their := range item.frontier {
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

		case <-flushTimer.C:
			flushPending()
		}
	}
}
