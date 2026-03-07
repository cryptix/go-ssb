// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package gossip

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"sync"

	"github.com/go-kit/kit/metrics"
	"github.com/ssbc/go-muxrpc/v3"
	margaret "github.com/ssbc/margaret/v2"
	"github.com/ssbc/margaret/v2/multilog/roaring"
	"go.mindeco.de/log"
	"go.mindeco.de/log/level"
	"go.mindeco.de/logging"

	"github.com/ssbc/go-ssb"
	refs "github.com/ssbc/go-ssb-refs"
	"github.com/ssbc/go-ssb/internal/luigiutils"
	"github.com/ssbc/go-ssb/internal/mutil"
	"github.com/ssbc/go-ssb/internal/storedrefs"
	"github.com/ssbc/go-ssb/message"
	"github.com/ssbc/go-ssb/message/multimsg"
)

// FeedManager handles serving gossip about User Feeds.
type FeedManager struct {
	rootCtx context.Context

	ReceiveLog margaret.Log[*multimsg.MultiMessage]
	UserFeeds  *roaring.MultiLog
	logger     logging.Interface

	liveFeeds    map[string]*luigiutils.MultiSink
	liveFeedsMut sync.Mutex

	// metrics
	sysGauge metrics.Gauge
	sysCtr   metrics.Counter
}

// NewFeedManager returns a new FeedManager used for gossiping about User
// Feeds.
func NewFeedManager(
	ctx context.Context,
	rxlog margaret.Log[*multimsg.MultiMessage],
	userFeeds *roaring.MultiLog,
	info logging.Interface,
	sysGauge metrics.Gauge,
	sysCtr metrics.Counter,
) *FeedManager {
	fm := &FeedManager{
		ReceiveLog: rxlog,
		UserFeeds:  userFeeds,
		logger:     info,
		rootCtx:    ctx,
		sysCtr:     sysCtr,
		sysGauge:   sysGauge,
		liveFeeds:  make(map[string]*luigiutils.MultiSink),
	}
	// QUESTION: How should the error case be handled?
	go fm.serveLiveFeeds()
	return fm
}

// Close shuts down all live feed sinks. After Close, no new messages
// will be forwarded to registered sinks.
func (m *FeedManager) Close() {
	m.liveFeedsMut.Lock()
	defer m.liveFeedsMut.Unlock()

	for id, sink := range m.liveFeeds {
		sink.Close()
		delete(m.liveFeeds, id)
	}
}

func (m *FeedManager) serveLiveFeeds() {
	qry := m.ReceiveLog.Query(
		margaret.Gt(m.ReceiveLog.Seq()),
		margaret.Live(m.rootCtx),
	)
	for _, mm := range qry.Iter() {
		if mm.Message == nil {
			continue
		}
		msg := mm.Message
		author := msg.Author()

		m.liveFeedsMut.Lock()
		sink, ok := m.liveFeeds[author.String()]
		if ok {
			sink.Send(msg.ValueContentJSON())
		}
		m.liveFeedsMut.Unlock()
	}
	if err := qry.Err(); err != nil && err != ssb.ErrShuttingDown && err != context.Canceled && !strings.HasSuffix(err.Error(), "file already closed") {
		err = fmt.Errorf("error while serving live feed: %w", err)
		panic(err)
	}
	level.Warn(m.logger).Log("event", "live qry on rxlog exited")
}

func (m *FeedManager) addLiveFeed(
	ctx context.Context,
	sink *muxrpc.ByteSink,
	ssbID string,
	seq, limit int64,
) error {
	// TODO: ensure all messages make it to the live query
	//  Messages could be lost when written after the non-live portion and
	//  registering to live feed.
	m.liveFeedsMut.Lock()
	defer m.liveFeedsMut.Unlock()

	liveFeed, ok := m.liveFeeds[ssbID]
	if !ok {
		m.liveFeeds[ssbID] = luigiutils.NewMultiSink(seq)
		liveFeed = m.liveFeeds[ssbID]
	}

	if m.sysGauge != nil {
		m.sysGauge.With("part", "gossip-livefeeds").Set(float64(len(m.liveFeeds)))
	}

	until := seq + limit
	if limit == -1 {
		until = math.MaxInt64
	}

	liveFeed.Register(ctx, sink, until)

	m.liveFeeds[ssbID] = liveFeed
	// TODO: Remove multiSink from map when complete
	return nil
}

// nonliveLimit returns the upper limit for a CreateStreamHistory request given
// the current User Feeds latest sequence.
func nonliveLimit(
	arg message.CreateHistArgs,
	curSeq int64,
) int64 {
	if arg.Limit == -1 {
		return -1
	}
	lastSeq := arg.Seq + arg.Limit - 1
	if lastSeq > curSeq {
		lastSeq = curSeq
	}
	return lastSeq - arg.Seq + 1
}

// liveLimit returns the limit for serving the 'live' portion for a
// CreateStreamHistory request given the current User Feeds latest sequence.
func liveLimit(
	arg message.CreateHistArgs,
	curSeq int64,
) int64 {
	if arg.Limit == -1 {
		return -1
	}

	startSeq := curSeq + 1
	lastSeq := arg.Seq + arg.Limit - 1
	if lastSeq < curSeq {
		return 0
	}
	return lastSeq - startSeq + 1
}

// writeMessage encodes a message to the muxrpc sink.
// If keys is true, it wraps the message in a KeyValueRaw envelope.
func writeMessage(w *muxrpc.ByteSink, msg refs.Message, keys bool) error {
	if keys {
		var kv refs.KeyValueRaw
		kv.Key_ = msg.Key()
		kv.Value = *msg.ValueContent()
		kv.Timestamp = refs.Millisecs(msg.Received())
		kvMsg, err := json.Marshal(kv)
		if err != nil {
			return fmt.Errorf("failed to encode key-value: %w", err)
		}
		_, err = w.Write(kvMsg)
		return err
	}
	_, err := w.Write(msg.ValueContentJSON())
	return err
}

// CreateStreamHistory serves the sink a CreateStreamHistory request to the sink.
func (m *FeedManager) CreateStreamHistory(
	ctx context.Context,
	sink *muxrpc.ByteSink,
	arg message.CreateHistArgs,
) error {
	feedLogger := log.With(m.logger, "fr", arg.ID.ShortSigil())

	// check what we got
	userLog, err := m.UserFeeds.Get(storedrefs.Feed(arg.ID))
	if err != nil {
		return fmt.Errorf("failed to open sublog for user: %w", err)
	}

	latest := int64(userLog.Seq())

	if arg.Seq != 0 {
		arg.Seq--             // our idx is 0 ed
		if arg.Seq > latest { // more than we got
			if arg.Live {
				return m.addLiveFeed(
					ctx, sink,
					arg.ID.String(),
					latest,
					liveLimit(arg, latest),
				)
			}
			err = sink.Close()
			if err != nil {
				err = fmt.Errorf("pour: failed to close: %w", err)
			}
			return err
		}
	}
	if arg.Live && arg.Limit == 0 {
		arg.Limit = -1
	}

	// Make query
	limit := nonliveLimit(arg, latest)
	qryArgs := []margaret.QueryOption{
		margaret.Limit(int(limit)),
		margaret.Reverse(arg.Reverse),
	}

	if arg.Seq > 0 {
		qryArgs = append(qryArgs, margaret.Gte(arg.Seq))
	}

	if arg.Lt > 0 {
		qryArgs = append(qryArgs, margaret.Lt(int64(arg.Lt)))
	}

	if arg.Gt > 0 {
		qryArgs = append(qryArgs, margaret.Gt(int64(arg.Gt)))
	}

	resolved := mutil.Indirect(m.ReceiveLog, userLog)
	qry := resolved.Query(qryArgs...)

	sink.SetEncoding(muxrpc.TypeJSON)

	sent := 0
	for _, mm := range qry.Iter() {
		select {
		case <-ctx.Done():
			break
		default:
		}

		if mm.Message == nil {
			continue
		}
		msg := mm.Message

		var writeErr error
		switch arg.ID.Algo() {
		case refs.RefAlgoFeedSSB1:
			writeErr = writeMessage(sink, msg, arg.Keys)

		case refs.RefAlgoFeedGabby:
			if arg.AsJSON {
				writeErr = writeMessage(sink, msg, arg.Keys)
			} else {
				tr, ok := mm.AsGabby()
				if !ok {
					continue
				}
				trdata, err := tr.MarshalCBOR()
				if err != nil {
					continue
				}
				_, writeErr = sink.Write(trdata)
			}

		case refs.RefAlgoFeedBendyButt:
			if arg.AsJSON {
				writeErr = writeMessage(sink, msg, arg.Keys)
			} else {
				mf, ok := mm.AsMetaFeed()
				if !ok {
					continue
				}
				mfData, err := mf.MarshalBencode()
				if err != nil {
					continue
				}
				_, writeErr = sink.Write(mfData)
			}

		default:
			return fmt.Errorf("unsupported feed format")
		}

		if writeErr != nil {
			if errors.Is(writeErr, context.Canceled) || muxrpc.IsSinkClosed(writeErr) || errors.Is(writeErr, io.EOF) {
				break
			}
			return fmt.Errorf("failed to write message to peer: %w", writeErr)
		}
		sent++
	}

	if err := qry.Err(); err != nil {
		if errors.Is(err, context.Canceled) || muxrpc.IsSinkClosed(err) || errors.Is(err, io.EOF) {
			sink.Close()
			return nil
		}
		return fmt.Errorf("failed to pump messages to peer: %w", err)
	}

	// track number of messages sent
	if m.sysCtr != nil {
		m.sysCtr.With("event", "gossiptx").Add(float64(sent))
	} else {
		if sent > 0 {
			level.Debug(feedLogger).Log("event", "gossiptx", "n", sent, "starting", arg.Seq)
		}
	}

	// cryptix: this seems to produce some hangs
	// TODO: make tests with leaving and joining peers while messages are published
	if arg.Live {
		return m.addLiveFeed(
			ctx, sink,
			arg.ID.String(),
			latest,
			liveLimit(arg, latest),
		)
	}
	closeErr := sink.Close()
	if closeErr != nil {
		return fmt.Errorf("failed to close sink after %d messages: %w", sent, closeErr)
	}
	return nil
}
