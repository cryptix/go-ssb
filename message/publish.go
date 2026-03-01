// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package message

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/ssbc/go-metafeed"

	margaret "github.com/ssbc/margaret/v2"
	"github.com/ssbc/margaret/v2/multilog/roaring"

	gabbygrove "github.com/ssbc/go-gabbygrove"
	"github.com/ssbc/go-ssb"
	refs "github.com/ssbc/go-ssb-refs"
	"github.com/ssbc/go-ssb/internal/mutil"
	"github.com/ssbc/go-ssb/internal/storedrefs"
	"github.com/ssbc/go-ssb/message/legacy"
	"github.com/ssbc/go-ssb/message/multimsg"
)

type publishLog struct {
	mu                     sync.Mutex
	byAuthor               margaret.Log[*roaring.Seq]
	receiveLog             *multimsg.WrappedLog
	waitForIndexesCallback func()

	create creater
}

func (pl *publishLog) Publish(content interface{}) (refs.Message, error) {
	if pl.waitForIndexesCallback != nil {
		pl.waitForIndexesCallback()
	}

	pl.mu.Lock()
	defer pl.mu.Unlock()

	// current state of the local sig-chain
	var (
		nextPrevious refs.MessageRef
		nextSequence = int64(-1)
	)

	seq := pl.byAuthor.Seq()

	if seq < 0 {
		// new feed
		nextSequence = 1
	} else {
		rootSeqVal, err := pl.byAuthor.Get(seq)
		if err != nil {
			return nil, fmt.Errorf("publishLog: failed to retrieve current msg: %w", err)
		}
		mm, err := pl.receiveLog.Get(int64(*rootSeqVal))
		if err != nil {
			return nil, fmt.Errorf("publishLog: failed to establish current seq: %w", err)
		}
		msg := mm.Message
		nextPrevious = msg.Key()
		nextSequence = msg.Seq() + 1
	}

	nextMsg, err := pl.create.Create(content, nextPrevious, nextSequence)
	if err != nil {
		return nil, fmt.Errorf("failed to create next msg: %w", err)
	}

	rlSeq, err := pl.receiveLog.AppendMessage(nextMsg)
	if err != nil {
		return nil, fmt.Errorf("failed to append new msg: %w", err)
	}

	mm, err := pl.receiveLog.Get(rlSeq)
	if err != nil {
		return nil, fmt.Errorf("publish: failed to get new stored message: %w", err)
	}

	return mm.Message, nil
}

func (pl *publishLog) Seq() int64 {
	if pl.waitForIndexesCallback != nil {
		pl.waitForIndexesCallback()
	}

	return pl.byAuthor.Seq()
}

// Get retrieves the message object by traversing the authors sublog to the root log
func (pl *publishLog) Get(s int64) (*multimsg.MultiMessage, error) {
	rootSeqVal, err := pl.byAuthor.Get(s)
	if err != nil {
		return nil, fmt.Errorf("publish get: failed to retrieve sequence for the root log: %w", err)
	}

	mm, err := pl.receiveLog.Get(int64(*rootSeqVal))
	if err != nil {
		return nil, fmt.Errorf("publish get: failed to retrieve message from rootlog: %w", err)
	}
	return mm, nil
}

func (pl *publishLog) Query(opts ...margaret.QueryOption) margaret.QueryIterator[*multimsg.MultiMessage] {
	return mutil.Indirect(pl.receiveLog, pl.byAuthor).Query(opts...)
}

// Append adds a raw MultiMessage to the receive log.
// For normal publishing, use Publish instead.
func (pl *publishLog) Append(mm *multimsg.MultiMessage) (int64, error) {
	return pl.receiveLog.Append(mm)
}

func (pl *publishLog) Close() error { return nil }

// OpenPublishLog needs the base datastore (root or receive log - offset2)
// and the userfeeds with all the sublog and uses the passed keypair to find the corresponding user feed
// the returned log's append function is then used to create new messages.
// these messages are constructed in the legacy SSB way: The poured object is JSON v8-like pretty printed and then NaCL signed,
// then it's pretty printed again (now with the signature inside the message) to construct it's SHA256 hash,
// which is used to reference it (by replys and it's previous)
func OpenPublishLog(receiveLog *multimsg.WrappedLog, authorLogs *roaring.MultiLog, kp ssb.KeyPair, opts ...PublishOption) (ssb.Publisher, error) {
	authorLog, err := authorLogs.Get(storedrefs.Feed(kp.ID()))
	if err != nil {
		return nil, fmt.Errorf("publish: failed to open sublog for author: %w", err)
	}

	pl := &publishLog{
		byAuthor:   authorLog,
		receiveLog: receiveLog,
	}

	switch kp.ID().Algo() {
	case refs.RefAlgoFeedSSB1:
		pl.create = &legacyCreate{
			key: kp,
		}
	case refs.RefAlgoFeedGabby:
		pl.create = &gabbyCreate{
			enc: gabbygrove.NewEncoder(kp.Secret()),
		}
	case refs.RefAlgoFeedBendyButt:
		pl.create = &metafeedCreate{
			enc: metafeed.NewEncoder(kp.Secret()),
		}
	default:
		return nil, fmt.Errorf("publish: unsupported feed algorithm: %s", kp.ID().Algo())
	}

	for i, o := range opts {
		if err := o(pl); err != nil {
			return nil, fmt.Errorf("publish: option %d failed: %w", i, err)
		}
	}

	return pl, nil
}

type PublishOption func(*publishLog) error

func SetHMACKey(hmackey *[32]byte) PublishOption {
	return func(pl *publishLog) error {
		if hmackey == nil {
			return nil
		}
		var err error
		switch cv := pl.create.(type) {
		case *legacyCreate:
			cv.hmac = hmackey
		case *gabbyCreate:
			err = cv.enc.WithHMAC(hmackey[:])
		case *metafeedCreate:
			err = cv.enc.WithHMAC(hmackey[:])
		default:
			err = fmt.Errorf("hmac: unknown creater: %T", cv)
		}
		return err
	}
}

func UseNowTimestamps(yes bool) PublishOption {
	return func(pl *publishLog) error {
		switch cv := pl.create.(type) {
		case *legacyCreate:
			cv.setTimestamp = yes
		case *gabbyCreate:
			cv.enc.WithNowTimestamps(yes)
		case *metafeedCreate:
			cv.enc.WithNowTimestamps(yes)
		default:
			return fmt.Errorf("setTimestamp: unknown creater: %T", cv)
		}
		return nil
	}
}

func UseWaitForIndexesCallback(cb func()) PublishOption {
	return func(pl *publishLog) error {
		pl.waitForIndexesCallback = cb
		return nil
	}
}

type creater interface {
	Create(val interface{}, prev refs.MessageRef, seq int64) (refs.Message, error)
}

type legacyCreate struct {
	key          ssb.KeyPair
	hmac         *[32]byte
	setTimestamp bool
}

func (lc legacyCreate) Create(val interface{}, prev refs.MessageRef, seq int64) (refs.Message, error) {
	// prepare persisted message
	var stored legacy.StoredMessage
	stored.Timestamp_ = time.Now() // "rx"
	stored.Author_ = storedrefs.SerialzedFeed{FeedRef: lc.key.ID()}

	// set metadata
	var newMsg legacy.LegacyMessage
	newMsg.Hash = "sha256"
	newMsg.Author = lc.key.ID().String()
	if seq > 1 {
		newMsg.Previous = &prev
	}
	newMsg.Sequence = int64(seq)

	if bindata, ok := val.([]byte); ok {
		bindata = bytes.TrimPrefix(bindata, []byte("box1:"))
		newMsg.Content = base64.StdEncoding.EncodeToString(bindata) + ".box"
	} else {
		newMsg.Content = val
	}

	if lc.setTimestamp {
		newMsg.Timestamp = time.Now().UnixNano() / 1000000
	}

	mr, signedMessage, err := newMsg.Sign(lc.key.Secret(), lc.hmac)
	if err != nil {
		return nil, err
	}

	// make sure the message we created verifies correctly (type check)
	_, _, err = legacy.Verify(signedMessage, lc.hmac)
	if err != nil {
		return nil, fmt.Errorf("failed to verify newly created message: %w", err)
	}

	stored.Key_ = storedrefs.SerialzedMessage{MessageRef: mr}
	stored.Sequence_ = seq
	if prev := newMsg.Previous; prev != nil {
		stored.Previous_ = &storedrefs.SerialzedMessage{MessageRef: *prev}
	}
	stored.Raw_ = signedMessage
	return &stored, nil
}

type gabbyCreate struct {
	enc *gabbygrove.Encoder
}

func (pc gabbyCreate) Create(val interface{}, prev refs.MessageRef, seq int64) (refs.Message, error) {
	br, err := gabbygrove.NewBinaryRef(prev)
	if err != nil {
		return nil, err
	}
	nextSeq := uint64(seq)
	tr, _, err := pc.enc.Encode(nextSeq, br, val)
	if err != nil {
		return nil, fmt.Errorf("gabby: failed to encode content: %w", err)
	}
	return tr, nil
}

type metafeedCreate struct {
	enc *metafeed.Encoder
}

func (pc metafeedCreate) Create(val interface{}, prev refs.MessageRef, seq int64) (refs.Message, error) {
	if seq > math.MaxInt32 {
		return nil, fmt.Errorf("metafeedCreate: sequence exceeded feed format capacity")
	}
	nextSeq := int32(seq)

	if seq == 1 {
		var err error
		prev, err = refs.NewMessageRefFromBytes(bytes.Repeat([]byte{0}, 32), refs.RefAlgoMessageBendyButt)
		if err != nil {
			return nil, err
		}
	}
	tr, _, err := pc.enc.Encode(nextSeq, prev, val)
	if err != nil {
		return nil, fmt.Errorf("metafeedCreate: failed to encode content: %w", err)
	}
	return tr, nil
}
