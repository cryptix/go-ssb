// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package graph

import (
	"testing"

	refs "github.com/ssbc/go-ssb-refs"
	"github.com/stretchr/testify/require"

	"github.com/ssbc/go-ssb"
	"github.com/ssbc/go-ssb/message"
	"github.com/ssbc/go-ssb/message/multimsg"
	"github.com/ssbc/go-ssb/multilogs"
	"github.com/ssbc/margaret/v2/multilog/roaring"
)

type publisher struct {
	r *require.Assertions

	key      ssb.KeyPair
	publish  ssb.Publisher
	root     *multimsg.WrappedLog
	userLogs *roaring.MultiLog
}

func newPublisher(t *testing.T, root *multimsg.WrappedLog, users *roaring.MultiLog) *publisher {
	r := require.New(t)
	kp, err := ssb.NewKeyPair(nil, refs.RefAlgoFeedSSB1)
	r.NoError(err)
	return newPublisherWithKP(t, root, users, kp)
}

func newPublisherWithKP(t *testing.T, root *multimsg.WrappedLog, users *roaring.MultiLog, kp ssb.KeyPair) *publisher {
	p := &publisher{}
	p.r = require.New(t)
	p.key = kp
	p.root = root
	p.userLogs = users

	var err error
	p.publish, err = message.OpenPublishLog(root, users, p.key)
	p.r.NoError(err)
	return p
}

// publishAndIndex publishes content and updates user feeds index
func (p publisher) publishAndIndex(content interface{}) {
	msg, err := p.publish.Publish(content)
	p.r.NoError(err)
	p.r.NotNil(msg)

	// Update user feeds
	seq := p.root.Seq()
	mm, err := p.root.Get(seq)
	p.r.NoError(err)
	err = multilogs.UserFeedsUpdate(seq, mm, p.userLogs)
	p.r.NoError(err)
}

func (p publisher) follow(ref refs.FeedRef) {
	p.publishAndIndex(map[string]interface{}{
		"type":      "contact",
		"contact":   ref.String(),
		"following": true,
	})
}

func (p publisher) unfollow(ref refs.FeedRef) {
	p.publishAndIndex(map[string]interface{}{
		"type":      "contact",
		"contact":   ref.String(),
		"following": false,
	})
}

func (p publisher) unblock(ref refs.FeedRef) {
	p.publishAndIndex(map[string]interface{}{
		"type":     "contact",
		"contact":  ref.String(),
		"blocking": false,
	})
}

func (p publisher) block(ref refs.FeedRef) {
	p.publishAndIndex(map[string]interface{}{
		"type":     "contact",
		"contact":  ref.String(),
		"blocking": true,
	})
}
