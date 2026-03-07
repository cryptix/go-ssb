// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package ebt

import (
	"sync"

	"github.com/ssbc/go-muxrpc/v3"
	"github.com/ssbc/margaret/v2/multilog/roaring"
	"go.mindeco.de/logging"

	refs "github.com/ssbc/go-ssb-refs"
	"github.com/ssbc/go-ssb/internal/statematrix"
	"github.com/ssbc/go-ssb/message"
	"github.com/ssbc/go-ssb/message/multimsg"
	"github.com/ssbc/go-ssb/plugins/gossip"
	margaret "github.com/ssbc/margaret/v2"
)

type Plugin struct{ *MUXRPCHandler }

func NewPlug(
	i logging.Interface,
	self refs.FeedRef,
	rootLog margaret.Log[*multimsg.MultiMessage],
	uf *roaring.MultiLog,
	fm *gossip.FeedManager,
	sm *statematrix.StateMatrix,
	v *message.VerificationRouter,
) *Plugin {

	return &Plugin{&MUXRPCHandler{
		info:      i,
		self:      self,
		rootLog:   rootLog,
		userFeeds: uf,

		livefeeds: fm,

		stateMatrix: sm,

		verify: v,

		Sessions: Sessions{
			mu:   new(sync.Mutex),
			open: make(map[string]*session),

			waitingFor: make(map[string]chan<- struct{}),
		},
	},
	}
}

// muxrpc plugin

func (p Plugin) Name() string            { return "ebt" }
func (p Plugin) Method() muxrpc.Method   { return muxrpc.Method{"ebt"} }
func (p Plugin) Handler() muxrpc.Handler { return p.MUXRPCHandler }
