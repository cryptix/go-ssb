// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package sbot

import (
	"fmt"
	"net"
	"time"

	"github.com/ssbc/go-muxrpc/v3"
	"go.mindeco.de/log/level"

	"github.com/ssbc/go-ssb"
	refs "github.com/ssbc/go-ssb-refs"
)

// makeHandler creates an muxrpc handler for an incoming connection by checking
// authorization against the local identity, invite service, graph, and feed
// format variants. This is the callback passed to network.Options.MakeHandler.
func (s *Sbot) makeHandler(conn net.Conn) (muxrpc.Handler, error) {
	s.closedMu.Lock()
	closed := s.closed
	s.closedMu.Unlock()
	if closed {
		return nil, fmt.Errorf("sbot: shutting down, rejecting connection")
	}

	remote, err := ssb.GetFeedRefFromAddr(conn.RemoteAddr())
	if err != nil {
		return nil, fmt.Errorf("sbot: expected an address containing an shs-bs addr: %w", err)
	}

	// TODO: we still can't see the feed format type from this

	if s.KeyPair.ID().PubKey().Equal(remote.PubKey()) {
		return s.master.MakeHandler(conn)
	}

	if s.inviteService != nil {
		err := s.inviteService.Authorize(remote)
		if err == nil {
			return s.inviteService.GuestHandler(), nil
		}
	}

	if s.promisc {
		return s.public.MakeHandler(conn)
	}

	auth := s.authorizer
	if auth == nil {
		auth = s.Replicator.Lister()
	}

	if s.latency != nil {
		start := time.Now()
		defer func() {
			s.latency.With("part", "graph_auth").Observe(time.Since(start).Seconds())
		}()
	}
	err = auth.Authorize(remote)
	if err == nil {
		return s.public.MakeHandler(conn)
	}

	// we also need to pass the other feed type up the stack...!
	// TODO: wrap conn with a new remoteAddr
	ggRemote, err := refs.NewFeedRefFromBytes(remote.PubKey(), refs.RefAlgoFeedGabby)
	if err == nil {
		err = auth.Authorize(ggRemote)
		if err == nil {
			level.Debug(s.info).Log("TODO", "found gg feed, using that. overhaul shs1 to support more payload in the handshake")
			return s.public.MakeHandler(conn)
		}
	}

	// we also need to pass the other feed type up the stack...!
	// TODO: wrap conn with a new remoteAddr
	bbRemote, err := refs.NewFeedRefFromBytes(remote.PubKey(), refs.RefAlgoFeedBendyButt)
	if err == nil {
		err = auth.Authorize(bbRemote)
		if err == nil {
			level.Debug(s.info).Log("TODO", "found bendy-butt feed, using that. overhaul shs1 to support more payload in the handshake")
			return s.public.MakeHandler(conn)
		}
	}

	// TOFU restore/resync
	if lst, err := s.Users.List(); err == nil && len(lst) == 0 {
		level.Warn(s.info).Log("event", "no stored feeds - attempting re-sync with trust-on-first-use")
		if err := s.Replicate(s.KeyPair.ID()); err != nil {
			return nil, fmt.Errorf("tofu replicate failed: %w", err)
		}
		return s.public.MakeHandler(conn)
	}
	return nil, err
}
