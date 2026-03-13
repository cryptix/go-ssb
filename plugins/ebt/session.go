// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package ebt

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/ssbc/go-muxrpc/v3"
	refs "github.com/ssbc/go-ssb-refs"
)

type session struct {
	ctx    context.Context // muxrpc session context, cancelled on disconnect
	remote net.Addr        // netwrap'ed shs address

	peer refs.FeedRef

	tx *muxrpc.ByteSink // the muxrpc writer to send updates

	// which feeds this session is currently subscribed to
	mu         sync.Mutex
	subscribed map[string]context.CancelFunc
}

func newSession(ctx context.Context, remote net.Addr, peer refs.FeedRef, tx *muxrpc.ByteSink) *session {
	return &session{
		ctx:    ctx,
		remote: remote,
		peer:   peer,
		tx:     tx,

		subscribed: make(map[string]context.CancelFunc),
	}
}

// Subscribed registers the cancel function for that stream in the session
func (s *session) Subscribed(feed refs.FeedRef, cancelFn context.CancelFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()

	fr := feed.String()
	if fn, has := s.subscribed[fr]; has {
		fn()
		delete(s.subscribed, fr)
	}

	s.subscribed[fr] = cancelFn
}

// Unsubscribe checks to see if there is one and cancels it
func (s *session) Unsubscribe(feed refs.FeedRef) {
	s.mu.Lock()
	defer s.mu.Unlock()

	fr := feed.String()
	if fn, has := s.subscribed[fr]; has {
		fn()
		delete(s.subscribed, fr)
	}
}

type Sessions struct {
	mu   *sync.Mutex
	open map[string]*session
	// to be able to correctly trigger fallback on the server we need to be able to wait for incoming sessions
	waitingFor map[string]chan<- struct{}
}

// Started registers a new session for the network address and returns it.
// It also closes open channels in waitingFor if they exist and thus makes WaitFor() calls return.
func (s *Sessions) Started(ctx context.Context, addr net.Addr, peer refs.FeedRef, tx *muxrpc.ByteSink) *session {
	s.mu.Lock()
	defer s.mu.Unlock()

	// we are using the full ip:port~pubkey notation as the map key
	mk := addr.String()

	session := newSession(ctx, addr, peer, tx)

	s.open[mk] = session

	if c, has := s.waitingFor[mk]; has {
		close(c)
		delete(s.waitingFor, mk)
	}

	return session
}

// Ended notifies the session store that a session has ended.
// It cancels all feed subscriptions for the session before removing it.
func (s *Sessions) Ended(addr net.Addr) {
	s.mu.Lock()
	defer s.mu.Unlock()

	mk := addr.String()

	if sess, has := s.open[mk]; has {
		sess.mu.Lock()
		for feed, cancel := range sess.subscribed {
			cancel()
			delete(sess.subscribed, feed)
		}
		sess.mu.Unlock()
	}

	delete(s.open, mk)
}

// ForEach calls fn for each active session while holding the lock.
func (s *Sessions) ForEach(fn func(sess *session)) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, sess := range s.open {
		fn(sess)
	}
}

// CloseAll cancels all feed subscriptions across all sessions and removes them.
// Called during shutdown to ensure all replication streams are torn down cleanly.
func (s *Sessions) CloseAll() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for mk, sess := range s.open {
		sess.mu.Lock()
		for feed, cancel := range sess.subscribed {
			cancel()
			delete(sess.subscribed, feed)
		}
		sess.mu.Unlock()
		delete(s.open, mk)
	}
}

// WaitFor returns true if addr manages to start a session before durration passes
func (s *Sessions) WaitFor(ctx context.Context, addr net.Addr, durr time.Duration) bool {

	// we are using the full ip:port~pubkey notation as the map key
	mk := addr.String()

	s.mu.Lock()

	// is there already an open session?
	if _, has := s.open[mk]; has {
		s.mu.Unlock()
		return true
	}

	if _, has := s.waitingFor[mk]; has {
		// hm... not sure this case is realistic but
		fmt.Printf("[warning] ebt waiting for session: already waiting for %s\n", mk)
		return true
	}

	c := make(chan struct{})
	s.waitingFor[mk] = c
	s.mu.Unlock()

	select {

	// we DID get a session
	case <-c:
		return true

	// we didn't get a session
	case <-ctx.Done():
		return false
	case <-time.After(durr):
		return false

	}
}
