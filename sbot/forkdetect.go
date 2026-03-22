// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package sbot

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"

	refs "github.com/ssbc/go-ssb-refs"
	"github.com/ssbc/go-ssb"
	"github.com/ssbc/go-ssb/internal/tracing"
	"github.com/ssbc/go-ssb/message"
	"go.mindeco.de/log/level"
)

// forkProofContent is the message content published when a fork is detected.
type forkProofContent struct {
	Type   string `json:"type"`
	Reason string `json:"reason"`

	// The forked feed
	Author string `json:"author"`

	// Evidence
	LeftKey  string `json:"left_key"`
	RightKey string `json:"right_key"`
	Seq      int64  `json:"seq"`

	// Detection context
	DetectedAt string `json:"detected_at"` // RFC3339 timestamp
}

// forkPublisher handles publishing fork proof messages and deduplication.
type forkPublisher struct {
	publisher ssb.Publisher
	rootCtx   context.Context

	mu   sync.Mutex
	seen map[string]struct{} // dedup: "author:seq" → already published
}

func newForkPublisher(ctx context.Context, pub ssb.Publisher) *forkPublisher {
	return &forkPublisher{
		publisher: pub,
		rootCtx:   ctx,
		seen:      make(map[string]struct{}),
	}
}

// HandleFork is a message.ForkHandler that publishes a fork-proof message.
func (fp *forkPublisher) HandleFork(existing, incoming refs.Message) {
	ctx, span := tracing.Tracer.Start(fp.rootCtx, "ssb.fork.detected",
		trace.WithAttributes(
			tracing.AttrForkAuthor.String(existing.Author().ShortSigil()),
			tracing.AttrForkSeq.Int64(existing.Seq()),
			tracing.AttrForkLeftKey.String(existing.Key().ShortSigil()),
			tracing.AttrForkRightKey.String(incoming.Key().ShortSigil()),
		),
	)
	defer span.End()

	_ = ctx // context used by span

	fp.mu.Lock()
	defer fp.mu.Unlock()

	// Deduplicate: don't publish multiple proofs for the same fork point
	dedup := fmt.Sprintf("%s:%d", existing.Author().String(), existing.Seq())
	if _, already := fp.seen[dedup]; already {
		return
	}
	fp.seen[dedup] = struct{}{}

	content := forkProofContent{
		Type:   "fork-proof",
		Reason: "feed fork detected during replication",

		Author: existing.Author().String(),

		LeftKey:  existing.Key().String(),
		RightKey: incoming.Key().String(),
		Seq:      existing.Seq(),

		DetectedAt: time.Now().UTC().Format(time.RFC3339),
	}

	_, err := fp.publisher.Publish(content)
	if err != nil {
		span.RecordError(err)
		return
	}
}

// setupForkDetection wires fork detection into the verification router.
// When a fork is detected during message verification, a fork-proof message
// is published to the node's own feed.
func (s *Sbot) setupForkDetection(verifyRouter *message.VerificationRouter) {
	if s.PublishLog == nil {
		level.Warn(s.info).Log("event", "fork detection disabled: no publish log")
		return
	}

	fp := newForkPublisher(s.rootCtx, s.PublishLog)
	verifyRouter.SetForkHandler(fp.HandleFork)
	level.Info(s.info).Log("event", "fork detection enabled")
}
