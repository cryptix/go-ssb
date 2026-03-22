// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

// Package tracing provides OpenTelemetry instrumentation for go-ssb.
//
// Library code uses the Tracer variable to create spans. When no OTel SDK is
// configured (the default), all calls are no-ops with zero overhead.
// Applications (e.g. cmd/go-sbot) configure the SDK to enable tracing.
package tracing

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Tracer is the go-ssb library tracer. All spans created by go-ssb use this tracer.
// When no OTel SDK is configured, this produces no-op spans.
var Tracer = otel.Tracer("go-ssb")

// SSB-specific attribute keys. Using constants avoids typos and enables
// tooling to discover all attributes used across the codebase.
var (
	// Peer and feed identification
	AttrPeerID    = attribute.Key("ssb.peer.id")
	AttrFeed      = attribute.Key("ssb.feed")
	AttrFeedCount = attribute.Key("ssb.feed.count")

	// Message and batch metrics
	AttrMessageCount = attribute.Key("ssb.message.count")
	AttrBatchSize    = attribute.Key("ssb.batch.size")
	AttrSeqFirst     = attribute.Key("ssb.seq.first")
	AttrSeqLast      = attribute.Key("ssb.seq.last")

	// Frontier attributes
	AttrFrontierSize     = attribute.Key("ssb.frontier.size")
	AttrFrontierAdvanced = attribute.Key("ssb.frontier.advanced")

	// Query attributes
	AttrQueryOp           = attribute.Key("ssb.query.op")
	AttrQueryType         = attribute.Key("ssb.query.type")
	AttrQueryAuthor       = attribute.Key("ssb.query.author")
	AttrQueryChannel      = attribute.Key("ssb.query.channel")
	AttrQueryArgs         = attribute.Key("ssb.query.args")
	AttrQueryJSON         = attribute.Key("ssb.query.json")
	AttrBitmapCardinality = attribute.Key("ssb.bitmap.cardinality")
	AttrResultCount       = attribute.Key("ssb.result.count")
	AttrSearchQuery       = attribute.Key("ssb.search.query")

	// Index attributes
	AttrIndexName = attribute.Key("ssb.index.name")
)

// WithQueryOp returns a SpanStartOption that sets the query operation attribute.
func WithQueryOp(op string) trace.SpanStartOption {
	return trace.WithAttributes(AttrQueryOp.String(op))
}

// SpanFromFrontier adds frontier-related attributes to a span.
// frontierSize is the number of feeds, advanced is from Diff().
func SpanFromFrontier(span trace.Span, frontierSize, advanced int) {
	span.SetAttributes(
		AttrFrontierSize.Int(frontierSize),
		AttrFrontierAdvanced.Int(advanced),
	)
}
