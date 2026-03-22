// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// setupOTelTracing configures the OpenTelemetry SDK with an OTLP HTTP exporter.
// The exporter endpoint is controlled by the standard OTEL_EXPORTER_OTLP_ENDPOINT
// environment variable (defaults to http://localhost:4318).
// Returns a shutdown function that flushes remaining spans.
func setupOTelTracing(ctx context.Context) (func(), error) {
	exporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating OTLP trace exporter: %w", err)
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewSchemaless(
			semconv.ServiceName("go-sbot"),
			semconv.ServiceVersion(Version),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("creating OTel resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)

	shutdown := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		tp.Shutdown(ctx)
	}

	return shutdown, nil
}
