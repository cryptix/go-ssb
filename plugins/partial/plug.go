// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

// Package partial is a helper module for ssb-browser-core, enabling to fetch subsets of feeds.
// See https://github.com/arj03/ssb-partial-replication for more.
package partial

import (
	"github.com/ssbc/go-muxrpc/v3"
	"github.com/ssbc/go-muxrpc/v3/typemux"
	margaret "github.com/ssbc/margaret/v2"
	"github.com/ssbc/margaret/v2/multilog/roaring"
	"go.mindeco.de/logging"

	"github.com/ssbc/go-ssb"
	"github.com/ssbc/go-ssb/message/multimsg"
	"github.com/ssbc/go-ssb/plugins/gossip"
	"github.com/ssbc/go-ssb/query"
)

type plugin struct {
	h muxrpc.Handler
}

const name = "partialReplication"

func (p plugin) Name() string {
	return name
}

func (p plugin) Method() muxrpc.Method {
	return muxrpc.Method{name}
}

func (p plugin) Handler() muxrpc.Handler {
	return p.h
}

func New(log logging.Interface,
	fm *gossip.FeedManager,
	feeds, bytype, roots *roaring.MultiLog,
	rxlog margaret.Log[*multimsg.MultiMessage],
	get ssb.Getter,
	search query.Searcher, // may be nil
) ssb.Plugin {
	rootHdlr := typemux.New(log)

	rootHdlr.RegisterAsync(muxrpc.Method{name, "getTangle"}, getTangleHandler{
		roots: roots,
		get:   get,
		rxlog: rxlog,
	})

	qp := query.NewSubsetPlaner(feeds, bytype)
	if search != nil {
		qp = qp.WithSearch(search)
	}

	rootHdlr.RegisterSource(muxrpc.Method{name, "getSubset"}, getSubsetHandler{
		queryPlaner: qp,
		rxLog:       rxlog,
	})

	// TODO:
	// rootHdlr.RegisterSource(muxrpc.Method{name, "resolveIndexFeed"}, getFeedReverseHandler{
	// 	fm: fm,
	// })

	return plugin{
		h: &rootHdlr,
	}
}
