// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package rawread

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/ssbc/go-muxrpc/v2"
	margaret "github.com/ssbc/margaret/v2"

	"github.com/ssbc/go-ssb"
	"github.com/ssbc/go-ssb/message"
	"github.com/ssbc/go-ssb/message/multimsg"
)

// ~> sbot createLogStream --help
// (log) Fetch messages ordered by the time received.
// log [--live] [--gt index] [--gte index] [--lt index] [--lte index] [--reverse]  [--keys] [--values] [--limit n]
type rxLogPlug struct {
	h muxrpc.Handler
}

func NewRXLog(rootLog margaret.Log[*multimsg.MultiMessage]) ssb.Plugin {
	plug := &rxLogPlug{}
	plug.h = rxLogHandler{
		root: rootLog,
	}
	return plug
}

func (lt rxLogPlug) Name() string { return "createLogStream" }

func (rxLogPlug) Method() muxrpc.Method {
	return muxrpc.Method{"createLogStream"}
}
func (lt rxLogPlug) Handler() muxrpc.Handler {
	return lt.h
}

type rxLogHandler struct {
	root margaret.Log[*multimsg.MultiMessage]
}

func (rxLogHandler) Handled(m muxrpc.Method) bool { return m.String() == "createLogStream" }

func (g rxLogHandler) HandleConnect(ctx context.Context, e muxrpc.Endpoint) {}

func (g rxLogHandler) HandleCall(ctx context.Context, req *muxrpc.Request) {
	var qry message.CreateLogArgs
	var args []message.CreateLogArgs
	err := json.Unmarshal(req.RawArgs, &args)
	if err != nil {
		fmt.Fprintln(os.Stderr, "createLogStream err:", err)
		req.CloseWithError(fmt.Errorf("bad request data: %w", err))
		return
	}
	if len(args) == 1 {
		qry = args[0]
	} else {
		// Defaults for no arguments
		qry.Keys = true
		qry.Limit = -1
	}

	// empty query doesn't make much sense...
	if qry.Limit == 0 {
		qry.Limit = -1
	}

	if qry.Gt == -1 {
		qry.Seq = int64(g.root.Seq()) - 1
	}

	qryOpts := []margaret.QueryOption{
		margaret.Gte(int64(qry.Seq)),
		margaret.Limit(int(qry.Limit)),
		margaret.Reverse(qry.Reverse),
	}
	if qry.Live {
		qryOpts = append(qryOpts, margaret.Live(ctx))
	}
	qryIter := g.root.Query(qryOpts...)

	snk, err := req.ResponseSink()
	if err != nil {
		req.CloseWithError(err)
		return
	}
	snk.SetEncoding(muxrpc.TypeJSON)

	for _, mm := range qryIter.Iter() {
		if mm.Message == nil {
			continue
		}
		if err := writeMessage(snk, mm.Message, qry.Keys); err != nil {
			fmt.Fprintln(os.Stderr, "createLogStream write err:", err)
			req.CloseWithError(fmt.Errorf("logStream: failed to write msg: %w", err))
			return
		}
	}
	if err := qryIter.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "createLogStream err:", err)
		req.CloseWithError(fmt.Errorf("logStream: query failed: %w", err))
		return
	}
	snk.Close()
}
