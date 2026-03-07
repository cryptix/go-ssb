// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package tangles

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	bmap "github.com/dgraph-io/sroar"
	"github.com/ssbc/go-muxrpc/v3"
	margaret "github.com/ssbc/margaret/v2"
	"github.com/ssbc/margaret/v2/multilog"
	"github.com/ssbc/margaret/v2/multilog/roaring"
	"go.mindeco.de/log"
	"go.mindeco.de/log/level"

	"github.com/ssbc/go-muxrpc/v3/typemux"
	"github.com/ssbc/go-ssb"
	refs "github.com/ssbc/go-ssb-refs"
	"github.com/ssbc/go-ssb/internal/mutil"
	"github.com/ssbc/go-ssb/internal/storedrefs"
	"github.com/ssbc/go-ssb/message"
	"github.com/ssbc/go-ssb/message/multimsg"
	"github.com/ssbc/go-ssb/private"
)

type Plugin struct {
	h muxrpc.Handler
}

func NewPlugin(logger log.Logger, getter ssb.Getter, rxlog margaret.Log[*multimsg.MultiMessage], tangles, private *roaring.MultiLog, unboxer *private.Manager, isSelf ssb.Authorizer) *Plugin {
	mux := typemux.New(log.NewNopLogger())

	mux.RegisterSource(muxrpc.Method{"tangles", "thread"}, repliesHandler{
		getter:  getter,
		rxlog:   rxlog,
		tangles: tangles,

		// private utils
		private: private,
		unboxer: unboxer,
		isSelf:  isSelf,

		info: logger,
	})

	return &Plugin{
		h: &mux,
	}
}

func (lt Plugin) Name() string            { return "tangles" }
func (Plugin) Method() muxrpc.Method      { return muxrpc.Method{"tangles"} }
func (lt Plugin) Handler() muxrpc.Handler { return lt.h }

type repliesHandler struct {
	getter ssb.Getter
	rxlog  margaret.Log[*multimsg.MultiMessage]

	tangles *roaring.MultiLog
	private *roaring.MultiLog

	isSelf  ssb.Authorizer
	unboxer *private.Manager

	info log.Logger
}

func (g repliesHandler) HandleSource(ctx context.Context, req *muxrpc.Request, snk *muxrpc.ByteSink) error {
	var (
		qryarr []message.TanglesArgs
		qry    message.TanglesArgs

		logger = log.With(g.info, "method", "tangles.thread")
		start  = time.Now()
	)

	err := json.Unmarshal(req.RawArgs, &qryarr)
	if err != nil {
		if req.RawArgs[0] != '"' {
			return fmt.Errorf("bad request - invalid root: %w", err)
		}

		var ref refs.MessageRef
		err := json.Unmarshal(req.RawArgs, &ref)
		if err != nil {
			return fmt.Errorf("bad request - invalid root (string?): %w", err)
		}
		qry.Root = ref
		qry.Limit = -1
		qry.Keys = true
	} else {
		if n := len(qryarr); n != 1 {
			return fmt.Errorf("expected 1 argument but got %d", n)
		}
		qry = qryarr[0]
	}

	if qry.Limit == 0 {
		qry.Limit = -1
	}

	remote, err := ssb.GetFeedRefFromAddr(req.RemoteAddr())
	if err != nil {
		return fmt.Errorf("failed to determain remote: %w", err)
	}

	isSelf := g.isSelf.Authorize(remote)
	if qry.Private && isSelf != nil {
		return fmt.Errorf("not authroized")
	}

	logger = log.With(logger, "root", qry.Root.ShortSigil())

	// lookup address depending if we have a name for the tangle or not
	addr := storedrefs.TangleV1(qry.Root)
	if qry.Name != "" {
		addr = storedrefs.TangleV2(qry.Name, qry.Root)
	}

	if qry.Live {
		if qry.Private {
			return fmt.Errorf("TODO: fix live && private")
		}
		threadLog, err := g.tangles.Get(addr)
		if err != nil {
			return fmt.Errorf("failed to load thread: %w", err)
		}

		resolved := mutil.Indirect(g.rxlog, threadLog)
		qryOpts := []margaret.QueryOption{margaret.Limit(int(qry.Limit)), margaret.Reverse(qry.Reverse)}
		if qry.Live {
			qryOpts = append(qryOpts, margaret.Live(ctx))
		}
		qryIter := resolved.Query(qryOpts...)

		snk.SetEncoding(muxrpc.TypeJSON)
		for _, mm := range qryIter.Iter() {
			if mm.Message == nil {
				continue
			}
			if err := writeMessage(snk, mm.Message, qry.Keys); err != nil {
				return fmt.Errorf("tangle: failed to write msg: %w", err)
			}
		}
		if err := qryIter.Err(); err != nil {
			return fmt.Errorf("tangle: query failed: %w", err)
		}

		return snk.Close()
	}

	// not live
	threadBmap, err := g.tangles.LoadInternalBitmap(addr)
	if err != nil {
		// TODO: check err == persist: not found
		return snk.Close()
	}

	if !qry.Private {
		// filter all boxed messages from the stream
		box1, err := g.private.LoadInternalBitmap(multilog.Addr("meta:box1"))
		if err != nil {
			box1 = bmap.NewBitmap()
		}

		box2, err := g.private.LoadInternalBitmap(multilog.Addr("meta:box2"))
		if err != nil {
			box2 = bmap.NewBitmap()
		}

		box1.Or(box2) // all the boxed messages

		// remove all the boxed ones from the type we are looking up
		it := box1.NewIterator()
		for i := 0; i < box1.GetCardinality(); i++ {
			v := it.Next()
			if threadBmap.Contains(v) {
				threadBmap.Remove(v)
			}
		}
	}

	// get root message
	var tps []refs.TangledPost
	root, err := g.getter.Get(qry.Root)
	if err == nil {
		var tp tangledPost

		var content = root.ContentBytes()
		if qry.Private {
			unboxed, err := g.unboxer.DecryptMessage(root)
			if err != nil && err != private.ErrNotBoxed {
				return err
			} else if err == nil {
				content = unboxed
			}
		}

		err = json.Unmarshal(content, &tp.Value.Content)
		if err == nil {
			tp.TheKey = root.Key()
			tp.Value.Author = root.Author()
			tp.Value.Sequence = root.Seq()
			tp.Value.Timestamp = refs.Millisecs(root.Claimed())
			tps = append(tps, tp)
		} else {
			if qry.Private {
				return fmt.Errorf("failed to unpack root message %s: %w", root.Key().String(), err)
			}
		}
	}

	// get replies and add them to sorter
	it := threadBmap.NewIterator()
	for i := 0; i < threadBmap.GetCardinality(); i++ {
		seq := int64(it.Next())
		mm, err := g.rxlog.Get(seq)
		if err != nil {
			if margaret.IsErrNulled(err) {
				continue
			}
			fmt.Fprintln(os.Stderr, "tangles failed to get seq:", seq, " with:", err)
			continue
		}

		if mm.Message == nil {
			continue
		}

		msg := mm.Message

		var content = msg.ContentBytes()
		if qry.Private {
			unboxed, err := g.unboxer.DecryptMessage(msg)
			if err != nil && err != private.ErrNotBoxed {
				return err
			} else if err == nil {
				content = unboxed
			}
		}

		// find tangles
		var tp tangledPost
		err = json.Unmarshal(content, &tp.Value.Content)
		if err != nil {
			return fmt.Errorf("failed to unpack message %s: %w", msg.Key().String(), err)
		}
		tp.TheKey = msg.Key()
		tp.Value.Author = msg.Author()
		tp.Value.Sequence = msg.Seq()
		tp.Value.Timestamp = refs.Millisecs(msg.Claimed())

		tps = append(tps, tp)

		if qry.Limit >= 0 {
			qry.Limit--
			if qry.Limit == 0 {
				break
			}
		}
	}

	// sort them
	sorter := &refs.ByPrevious{
		TangleName: qry.Name,
		Items:      tps,
	}
	sort.Sort(sorter)

	sorted := time.Now()
	level.Debug(logger).Log("event", "sorted seqs", "n", len(sorter.Items), "took", time.Since(start))

	// stream them out
	enc := json.NewEncoder(snk)
	snk.SetEncoding(muxrpc.TypeJSON)

	cnt := 0
	for i, p := range sorter.Items {
		if qry.Keys {
			err = enc.Encode(p)
		} else {
			err = enc.Encode(p.(tangledPost).Value)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "tangles failed send:", i, " with:", err)
			break
		}
		cnt++
	}
	level.Debug(logger).Log("event", "messages streamed", "cnt", cnt, "took", time.Since(sorted))
	return snk.Close()
}

// writeMessage encodes a message to the muxrpc sink.
func writeMessage(w *muxrpc.ByteSink, msg refs.Message, keys bool) error {
	if keys {
		var kv refs.KeyValueRaw
		kv.Key_ = msg.Key()
		kv.Value = *msg.ValueContent()
		kv.Timestamp = refs.Millisecs(msg.Received())
		kvMsg, err := json.Marshal(kv)
		if err != nil {
			return fmt.Errorf("failed to encode key-value: %w", err)
		}
		_, err = w.Write(kvMsg)
		return err
	}
	_, err := w.Write(msg.ValueContentJSON())
	return err
}

type tangledPost struct {
	TheKey refs.MessageRef `json:"key"`
	Value  struct {
		refs.Value
		// substitute Content with refs.Post
		Content refs.Post `json:"content"`
	} `json:"value"`
}

func (tm tangledPost) Key() refs.MessageRef {
	return tm.TheKey
}

func (tm tangledPost) Tangle(name string) (*refs.MessageRef, refs.MessageRefs) {
	if name == "" {
		return tm.Value.Content.Root, tm.Value.Content.Branch
	}

	tp, has := tm.Value.Content.Tangles[name]
	if !has {
		return nil, nil
	}

	return tp.Root, tp.Previous
}
