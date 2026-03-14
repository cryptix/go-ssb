// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package graph

import (
	"bytes"
	"fmt"
	"math"
	"net/http"
	"sync"

	"github.com/ssbc/margaret/v2/multilog"
	"go.mindeco.de/log"
	"go.mindeco.de/log/level"
	"gonum.org/v1/gonum/graph"
	"gonum.org/v1/gonum/graph/path"
	"gonum.org/v1/gonum/graph/simple"

	"github.com/ssbc/go-ssb"
	refs "github.com/ssbc/go-ssb-refs"
	"github.com/ssbc/go-ssb-refs/tfk"
	"github.com/ssbc/go-ssb/internal/storedrefs"
)

// Builder can build a trust graph and answer other questions
type Builder interface {

	// Build a complete graph of all follow/block relations
	Build() (*Graph, error)

	// Follows returns a set of all people ref follows
	Follows(refs.FeedRef) (*ssb.StrFeedSet, error)

	// TODO: move this into the graph
	Hops(refs.FeedRef, int) *ssb.StrFeedSet

	Authorizer(from refs.FeedRef, maxHops int) ssb.Authorizer

	DeleteAuthor(who refs.FeedRef) error
}

// dbKeyPrefix is still used by BadgerGraphStore to namespace keys in a shared BadgerDB.
var (
	dbKeyPrefix    = []byte("trust-graph")
	dbKeyPrefixLen = len(dbKeyPrefix)
)

// GraphBuilder implements the Builder interface using a pluggable GraphStore backend.
type GraphBuilder struct {
	store GraphStore

	idxInSync sync.WaitGroup

	log log.Logger

	cacheLock   sync.Mutex
	cachedGraph *Graph

	hmacSecret *[32]byte
}

// BadgerBuilder is an alias for GraphBuilder for backwards compatibility.
type BadgerBuilder = GraphBuilder

// NewBuilder creates a Builder backed by the given GraphStore.
func NewBuilder(log log.Logger, store GraphStore, hmacSecret *[32]byte) *GraphBuilder {
	b := &GraphBuilder{
		store: store,
		log:   log,

		hmacSecret: hmacSecret,
	}

	// make sure we initialize the waitgroup so we have an opportunity to index
	b.indexSyncStart()
	defer b.indexSyncDone()

	return b
}

// setRelation stores a contact/metafeed relationship state.
func (b *GraphBuilder) setRelation(addr multilog.Addr, state idxRelationState) error {
	return b.store.SetRelation([]byte(addr), []byte{byte('0' + state)})
}

// setAnnouncement stores a metafeed announcement (raw TFK bytes).
func (b *GraphBuilder) setAnnouncement(addr multilog.Addr, tfkFeed []byte) error {
	return b.store.SetRelation([]byte(addr), tfkFeed)
}

func (b *GraphBuilder) DeleteAuthor(who refs.FeedRef) error {
	b.WaitUntilIndexesAreSynced()
	b.cacheLock.Lock()
	defer b.cacheLock.Unlock()
	b.cachedGraph = nil
	prefix := []byte(storedrefs.Feed(who))
	return b.store.DeletePrefix(prefix)
}

func (b *GraphBuilder) Authorizer(from refs.FeedRef, maxHops int) ssb.Authorizer {
	return &authorizer{
		b:       b,
		from:    from,
		maxHops: maxHops,
		log:     b.log,
	}
}

func (b *GraphBuilder) Build() (*Graph, error) {
	b.WaitUntilIndexesAreSynced()
	dg := NewGraph()

	b.cacheLock.Lock()
	defer b.cacheLock.Unlock()

	if b.cachedGraph != nil {
		return b.cachedGraph, nil
	}

	err := b.store.IterateAll(func(k, v []byte) error {
		if len(k) != 68 {
			return nil
		}

		rawFrom := k[:34]
		rawTo := k[34:]

		if bytes.Equal(rawFrom, rawTo) {
			// contact self?!
			return nil
		}

		var to, from tfk.Feed
		if err := from.UnmarshalBinary(rawFrom); err != nil {
			return fmt.Errorf("builder: couldnt idx key value (from): %w", err)
		}
		if err := to.UnmarshalBinary(rawTo); err != nil {
			return fmt.Errorf("builder: couldnt idx key value (to): %w", err)
		}

		bfrom := multilog.Addr(rawFrom)
		nFrom, has := dg.lookup[bfrom]
		if !has {
			fromRef, err := from.Feed()
			if err != nil {
				return err
			}

			nFrom = &contactNode{dg.NewNode(), fromRef, ""}
			dg.AddNode(nFrom)
			dg.lookup[bfrom] = nFrom
		}

		bto := multilog.Addr(rawTo)
		nTo, has := dg.lookup[bto]
		if !has {
			toRef, err := to.Feed()
			if err != nil {
				return err
			}
			nTo = &contactNode{dg.NewNode(), toRef, ""}
			dg.AddNode(nTo)
			dg.lookup[bto] = nTo
		}

		if nFrom.ID() == nTo.ID() {
			return nil
		}

		var edg graph.WeightedEdge

		if len(v) >= 1 {
			switch v[0] {
			case '0': // not following
				edg = contactEdge{
					WeightedEdge: simple.WeightedEdge{F: nFrom, T: nTo, W: math.Inf(-1)},
					isBlock:      false,
				}
			case '1': // following
				edg = contactEdge{
					WeightedEdge: simple.WeightedEdge{F: nFrom, T: nTo, W: 1},
					isBlock:      false,
				}
			case '2': // blocking
				edg = contactEdge{
					WeightedEdge: simple.WeightedEdge{F: nFrom, T: nTo, W: math.Inf(1)},
					isBlock:      true,
				}
			case '3': // metafeed
				edg = metafeedEdge{
					WeightedEdge: simple.WeightedEdge{F: nFrom, T: nTo, W: 0.1},
				}
			default:
				return fmt.Errorf("barbage value in graph strore %q", string(v))
			}
		}

		if edg == nil || math.IsInf(edg.Weight(), -1) {
			return nil
		}

		dg.SetWeightedEdge(edg)
		return nil
	})

	b.cachedGraph = dg
	return dg, err
}

type Lookup struct {
	dijk   path.Shortest
	lookup key2node
}

func (l Lookup) Dist(to refs.FeedRef) ([]graph.Node, float64) {
	bto := storedrefs.Feed(to)
	nTo, has := l.lookup[bto]
	if !has {
		return nil, math.Inf(-1)
	}
	return l.dijk.To(nTo.ID())
}

func (b *GraphBuilder) Follows(forRef refs.FeedRef) (*ssb.StrFeedSet, error) {
	b.WaitUntilIndexesAreSynced()
	fs := ssb.NewFeedSet(50)
	prefix := []byte(storedrefs.Feed(forRef))
	err := b.store.IteratePrefix(prefix, func(k, v []byte) error {
		if len(v) >= 1 && v[0] == '1' {
			// extract 2nd feed ref out of key
			var sr tfk.Feed
			err := sr.UnmarshalBinary(k[34:])
			if err != nil {
				return fmt.Errorf("follows(%s): invalid ref entry in db for feed: %w", forRef.String(), err)
			}
			fr, err := sr.Feed()
			if err != nil {
				return err
			}
			if err := fs.AddRef(fr); err != nil {
				return fmt.Errorf("follows(%s): couldn't add parsed ref feed: %w", forRef.String(), err)
			}
		}
		return nil
	})
	return fs, err
}

// Metafeed returns the metafeed for a subfeed, or an error if it has none.
func (b *GraphBuilder) Metafeed(subfeed refs.FeedRef) (refs.FeedRef, error) {
	b.WaitUntilIndexesAreSynced()
	var found refs.FeedRef

	key := []byte(storedrefs.Feed(subfeed))
	v, err := b.store.GetRelation(key)
	if err != nil {
		return found, err
	}

	var sr tfk.Feed
	err = sr.UnmarshalBinary(v)
	if err != nil {
		return found, fmt.Errorf("metafeed(%s): invalid ref entry in db for feed: %w", subfeed.String(), err)
	}
	fr, err := sr.Feed()
	if err != nil {
		return found, err
	}
	found = fr

	return found, nil
}

// Subfeeds returns the set of subfeeds for a particular metafeed.
func (b *GraphBuilder) Subfeeds(metaFeed refs.FeedRef) (*ssb.StrFeedSet, error) {
	b.WaitUntilIndexesAreSynced()
	fs := ssb.NewFeedSet(50)
	prefix := []byte(storedrefs.Feed(metaFeed))
	err := b.store.IteratePrefix(prefix, func(k, v []byte) error {
		if len(v) >= 1 && v[0] == '3' {
			// extract 2nd feed ref out of key
			var sr tfk.Feed
			err := sr.UnmarshalBinary(k[34:])
			if err != nil {
				return fmt.Errorf("subfeeds(%s): invalid ref entry in db for feed: %w", metaFeed.String(), err)
			}
			fr, err := sr.Feed()
			if err != nil {
				return err
			}
			if err := fs.AddRef(fr); err != nil {
				return fmt.Errorf("subfeeds(%s): couldn't add parsed ref feed: %w", metaFeed.String(), err)
			}
		}
		return nil
	})
	return fs, err
}

// Hops returns a slice of feed refrences that are in a particulare range of from
//
//   - max == 0: only direct follows of from
//   - max == 1: max:0 + follows of friends of from
//   - max == 2: max:1 + follows of their friends
//
// See hops_test.go for concrete examples.
func (b *GraphBuilder) Hops(from refs.FeedRef, max int) *ssb.StrFeedSet {
	b.WaitUntilIndexesAreSynced()
	max++
	walked := ssb.NewFeedSet(0)
	visited := make(map[string]struct{}) // tracks the nodes we already recursed from (so we don't do them multiple times on common friends)
	err := b.recurseHops(walked, visited, from, max)
	if err != nil {
		b.log.Log("event", "error", "msg", "recurse failed", "err", err)
		return nil
	}
	walked.Delete(from)
	return walked
}

func (b *GraphBuilder) recurseHops(walked *ssb.StrFeedSet, vis map[string]struct{}, who refs.FeedRef, depth int) error {
	if depth <= 0 {
		return nil
	}

	// skip if we already visited this peer
	if _, ok := vis[who.String()]; ok {
		return nil
	}

	// utility function encapsulating logic around recursing subfeeds
	recurseSubfeeds := func(feedId refs.FeedRef) error {
		// find all their subfeeds
		subfeeds, err := b.Subfeeds(feedId)
		if err != nil {
			return fmt.Errorf("recurseHops(%d): couldnt estblish subfeeds for %s: %w", depth, feedId.String(), err)
		}

		// TODO: add iteration to reduce memory overhead of creating a bunch of slices all the time
		// ie. feedset.Each(func(f refs.FeedRef) { ... })
		subfeedList, err := subfeeds.List()
		if err != nil {
			return fmt.Errorf("recurseHops(%d): couldnt list subfeeds for list for %s: %w", depth, feedId.String(), err)
		}

		// add them to the set and recurse their follows
		for j, subfeed := range subfeedList {
			err = walked.AddRef(subfeed)
			if err != nil {
				return fmt.Errorf("recurseHops(%d): add subfeed entry(%d) of %s failed: %w", depth, j, feedId.String(), err)
			}

			// also iterate their follows. same depth because they count as the same identity as the metafeed that linked them
			if err := b.recurseHops(walked, vis, subfeed, depth); err != nil {
				return err
			}
		}
		return nil
	}

	if err := recurseSubfeeds(who); err != nil {
		return err
	}

	whosFollows, err := b.Follows(who)
	if err != nil {
		return fmt.Errorf("recurseHops(%d): follow listing for target failed: %w", depth, err)
	}

	theirFollowList, err := whosFollows.List()
	if err != nil {
		return fmt.Errorf("recurseHops(%d): invalid entry in feed set: %w", depth, err)
	}

	for i, followedByWho := range theirFollowList {
		err := walked.AddRef(followedByWho)
		if err != nil {
			return fmt.Errorf("recurseHops(%d): add list entry(%d) failed: %w", depth, i, err)
		}

		// looking for metafeed of iterated follow
		if mf, err := b.Metafeed(followedByWho); err == nil {
			// add the retrieved metafeed as one of the visited hops (note: it is at distance 0 from its corresponding main feed)
			err := walked.AddRef(mf)
			if err != nil {
				return fmt.Errorf("recurseHops(%d): add metafeed entry(%d) failed: %w", depth, i, err)
			}

			if err := recurseSubfeeds(mf); err != nil {
				return err
			}
		}

		if err := recurseSubfeeds(followedByWho); err != nil {
			return err
		}

		// TODO: use from follows followedByWho
		dstFollows, err := b.Follows(followedByWho)
		if err != nil {
			return fmt.Errorf("recurseHops(%d): follows from entry(%d) failed: %w", depth, i, err)
		}

		isF := dstFollows.Has(who)
		if isF { // found a friend, recurse
			if err := b.recurseHops(walked, vis, followedByWho, depth-1); err != nil {
				return err
			}
		}
		// b.log.Log("depth", depth, "from", from.ShortRef(), "follows", followedByWho.ShortRef(), "friend", isF, "cnt", dstFollows.Count())
	}

	// mark them as visited
	vis[who.String()] = struct{}{}

	return nil
}

func (b *GraphBuilder) DumpXMLOverHTTP(self refs.FeedRef, w http.ResponseWriter, req *http.Request) {
	hlog := log.With(b.log, "http-handler", req.URL.Path)
	g, err := b.Build()
	if err != nil {
		level.Error(hlog).Log("http-err", err.Error())
		http.Error(w, "graph build failure", http.StatusInternalServerError)
		return
	}

	// initialze new reducer
	var rg graphReducer
	rg.wanted = make(wantedMap)
	rg.graph = simple.NewWeightedDirectedGraph(0, math.Inf(1))

	// find the nodes we are interested in

	selfNode, has := g.getNode(self)
	if !has {
		level.Error(hlog).Log("http-err", "no self node in graph")
		http.Error(w, "graph build failure", http.StatusInternalServerError)
		return
	}
	rg.wanted[selfNode.ID()] = struct{}{}

	hopsSet := b.Hops(self, 1) // TODO: parametize
	hopsList, err := hopsSet.List()
	if err != nil {
		level.Error(hlog).Log("http-err", err.Error())
		http.Error(w, "graph build failure", http.StatusInternalServerError)
		return
	}

	for _, feed := range hopsList {
		node, has := g.getNode(feed)
		if !has {
			continue
		}
		rg.wanted[node.ID()] = struct{}{}
	}

	graph.CopyWeighted(rg, g)

	var smallerGraph = new(Graph)
	smallerGraph.lookup = g.lookup
	smallerGraph.WeightedDirectedGraph = rg.graph

	n := smallerGraph.NodeCount()
	if n > 100 {
		level.Error(hlog).Log("http-err", "too many nodes", "count", n)
		http.Error(w, "too many nodes", http.StatusInternalServerError)
		return
	}

	wh := w.Header()
	wh.Set("Content-Type", "image/svg+xml")
	w.WriteHeader(http.StatusOK)
	err = smallerGraph.RenderSVG(w)
	if err != nil {
		level.Error(hlog).Log("http-err", err.Error())
	}

	level.Info(hlog).Log("graph", "dumped", "nodes", n)
}

type wantedMap map[int64]struct{}

type graphReducer struct {
	graph *simple.WeightedDirectedGraph

	wanted wantedMap
}

// NewNode is required by the graph.NodeAdder interface but is intentionally
// unimplemented. CopyWeighted never calls NewNode on the destination graph;
// it only calls AddNode with existing nodes from the source graph.
func (gs graphReducer) NewNode() graph.Node {
	panic("graphReducer: NewNode should never be called during CopyWeighted")
}

// AddNode adds a node to the graph. AddNode panics if
// the added node ID matches an existing node ID.
func (gs graphReducer) AddNode(a graph.Node) {
	if _, has := gs.wanted[a.ID()]; !has {
		return
	}
	gs.graph.AddNode(a)
}

// NewWeightedEdge is required by the graph.WeightedEdgeAdder interface but is
// intentionally unimplemented. CopyWeighted never calls NewWeightedEdge on the
// destination; it copies existing edges from the source graph directly.
func (gs graphReducer) NewWeightedEdge(from graph.Node, to graph.Node, weight float64) graph.WeightedEdge {
	panic("graphReducer: NewWeightedEdge should never be called during CopyWeighted")
}

// SetWeightedEdge adds an edge from one node to
// another. If the graph supports node addition
// the nodes will be added if they do not exist,
// otherwise SetWeightedEdge will panic.
// The behavior of a WeightedEdgeAdder when the IDs
// returned by e.From() and e.To() are equal is
// implementation-dependent.
// Whether e, e.From() and e.To() are stored
// within the graph is implementation dependent.
func (gs graphReducer) SetWeightedEdge(e graph.WeightedEdge) {
	if _, has := gs.wanted[e.From().ID()]; !has {
		// fmt.Println("ignoring from", e.From().(*contactNode).feed.Ref())
		return
	}

	if _, has := gs.wanted[e.To().ID()]; !has {
		// fmt.Println("ignoring to", e.From().(*contactNode).feed.Ref())
		return
	}

	gs.graph.SetWeightedEdge(e)
}
