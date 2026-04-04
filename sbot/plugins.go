// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package sbot

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/rs/cors"
	"github.com/ssbc/margaret/v2/multilog"
	"go.mindeco.de/log"
	"go.mindeco.de/log/level"

	refs "github.com/ssbc/go-ssb-refs"
	"github.com/ssbc/go-ssb/internal/storedrefs"
	"github.com/ssbc/go-ssb/message"
	"github.com/ssbc/go-ssb/message/multimsg"
	"github.com/ssbc/go-ssb/network"
	"github.com/ssbc/go-ssb/plugins/blobs"
	"github.com/ssbc/go-ssb/plugins/conn"
	"github.com/ssbc/go-ssb/plugins/ebt"
	"github.com/ssbc/go-ssb/plugins/friends"
	"github.com/ssbc/go-ssb/plugins/get"
	"github.com/ssbc/go-ssb/plugins/gossip"
	"github.com/ssbc/go-ssb/plugins/groups"
	"github.com/ssbc/go-ssb/plugins/legacyinvites"
	"github.com/ssbc/go-ssb/plugins/partial"
	privplug "github.com/ssbc/go-ssb/plugins/private"
	"github.com/ssbc/go-ssb/plugins/publish"
	"github.com/ssbc/go-ssb/plugins/rawread"
	"github.com/ssbc/go-ssb/plugins/replicate"
	"github.com/ssbc/go-ssb/plugins/status"
	"github.com/ssbc/go-ssb/plugins/tangles"
	"github.com/ssbc/go-ssb/plugins/whoami"
	"github.com/ssbc/go-ssb/plugins2/names"
	"github.com/ssbc/go-ssb/private"
	"github.com/ssbc/go-ssb/query"
	"github.com/ssbc/go-ssb/repo"
)

// registerPlugins creates all muxrpc plugins and sets up the network node.
// Called from New() when networking is enabled.
func (s *Sbot) registerPlugins(ctx context.Context, storageRepo repo.Interface, namesPlug names.Plugin) error {
	// publish
	authorLog, err := s.Users.Get(storedrefs.Feed(s.KeyPair.ID()))
	if err != nil {
		return fmt.Errorf("failed to open user private index: %w", err)
	}
	s.master.Register(publish.NewPlug(log.With(s.info, "unit", "publish"), s.PublishLog, s.Groups, authorLog))

	// private
	// TODO: box2
	userPrivs, err := s.Private.Get(multilog.Addr("box1:") + storedrefs.Feed(s.KeyPair.ID()))
	if err != nil {
		return fmt.Errorf("failed to open user private index: %w", err)
	}
	s.master.Register(privplug.NewPlug(
		log.With(s.info, "unit", "private"),
		s.KeyPair.ID(),
		s.Groups,
		s.PublishLog,
		private.NewUnboxerLog(s.ReceiveLog, userPrivs, s.KeyPair)))

	// whoami
	whoami := whoami.New(log.With(s.info, "unit", "whoami"), s.KeyPair.ID())
	s.public.Register(whoami)
	s.master.Register(whoami)

	// blobs
	blobs := blobs.New(log.With(s.info, "unit", "blobs"), s.KeyPair.ID(), s.BlobStore, s.WantManager)
	s.public.Register(blobs)
	s.master.Register(blobs) // TODO: does not need to open a createWants on this one?!

	// gossiping (legacy and ebt)
	fm := gossip.NewFeedManager(
		ctx,
		s.ReceiveLog,
		s.Users,
		log.With(s.info, "unit", "gossip"),
		s.systemGauge,
		s.eventCounter,
	)
	s.feedManager = fm

	// outgoing gossip behavior
	var histOpts = []interface{}{
		gossip.Promisc(s.promisc),
	}

	if s.systemGauge != nil {
		histOpts = append(histOpts, s.systemGauge)
	}

	if s.eventCounter != nil {
		histOpts = append(histOpts, s.eventCounter)
	}

	if s.signHMACsecret != nil {
		histOpts = append(histOpts, gossip.HMACSecret(s.signHMACsecret))
	}

	if s.numberOfConcurrentReplicationsPerPeer != 0 {
		histOpts = append(histOpts, gossip.NumberOfConcurrentReplicationsPerPeer(s.numberOfConcurrentReplicationsPerPeer))
	}

	if s.numberOfConcurrentReplications != 0 {
		histOpts = append(histOpts, gossip.NumberOfConcurrentReplications(s.numberOfConcurrentReplications))
	}

	s.verifyRouter, err = message.NewVerificationRouter(s.ReceiveLog.(*multimsg.WrappedLog), s.Users, s.signHMACsecret)
	if err != nil {
		return err
	}
	s.setupForkDetection(s.verifyRouter)

	if s.disableLegacyLiveReplication {
		histOpts = append(histOpts, gossip.WithLive(!s.disableLegacyLiveReplication))
	}

	gossipPlug := gossip.NewFetcher(ctx,
		log.With(s.info, "plugin", "gossip"),
		storageRepo,
		s.KeyPair.ID(),
		s.ReceiveLog, s.Users,
		fm, s.Replicator.Lister(),
		s.verifyRouter,
		histOpts...)

	if s.disableEBT {
		s.public.Register(gossipPlug)
	} else {
		ebtPlug := ebt.NewPlug(
			log.With(s.info, "plugin", "ebt"),
			s.KeyPair.ID(),
			s.ReceiveLog,
			s.Users,
			fm,
			s.ebtState,
			s.verifyRouter,
		)
		s.public.Register(ebtPlug)
		s.ebtHandler = ebtPlug.MUXRPCHandler

		rn := negPlugin{replicateNegotiator{
			logger:  log.With(s.info, "module", "replicate-negotiator"),
			ebtOnly: s.ebtOnly,

			lg:  gossipPlug.LegacyGossip,
			ebt: ebtPlug.MUXRPCHandler,
		}}
		s.public.Register(rn)
	}

	// incoming createHistoryStream handler
	hist := gossip.NewServer(ctx,
		log.With(s.info, "unit", "gossip/hist"),
		s.KeyPair.ID(),
		s.ReceiveLog, s.Users,
		s.Replicator.Lister(),
		fm,
		histOpts...)
	s.public.Register(hist)

	// get idx muxrpc handler
	s.master.Register(get.New(s, s.Groups))

	// about information
	s.master.Register(namesPlug)

	// (insecure) partial proof-of-concept for browser-core/demo
	var searchIdx query.Searcher
	if s.SearchIndex != nil {
		searchIdx = s.SearchIndex
	}
	plug := partial.New(s.info,
		fm,
		s.Users,
		s.ByType,
		s.Tangles,
		s.Channels,
		s.Mentions,
		s.Backlinks,
		s.ReceiveLog, s,
		s.GraphBuilder,
		s.SeqResolver,
		searchIdx)
	s.public.Register(plug)
	s.master.Register(plug)

	// group managment
	s.master.Register(groups.New(s.info, s.Groups))

	// raw log plugins
	sc := selfChecker{s.KeyPair.ID()}
	s.master.Register(rawread.NewByTypePlugin(
		s.info,
		s.ReceiveLog,
		s.ByType,
		s.Private,
		s.Groups,
		s.SeqResolver,
		sc))

	s.master.Register(rawread.NewRXLog(s.ReceiveLog)) // createLogStream
	s.master.Register(rawread.NewSortedStream(s.info, s.ReceiveLog, s.SeqResolver))
	s.master.Register(hist) // createHistoryStream

	s.master.Register(replicate.NewPlug(s.Users, s.KeyPair.ID(), s.Lister()))

	s.master.Register(friends.New(s.info, s.KeyPair.ID(), s.GraphBuilder))

	mh := namedPlugin{
		h:    getManifest(),
		name: "manifest"}
	s.master.Register(mh)
	s.public.Register(mh)

	var tplug = tangles.NewPlugin(
		s.info,
		s,
		s.ReceiveLog,
		s.Tangles,
		s.Private,
		s.Groups,
		sc)
	s.master.Register(tplug)

	// tcp+shs network node
	opts := network.Options{
		Logger:              s.info,
		Dialer:              s.dialer,
		ListenAddr:          s.listenAddr,
		AdvertsSend:         s.enableAdverts,
		AdvertsConnectTo:    s.enableDiscovery,
		KeyPair:             s.KeyPair,
		AppKey:              s.appKey[:],
		MakeHandler:         s.makeHandler,
		ConnTracker:         s.networkConnTracker,
		BefreCryptoWrappers: s.preSecureWrappers,
		AfterSecureWrappers: s.postSecureWrappers,

		EventCounter:    s.eventCounter,
		SystemGauge:     s.systemGauge,
		EndpointWrapper: s.edpWrapper,
		Latency:         s.latency,

		WebsocketAddr:    s.websocketAddr,
		WebsocketTLSCert: s.websocketTLSCert,
		WebsocketTLSKey:  s.websocketTLSKey,
	}

	networkNode, err := network.New(opts)
	if err != nil {
		return fmt.Errorf("failed to create network node: %w", err)
	}
	blobsGetPathPrefix := "/blobs/get/"
	httpBlogsGet := func(w http.ResponseWriter, req *http.Request) {
		hlog := log.With(s.info, "http-handler", "blobs/get")
		rest := strings.TrimPrefix(req.URL.Path, blobsGetPathPrefix)
		blobRef, err := refs.ParseBlobRef(rest)
		if err != nil {
			level.Error(hlog).Log("err", err.Error())
			http.Error(w, "bad blob", http.StatusBadRequest)
			return
		}

		br, err := s.BlobStore.Get(blobRef)
		if err != nil {
			s.WantManager.Want(blobRef)
			level.Error(hlog).Log("err", err.Error())
			http.Error(w, "no such blob", http.StatusNotFound)
			return
		}

		// wh := w.Header()
		// sniff content-type?
		w.WriteHeader(http.StatusOK)
		_, err = io.Copy(w, br)
		if err != nil {
			level.Error(hlog).Log("err", err.Error())
		}
	}

	graphDumpPathPrefix := "/graph/dump"

	simpleRouter := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if strings.HasPrefix(req.URL.Path, blobsGetPathPrefix) {
			httpBlogsGet(w, req)
			return
		}

		if strings.HasPrefix(req.URL.Path, graphDumpPathPrefix) {
			s.GraphBuilder.DumpXMLOverHTTP(s.KeyPair.ID(), w, req)
			return
		}

		http.Error(w, "404", http.StatusNotFound)
	})
	networkNode.HandleHTTP(cors.Default().Handler(simpleRouter))

	s.inviteService, err = legacyinvites.New(
		log.With(s.info, "unit", "legacyInvites"),
		storageRepo,
		s.KeyPair.ID(),
		networkNode,
		s.PublishLog,
		s.ReceiveLog,
		s.Replicator,
		s.indexStore,
	)
	if err != nil {
		return fmt.Errorf("sbot: failed to open legacy invites plugin: %w", err)
	}
	s.master.Register(s.inviteService.MasterPlugin())

	// TODO: should be gossip.connect but conflicts with our namespace assumption
	s.master.Register(conn.NewPlug(log.With(s.info, "unit", "conn"), networkNode, s))
	s.master.Register(status.New(s))

	s.public.Register(networkNode.TunnelPlugin())
	s.Network = networkNode

	return nil
}
