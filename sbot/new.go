// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package sbot

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/go-kit/kit/metrics"
	"github.com/rs/cors"
	"github.com/ssbc/go-muxrpc/v3"
	"github.com/ssbc/go-netwrap"
	mindexes "github.com/ssbc/margaret/v2/indexes"
	"github.com/ssbc/margaret/v2/multilog"
	"github.com/ssbc/margaret/v2/multilog/roaring"
	multibbolt "github.com/ssbc/margaret/v2/multilog/roaring/bbolt"
	multifs "github.com/ssbc/margaret/v2/multilog/roaring/fs"
	bolt "go.etcd.io/bbolt"
	"go.mindeco.de/log"
	"go.mindeco.de/log/level"
	"golang.org/x/sync/errgroup"

	"github.com/ssbc/go-ssb"
	refs "github.com/ssbc/go-ssb-refs"
	"github.com/ssbc/go-ssb/blobstore"
	"github.com/ssbc/go-ssb/graph"
	"github.com/ssbc/go-ssb/indexes"
	"github.com/ssbc/go-ssb/internal/multicloser"
	"github.com/ssbc/go-ssb/internal/mutil"
	"github.com/ssbc/go-ssb/internal/statematrix"
	"github.com/ssbc/go-ssb/internal/storedrefs"
	"github.com/ssbc/go-ssb/message"
	"github.com/ssbc/go-ssb/message/multimsg"
	"github.com/ssbc/go-ssb/multilogs"
	"github.com/ssbc/go-ssb/network"
	"github.com/ssbc/go-ssb/plugins/blobs"
	"github.com/ssbc/go-ssb/plugins/conn"
	"github.com/ssbc/go-ssb/plugins/ebt"
	"github.com/ssbc/go-ssb/plugins/friends"
	"github.com/ssbc/go-ssb/query"
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
	"github.com/ssbc/go-ssb/private/keys"
	"github.com/ssbc/go-ssb/repo"
)

// Sbot is the database and replication server
type Sbot struct {
	info log.Logger

	// TODO: this thing is way to big right now
	// because it's options and the resulting thing in one

	// lateInit are options that need to be applied after others (like plugins that depend on keypairs)
	lateInit []Option

	rootCtx context.Context
	// Shutdown needs to be called to shutdown indexing
	Shutdown      context.CancelFunc
	closers       multicloser.MultiCloser
	combIdx       *multilogs.CombinedIndex
	feedManager   *gossip.FeedManager
	idxDone       errgroup.Group
	idxInSync     sync.WaitGroup
	idxNumSyncing int64

	closed   bool
	closedMu sync.Mutex
	closeErr error

	promisc  bool
	hopCount uint

	disableEBT                   bool
	ebtOnly                      bool // EBT-only mode: no legacy gossip fallback
	disableLegacyLiveReplication bool

	Network *network.Node
	// TODO: these should all be options that are applied on the network construction...
	disableNetwork     bool
	appKey             []byte
	listenAddr         net.Addr
	dialer             netwrap.Dialer
	edpWrapper         MuxrpcEndpointWrapper
	networkConnTracker ssb.ConnTracker
	preSecureWrappers  []netwrap.ConnWrapper
	postSecureWrappers []netwrap.ConnWrapper

	public ssb.PluginManager
	master ssb.PluginManager

	authorizer ssb.Authorizer

	enableAdverts   bool
	enableDiscovery bool

	websocketAddr    string
	websocketTLSCert string
	websocketTLSKey  string

	numberOfConcurrentReplicationsPerPeer uint
	numberOfConcurrentReplications        uint

	repoPath string
	KeyPair  ssb.KeyPair

	Groups *private.Manager

	ReceiveLog multimsg.AlterableLog // the stream of messages as they arrived

	SeqResolver *repo.SequenceResolver

	PublishLog     ssb.Publisher
	signHMACsecret *[32]byte

	// hardcoded default indexes
	Users    *roaring.MultiLog // one sublog per feed
	Private  *roaring.MultiLog // one sublog per keypair
	ByType   *roaring.MultiLog // one sublog per type: ... (special cases for private messages by suffix)
	Tangles  *roaring.MultiLog // one sublog per root:%ref (actual root is in the get index)
	Channels *roaring.MultiLog // one sublog per channel name
	Mentions *roaring.MultiLog // one sublog per mentioned ref (feed, message, or blob)

	indexStore *badger.DB
	boltDB     *bolt.DB

	// plugin indexes
	mlogIndicies map[string]*roaring.MultiLog
	simpleIndex  map[string]mindexes.Index[int64]

	liveIndexUpdates       bool
	skipConsistencyCheck   bool
	indexStateMu     sync.Mutex
	indexStates      map[string]string

	ebtState   *statematrix.StateMatrix
	ebtHandler *ebt.MUXRPCHandler

	verifyRouter *message.VerificationRouter

	GraphBuilder *graph.BadgerBuilder

	BlobStore   ssb.BlobStore
	WantManager ssb.WantManager

	// TODO: wrap better
	eventCounter metrics.Counter
	systemGauge  metrics.Gauge
	latency      metrics.Histogram

	enableSearch bool
	SearchIndex  *multilogs.SearchIndex

	enableMetafeeds bool
	MetaFeeds       ssb.MetaFeeds
	IndexFeeds      ssb.IndexFeedManager

	ssb.Replicator
}

// New creates an sbot instance using the passed options to configure it.
func New(fopts ...Option) (*Sbot, error) {
	var s = new(Sbot)
	s.liveIndexUpdates = true

	s.public = ssb.NewPluginManager()
	s.master = ssb.NewPluginManager()

	s.mlogIndicies = make(map[string]*roaring.MultiLog)
	s.simpleIndex = make(map[string]mindexes.Index[int64])
	s.indexStates = make(map[string]string)

	s.disableLegacyLiveReplication = true

	for i, opt := range fopts {
		err := opt(s)
		if err != nil {
			return nil, fmt.Errorf("error applying option #%d: %w", i, err)
		}
	}

	if s.repoPath == "" {
		u, err := user.Current()
		if err != nil {
			return nil, fmt.Errorf("error getting info on current user: %w", err)
		}

		s.repoPath = filepath.Join(u.HomeDir, ".ssb-go")
	}

	if s.appKey == nil {
		ak, err := base64.StdEncoding.DecodeString("1KHLiKZvAvjbY1ziZEHMXawbCEIM6qwjCDm3VYRan/s=")
		if err != nil {
			return nil, fmt.Errorf("failed to decode default appkey: %w", err)
		}
		s.appKey = ak
	}

	if s.dialer == nil {
		s.dialer = netwrap.Dial
	}

	if s.listenAddr == nil {
		s.listenAddr = &net.TCPAddr{Port: network.DefaultPort}
	}

	if s.info == nil {
		logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stdout))
		logger = log.With(logger, "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller)
		s.info = logger
	}

	if s.rootCtx == nil {
		s.rootCtx, s.Shutdown = ShutdownContext(context.Background())
	}
	ctx := s.rootCtx

	storageRepo := repo.New(s.repoPath)

	var err error
	if s.KeyPair == nil {
		algo := refs.RefAlgoFeedSSB1
		if s.enableMetafeeds {
			algo = refs.RefAlgoFeedBendyButt
		}
		s.KeyPair, err = repo.DefaultKeyPair(storageRepo, algo)
		if err != nil {
			return nil, fmt.Errorf("sbot: failed to get keypair: %w", err)
		}
	}

	// TODO: optionize
	s.ReceiveLog, err = repo.OpenLog(storageRepo)
	if err != nil {
		return nil, fmt.Errorf("sbot: failed to open rootlog: %w", err)
	}
	s.closers.AddCloser(s.ReceiveLog.(io.Closer))

	// if not configured
	if s.BlobStore == nil {
		// load default, local file blob store
		s.BlobStore, err = repo.OpenBlobStore(storageRepo)
		if err != nil {
			return nil, fmt.Errorf("sbot: failed to open blob store: %w", err)
		}
	}

	wantsLog := log.With(s.info, "module", "WantManager")
	wm := blobstore.NewWantManager(s.BlobStore,
		blobstore.WantWithLogger(wantsLog),
		blobstore.WantWithContext(s.rootCtx),
		blobstore.WantWithMetrics(s.systemGauge, s.eventCounter),
	)
	s.WantManager = wm
	s.closers.AddCloser(wm)

	for _, opt := range s.lateInit {
		err := opt(s)
		if err != nil {
			return nil, fmt.Errorf("sbot: failed to apply late option: %w", err)
		}
	}

	sm, err := statematrix.New(
		storageRepo.GetPath("ebt-state-matrix"),
		s.KeyPair.ID(),
	)
	if err != nil {
		return nil, err
	}
	s.closers.AddCloser(sm)
	s.ebtState = sm

	// open timestamp and sequence resovlers
	s.SeqResolver, err = repo.NewSequenceResolver(storageRepo)
	if err != nil {
		return nil, fmt.Errorf("error opening sequence resolver: %w", err)
	}
	idxTimestamps := indexes.NewTimestampSorter(s.SeqResolver)
	s.closers.AddCloser(idxTimestamps)
	s.serveIndex("timestamps", idxTimestamps)

	s.indexStore, err = repo.OpenBadgerDB(storageRepo.GetPath(repo.PrefixMultiLog, "shared-badger"))
	if err != nil {
		return nil, err
	}

	boltPath := storageRepo.GetPath(repo.PrefixIndex, "bolt.db")
	os.MkdirAll(filepath.Dir(boltPath), 0700)
	s.boltDB, err = bolt.Open(boltPath, 0600, bolt.DefaultOptions)
	if err != nil {
		return nil, fmt.Errorf("sbot: failed to open bolt db: %w", err)
	}

	// Dense multilogs — backed by filesystem (few sublogs, large bitmaps).
	var denseMlogs = []struct {
		Name string
		Mlog **roaring.MultiLog
	}{
		{multilogs.IndexNameFeeds, &s.Users},
		{multilogs.IndexNamePrivates, &s.Private},
		{"msgTypes", &s.ByType},
		{"channels", &s.Channels},
	}
	for _, index := range denseMlogs {
		mlog := multifs.NewMultiLog(storageRepo.GetPath(repo.PrefixMultiLog, index.Name))
		s.closers.AddCloser(mlog)
		s.mlogIndicies[index.Name] = mlog
		*index.Mlog = mlog
	}

	// Sparse multilogs — backed by bbolt (many sublogs with few entries each).
	// Uses the shared bolt.DB with per-multilog bucket isolation, avoiding
	// thousands of tiny files on the filesystem.
	var sparseMlogs = []struct {
		Name string
		Mlog **roaring.MultiLog
	}{
		{"tangles", &s.Tangles},
		{"mentions", &s.Mentions},
	}
	for _, index := range sparseMlogs {
		mlog, err := multibbolt.NewMultiLogWithDB(s.boltDB, "mlog:"+index.Name)
		if err != nil {
			return nil, fmt.Errorf("sbot: failed to open bbolt multilog %s: %w", index.Name, err)
		}
		s.closers.AddCloser(mlog)
		s.mlogIndicies[index.Name] = mlog
		*index.Mlog = mlog
	}

	// publish
	var pubopts = []message.PublishOption{
		message.UseNowTimestamps(true),
		message.UseWaitForIndexesCallback(s.WaitUntilIndexesAreSynced),
	}
	if s.signHMACsecret != nil {
		pubopts = append(pubopts, message.SetHMACKey(s.signHMACsecret))
	}
	s.PublishLog, err = message.OpenPublishLog(s.ReceiveLog.(*multimsg.WrappedLog), s.Users, s.KeyPair, pubopts...)
	if err != nil {
		return nil, fmt.Errorf("sbot: failed to create publish log: %w", err)
	}

	// get(msgRef) -> rxLog sequence index
	getIdx, getIdxSink := indexes.OpenGet(s.indexStore)
	s.serveIndex("get", getIdxSink)
	s.simpleIndex["get"] = getIdx

	// groups2
	keysStore := keys.NewStore(s.indexStore, []byte("group-and-signing"))

	s.Groups = private.NewManager(s.KeyPair, s.PublishLog, keysStore, s.ReceiveLog, s, s.Tangles)

	groupsHelperMlog := multifs.NewMultiLog(storageRepo.GetPath(repo.PrefixMultiLog, "group-member-helper"))
	s.closers.AddCloser(groupsHelperMlog)

	// the big combined index of most the things
	combIdx, err := multilogs.NewCombinedIndex(
		s.repoPath,
		s.Groups,
		s.KeyPair.ID(),
		s.ReceiveLog,
		s.Users,
		s.Private,
		s.ByType,
		s.Tangles,
		s.Channels,
		s.Mentions,
		groupsHelperMlog,
		sm,
	)
	if err != nil {
		return nil, fmt.Errorf("sbot: failed to open combined application index: %w", err)
	}

	if !s.skipConsistencyCheck {
		// Check that the combined index state is consistent with the user-feeds
		// multilog. A crash between state-save and multilog flush can leave the
		// state file ahead of reality, causing feeds to be silently skipped.
		rewound, err := combIdx.VerifyConsistency(s.ReceiveLog)
		if err != nil {
			return nil, fmt.Errorf("sbot: combined index consistency check failed: %w", err)
		}
		if rewound {
			level.Warn(s.info).Log("event", "combined-index-rewound",
				"msg", "index state was ahead of multilog, rewound to force re-indexing")
		}
	}

	s.combIdx = combIdx
	s.serveIndex("combined", combIdx)
	s.closers.AddCloser(combIdx)

	// full-text search index (optional)
	if s.enableSearch {
		searchIdx, err := multilogs.NewSearchIndex(s.repoPath)
		if err != nil {
			return nil, fmt.Errorf("sbot: failed to open search index: %w", err)
		}
		s.SearchIndex = searchIdx
		s.serveIndex("search", searchIdx)
		s.closers.AddCloser(searchIdx)
	}

	// groups re-indexing
	members := multilogs.NewMembershipIndex(
		log.With(s.info, "unit", "private-groups"),
		s.indexStore,
		s.KeyPair.ID(),
		s.Groups,
		combIdx,
	)
	s.closers.AddCloser(members)

	addMemberIdxAddr := multilog.Addr("string:group/add-member")
	addMemberSeqs, err := groupsHelperMlog.Get(addMemberIdxAddr)
	if err != nil {
		return nil, fmt.Errorf("sbot: failed to open sublog for add-member messages: %w", err)
	}
	justAddMemberMsgs := mutil.Indirect(s.ReceiveLog, addMemberSeqs)

	s.serveIndexFrom("group-members", members, justAddMemberMsgs)

	/* TODO: fix deadlock in index update locking
	if _, ok := s.simpleIndex["content-delete-requests"]; !ok {
		var dcrTrigger dropContentTrigger
		dcrTrigger.logger = log.With(s.info, "module", "dcrTrigger")
		dcrTrigger.root = s.ReceiveLog
		dcrTrigger.feeds = uf
		dcrTrigger.nuller = s
		err = MountSimpleIndex("content-delete-requests", dcrTrigger.MakeSimpleIndex)(s)
		if err != nil {
			return nil, errors.Wrap(err, "sbot: failed to open load default DCR index")
		}
	}
	*/

	// contact/follow graph — uses shared badger to avoid bbolt contention
	// with tangles/mentions multilogs that also use s.boltDB
	graphStore := graph.NewBadgerGraphStore(s.indexStore)
	gb := graph.NewBuilder(log.With(s.info, "module", "graph"), graphStore, s.signHMACsecret)
	contactsIdx := gb.OpenContactsIndex()

	// create data source for contacts
	contactLog, err := s.ByType.Get(multilog.Addr("string:contact"))
	if err != nil {
		return nil, fmt.Errorf("sbot: failed to open message contact sublog: %w", err)
	}
	justContacts := mutil.Indirect(s.ReceiveLog, contactLog)

	// fill the index
	s.serveIndexFrom("contacts", contactsIdx, justContacts)
	s.GraphBuilder = gb

	// abouts

	// create data source for abouts
	aboutSeqs, err := s.ByType.Get(multilog.Addr("string:about"))
	if err != nil {
		return nil, fmt.Errorf("sbot: failed to open message about sublog: %w", err)
	}
	aboutsOnly := mutil.Indirect(s.ReceiveLog, aboutSeqs)

	var namesPlug names.Plugin
	aboutIdx := namesPlug.OpenSharedIndex(s.indexStore)
	s.serveIndexFrom("abouts", aboutIdx, aboutsOnly)

	// need to close s.indexStore and boltDB _after_ the all the indexes closed and flushed
	s.closers.AddCloser(s.indexStore)
	s.closers.AddCloser(s.boltDB)

	// which feeds to replicate (only needed when networking is enabled)
	if !s.disableNetwork {
		if s.Replicator == nil {
			s.Replicator, err = s.newGraphReplicator()
			if err != nil {
				return nil, err
			}
		}

		// load our network frontier
		ownFrontier, err := s.ebtState.Inspect(s.KeyPair.ID())
		if err != nil {
			return nil, err
		}

		// this peer has no ebt state yet
		if len(ownFrontier) == 0 {
			// use the replication lister and determine the stored feeds lenghts
			lister := s.Replicator.Lister().ReplicationList()

			feeds, err := lister.List()
			if err != nil {
				return nil, fmt.Errorf("ebt init state: failed to get userlist: %w", err)
			}

			for i, feed := range feeds {
				seq, err := s.CurrentSequence(feed)
				if err != nil {
					return nil, fmt.Errorf("failed to get sequence for entry %d: %w", i, err)
				}
				ownFrontier[feed.String()] = seq
			}

			// also update our own
			ownFrontier[s.KeyPair.ID().String()], err = s.CurrentSequence(s.KeyPair.ID())
			if err != nil {
				return nil, fmt.Errorf("failed to get our sequence: %w", err)
			}

			_, err = s.ebtState.Update(s.KeyPair.ID(), ownFrontier)
			if err != nil {
				return nil, err
			}
		}
	}

	s.MetaFeeds = disabledMetaFeeds{}
	if s.enableMetafeeds {
		// a user might want to be able to read/replicate metafeeds without using bendybutt themselves
		if s.KeyPair.ID().Algo() == refs.RefAlgoFeedBendyButt {
			s.IndexFeeds, err = newIndexFeedManager(storageRepo.GetPath("indexfeeds"))
			if err != nil {
				return nil, fmt.Errorf("failed to initialize index feed manager: %w", err)
			}

			s.MetaFeeds, err = newMetaFeedService(s.ReceiveLog.(*multimsg.WrappedLog), s.IndexFeeds, s.Users, keysStore, s.KeyPair, s.signHMACsecret)
			if err != nil {
				return nil, fmt.Errorf("failed to initialize metafeed service: %w", err)
			}
		}

		// setup indexing

		// 1) all metafeed/* messages in bendybutt format
		justMetafeedMessages := repo.NewFilteredLog(s.ReceiveLog, graph.IsMetafeedMessage)

		mfIdx := gb.OpenMetafeedsIndex()
		s.serveIndexFrom("metafeed", mfIdx, justMetafeedMessages)

		// 2) metafeed/announce on normal format
		byTypeAnnouncementSeqs, err := s.ByType.Get(multilog.Addr("string:metafeed/announce"))
		if err != nil {
			return nil, fmt.Errorf("sbot: failed to open by type 'metafeed/announce' sublog: %w", err)
		}

		// convert sequences only to their actual messages using mutil.Indirect
		byTypeAnnouncements := mutil.Indirect(s.ReceiveLog, byTypeAnnouncementSeqs)

		announcementIdx := gb.OpenAnnouncementIndex()
		s.serveIndexFrom("metafeed announcements", announcementIdx, byTypeAnnouncements)
	}

	// from here on just network related stuff
	if s.disableNetwork {
		return s, nil
	}

	var inviteService *legacyinvites.Service

	// muxrpc handler creation and authoratization decider
	mkHandler := func(conn net.Conn) (muxrpc.Handler, error) {
		// bypassing badger-close bug to go through with an accept (or not) before closing the bot
		s.closedMu.Lock()
		defer s.closedMu.Unlock()

		remote, err := ssb.GetFeedRefFromAddr(conn.RemoteAddr())
		if err != nil {
			return nil, fmt.Errorf("sbot: expected an address containing an shs-bs addr: %w", err)
		}

		// TODO: we still can't see the feed format type from this

		if s.KeyPair.ID().PubKey().Equal(remote.PubKey()) {
			return s.master.MakeHandler(conn)
		}

		if inviteService != nil {
			err := inviteService.Authorize(remote)
			if err == nil {
				return inviteService.GuestHandler(), nil
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

	// publish
	authorLog, err := s.Users.Get(storedrefs.Feed(s.KeyPair.ID()))
	if err != nil {
		return nil, fmt.Errorf("failed to open user private index: %w", err)
	}
	s.master.Register(publish.NewPlug(log.With(s.info, "unit", "publish"), s.PublishLog, s.Groups, authorLog))

	// private
	// TODO: box2
	userPrivs, err := s.Private.Get(multilog.Addr("box1:") + storedrefs.Feed(s.KeyPair.ID()))
	if err != nil {
		return nil, fmt.Errorf("failed to open user private index: %w", err)
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
	blobs := blobs.New(log.With(s.info, "unit", "blobs"), s.KeyPair.ID(), s.BlobStore, wm)
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
		return nil, err
	}

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
			sm,
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
		s.ReceiveLog, s,
		s.GraphBuilder,
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
		h:    manifestBlob,
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

	// tcp+shs
	opts := network.Options{
		Logger:              s.info,
		Dialer:              s.dialer,
		ListenAddr:          s.listenAddr,
		AdvertsSend:         s.enableAdverts,
		AdvertsConnectTo:    s.enableDiscovery,
		KeyPair:             s.KeyPair,
		AppKey:              s.appKey[:],
		MakeHandler:         mkHandler,
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
		return nil, fmt.Errorf("failed to create network node: %w", err)
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

	inviteService, err = legacyinvites.New(
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
		return nil, fmt.Errorf("sbot: failed to open legacy invites plugin: %w", err)
	}
	s.master.Register(inviteService.MasterPlugin())

	// TODO: should be gossip.connect but conflicts with our namespace assumption
	s.master.Register(conn.NewPlug(log.With(s.info, "unit", "conn"), networkNode, s))
	s.master.Register(status.New(s))

	s.public.Register(networkNode.TunnelPlugin())
	s.Network = networkNode

	return s, nil
}

// ReindexAll forces a full re-index of all sublogs from the rxlog.
// This is a non-destructive repair that clears all indexes and rebuilds them.
func (s *Sbot) ReindexAll() error {
	if s.combIdx == nil {
		return fmt.Errorf("sbot: combined index not initialized")
	}

	// Force re-index by triggering VerifyConsistency's rewind path.
	// We need to clear sublogs and set state to -1, then re-index.
	// The simplest way: clear all user sublogs, reset state, call Index.

	// Clear user sublogs
	feeds, err := s.Users.List()
	if err != nil {
		return fmt.Errorf("reindex: failed to list feeds: %w", err)
	}
	for _, addr := range feeds {
		s.Users.Delete(addr)
	}

	// Clear other sublogs
	if addrs, err := s.ByType.List(); err == nil {
		for _, addr := range addrs {
			s.ByType.Delete(addr)
		}
	}
	if addrs, err := s.Tangles.List(); err == nil {
		for _, addr := range addrs {
			s.Tangles.Delete(addr)
		}
	}
	if addrs, err := s.Private.List(); err == nil {
		for _, addr := range addrs {
			s.Private.Delete(addr)
		}
	}

	// Reset the combined index state and re-index
	s.combIdx.ResetState()
	return s.combIdx.Index(s.ReceiveLog)
}

// Close closes the bot by stopping network connections and closing the internal databases
func (s *Sbot) Close() error {
	s.closedMu.Lock()
	defer s.closedMu.Unlock()

	if s.closed {
		return s.closeErr
	}

	closeEvt := log.With(s.info, "event", "sbot closing")
	s.closed = true

	// Cancel the root context first so that all goroutines watching ctx.Done()
	// (live queries, debounce loops, progress tickers) begin winding down.
	s.Shutdown()

	// Cancel all active replication streams before closing the network.
	// This ensures feed subscriptions and live sinks are torn down cleanly
	// rather than failing with broken-pipe errors during network close.
	if s.ebtHandler != nil {
		s.ebtHandler.Close()
		level.Debug(closeEvt).Log("msg", "ebt sessions closed")
	}
	if s.feedManager != nil {
		s.feedManager.Close()
		level.Debug(closeEvt).Log("msg", "feed manager closed")
	}

	if s.Network != nil {
		if err := s.Network.Close(); err != nil {
			s.closeErr = fmt.Errorf("sbot: failed to close own network node: %w", err)
		}
		s.Network.GetConnTracker().CloseAll()
		level.Debug(closeEvt).Log("msg", "connections closed")
	}

	if err := s.idxDone.Wait(); err != nil {
		if s.closeErr == nil {
			s.closeErr = fmt.Errorf("sbot: index group shutdown failed: %w", err)
		}
		level.Warn(closeEvt).Log("msg", "index group had errors", "err", err)
	}
	level.Debug(closeEvt).Log("msg", "waited for indexes to close")

	// Flush all roaring bitmap data and persist the CombinedIndex state BEFORE
	// closing the multilogs. This ensures the state file matches the flushed
	// bitmap data on disk, preventing the state-ahead-of-data issue that causes
	// feeds to appear corrupted on the next startup.
	if s.combIdx != nil {
		if err := s.combIdx.FlushAndSave(); err != nil {
			level.Warn(closeEvt).Log("msg", "combined index flush failed", "err", err)
		}
	}

	if err := s.closers.Close(); err != nil {
		if s.closeErr == nil {
			s.closeErr = err
		}
	}

	level.Info(closeEvt).Log("msg", "closers closed")
	return s.closeErr
}

type selfChecker struct {
	me refs.FeedRef
}

func (sc selfChecker) Authorize(remote refs.FeedRef) error {
	if sc.me.Equal(remote) {
		return nil
	}
	return fmt.Errorf("not authorized")
}
