// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

// go-sbot hosts the database and p2p server for replication.
// It supplies various flags to contol options.
// See 'go-sbot -h' for a list and their usage.
package main

import (
	"context"
	_ "embed"
	"encoding/base64"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	// debug
	_ "net/http/pprof"

	"github.com/ssbc/go-muxrpc/v3/debug"
	"go.mindeco.de/log/level"
	"go.mindeco.de/logging"

	"github.com/ssbc/go-ssb"
	"github.com/ssbc/go-ssb/internal/ctxutils"
	"github.com/ssbc/go-ssb/internal/muxrpctracing"
	"github.com/ssbc/go-ssb/internal/storedrefs"
	"github.com/ssbc/go-ssb/internal/testutils"
	"github.com/ssbc/go-ssb/multilogs"
	mksbot "github.com/ssbc/go-ssb/sbot"
)

var (
	// helper
	log        logging.Interface
	checkFatal = logging.CheckFatal

	//go:embed default-config.toml
	defaultConfig string
)

func checkAndLog(err error) {
	if err != nil {
		level.Error(log).Log("event", "fatal error", "err", err)
		if err := logging.LogPanicWithStack(log, "checkAndLog", err); err != nil {
			panic(err)
		}
	}
}

func runSbot() error {
	initFlags()

	//log = logging.Logger("sbot")

	// 2022-02-22: cryptix wants to change away from NewRelativeTimeLogger because for long running code it doesn't make
	// sense; it's mostly a development convienence (which is why it's often used in the tests)
	log = testutils.NewRelativeTimeLogger(nil)

	if flagPrintVersion {
		log.Log("version", Version, "build", Build)
		return nil
	}

	ctx, cancel := ctxutils.WithError(context.Background(), ssb.ErrShuttingDown)
	defer func() {
		cancel()
		if r := recover(); r != nil {
			logging.LogPanicWithStack(log, "main-panic", r)
		}
	}()

	// try to read config && environment variables, and apply any set values on variables that
	// have not been explicitly configured using flags on startup
	applyConfigValues()

	// add a log on is used by the sbot to aid ambient debugging for operators
	absRepo, err := filepath.Abs(repoDir)
	if err == nil {
		level.Info(log).Log("event", "set repo", "path", absRepo)
	}

	if debugLogDir != "" {
		logDir := filepath.Join(repoDir, debugLogDir)
		os.MkdirAll(logDir, 0700) // nearly everything is a log here so..
		logFileName := fmt.Sprintf("%s-%s.log",
			filepath.Base(os.Args[0]),
			time.Now().Format("2006-01-02_15-04"))
		logFile, err := os.Create(filepath.Join(logDir, logFileName))
		if err != nil {
			panic(err) // logging not ready yet...
		}
		logging.SetupLogging(logFile)
	} else {
		//logging.SetupLogging(os.Stderr)
	}

	ak, err := base64.StdEncoding.DecodeString(appKey)
	if err != nil {
		return fmt.Errorf("invalid application key/shs-cap: %w", err)
	}

	if flagEnableOTel {
		shutdownTracing, err := setupOTelTracing(ctx)
		if err != nil {
			return fmt.Errorf("failed to setup OpenTelemetry tracing: %w", err)
		}
		defer shutdownTracing()
		level.Info(log).Log("event", "otel-tracing", "msg", "OpenTelemetry tracing enabled")
	}

	startDebug()
	opts := []mksbot.Option{
		mksbot.WithHops(flagHops),
		mksbot.WithPromisc(flagPromisc),
		mksbot.WithInfo(log),
		mksbot.WithAppKey(ak),
		mksbot.WithRepoPath(repoDir),
		mksbot.WithListenAddr(listenAddr),
		mksbot.EnableAdvertismentBroadcasts(flagEnAdv),
		mksbot.EnableAdvertismentDialing(flagEnDiscov),
		mksbot.WithWebsocketAddress(wsLisAddr),
		mksbot.WithWebsocketTLSCert(wsTLSCert),
		mksbot.WithWebsocketTLSKey(wsTLSKey),
		// enabling this might consume a lot of resources
		mksbot.DisableLegacyLiveReplication(true),
		// new code, test with caution
		mksbot.DisableEBT(!flagEnableEBT),
		mksbot.WithNumberOfConcurrentReplicationsPerPeer(flagNumPeer),
		mksbot.WithNumberOfConcurrentReplications(flagNumRepl),
	}

	if flagEnableSearch {
		opts = append(opts, mksbot.EnableSearch())
	}

	if !flagDisableUNIXSock {
		opts = append(opts, mksbot.LateOption(mksbot.WithUNIXSocket()))
	}

	if debugLogDir != "" {
		opts = append(opts, mksbot.WithPostSecureConnWrapper(func(conn net.Conn) (net.Conn, error) {
			parts := strings.Split(conn.RemoteAddr().String(), "|")

			if len(parts) != 2 {
				return conn, nil
			}

			muxrpcDumpDir := filepath.Join(
				repoDir,
				debugLogDir,
				parts[1], // key first
				parts[0],
			)

			return debug.WrapDump(muxrpcDumpDir, conn)
		}))
	}

	if debugAddr != "" {
		opts = append(opts,
			mksbot.WithEventMetrics(SystemEvents, RepoStats, SystemSummary),
			mksbot.WithPreSecureConnWrapper(promCountConn()),
		)
	}

	if hmacSec != "" {
		hcbytes, err := base64.StdEncoding.DecodeString(hmacSec)
		if err != nil {
			return fmt.Errorf("invalid base64 string for HMAC signing secret: %w", err)
		}
		opts = append(opts, mksbot.WithHMACSigning(hcbytes))
	}

	if flagFSCK != "" {
		opts = append(opts, mksbot.DisableNetworkNode(), mksbot.SkipConsistencyCheck())
	}

	if flagWipeIndexes {
		if !flagReindex {
			return fmt.Errorf("-wipe-indexes requires -reindex")
		}
		level.Warn(log).Log("event", "wiping indexes", "repo", repoDir)
		for _, dir := range []string{"sublogs", "indexes"} {
			p := filepath.Join(repoDir, dir)
			if err := os.RemoveAll(p); err != nil {
				return fmt.Errorf("failed to wipe %s: %w", dir, err)
			}
			level.Info(log).Log("event", "wiped", "dir", p)
		}
	}

	sbot, err := mksbot.New(opts...)
	if err != nil {
		return fmt.Errorf("failed to instantiate ssb server: %w", err)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-c
		level.Warn(log).Log("event", "killed", "msg", "received signal, shutting down", "signal", sig.String())
		cancel()
		sbot.Shutdown()

		err := sbot.Close()
		checkAndLog(err)

		os.Exit(0)
	}()
	logging.SetCloseChan(c)

	id := sbot.KeyPair.ID()
	uf, ok := sbot.GetMultiLog(multilogs.IndexNameFeeds)
	if !ok {
		checkAndLog(fmt.Errorf("missing userFeeds"))
		return nil
	}

	level.Info(log).Log("event", "waiting for indexes to catch up")
	sbot.WaitUntilIndexesAreSynced()

	var fsckMode = mksbot.FSCKModeLength
	var exitAfterFSCK = false
	if flagFSCK != "" {
		switch flagFSCK {
		case "sequences":
			fsckMode = mksbot.FSCKModeSequences
		case "length":
			fsckMode = mksbot.FSCKModeLength
		default:
			return fmt.Errorf("unknown fsck mode: %q", flagFSCK)
		}
		exitAfterFSCK = true
	}

	err = sbot.FSCK(mksbot.FSCKWithFeedIndex(uf), mksbot.FSCKWithMode(fsckMode))
	if err != nil {
		if !flagRepair && exitAfterFSCK {
			// Explicit -fsck without -repair: just report and exit
			return fmt.Errorf("fsck returned: %w", err)
		}

		report, ok := err.(mksbot.ErrConsistencyProblems)
		if !ok {
			return fmt.Errorf("fsck returned unexpected error type %T: %w", err, err)
		}

		// Log all broken feeds
		for _, e := range report.Errors {
			level.Warn(log).Log("fsck", "broken-feed",
				"feed", e.Ref.ShortSigil(),
				"stored-seq", e.Stored,
				"logical-seq", e.Logical)
		}

		if flagRepair {
			// Explicit -repair: use HealRepo (scans rxlog, nulls bad entries, rebuilds sublogs)
			err = sbot.HealRepo(report)
			if err != nil {
				return fmt.Errorf("fsck: repair failed: %w", err)
			}

			// Verify the repair was successful by re-running fsck
			level.Info(log).Log("fsck", "verifying repair")
			verifyErr := sbot.FSCK(mksbot.FSCKWithFeedIndex(uf), mksbot.FSCKWithMode(fsckMode))
			if verifyErr != nil {
				level.Error(log).Log("fsck", "repair-incomplete",
					"err", verifyErr,
					"msg", "repair did not produce a clean state")
				sbot.Shutdown()
				err := sbot.Close()
				if err != nil {
					return fmt.Errorf("fsck: failed to stop sbot: %w", err)
				}
				return fmt.Errorf("fsck: repair did not produce a clean state: %w", verifyErr)
			}

			level.Info(log).Log("fsck", "repair-complete",
				"feeds-repaired", len(report.Errors),
				"msg", "verification passed")

			sbot.Shutdown()
			err = sbot.Close()
			if err != nil {
				return fmt.Errorf("fsck: failed to stop sbot after repair: %w", err)
			}
			return nil
		}

		// Normal startup: auto-repair index issues by re-indexing.
		// This is a non-destructive repair — it doesn't null any rxlog entries,
		// it just rebuilds the sublogs from the rxlog data.
		level.Warn(log).Log("fsck", "auto-repair",
			"broken-feeds", len(report.Errors),
			"msg", "re-indexing to repair index inconsistencies")

		err = sbot.ReindexAll()
		if err != nil {
			return fmt.Errorf("fsck: auto-repair re-index failed: %w", err)
		}

		// Re-run fsck to verify
		verifyErr := sbot.FSCK(mksbot.FSCKWithFeedIndex(uf), mksbot.FSCKWithMode(fsckMode))
		if verifyErr != nil {
			return fmt.Errorf("fsck: auto-repair did not produce a clean state: %w", verifyErr)
		}
		level.Info(log).Log("fsck", "auto-repair-complete",
			"feeds-repaired", len(report.Errors))
	}
	if exitAfterFSCK {
		level.Info(log).Log("fsck", "completed", "mode", fsckMode)
		sbot.Shutdown()
		err := sbot.Close()
		checkAndLog(err)
		return nil
	}
	SystemEvents.With("event", "openedRepo").Add(1)
	// establish message anf feed numbers in the repo

	feeds, err := uf.List()
	if err != nil {
		return fmt.Errorf("user feed: %w", err)
	}
	RepoStats.With("part", "feeds").Set(float64(len(feeds)))

	msgCount := sbot.ReceiveLog.Seq() + 1
	RepoStats.With("part", "msgs").Set(float64(msgCount))

	level.Info(log).Log("event", "repo open", "feeds", len(feeds), "msgs", msgCount)

	if flagReindex {
		level.Warn(log).Log("mode", "reindexing")
		if fsckMode != mksbot.FSCKModeSequences {
			err = sbot.FSCK(mksbot.FSCKWithMode(mksbot.FSCKModeSequences))
			if err != nil {
				return err
			}
		}
		level.Warn(log).Log("mode", "fsck done")
		err = sbot.Close()
		checkAndLog(err)
		return nil
	}

	// removes blocked feeds
	if flagCleanup {
		level.Warn(log).Log("mode", "cleanup")

		tg, err := sbot.GraphBuilder.Build()
		if err != nil {
			return fmt.Errorf("failed to build graph during cleanup: %w", err)
		}

		botRef := sbot.KeyPair.ID()
		lst, err := tg.BlockedList(botRef).List()
		if err != nil {
			return fmt.Errorf("cleanup: failed to get blocked list: %w", err)
		}

		for _, blocked := range lst {
			isStored, err := uf.Has(storedrefs.Feed(blocked))
			if err != nil {
				return fmt.Errorf("blocked lookup in multilog: %w", err)
			}

			if isStored {
				level.Info(log).Log("event", "nulled feed", "ref", blocked.String())
				err = sbot.NullFeed(blocked)
				if err != nil {
					return fmt.Errorf("failed to null blocked feed %s: %w", blocked.String(), err)
				}
			}
		}

		sbot.Shutdown()
		return sbot.Close()
	}

	level.Info(log).Log("event", "serving", "ID", id.String(), "addr", listenAddr, "version", Version, "build", Build)
	for {
		// Note: This is where the serving starts ;)
		err = sbot.Network.Serve(ctx, muxrpctracing.NewHandlerWrapper(log))
		if err != nil {
			level.Warn(log).Log("event", "sbot node.Serve returned", "err", err)
		}
		SystemEvents.With("event", "nodeServ exited").Add(1)
		time.Sleep(1 * time.Second)
		select {
		case <-ctx.Done():
			err := sbot.Close()
			return err
		default:
		}
	}
}

func main() {
	if err := runSbot(); err != nil {
		fmt.Fprintf(os.Stderr, "go-sbot: %s\n", err)
		os.Exit(1)
	}
}
