// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package main

import (
	"encoding/json"
	"flag"
	"os"
	"os/user"
	"path/filepath"

	"go.mindeco.de/log/level"
)

var (
	// flags
	flagCleanup     bool
	flagReindex     bool
	flagWipeIndexes bool
	flagFSCK        string
	flagRepair      bool
	flagFatBot      bool
	flagHops        uint
	flagEnAdv       bool
	flagEnDiscov    bool
	flagPromisc     bool
	flagNumPeer     uint
	flagNumRepl     uint

	flagEnableEBT    bool
	flagEnableSearch bool
	flagEnableOTel   bool

	flagDisableUNIXSock bool

	repoDir     string
	listenAddr  string
	wsLisAddr   string
	wsTLSCert   string
	wsTLSKey    string
	debugAddr   string
	debugLogDir string
	configPath  string

	// juicy bits
	appKey  string
	hmacSec string
)

// Version and Build are set by ldflags
var (
	Version = "snapshot"
	Build   = ""

	flagPrintVersion bool
)

const DEFAULT_GO_SSB_DIR string = ".ssb-go"

func initFlags() {
	u, err := user.Current()
	checkFatal(err)

	flag.UintVar(&flagNumPeer, "numPeer", 5, "how many feeds can be replicated with one peer connection using legacy gossip replication (shouldn't be higher than numRepl)")
	flag.UintVar(&flagNumRepl, "numRepl", 10, "how many feeds can be replicated concurrently using legacy gossip replication")
	flag.UintVar(&flagHops, "hops", 1, "how many hops to fetch (1: friends, 2:friends of friends)")
	flag.BoolVar(&flagPromisc, "promisc", false, "bypass graph auth and fetch remote's feed")

	flag.StringVar(&appKey, "shscap", "1KHLiKZvAvjbY1ziZEHMXawbCEIM6qwjCDm3VYRan/s=", "secret-handshake app-key (or capability)")
	flag.StringVar(&hmacSec, "hmac", "", "if set, sign with hmac hash of msg, instead of plain message object, using this key")

	flag.StringVar(&listenAddr, "lis", ":8008", "address to listen on")
	flag.BoolVar(&flagEnAdv, "localadv", false, "enable sending local UDP brodcasts")
	flag.BoolVar(&flagEnDiscov, "localdiscov", false, "enable connecting to incomming UDP brodcasts")

	flag.StringVar(&wsLisAddr, "wslis", ":8989", "address to listen on for ssb-ws connections")
	flag.StringVar(&wsTLSCert, "wstlscert", "", "tls certificate file for ssb-ws connections")
	flag.StringVar(&wsTLSKey, "wstlskey", "", "tls key file for ssb-ws connections")

	flag.BoolVar(&flagEnableEBT, "enable-ebt", false, "enable syncing by using epidemic-broadcast-trees (new code, test with caution)")
	flag.BoolVar(&flagEnableSearch, "enable-search", false, "enable full-text search indexing using Bleve")
	flag.BoolVar(&flagEnableOTel, "enable-otel", false, "enable OpenTelemetry tracing (configure endpoint via OTEL_EXPORTER_OTLP_ENDPOINT)")

	flag.BoolVar(&flagDisableUNIXSock, "nounixsock", false, "disable the UNIX socket RPC interface")

	flag.StringVar(&repoDir, "repo", filepath.Join(u.HomeDir, DEFAULT_GO_SSB_DIR), "where to put the log and indexes")

	flag.StringVar(&debugAddr, "debuglis", "localhost:6078", "listen addr for metrics and pprof HTTP server")
	flag.StringVar(&debugLogDir, "debugdir", "", "where to write debug output to")

	flag.StringVar(&configPath, "config", filepath.Join(u.HomeDir, DEFAULT_GO_SSB_DIR), "path to config file; if filename is omitted from config path config.toml is used")

	flag.BoolVar(&flagReindex, "reindex", false, "if set, sbot exits after having its indicies updated")
	flag.BoolVar(&flagWipeIndexes, "wipe-indexes", false, "if set with -reindex, removes all index directories before rebuilding")

	flag.BoolVar(&flagCleanup, "cleanup", false, "remove blocked feeds")

	flag.StringVar(&flagFSCK, "fsck", "", "run a filesystem check on the repo (possible values: length, sequences)")
	flag.BoolVar(&flagRepair, "repair", false, "run repo healing if fsck fails")

	flag.BoolVar(&flagPrintVersion, "version", false, "print version number and build date")

	flag.Parse()
}

func applyConfigValues() {
	/*
	 It's config & environment variable reading time! We read the config and/or any set environment variables first.
	 Then, for each flag that has NOT been set and which corresponds to a config/env value, we set the flag variable's
	 value to the value's found in conf / env variable.

	 The hierarchy goes as follows:
	 * flag set values trump environment variables
	 * environment variables trumps config values
	 * set config values trump default flag values
	 * default flag values are the final fallback, if the corresponding config value or environment variable has not been
	   set
	*/
	// returns true if the named flag was passed to go-sbot on startup
	isFlagPassed := func(name string) bool {
		found := false
		flag.Visit(func(f *flag.Flag) {
			if f.Name == name {
				found = true
			}
		})
		return found
	}

	/* order of looking for a config file:
	* 1. $SSB_CONFIG_FILE or --config passed
	* 2. --repo is passed (=> used as configdir)
	* 3. fallback to default location at ~/.ssb-go/config.toml
	 */
	if isFlagPassed("repo") {
		configPath = repoDir
	}
	if err := os.Mkdir(configPath, 0700); err != nil {
		if !os.IsExist(err) {
			panic(err)
		}
	}
	if filepath.Ext(configPath) != ".toml" {
		configPath = filepath.Join(configPath, "config.toml")
	}
	if val := os.Getenv("SSB_CONFIG_FILE"); val != "" {
		configPath = val
	}
	configDir := filepath.Dir(configPath)
	config, exists := readConfigAndEnv(configPath)

	if !exists {
		err := os.WriteFile(configPath, []byte(defaultConfig), 0644)
		if err != nil {
			panic(err)
		}
		level.Info(log).Log("event", "write config.toml", "msg", "default config has been written", "path", configPath)
	}

	bconfig, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		panic(err)
	}
	runningConfigPath := filepath.Join(configDir, "running-config.json")
	err = os.WriteFile(runningConfigPath, bconfig, 0644)
	if err != nil {
		panic(err)
	} else {
		level.Info(log).Log("event", "write running-config.json", "msg", "active config and env vars have been persisted", "path", runningConfigPath)
	}
	// Returns true if the config has a value for flagname set, and the flag itself isn't passed on invocation
	UseConfigValue := func(flagname string) bool {
		return config.Has(flagname) && !isFlagPassed(flagname)
	}

	if UseConfigValue("hops") {
		flagHops = config.Hops
	}
	if UseConfigValue("numPeer") {
		flagNumPeer = config.NumPeer
	}
	if UseConfigValue("numRepl") {
		flagNumRepl = config.NumRepl
	}
	if UseConfigValue("promisc") {
		flagPromisc = (bool)(config.EnableFirewall)
	}
	if UseConfigValue("shscap") {
		appKey = config.ShsCap
	}
	if UseConfigValue("repo") {
		repoDir = config.Repo
	}
	if UseConfigValue("lis") {
		listenAddr = config.MuxRPCAddress
	}
	if UseConfigValue("localadv") {
		flagEnAdv = (bool)(config.EnableAdvertiseUDP)
	}
	if UseConfigValue("localdiscov") {
		flagEnDiscov = (bool)(config.EnableDiscoveryUDP)
	}
	if UseConfigValue("wslis") {
		wsLisAddr = config.WebsocketAddress
	}
	if UseConfigValue("wstlscert") {
		wsTLSCert = config.WebsocketTLSCert
	}
	if UseConfigValue("wstlskey") {
		wsTLSKey = config.WebsocketTLSKey
	}
	if UseConfigValue("enable-ebt") {
		flagEnableEBT = (bool)(config.EnableEBT)
	}
	if UseConfigValue("enable-search") {
		flagEnableSearch = (bool)(config.EnableSearch)
	}
	if UseConfigValue("enable-otel") {
		flagEnableOTel = (bool)(config.EnableOTel)
	}
	if UseConfigValue("nounixsock") {
		flagDisableUNIXSock = (bool)(config.NoUnixSocket)
	}
	if UseConfigValue("hmac") {
		hmacSec = config.Hmac
	}
	if UseConfigValue("debugdir") {
		debugLogDir = config.DebugDir
	}
	if UseConfigValue("debuglis") {
		debugAddr = config.MetricsAddress
	}
	if UseConfigValue("repair") {
		flagRepair = (bool)(config.RepairFSBeforeStart)
	}
}
