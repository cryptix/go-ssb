// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

module github.com/ssbc/go-ssb

go 1.25

toolchain go1.25.0

require (
	filippo.io/edwards25519 v1.1.0
	github.com/RoaringBitmap/roaring v0.6.1
	github.com/VividCortex/gohistogram v1.0.0
	github.com/davecgh/go-spew v1.1.1
	github.com/dgraph-io/badger/v3 v3.2103.5
	github.com/dgraph-io/sroar v0.0.0-20220527172339-b92b7eaaf6e0
	github.com/dustin/go-humanize v1.0.1
	github.com/go-kit/kit v0.13.0
	github.com/gorilla/websocket v1.5.3
	github.com/hashicorp/go-multierror v1.1.1
	github.com/json-iterator/go v1.1.12
	github.com/keks/persist v0.0.0-20210520094901-9bdd97c1fad2
	github.com/keks/testops v0.1.0
	github.com/komkom/toml v0.1.2
	github.com/kylelemons/godebug v1.1.0
	github.com/libp2p/go-reuseport v0.4.0
	github.com/machinebox/progress v0.2.0
	github.com/maxbrunsfeld/counterfeiter/v6 v6.11.2
	github.com/prometheus/client_golang v1.20.5
	github.com/rs/cors v1.11.1
	github.com/shurcooL/go-goon v1.0.0
	github.com/ssbc/go-gabbygrove v0.2.2
	github.com/ssbc/go-luigi v0.3.7-0.20230119190114-bd28e676fa99
	github.com/ssbc/go-metafeed v1.1.3
	github.com/ssbc/go-muxrpc/v3 v3.0.0-00010101000000-000000000000
	github.com/ssbc/go-netwrap v0.1.5-0.20221019160355-cd323bb2e29d
	github.com/ssbc/go-secretstream v1.2.11-0.20221019175226-fa042d4912fe
	github.com/ssbc/go-ssb-multiserver v0.1.5-0.20221019203850-917ae0e23d57
	github.com/ssbc/go-ssb-refs v0.5.2
	github.com/ssbc/margaret/v2 v2.0.0-00010101000000-000000000000
	github.com/stretchr/testify v1.10.0
	github.com/ugorji/go/codec v1.2.12
	github.com/urfave/cli/v2 v2.27.5
	github.com/zeebo/bencode v1.0.0
	go.cryptoscope.co/nocomment v0.0.0-20210520094614-fb744e81f810
	go.mindeco.de v1.12.0
	golang.org/x/crypto v0.32.0
	golang.org/x/sync v0.10.0
	golang.org/x/text v0.21.0
	gonum.org/v1/gonum v0.15.1
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash v1.1.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.6 // indirect
	github.com/dgraph-io/ristretto v0.2.0 // indirect
	github.com/go-logfmt/logfmt v0.6.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/groupcache v0.0.0-20241129210726-2c02b8208cf8 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/flatbuffers v25.1.24+incompatible // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/karrick/bufpool v1.2.0 // indirect
	github.com/karrick/gopool v1.2.2 // indirect
	github.com/klauspost/compress v1.17.11 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/matryer/is v1.3.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/mschoch/smat v0.0.0-20160514031455-90eadee771ae // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.62.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/shurcooL/go v0.0.0-20230706063926-5fe729b41b3a // indirect
	github.com/willf/bitset v1.1.10 // indirect
	github.com/xrash/smetrics v0.0.0-20240521201337-686a1a2994c1 // indirect
	go.opencensus.io v0.24.0 // indirect
	golang.org/x/exp v0.0.0-20250128182459-e0ece0dbea4c // indirect
	golang.org/x/mod v0.22.0 // indirect
	golang.org/x/net v0.34.0 // indirect
	golang.org/x/sys v0.29.0 // indirect
	golang.org/x/tools v0.29.0 // indirect
	google.golang.org/protobuf v1.36.4 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/ssbc/margaret/v2 => ../margaret

replace github.com/ssbc/go-muxrpc/v3 => ../go-muxrpc
