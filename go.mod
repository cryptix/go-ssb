// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

module github.com/ssbc/go-ssb

go 1.25.0

require (
	filippo.io/edwards25519 v1.2.0
	github.com/RoaringBitmap/roaring v1.9.4
	github.com/VividCortex/gohistogram v1.0.0
	github.com/blevesearch/bleve/v2 v2.5.7
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
	github.com/maxbrunsfeld/counterfeiter/v6 v6.12.1
	github.com/prometheus/client_golang v1.23.2
	github.com/rs/cors v1.11.1
	github.com/shurcooL/go-goon v1.0.0
	github.com/ssbc/go-gabbygrove v0.2.2
	github.com/ssbc/go-metafeed v1.1.3
	github.com/ssbc/go-muxrpc/v3 v3.0.0-20260307231653-358199835fc8
	github.com/ssbc/go-netwrap v0.1.5-0.20221019160355-cd323bb2e29d
	github.com/ssbc/go-secretstream v1.2.11-0.20221019175226-fa042d4912fe
	github.com/ssbc/go-ssb-multiserver v0.1.5-0.20221019203850-917ae0e23d57
	github.com/ssbc/go-ssb-refs v0.5.2
	github.com/ssbc/margaret/v2 v2.0.0-20260308112302-d31aa606b278
	github.com/stretchr/testify v1.11.1
	github.com/ugorji/go/codec v1.3.1
	github.com/urfave/cli/v2 v2.27.7
	github.com/zeebo/bencode v1.0.0
	go.cryptoscope.co/nocomment v0.0.0-20210520094614-fb744e81f810
	go.etcd.io/bbolt v1.4.3
	go.mindeco.de v1.12.0
	golang.org/x/crypto v0.49.0
	golang.org/x/sync v0.20.0
	golang.org/x/text v0.35.0
	gonum.org/v1/gonum v0.17.0
)

require (
	github.com/RoaringBitmap/roaring/v2 v2.15.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.24.4 // indirect
	github.com/blevesearch/bleve_index_api v1.3.2 // indirect
	github.com/blevesearch/geo v0.2.5 // indirect
	github.com/blevesearch/go-faiss v1.0.27 // indirect
	github.com/blevesearch/go-porterstemmer v1.0.3 // indirect
	github.com/blevesearch/gtreap v0.1.1 // indirect
	github.com/blevesearch/mmap-go v1.2.0 // indirect
	github.com/blevesearch/scorch_segment_api/v2 v2.4.1 // indirect
	github.com/blevesearch/segment v0.9.1 // indirect
	github.com/blevesearch/snowballstem v0.9.0 // indirect
	github.com/blevesearch/upsidedown_store_api v1.0.2 // indirect
	github.com/blevesearch/vellum v1.2.0 // indirect
	github.com/blevesearch/zapx/v11 v11.4.3 // indirect
	github.com/blevesearch/zapx/v12 v12.4.3 // indirect
	github.com/blevesearch/zapx/v13 v13.4.3 // indirect
	github.com/blevesearch/zapx/v14 v14.4.3 // indirect
	github.com/blevesearch/zapx/v15 v15.4.3 // indirect
	github.com/blevesearch/zapx/v16 v16.3.1 // indirect
	github.com/cespare/xxhash v1.1.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.7 // indirect
	github.com/dgraph-io/ristretto v0.2.0 // indirect
	github.com/go-logfmt/logfmt v0.6.1 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/groupcache v0.0.0-20241129210726-2c02b8208cf8 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/golang/snappy v1.0.0 // indirect
	github.com/google/flatbuffers v25.12.19+incompatible // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/karrick/bufpool v1.2.0 // indirect
	github.com/karrick/gopool v1.2.2 // indirect
	github.com/klauspost/compress v1.18.4 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/matryer/is v1.3.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.67.5 // indirect
	github.com/prometheus/procfs v0.20.1 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/shurcooL/go v0.0.0-20230706063926-5fe729b41b3a // indirect
	github.com/xrash/smetrics v0.0.0-20250705151800-55b8f293f342 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.yaml.in/yaml/v2 v2.4.4 // indirect
	golang.org/x/mod v0.33.0 // indirect
	golang.org/x/net v0.52.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
	golang.org/x/tools v0.42.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
