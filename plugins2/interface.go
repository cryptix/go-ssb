// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package plugins2

import (
	"github.com/ssbc/go-ssb"
	margaret "github.com/ssbc/margaret/v2"
	"github.com/ssbc/go-ssb/message/multimsg"
)

type AuthMode uint

/*
	we currently support two auth levels: master (same key-pair as the local node) and public (on the trust graph).

Both registers the plugin to both of them.
*/
const (
	AuthPublic AuthMode = iota
	AuthMaster
	AuthBoth
)

type NeedsRootLog interface {
	WantRootLog(rl margaret.Log[*multimsg.MultiMessage]) error
}

type NeedsMultiLog interface {
	WantMultiLog(ssb.MultiLogGetter) error
}
