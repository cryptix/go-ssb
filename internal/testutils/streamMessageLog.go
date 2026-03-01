// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package testutils

import (
	"encoding/hex"
	"testing"

	margaret "github.com/ssbc/margaret/v2"
	"github.com/stretchr/testify/require"

	refs "github.com/ssbc/go-ssb-refs"
	"github.com/ssbc/go-ssb/message/multimsg"
)

func StreamLog(t *testing.T, l margaret.Log[*multimsg.MultiMessage]) {
	r := require.New(t)

	seq := l.Seq()
	i := int64(0)

	qry := l.Query()
	for _, mm := range qry.Iter() {
		var msg refs.Message = mm

		t.Logf("log seq: %d - %s:%d (%s)",
			i,
			msg.Author().ShortSigil(),
			msg.Seq(),
			msg.Key().ShortSigil())

		b := msg.ContentBytes()
		if n := len(b); n > 128 {
			t.Log("truncating", n, " to last 32 bytes")
			b = b[len(b)-32:]
		}
		t.Logf("\n%s", hex.Dump(b))

		i++
	}
	r.NoError(qry.Err())

	// margaret is 0-indexed
	seq += 1
	if seq != i {
		t.Errorf("seq differs from iterated count: %d vs %d", seq, i)
	}
}
