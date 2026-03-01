// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package message

import (
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/ssbc/margaret/v2/multilog/roaring"
	roaringfs "github.com/ssbc/margaret/v2/multilog/roaring/fs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ssbc/go-ssb"
	refs "github.com/ssbc/go-ssb-refs"
	"github.com/ssbc/go-ssb/internal/multicloser"
	"github.com/ssbc/go-ssb/internal/storedrefs"
	"github.com/ssbc/go-ssb/message/legacy"
	"github.com/ssbc/go-ssb/message/multimsg"
	"github.com/ssbc/go-ssb/multilogs"
	"github.com/ssbc/go-ssb/repo"
)

func TestFormatsSimple(t *testing.T) {

	type testCase struct {
		// feed format
		ff refs.RefAlgo
	}
	var testCases = []testCase{
		{refs.RefAlgoFeedSSB1},
		{refs.RefAlgoFeedGabby},
		{refs.RefAlgoFeedBendyButt},
	}

	ts := newPublishtestSession(t)

	for _, tc := range testCases {
		t.Run(string(tc.ff), ts.makeFormatTest(tc.ff))
	}

	if err := ts.logCloser.Close(); err != nil {
		t.Error(err)
	}
}

type publishTestSession struct {
	rxLog *multimsg.WrappedLog

	logCloser io.Closer

	userLogs *roaring.MultiLog
}

func newPublishtestSession(t *testing.T) publishTestSession {
	r := require.New(t)
	rpath := filepath.Join("testrun", t.Name())
	os.RemoveAll(rpath)

	var mc multicloser.MultiCloser

	testRepo := repo.New(rpath)

	rxl, err := repo.OpenLog(testRepo)
	r.NoError(err, "failed to open receive log")
	mc.AddCloser(rxl)

	r.Equal(int64(-1), rxl.Seq(), "not empty")

	userLogs := roaringfs.NewMultiLog(testRepo.GetPath("testUsers"))
	mc.AddCloser(userLogs)

	return publishTestSession{
		rxLog: rxl,

		logCloser: &mc,

		userLogs: userLogs,
	}
}

func (ts publishTestSession) makeFormatTest(ff refs.RefAlgo) func(t *testing.T) {
	staticRand := rand.New(rand.NewSource(42))
	return func(t *testing.T) {
		r := require.New(t)
		a := assert.New(t)

		testAuthor, err := ssb.NewKeyPair(staticRand, ff)
		r.NoError(err)

		authorLog, err := ts.userLogs.Get(storedrefs.Feed(testAuthor.ID()))
		r.NoError(err)

		w, err := OpenPublishLog(ts.rxLog, ts.userLogs, testAuthor)
		r.NoError(err, "publish log didnt open")

		var tmsgs = []interface{}{
			map[string]interface{}{
				"type":  "about",
				"about": testAuthor.ID().String(),
				"name":  "test user",
			},
			map[string]interface{}{
				"type":      "contact",
				"contact":   "@p13zSAiOpguI9nsawkGijsnMfWmFd5rlUNpzekEE+vI=.ed25519",
				"following": true,
			},
			map[string]interface{}{
				"type": "text",
				"text": `# hello world!`,
			},
		}
		for i, msg := range tmsgs {
			mr, err := w.Publish(msg)
			r.NoError(err, "failed to pour test message %d", i)
			r.NotNil(mr)

			// Update user feeds index
			seq := ts.rxLog.Seq()
			mm, err := ts.rxLog.Get(seq)
			r.NoError(err)
			err = multilogs.UserFeedsUpdate(seq, mm, ts.userLogs)
			r.NoError(err)

			r.EqualValues(i, authorLog.Seq(), "failed to ")
		}

		r.EqualValues(2, authorLog.Seq(), "not empty %s", ff)

		for i := 0; i < len(tmsgs); i++ {
			rootSeqEntry, err := authorLog.Get(int64(i))
			r.NoError(err)
			rootSeq := int64(*rootSeqEntry)
			mm, err := ts.rxLog.Get(rootSeq)
			r.NoError(err)
			r.NotNil(mm.Message)
			storedMsg := mm.Message
			t.Logf("msg:%d\n%s", i, storedMsg.ValueContentJSON())
			a.NotNil(storedMsg.Key(), "msg:%d - key", i)

			// previous is correctly set
			if i != 0 {
				a.NotNil(storedMsg.Previous(), "msg:%d - previous", i)
				// get previous message
				prevEntry, err := authorLog.Get(int64(i - 1))
				r.NoError(err)
				prevSeq := int64(*prevEntry)
				prevMM, err := ts.rxLog.Get(prevSeq)
				r.NoError(err)
				r.NotNil(prevMM.Message)
				prevMsg := prevMM.Message

				a.True(prevMsg.Key().Equal(*storedMsg.Previous()), "msg:%d - wrong previous", i)
			} else {
				a.Nil(storedMsg.Previous(), "msg:%d - previous", i)
			}

			a.Equal(int64(i+1), storedMsg.Seq(), "msg:%d - has incorrect sequence")

			// verifies
			switch ff {
			case refs.RefAlgoFeedSSB1:
				msg, ok := mm.AsLegacy()
				r.True(ok)

				_, _, err = legacy.Verify(msg.Raw_, nil)
				r.NoError(err)

			case refs.RefAlgoFeedGabby:
				g, ok := mm.AsGabby()
				r.True(ok)
				a.True(g.Verify(nil), "gabby failed to validate msg:%d", i)

			case refs.RefAlgoFeedBendyButt:
				mf, ok := mm.AsMetaFeed()
				r.True(ok)
				a.True(mf.Verify(nil))
			default:
				r.FailNow("unhandled feed format", "format:%s", ff)
			}
		}
	}
}
