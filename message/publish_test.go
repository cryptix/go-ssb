// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package message

import (
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	roaringfs "github.com/ssbc/margaret/v2/multilog/roaring/fs"

	"github.com/ssbc/go-ssb"
	refs "github.com/ssbc/go-ssb-refs"
	"github.com/ssbc/go-ssb/multilogs"
	"github.com/ssbc/go-ssb/repo"
)

func TestSignMessages(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)

	rpath := filepath.Join("testrun", t.Name())
	os.RemoveAll(rpath)

	testRepo := repo.New(rpath)
	rl, err := repo.OpenLog(testRepo)
	t.Cleanup(func() {
		rl.Close()
	})

	r.NoError(err, "failed to open root log")
	r.EqualValues(-1, rl.Seq(), "not empty")

	userFeeds := roaringfs.NewMultiLog(testRepo.GetPath("testUsers"))
	t.Cleanup(func() {
		userFeeds.Close()
	})

	staticRand := rand.New(rand.NewSource(42))
	testAuthor, err := ssb.NewKeyPair(staticRand, refs.RefAlgoFeedSSB1)
	r.NoError(err)

	w, err := OpenPublishLog(rl, userFeeds, testAuthor)
	r.NoError(err)

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
		_, err := w.Publish(msg)
		r.NoError(err, "failed to pour test message %d", i)

		newSeq := rl.Seq()
		r.EqualValues(i, newSeq, "advanced")

		// Update user feeds index so the next publish can find the previous message
		mm, err := rl.Get(newSeq)
		r.NoError(err)
		err = multilogs.UserFeedsUpdate(newSeq, mm, userFeeds)
		r.NoError(err)
	}

	for i := 0; i < len(tmsgs); i++ {
		storedMM, err := rl.Get(int64(i))
		r.NoError(err)
		r.NotNil(storedMM.Message)
		storedMsg := storedMM.Message
		t.Logf("msg:%d\n%s", i, storedMsg.ContentBytes())
		a.NotNil(storedMsg.Key(), "msg:%d - key", i)
		if i != 0 {
			a.NotNil(storedMsg.Previous(), "msg:%d - previous", i)
		} else {
			a.Nil(storedMsg.Previous(), "msg:%d - expected nil previous", i)
		}
		a.NotNil(storedMsg.ContentBytes(), "msg:%d - raw", i)
		value := storedMsg.ValueContent()
		a.NotNil(value.Signature, "msg:%d - expected signature", i)
		a.NotNil(value.Sequence, "msg:%d - expected sequence number", i)
	}
}
