// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package multilogs

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	margaret "github.com/ssbc/margaret/v2"
	"github.com/ssbc/margaret/v2/multilog/roaring"
	multifs "github.com/ssbc/margaret/v2/multilog/roaring/fs"
	"github.com/stretchr/testify/require"

	"github.com/ssbc/go-ssb"
	refs "github.com/ssbc/go-ssb-refs"
	"github.com/ssbc/go-ssb/internal/multicloser"
	"github.com/ssbc/go-ssb/internal/statematrix"
	"github.com/ssbc/go-ssb/message/multimsg"
	"github.com/ssbc/go-ssb/private"
	"github.com/ssbc/go-ssb/private/keys"
	"github.com/ssbc/go-ssb/repo"
)

func BenchmarkIndexFixturesCombined(b *testing.B) {
	r := require.New(b)

	testPath := filepath.Join("testrun", b.Name())

	fetchFixture := exec.Command("bash", "./integration_prep.bash", filepath.Join(testPath, "log"))
	out, err := fetchFixture.CombinedOutput()
	if err != nil {
		b.Log(string(out))
		r.NoError(err)
	}

	tr := repo.New(testPath)

	testLog, err := repo.OpenLog(tr)
	r.NoError(err, "case %s failed to open", b.Name())

	r.EqualValues(100000, testLog.Seq()+1, "testLog has wrong number of messages")

	b.ResetTimer()

	for n := 0; n < b.N; n++ {

		b.StopTimer()
		_, idx, closer := setupCombinedIndex(b, testLog, makeFsMlog)
		r.NoError(err)
		b.StartTimer()

		err = idx.Index(testLog)
		r.NoError(err)
		b.StopTimer()
		closer.Close()
		os.RemoveAll(filepath.Join(testPath, "combinedIndexes"))
	}

}

func setupCombinedIndex(t testing.TB, rxlog margaret.Log[*multimsg.MultiMessage], mkMlog makeMultilog) (*roaring.MultiLog, *CombinedIndex, *multicloser.MultiCloser) {
	r := require.New(t)
	testPath := filepath.Join("testrun", t.Name(), "combinedIndexes")
	testRepo := repo.New(testPath)

	keysDB, err := repo.OpenBadgerDB(testPath)
	r.NoError(err, "openIndex: failed to open keys database")

	ks := keys.NewStore(keysDB, []byte("keys"))

	var (
		tp testPublisher
		tg testGetter
	)

	tkp, err := ssb.NewKeyPair(nil, refs.RefAlgoFeedSSB1)
	r.NoError(err)

	sm, err := statematrix.New(
		testRepo.GetPath("ebt-state-matrix"),
		tkp.ID(),
	)
	r.NoError(err)

	var mc multicloser.MultiCloser

	tangles := mkMlog(t, testRepo, "tangles", &mc)

	gm := private.NewManager(tkp, tp, ks, rxlog, tg, tangles)

	user := mkMlog(t, testRepo, "user", &mc)
	priv := mkMlog(t, testRepo, "private", &mc)
	byType := mkMlog(t, testRepo, "byType", &mc)
	channels := mkMlog(t, testRepo, "channels", &mc)
	mentions := mkMlog(t, testRepo, "mentions", &mc)
	backlinks := mkMlog(t, testRepo, "backlinks", &mc)
	groupMembers := mkMlog(t, testRepo, "groupMembers", &mc)

	idx, err := NewCombinedIndex(testPath,
		gm,
		tkp.ID(),
		rxlog,

		user,
		priv,
		byType,
		tangles,
		channels,
		mentions,
		backlinks,
		groupMembers,

		sm,
	)
	if err != nil {
		t.Fatal(err)
	}
	mc.AddCloser(idx)

	return user, idx, &mc
}

func makeFsMlog(t testing.TB, r repo.Interface, name string, mc *multicloser.MultiCloser) *roaring.MultiLog {
	ml := multifs.NewMultiLog(r.GetPath("mlog", name))
	mc.AddCloser(ml)
	return ml
}

type makeMultilog func(t testing.TB, r repo.Interface, name string, mc *multicloser.MultiCloser) *roaring.MultiLog

type testPublisher struct{}

func (tp testPublisher) Get(_ int64) (*multimsg.MultiMessage, error) {
	return nil, fmt.Errorf("cant get from test publisher (just a stub)")
}

func (tp testPublisher) Append(_ *multimsg.MultiMessage) (int64, error) {
	return -1, fmt.Errorf("cant append in test setting")
}

func (tp testPublisher) Publish(_ interface{}) (refs.Message, error) {
	return nil, fmt.Errorf("cant publish in test setting")
}

func (tp testPublisher) LastMsg() (refs.Message, error) {
	return nil, nil
}

func (tp testPublisher) Seq() int64 {
	return -1
}

func (tp testPublisher) Query(_ ...margaret.QueryOption) margaret.QueryIterator[*multimsg.MultiMessage] {
	return margaret.NewIterWrapper(func(yield func(int64, *multimsg.MultiMessage) bool) {})
}

func (tp testPublisher) Close() error {
	return nil
}

type testGetter struct{}

func (tg testGetter) Get(_ refs.MessageRef) (refs.Message, error) {
	panic("not implemented") // TODO: Implement
}
