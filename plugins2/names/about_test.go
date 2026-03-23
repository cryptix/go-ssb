// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package names_test

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	refs "github.com/ssbc/go-ssb-refs"
	"github.com/ssbc/go-ssb/client"
	"github.com/ssbc/go-ssb/internal/testutils"
	"github.com/ssbc/go-ssb/sbot"
)

func TestNamesImageFor(t *testing.T) {
	if testutils.SkipOnCI(t) {
		return
	}

	r := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())

	hk := make([]byte, 32)
	n, err := rand.Read(hk)
	r.Equal(32, n)

	repoPath := filepath.Join("testrun", t.Name(), "about")
	os.RemoveAll(repoPath)

	ali, err := sbot.New(
		sbot.WithHMACSigning(hk),
		sbot.WithInfo(testutils.NewRelativeTimeLogger(nil)),
		sbot.WithRepoPath(repoPath),
		sbot.WithListenAddr(":0"),
		sbot.LateOption(sbot.WithUNIXSocket()),
	)
	r.NoError(err)

	var aliErrc = make(chan error, 1)
	go func() {
		err := ali.Network.Serve(ctx)
		if err != nil && err != context.Canceled {
			aliErrc <- fmt.Errorf("ali serve exited: %w", err)
		}
		close(aliErrc)
	}()

	// Parse a well-formed blob ref to use as the avatar image
	testBlobRef, err := refs.ParseBlobRef("&0000000000000000000000000000000000000000000=.sha256")
	r.NoError(err)

	// Publish a self-about message with an image field
	var aboutMsg refs.About
	aboutMsg.Type = "about"
	aboutMsg.About = ali.KeyPair.ID()
	aboutMsg.Name = fmt.Sprintf("testName:%x", hk[:16])
	aboutMsg.Image = &testBlobRef

	_, err = ali.PublishLog.Publish(aboutMsg)
	r.NoError(err)

	ali.WaitUntilIndexesAreSynced()

	c, err := client.NewUnix(filepath.Join(repoPath, "socket"))
	r.NoError(err)

	gotBlob, err := c.NamesImageFor(ali.KeyPair.ID())
	r.NoError(err)
	r.Equal(testBlobRef.Sigil(), gotBlob.Sigil(), "returned blob ref should match the published image")

	r.NoError(c.Close())

	cancel()
	ali.Shutdown()
	r.NoError(ali.Close())
	r.NoError(<-aliErrc)
}

func TestNamesImageForNoAvatar(t *testing.T) {
	if testutils.SkipOnCI(t) {
		return
	}

	r := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())

	hk := make([]byte, 32)
	n, err := rand.Read(hk)
	r.Equal(32, n)

	repoPath := filepath.Join("testrun", t.Name(), "about")
	os.RemoveAll(repoPath)

	ali, err := sbot.New(
		sbot.WithHMACSigning(hk),
		sbot.WithInfo(testutils.NewRelativeTimeLogger(nil)),
		sbot.WithRepoPath(repoPath),
		sbot.WithListenAddr(":0"),
		sbot.LateOption(sbot.WithUNIXSocket()),
	)
	r.NoError(err)

	var aliErrc = make(chan error, 1)
	go func() {
		err := ali.Network.Serve(ctx)
		if err != nil && err != context.Canceled {
			aliErrc <- fmt.Errorf("ali serve exited: %w", err)
		}
		close(aliErrc)
	}()

	// Publish a self-about message with name only, no image
	var aboutMsg refs.About
	aboutMsg.Type = "about"
	aboutMsg.About = ali.KeyPair.ID()
	aboutMsg.Name = fmt.Sprintf("testName:%x", hk[:16])

	_, err = ali.PublishLog.Publish(aboutMsg)
	r.NoError(err)

	ali.WaitUntilIndexesAreSynced()

	c, err := client.NewUnix(filepath.Join(repoPath, "socket"))
	r.NoError(err)

	_, err = c.NamesImageFor(ali.KeyPair.ID())
	r.Error(err, "NamesImageFor should return an error when no image is set")

	r.NoError(c.Close())

	cancel()
	ali.Shutdown()
	r.NoError(ali.Close())
	r.NoError(<-aliErrc)
}

func TestAboutNames(t *testing.T) {
	if testutils.SkipOnCI(t) {
		// https://github.com/ssbc/go-ssb/issues/282
		return
	}

	r := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())

	hk := make([]byte, 32)
	n, err := rand.Read(hk)
	r.Equal(32, n)

	repoPath := filepath.Join("testrun", t.Name(), "about")
	os.RemoveAll(repoPath)

	// names plugin is mounted by default in sbot.New
	ali, err := sbot.New(
		sbot.WithHMACSigning(hk),
		sbot.WithInfo(testutils.NewRelativeTimeLogger(nil)),
		sbot.WithRepoPath(repoPath),
		sbot.LateOption(sbot.WithUNIXSocket()),
	)
	r.NoError(err)

	var aliErrc = make(chan error, 1)
	go func() {
		err := ali.Network.Serve(ctx)
		if err != nil && err != context.Canceled {
			aliErrc <- fmt.Errorf("ali serve exited: %w", err)
		}
		close(aliErrc)
	}()

	var newName refs.About
	newName.Type = "about"
	newName.Name = fmt.Sprintf("testName:%x", hk[:16])
	newName.About = ali.KeyPair.ID()

	_, err = ali.PublishLog.Publish(newName)
	r.NoError(err)

	qry := ali.ReceiveLog.Query()
	var i = 0
	for _, mm := range qry.Iter() {
		if mm.Message == nil {
			continue
		}
		sm := mm.Message
		var a refs.About
		c := sm.ContentBytes()
		err = json.Unmarshal(c, &a)
		r.NoError(err)
		r.Equal(newName.Name, a.Name)
		i++
	}
	r.NoError(qry.Err())
	r.Equal(i, 1)

	c, err := client.NewUnix(filepath.Join(repoPath, "socket"))
	r.NoError(err)

	all, err := c.NamesGet()
	r.NoError(err)
	t.Log(all)

	name, ok := all.GetCommonName(ali.KeyPair.ID())
	r.True(ok, "name for ali not found")
	r.Equal(newName.Name, name)

	name2, err := c.NamesSignifier(ali.KeyPair.ID())
	r.NoError(err)
	r.Equal(newName.Name, name2)

	r.NoError(c.Close())

	cancel()
	ali.Shutdown()
	r.NoError(ali.Close())
	r.NoError(<-aliErrc)
}
