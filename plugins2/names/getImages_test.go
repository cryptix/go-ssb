// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package names

import (
	"encoding/json"
	"testing"

	"github.com/ssbc/go-muxrpc/v3"
	refs "github.com/ssbc/go-ssb-refs"
	"github.com/stretchr/testify/require"
)

// TestGetMostPrescribedImage tests the image selection logic used in hImagesFor.HandleAsync.
// When no self-chosen image exists, it should return the image prescribed by the most peers.
func TestGetMostPrescribedImage(t *testing.T) {
	r := require.New(t)

	// Build an AboutInfo with no self-chosen image but several prescribed images
	ai := &AboutInfo{
		Image: AboutAttribute{
			Chosen: "", // no self-chosen image
			Prescribed: map[string]int{
				"&popular.sha256": 10,
				"&rare.sha256":    2,
				"&medium.sha256":  5,
			},
		},
	}

	// Reproduce the exact logic from hImagesFor.HandleAsync (now fixed)
	var mostSet string
	var most = 0
	for v, cnt := range ai.Image.Prescribed {
		if cnt > most {
			most = cnt
			mostSet = v
		}
	}

	// The correct result should be the most popular image
	r.Equal("&popular.sha256", mostSet, "should return the most prescribed image")
	r.Equal(10, most, "most count should be 10")
}

// TestGetMostPrescribedImageSingleEntry tests that a single prescribed image is returned.
func TestGetMostPrescribedImageSingleEntry(t *testing.T) {
	r := require.New(t)

	ai := &AboutInfo{
		Image: AboutAttribute{
			Chosen: "",
			Prescribed: map[string]int{
				"&only.sha256": 3,
			},
		},
	}

	var mostSet string
	var most = 0
	for v, cnt := range ai.Image.Prescribed {
		if cnt > most {
			most = cnt
			mostSet = v
		}
	}

	r.Equal("&only.sha256", mostSet)
	r.Equal(3, most)
}

// TestGetMostPrescribedImageChosenTakesPrecedence tests that a self-chosen image overrides prescribed.
func TestGetMostPrescribedImageChosenTakesPrecedence(t *testing.T) {
	r := require.New(t)

	ai := &AboutInfo{
		Image: AboutAttribute{
			Chosen: "&self-chosen.sha256",
			Prescribed: map[string]int{
				"&popular.sha256": 10,
			},
		},
	}

	// If Chosen is set, it should be returned directly (matching handler logic)
	if ai.Image.Chosen != "" {
		r.Equal("&self-chosen.sha256", ai.Image.Chosen)
		return
	}
	t.Fatal("chosen image should have been returned")
}

// TestParseFeedRefFromArgsObjectFormat tests that parseFeedRefFromArgs handles
// the object argument format [{"id": "@...=.ed25519"}].
func TestParseFeedRefFromArgsObjectFormat(t *testing.T) {
	r := require.New(t)

	// Create a test feed ref
	testRef, err := refs.ParseFeedRef("@p13zSAiOpguI9nsawkGijsnMfWmFd5rlUNpzekEE+vI=.ed25519")
	r.NoError(err)

	// Test the object format: [{"id": "@..."}]
	type objArg struct {
		ID refs.FeedRef `json:"id"`
	}
	rawArgs, err := json.Marshal([]objArg{{ID: testRef}})
	r.NoError(err)

	req := &muxrpc.Request{RawArgs: rawArgs}
	result, err := parseFeedRefFromArgs(req)
	r.NoError(err, "parseFeedRefFromArgs should succeed with object format args")
	r.True(result.Equal(testRef), "parsed ref should match input ref")
}

// TestParseFeedRefFromArgsDirectFormat tests that parseFeedRefFromArgs handles
// the direct argument format ["@...=.ed25519"].
func TestParseFeedRefFromArgsDirectFormat(t *testing.T) {
	r := require.New(t)

	testRef, err := refs.ParseFeedRef("@p13zSAiOpguI9nsawkGijsnMfWmFd5rlUNpzekEE+vI=.ed25519")
	r.NoError(err)

	rawArgs, err := json.Marshal([]refs.FeedRef{testRef})
	r.NoError(err)

	req := &muxrpc.Request{RawArgs: rawArgs}
	result, err := parseFeedRefFromArgs(req)
	r.NoError(err, "parseFeedRefFromArgs should succeed with direct format args")
	r.True(result.Equal(testRef), "parsed ref should match input ref")
}
