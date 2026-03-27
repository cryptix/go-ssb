// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package names

import (
	"context"
	"fmt"

	"github.com/ssbc/go-muxrpc/v3"
	"go.mindeco.de/logging"
)

// ProfileInfo holds the resolved profile fields for a single feed.
type ProfileInfo struct {
	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`
	Image       string `json:"image,omitempty"`
}

type hGetFor struct {
	as  aboutStore
	log logging.Interface
}

// HandleAsync returns the resolved name, description, and image for a feed.
func (h hGetFor) HandleAsync(ctx context.Context, req *muxrpc.Request) (interface{}, error) {
	ref, err := parseFeedRefFromArgs(req)
	if err != nil {
		return nil, err
	}

	ai, err := h.as.CollectedFor(ref)
	if err != nil {
		return nil, fmt.Errorf("do not have about for: %s: %w", ref.String(), err)
	}

	var info ProfileInfo

	// resolve name: self-assigned first, then most-prescribed
	info.Name = ai.Name.Chosen
	if info.Name == "" {
		for n := range ai.Name.Prescribed {
			info.Name = n
			break
		}
	}

	// resolve description: self-assigned first, then most-prescribed
	info.Description = ai.Description.Chosen
	if info.Description == "" {
		for d := range ai.Description.Prescribed {
			info.Description = d
			break
		}
	}

	// resolve image: self-assigned first, then highest-count prescribed
	info.Image = ai.Image.Chosen
	if info.Image == "" {
		var mostSet string
		var most int
		for v, cnt := range ai.Image.Prescribed {
			if cnt > most {
				most = cnt
				mostSet = v
			}
		}
		info.Image = mostSet
	}

	return info, nil
}
