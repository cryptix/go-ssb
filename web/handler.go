// SPDX-FileCopyrightText: 2026 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

// Package web provides a minimal web UI for go-ssb, starting with a profile page.
package web

import (
	"encoding/json"
	"html/template"
	"net/http"
	"sort"
	"time"

	refs "github.com/ssbc/go-ssb-refs"

	"github.com/ssbc/go-ssb/internal/storedrefs"
	"github.com/ssbc/go-ssb/message/multimsg"
	"github.com/ssbc/go-ssb/plugins2/names"
	margaret "github.com/ssbc/margaret/v2"
	"github.com/ssbc/margaret/v2/multilog/roaring"
)

// Handler serves the web UI.
type Handler struct {
	FeedID     refs.FeedRef
	Names      *names.Plugin
	Users      *roaring.MultiLog
	ReceiveLog margaret.Log[*multimsg.MultiMessage]
	mux        *http.ServeMux
}

type profileData struct {
	FeedID      string
	Name        string
	Description string
	ImageRef    string
	Posts       []postData
}

type postData struct {
	Key       string
	Text      string
	Timestamp time.Time
}

// NewHandler creates a Handler and sets up routes.
func NewHandler(
	feedID refs.FeedRef,
	namesPlug *names.Plugin,
	users *roaring.MultiLog,
	rxLog margaret.Log[*multimsg.MultiMessage],
) *Handler {
	h := &Handler{
		FeedID:     feedID,
		Names:      namesPlug,
		Users:      users,
		ReceiveLog: rxLog,
		mux:        http.NewServeMux(),
	}
	h.mux.HandleFunc("/web", h.profilePage)
	h.mux.HandleFunc("/web/", h.profilePage)
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

func (h *Handler) profilePage(w http.ResponseWriter, r *http.Request) {
	data := profileData{
		FeedID: h.FeedID.String(),
	}

	// Load about info
	if info, err := h.Names.CollectedFor(h.FeedID); err == nil {
		data.Name = info.Name.Chosen
		data.Description = info.Description.Chosen
		data.ImageRef = info.Image.Chosen
	}

	// Load last 5 posts
	bm, err := h.Users.LoadInternalBitmap(storedrefs.Feed(h.FeedID))
	if err == nil {
		seqs := bm.ToArray()
		// Sort descending to get most recent first
		sort.Slice(seqs, func(i, j int) bool { return seqs[i] > seqs[j] })

		// Collect up to 5 posts (skip non-post messages)
		count := 0
		for _, seq := range seqs {
			if count >= 5 {
				break
			}
			mm, err := h.ReceiveLog.Get(int64(seq))
			if err != nil || mm.Message == nil {
				continue
			}
			msg := mm.Message

			var typed struct {
				Type string `json:"type"`
				Text string `json:"text"`
			}
			if err := json.Unmarshal(msg.ContentBytes(), &typed); err != nil {
				continue
			}
			if typed.Type != "post" {
				continue
			}

			data.Posts = append(data.Posts, postData{
				Key:       msg.Key().String(),
				Text:      typed.Text,
				Timestamp: msg.Claimed(),
			})
			count++
		}
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := profileTmpl.Execute(w, data); err != nil {
		http.Error(w, err.Error(), 500)
	}
}

var profileTmpl = template.Must(template.New("profile").Parse(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{{if .Name}}{{.Name}}{{else}}Profile{{end}} - go-ssb</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    max-width: 640px;
    margin: 2rem auto;
    padding: 0 1rem;
    color: #222;
    background: #fafafa;
  }
  .profile-header {
    display: flex;
    align-items: center;
    gap: 1.5rem;
    margin-bottom: 2rem;
    padding-bottom: 1.5rem;
    border-bottom: 1px solid #ddd;
  }
  .avatar {
    width: 80px;
    height: 80px;
    border-radius: 50%;
    background: #ccc;
    flex-shrink: 0;
  }
  .profile-info h1 { font-size: 1.5rem; margin-bottom: 0.25rem; }
  .feed-id {
    font-size: 0.75rem;
    color: #888;
    word-break: break-all;
    font-family: monospace;
  }
  .description {
    margin-top: 0.5rem;
    color: #555;
    white-space: pre-wrap;
  }
  .post {
    background: #fff;
    border: 1px solid #e0e0e0;
    border-radius: 8px;
    padding: 1rem;
    margin-bottom: 1rem;
  }
  .post-text { white-space: pre-wrap; line-height: 1.5; }
  .post-meta {
    margin-top: 0.5rem;
    font-size: 0.8rem;
    color: #999;
  }
  h2 { margin-bottom: 1rem; font-size: 1.1rem; }
</style>
</head>
<body>
  <div class="profile-header">
    {{if .ImageRef}}<img class="avatar" src="/blobs/get/{{.ImageRef}}" alt="avatar">{{else}}<div class="avatar"></div>{{end}}
    <div class="profile-info">
      <h1>{{if .Name}}{{.Name}}{{else}}Unknown{{end}}</h1>
      <div class="feed-id">{{.FeedID}}</div>
      {{if .Description}}<div class="description">{{.Description}}</div>{{end}}
    </div>
  </div>

  <h2>Recent Posts</h2>
  {{if .Posts}}
    {{range .Posts}}
    <div class="post">
      <div class="post-text">{{.Text}}</div>
      <div class="post-meta">{{.Timestamp.Format "Jan 2, 2006 3:04 PM"}} &middot; <code>{{.Key}}</code></div>
    </div>
    {{end}}
  {{else}}
    <p>No posts yet.</p>
  {{end}}
</body>
</html>
`))
