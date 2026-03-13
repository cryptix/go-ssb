// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package ql1

import (
	"encoding/json"
	"testing"

	refs "github.com/ssbc/go-ssb-refs"
)

func TestLeafOperations(t *testing.T) {
	cases := []struct {
		name string
		q    Query
		want string
	}{
		{"type", Type("post"), `{"op":"type","string":"post"}`},
		{"channel", Channel("ssb"), `{"op":"channel","string":"ssb"}`},
		{"search", Search("hello"), `{"op":"search","string":"hello"}`},
		{"isRoot", IsRoot(), `{"op":"isRoot"}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := json.Marshal(tc.q.Operation())
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != tc.want {
				t.Errorf("got  %s\nwant %s", got, tc.want)
			}
		})
	}
}

func TestCombinators(t *testing.T) {
	q := And(Type("post"), Channel("ssb"))
	got, err := json.Marshal(q.Operation())
	if err != nil {
		t.Fatal(err)
	}
	want := `{"op":"and","args":[{"op":"type","string":"post"},{"op":"channel","string":"ssb"}]}`
	if string(got) != want {
		t.Errorf("And:\ngot  %s\nwant %s", got, want)
	}

	q = Not(Type("contact"))
	got, err = json.Marshal(q.Operation())
	if err != nil {
		t.Fatal(err)
	}
	want = `{"op":"not","args":[{"op":"type","string":"contact"}]}`
	if string(got) != want {
		t.Errorf("Not:\ngot  %s\nwant %s", got, want)
	}
}

func TestOptions(t *testing.T) {
	q := Type("post")

	// no options set
	if q.Options() != nil {
		t.Fatal("expected nil options for bare query")
	}

	// descending + limit
	q2 := q.Descending().Limit(20)
	opts := q2.Options()
	if opts == nil {
		t.Fatal("expected non-nil options")
	}
	if !opts.Descending {
		t.Error("expected Descending=true")
	}
	if opts.PageLimit != 20 {
		t.Errorf("expected PageLimit=20, got %d", opts.PageLimit)
	}

	// original unchanged (value semantics)
	if q.Options() != nil {
		t.Error("original query was mutated")
	}
}

func TestAuthorJSON(t *testing.T) {
	feed, err := refs.ParseFeedRef("@p13zSAiOpguI9nsawkGijsnMfWmFd5rlUNpzekEE+vI=.ed25519")
	if err != nil {
		t.Fatal(err)
	}

	q := Author(feed)
	got, err := json.Marshal(q.Operation())
	if err != nil {
		t.Fatal(err)
	}
	want := `{"op":"author","feed":"@p13zSAiOpguI9nsawkGijsnMfWmFd5rlUNpzekEE+vI=.ed25519"}`
	if string(got) != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
}

func TestMultiValueConvenience(t *testing.T) {
	// single type should not wrap in OR
	q := Types("post")
	got, err := json.Marshal(q.Operation())
	if err != nil {
		t.Fatal(err)
	}
	want := `{"op":"type","string":"post"}`
	if string(got) != want {
		t.Errorf("single type:\ngot  %s\nwant %s", got, want)
	}

	// multiple types should wrap in OR
	q = Types("post", "about")
	got, err = json.Marshal(q.Operation())
	if err != nil {
		t.Fatal(err)
	}
	want = `{"op":"or","args":[{"op":"type","string":"post"},{"op":"type","string":"about"}]}`
	if string(got) != want {
		t.Errorf("multi type:\ngot  %s\nwant %s", got, want)
	}
}

func TestTimestampShorthands(t *testing.T) {
	q := After(1000)
	got, err := json.Marshal(q.Operation())
	if err != nil {
		t.Fatal(err)
	}
	want := `{"op":"timestamp","gt":1000}`
	if string(got) != want {
		t.Errorf("After:\ngot  %s\nwant %s", got, want)
	}

	q = Before(2000)
	got, err = json.Marshal(q.Operation())
	if err != nil {
		t.Fatal(err)
	}
	want = `{"op":"timestamp","lt":2000}`
	if string(got) != want {
		t.Errorf("Before:\ngot  %s\nwant %s", got, want)
	}
}
