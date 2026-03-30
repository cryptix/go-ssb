// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package get

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/ssbc/go-muxrpc/v3"
	refs "github.com/ssbc/go-ssb-refs"
	"github.com/stretchr/testify/require"
)

// mockGetter implements ssb.Getter for testing.
type mockGetter struct {
	msgs map[string]refs.Message
}

func newMockGetter() *mockGetter {
	return &mockGetter{msgs: make(map[string]refs.Message)}
}

func (m *mockGetter) Get(ref refs.MessageRef) (refs.Message, error) {
	msg, ok := m.msgs[ref.String()]
	if !ok {
		return nil, fmt.Errorf("message not found: %s", ref.String())
	}
	return msg, nil
}

func (m *mockGetter) add(kv refs.KeyValueRaw) {
	m.msgs[kv.Key_.String()] = kv
}

// makeTestMessage creates a KeyValueRaw with the given key string and content.
func makeTestMessage(t *testing.T, keyStr string, content json.RawMessage) refs.KeyValueRaw {
	t.Helper()
	ref, err := refs.ParseMessageRef(keyStr)
	if err != nil {
		t.Fatalf("failed to parse ref %s: %s", keyStr, err)
	}
	return refs.KeyValueRaw{
		Key_: ref,
		Value: refs.Value{
			Sequence: 1,
			Hash:     "sha256",
			Content:  content,
		},
	}
}

func TestSingleHandler(t *testing.T) {
	ref1 := "%YP8wYiFFYHQJB7Mmjkl4idVA1nOsmRz3aX1bJj0GHUI=.sha256"

	getter := newMockGetter()
	msg1 := makeTestMessage(t, ref1, json.RawMessage(`{"type":"post","text":"hello"}`))
	getter.add(msg1)

	h := singleHandler{get: getter, unboxer: nil}

	tests := []struct {
		name    string
		args    interface{}
		wantErr string
		wantKey string
	}{
		{
			name:    "bare string ref",
			args:    []string{ref1},
			wantKey: ref1,
		},
		{
			name:    "object with id",
			args:    []Option{{ID: msg1.Key_}},
			wantKey: ref1,
		},
		{
			name:    "empty args",
			args:    []string{},
			wantErr: "invalid argument count",
		},
		{
			name:    "unknown ref",
			args:    []string{"%AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=.sha256"},
			wantErr: "failed to load message",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			rawArgs, err := json.Marshal(tc.args)
			r.NoError(err)

			req := &muxrpc.Request{RawArgs: rawArgs}
			result, err := h.HandleAsync(nil, req)

			if tc.wantErr != "" {
				r.Error(err)
				r.Contains(err.Error(), tc.wantErr)
				return
			}

			r.NoError(err)
			kv, ok := result.(refs.KeyValueRaw)
			r.True(ok, "expected KeyValueRaw result")
			r.Equal(tc.wantKey, kv.Key_.String())
		})
	}
}

func TestManyHandler(t *testing.T) {
	ref1 := "%YP8wYiFFYHQJB7Mmjkl4idVA1nOsmRz3aX1bJj0GHUI=.sha256"
	ref2 := "%AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=.sha256"
	refMissing := "%BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB=.sha256"

	getter := newMockGetter()
	msg1 := makeTestMessage(t, ref1, json.RawMessage(`{"type":"post","text":"hello"}`))
	msg2 := makeTestMessage(t, ref2, json.RawMessage(`{"type":"post","text":"world"}`))
	getter.add(msg1)
	getter.add(msg2)

	h := manyHandler{get: getter, unboxer: nil}

	t.Run("all found", func(t *testing.T) {
		r := require.New(t)
		parsedRef1, err := refs.ParseMessageRef(ref1)
		r.NoError(err)
		parsedRef2, err := refs.ParseMessageRef(ref2)
		r.NoError(err)

		opts := ManyOption{IDs: []refs.MessageRef{parsedRef1, parsedRef2}}
		rawArgs, err := json.Marshal([]ManyOption{opts})
		r.NoError(err)

		req := &muxrpc.Request{RawArgs: rawArgs}
		result, err := h.HandleAsync(nil, req)
		r.NoError(err)

		results, ok := result.([]*refs.KeyValueRaw)
		r.True(ok, "expected []*KeyValueRaw result")
		r.Len(results, 2)
		r.NotNil(results[0])
		r.NotNil(results[1])
		r.Equal(ref1, results[0].Key_.String())
		r.Equal(ref2, results[1].Key_.String())
	})

	t.Run("partial found", func(t *testing.T) {
		r := require.New(t)
		parsedRef1, err := refs.ParseMessageRef(ref1)
		r.NoError(err)
		parsedMissing, err := refs.ParseMessageRef(refMissing)
		r.NoError(err)

		opts := ManyOption{IDs: []refs.MessageRef{parsedRef1, parsedMissing}}
		rawArgs, err := json.Marshal([]ManyOption{opts})
		r.NoError(err)

		req := &muxrpc.Request{RawArgs: rawArgs}
		result, err := h.HandleAsync(nil, req)
		r.NoError(err)

		results, ok := result.([]*refs.KeyValueRaw)
		r.True(ok, "expected []*KeyValueRaw result")
		r.Len(results, 2)
		r.NotNil(results[0], "first message should be found")
		r.Nil(results[1], "missing message should be nil")
		r.Equal(ref1, results[0].Key_.String())
	})

	t.Run("empty ids", func(t *testing.T) {
		r := require.New(t)
		opts := ManyOption{IDs: []refs.MessageRef{}}
		rawArgs, err := json.Marshal([]ManyOption{opts})
		r.NoError(err)

		req := &muxrpc.Request{RawArgs: rawArgs}
		_, err = h.HandleAsync(nil, req)
		r.Error(err)
		r.Contains(err.Error(), "ids array must not be empty")
	})

	t.Run("missing args", func(t *testing.T) {
		r := require.New(t)
		rawArgs, err := json.Marshal([]interface{}{})
		r.NoError(err)

		req := &muxrpc.Request{RawArgs: rawArgs}
		_, err = h.HandleAsync(nil, req)
		r.Error(err)
		r.Contains(err.Error(), "invalid argument count")
	})
}
