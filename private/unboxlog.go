// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package private

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"iter"

	margaret "github.com/ssbc/margaret/v2"
	"github.com/ssbc/margaret/v2/multilog/roaring"

	"github.com/ssbc/go-ssb"
	refs "github.com/ssbc/go-ssb-refs"
	"github.com/ssbc/go-ssb/message/multimsg"
	"github.com/ssbc/go-ssb/private/box"
)

// UnboxedLog provides access to decrypted private messages.
// It resolves sequence numbers from a sublog through the root log and decrypts them.
type UnboxedLog struct {
	root   margaret.Log[*multimsg.MultiMessage]
	seqlog margaret.Log[*roaring.Seq]
	kp     ssb.KeyPair
	boxer  *box.Boxer
}

// NewUnboxerLog expects the sequence numbers, that are returned from seqlog, to be decryptable by kp.
func NewUnboxerLog(root margaret.Log[*multimsg.MultiMessage], seqlog margaret.Log[*roaring.Seq], kp ssb.KeyPair) *UnboxedLog {
	return &UnboxedLog{
		root:   root,
		seqlog: seqlog,
		kp:     kp,
		boxer:  box.NewBoxer(nil),
	}
}

func (il *UnboxedLog) Seq() int64 {
	return il.seqlog.Seq()
}

func (il *UnboxedLog) Get(seq int64) (refs.KeyValueRaw, error) {
	v, err := il.seqlog.Get(seq)
	if err != nil {
		return refs.KeyValueRaw{}, fmt.Errorf("seqlog: 1st lookup failed: %w", err)
	}

	return il.resolveAndDecrypt(int64(*v))
}

// unboxedIterator implements margaret.QueryIterator[refs.KeyValueRaw]
type unboxedIterator struct {
	iterFn func(yield func(int64, refs.KeyValueRaw) bool)
	errFn  func() error
}

func (ui *unboxedIterator) Iter() iter.Seq2[int64, refs.KeyValueRaw] {
	return ui.iterFn
}

func (ui *unboxedIterator) Err() error {
	return ui.errFn()
}

// Query maps the sequence values in seqlog to an unboxed version of the message.
func (il *UnboxedLog) Query(args ...margaret.QueryOption) margaret.QueryIterator[refs.KeyValueRaw] {
	innerQry := il.seqlog.Query(args...)

	var resolveErr error
	return &unboxedIterator{
		iterFn: func(yield func(int64, refs.KeyValueRaw) bool) {
			for seq, seqVal := range innerQry.Iter() {
				rootSeq := int64(*seqVal)
				msg, err := il.resolveAndDecrypt(rootSeq)
				if err != nil {
					if margaret.IsErrNulled(err) {
						continue
					}
					resolveErr = fmt.Errorf("unboxLog: resolve seq %d failed: %w", rootSeq, err)
					return
				}
				if !yield(seq, msg) {
					return
				}
			}
		},
		errFn: func() error {
			if resolveErr != nil {
				return resolveErr
			}
			return innerQry.Err()
		},
	}
}

func (il *UnboxedLog) resolveAndDecrypt(rootSeq int64) (refs.KeyValueRaw, error) {
	mm, err := il.root.Get(rootSeq)
	if err != nil {
		return refs.KeyValueRaw{}, fmt.Errorf("unboxLog: error getting v(%d) from root log: %w", rootSeq, err)
	}

	amsg := mm.Message
	if amsg == nil {
		return refs.KeyValueRaw{}, fmt.Errorf("unboxLog: nulled message at %d", rootSeq)
	}

	author := amsg.Author()

	var boxedContent []byte
	switch author.Algo() {
	case refs.RefAlgoFeedSSB1:
		input := amsg.ContentBytes()
		if !(input[0] == '"' && input[len(input)-1] == '"') {
			return refs.KeyValueRaw{}, fmt.Errorf("expected json string with quotes")
		}
		b64data := bytes.TrimSuffix(input[1:], []byte(".box\""))
		boxedData := make([]byte, len(b64data))

		n, err := base64.StdEncoding.Decode(boxedData, b64data)
		if err != nil {
			return refs.KeyValueRaw{}, fmt.Errorf("decode pm: invalid b64 encoding: %w", err)
		}
		boxedContent = boxedData[:n]

	case refs.RefAlgoFeedGabby:
		boxedContent = bytes.TrimPrefix(amsg.ContentBytes(), []byte("box1:"))

	default:
		return refs.KeyValueRaw{}, fmt.Errorf("decode pm: unknown feed type: %s", author.Algo())
	}

	clearContent, err := il.boxer.Decrypt(il.kp, boxedContent)
	if err != nil {
		return refs.KeyValueRaw{}, fmt.Errorf("unboxLog: unbox failed: %w", err)
	}

	var msg refs.KeyValueRaw
	msg.Key_ = amsg.Key()
	msg.Timestamp = refs.Millisecs(amsg.Received())
	msg.Value.Previous = amsg.Previous()
	msg.Value.Author = author
	msg.Value.Sequence = amsg.Seq()
	msg.Value.Timestamp = refs.Millisecs(amsg.Claimed())
	msg.Value.Hash = "go-ssb-unboxed"
	msg.Value.Content = clearContent
	msg.Value.Signature = "go-ssb-unboxed"

	return msg, nil
}
