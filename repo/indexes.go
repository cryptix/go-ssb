// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package repo

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"

	"github.com/dgraph-io/badger/v3"
	"github.com/ssbc/margaret/v2/indexes"

	"github.com/ssbc/go-ssb/message/multimsg"
)

const PrefixIndex = "indexes"

func OpenIndex(db *badger.DB, name string, f func(indexes.SeqIndex) *indexes.SinkIndex[*multimsg.MultiMessage, int64]) (indexes.Index[int64], *indexes.SinkIndex[*multimsg.MultiMessage, int64], error) {
	seqSetter := NewBadgerSeqIndex(db, []byte("index"+name))
	return seqSetter, f(seqSetter), nil
}

// utils

var lockFileExistsRe = regexp.MustCompile(`cannot access DB \"(.*)\": lock file \"(.*)\" exists`)

// TODO: add test
func isLockFileExistsErr(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()
	if !lockFileExistsRe.MatchString(errStr) {
		return false
	}
	matches := lockFileExistsRe.FindStringSubmatch(errStr)
	if len(matches) == 3 {
		return true
	}
	return false
}

func cleanupLockFiles(root string) error {
	return filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		name := filepath.Base(path)
		if info.Size() == 0 && len(name) == 41 && name[0] == '.' {
			log.Println("dropping empty lockflile", path)
			if err := os.Remove(path); err != nil {
				return fmt.Errorf("failed to remove %s: %w", name, err)
			}
		}
		return nil
	})
}
