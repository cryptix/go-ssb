// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package names

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
	margaret "github.com/ssbc/margaret/v2"

	refs "github.com/ssbc/go-ssb-refs"
	"github.com/ssbc/go-ssb/client"
	"github.com/ssbc/go-ssb/message/multimsg"
)

type aboutStore struct {
	kv *badger.DB
}

type AboutInfo struct {
	Name, Description, Image AboutAttribute
}

type AboutAttribute struct {
	Chosen     string
	Prescribed map[string]int
}

var idxKeyPrefix = []byte("idx-abouts")
var idxInSync sync.WaitGroup

func (ab aboutStore) waitForIndexes() {
	idxInSync.Wait()
}

func (ab aboutStore) startIndexing() {
	idxInSync.Add(1)
}

func (ab aboutStore) doneIndexing() {
	time.AfterFunc(100*time.Millisecond, func() {
		idxInSync.Done()
	})
}

func (ab aboutStore) ImageFor(ref *refs.FeedRef) (*refs.BlobRef, error) {
	var br refs.BlobRef

	ab.waitForIndexes()

	err := ab.kv.View(func(txn *badger.Txn) error {

		addr := ref.Sigil()
		addr += ":"
		addr += ref.Sigil()
		addr += ":image"
		it, err := txn.Get(append(idxKeyPrefix, []byte(addr)...))
		if err != nil {
			return err
		}

		err = it.Value(func(v []byte) error {
			// values are stored as JSON strings, so unmarshal first
			var blobStr string
			if err := json.Unmarshal(v, &blobStr); err != nil {
				// fallback: try raw string for backwards compatibility
				blobStr = string(v)
			}
			newBlobR, err := refs.ParseBlobRef(blobStr)
			if err != nil {
				return err
			}
			br = newBlobR
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	})

	return &br, err
}

func (ab aboutStore) All() (client.NamesGetResult, error) {
	ab.waitForIndexes()

	var ngr = make(client.NamesGetResult)
	err := ab.kv.View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		for iter.Seek(idxKeyPrefix); iter.ValidForPrefix(idxKeyPrefix); iter.Next() {
			it := iter.Item()
			k := it.Key()

			kWoPrefix := bytes.TrimPrefix(k, idxKeyPrefix)

			if string(kWoPrefix) == "__current_observable" {
				return nil // skip
			}

			parts := strings.Split(string(kWoPrefix), ":")
			if len(parts) != 3 {
				return fmt.Errorf("about.All: illegal key:%q", string(k))
			}

			about := parts[0]
			author := parts[1]
			field := parts[2]

			if string(field) == "name" {
				err := it.Value(func(v []byte) error {
					name := string(v)
					name = strings.TrimPrefix(name, "\"")
					name = strings.TrimSuffix(name, "\"")

					abouts, ok := ngr[about]
					if !ok {
						abouts = make(map[string]string)
						abouts[author] = name
						ngr[about] = abouts
						return nil
					}

					abouts[author] = name

					return nil
				})
				if err != nil {
					return fmt.Errorf("about.All: value of item %q failed: %w", k, err)
				}
			}

		}
		return nil
	})
	return ngr, err
}

func (ab aboutStore) CollectedFor(ref refs.FeedRef) (*AboutInfo, error) {
	ab.waitForIndexes()

	addr := append(idxKeyPrefix, []byte(ref.Sigil()+":")...)

	var reduced AboutInfo
	reduced.Name.Prescribed = make(map[string]int)
	reduced.Description.Prescribed = make(map[string]int)
	reduced.Image.Prescribed = make(map[string]int)

	err := ab.kv.View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		for iter.Seek(addr); iter.ValidForPrefix(addr); iter.Next() {
			it := iter.Item()
			k := it.Key()
			k = bytes.TrimPrefix(k, idxKeyPrefix)
			splitted := bytes.Split(k, []byte(":"))

			c, err := refs.ParseFeedRef(string(splitted[1]))
			if err != nil {
				return fmt.Errorf("about: couldnt make author ref from db key: %s: %w", splitted, err)
			}

			err = it.Value(func(v []byte) error {
				var fieldPtr *AboutAttribute
				var foundVal string
				if err := json.Unmarshal(v, &foundVal); err != nil {
					return err
				}

				switch {
				case bytes.HasSuffix(k, []byte(":name")):
					fieldPtr = &reduced.Name
				case bytes.HasSuffix(k, []byte(":description")):
					fieldPtr = &reduced.Description
				case bytes.HasSuffix(k, []byte(":image")):
					fieldPtr = &reduced.Image
				default:
					log.Printf("about debug: %s ", c.Sigil())
					log.Printf("no field for: %q", string(k))
					return nil
				}

				if c.Equal(ref) {
					fieldPtr.Chosen = foundVal
				} else {
					cnt, has := fieldPtr.Prescribed[foundVal]
					if has {
						cnt++
					} else {
						cnt = 1
					}
					fieldPtr.Prescribed[foundVal] = cnt
				}

				return nil
			})
			if err != nil {
				return fmt.Errorf("about: couldnt get idx value: %w", err)
			}

		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("name db lookup failed: %w", err)
	}

	return &reduced, nil
}

const FolderNameAbout = "about"

// aboutLogIndexer processes about messages and stores name/description/image data in badger.
type aboutLogIndexer struct {
	db     *badger.DB
	seqKey []byte
	about  aboutStore
}

// OpenSharedIndex creates the about index backed by the given badger database.
func (plug *Plugin) OpenSharedIndex(db *badger.DB) *aboutLogIndexer {
	plug.about = aboutStore{db}

	plug.about.startIndexing()
	defer plug.about.doneIndexing()

	return &aboutLogIndexer{
		db:     db,
		seqKey: append(append([]byte(nil), idxKeyPrefix...), []byte("__seq")...),
		about:  plug.about,
	}
}

func (ai *aboutLogIndexer) lastProcessedSeq() int64 {
	var val int64 = margaret.SeqEmpty
	_ = ai.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(ai.seqKey)
		if err != nil {
			return err
		}
		return item.Value(func(data []byte) error {
			if len(data) == 8 {
				val = int64(binary.BigEndian.Uint64(data))
			}
			return nil
		})
	})
	return val
}

func (ai *aboutLogIndexer) setLastProcessedSeq(seq int64) {
	_ = ai.db.Update(func(txn *badger.Txn) error {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(seq))
		return txn.Set(ai.seqKey, buf[:])
	})
}

// Index processes all unprocessed about messages from the log.
func (ai *aboutLogIndexer) Index(logV margaret.Log[*multimsg.MultiMessage]) error {
	ai.about.startIndexing()
	defer ai.about.doneIndexing()

	lastSeq := ai.lastProcessedSeq()
	var opts []margaret.QueryOption
	if lastSeq >= 0 {
		opts = append(opts, margaret.Gt(lastSeq))
	}
	qry := logV.Query(opts...)
	for seq, mm := range qry.Iter() {
		if mm.Message == nil {
			ai.setLastProcessedSeq(seq)
			continue
		}
		if err := ai.updateAboutMessage(mm.Message); err != nil {
			return err
		}
		ai.setLastProcessedSeq(seq)
	}
	return qry.Err()
}

func (ai *aboutLogIndexer) Close() error { return nil }

func (ai *aboutLogIndexer) setAboutField(addr string, val string) error {
	key := append(append([]byte(nil), idxKeyPrefix...), []byte(addr)...)
	jsonVal, err := json.Marshal(val)
	if err != nil {
		return err
	}
	return ai.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, jsonVal)
	})
}

func (ai *aboutLogIndexer) updateAboutMessage(msg refs.Message) error {
	ai.about.startIndexing()
	defer ai.about.doneIndexing()

	var aboutMSG refs.About
	err := json.Unmarshal(msg.ContentBytes(), &aboutMSG)
	if err != nil {
		return nil
	}

	// about:from:field
	addr := aboutMSG.About.Sigil()
	addr += ":"
	addr += msg.Author().Sigil()
	addr += ":"

	if aboutMSG.Name != "" {
		if err := ai.setAboutField(addr+"name", aboutMSG.Name); err != nil {
			return fmt.Errorf("db/idx about: failed to update name: %w", err)
		}
	}
	if aboutMSG.Description != "" {
		if err := ai.setAboutField(addr+"description", aboutMSG.Description); err != nil {
			return fmt.Errorf("db/idx about: failed to update description: %w", err)
		}
	}
	if aboutMSG.Image != nil {
		if err := ai.setAboutField(addr+"image", aboutMSG.Image.Sigil()); err != nil {
			return fmt.Errorf("db/idx about: failed to update image: %w", err)
		}
	}

	return nil
}
