// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package multilogs

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/dgraph-io/badger/v3"
	margaret "github.com/ssbc/margaret/v2"

	refs "github.com/ssbc/go-ssb-refs"
	"github.com/ssbc/go-ssb/internal/storedrefs"
	"github.com/ssbc/go-ssb/message/multimsg"
	"github.com/ssbc/go-ssb/private"
	"go.mindeco.de/log"
	"go.mindeco.de/log/level"
)

type Members map[string]bool

// MembershipStore isn't strictly a multilog but putting it in package private gave cyclic import
type MembershipStore struct {
	logger log.Logger

	db     *badger.DB
	prefix []byte

	self        refs.FeedRef
	unboxer     *private.Manager
	combinedidx *CombinedIndex
}

var _ io.Closer = (*MembershipStore)(nil)

var keyPrefix = []byte("group-members")

// NewMembershipIndex tracks group/add-member messages and triggers re-reading box2 messages
// by the invited people that couldn't be read before.
func NewMembershipIndex(logger log.Logger, db *badger.DB, self refs.FeedRef, unboxer *private.Manager, comb *CombinedIndex) *MembershipStore {
	return &MembershipStore{
		logger: logger,

		db:     db,
		prefix: keyPrefix,

		self:        self,
		unboxer:     unboxer,
		combinedidx: comb,
	}
}

func (mc *MembershipStore) Close() error {
	return nil
}

var memberSeqKey = []byte("group-members__seq")

// LastProcessedSeq returns the last sequence processed by this index.
func (mc *MembershipStore) LastProcessedSeq() int64 {
	var val int64 = margaret.SeqEmpty
	_ = mc.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(memberSeqKey)
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

func (mc *MembershipStore) setLastProcessedSeq(seq int64) {
	_ = mc.db.Update(func(txn *badger.Txn) error {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(seq))
		return txn.Set(memberSeqKey, buf[:])
	})
}

// Index processes all unprocessed messages from the log.
func (mc *MembershipStore) Index(log margaret.Log[*multimsg.MultiMessage]) error {
	lastSeq := mc.LastProcessedSeq()
	var opts []margaret.QueryOption
	if lastSeq >= 0 {
		opts = append(opts, margaret.Gt(lastSeq))
	}
	qry := log.Query(opts...)
	for seq, mm := range qry.Iter() {
		if err := mc.ProcessEntry(seq, mm); err != nil {
			return err
		}
		mc.setLastProcessedSeq(seq)
	}
	return qry.Err()
}

func (mc *MembershipStore) getMembers(key []byte) (Members, error) {
	var members Members
	err := mc.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return nil
			}
			return err
		}
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &members)
		})
	})
	if err != nil {
		return nil, err
	}
	if members == nil {
		members = make(Members)
	}
	return members, nil
}

func (mc *MembershipStore) setMembers(key []byte, members Members) error {
	return mc.db.Update(func(txn *badger.Txn) error {
		val, err := json.Marshal(members)
		if err != nil {
			return err
		}
		return txn.Set(key, val)
	})
}

// ProcessEntry processes a single group/add-member message, updating membership tracking
// and triggering Box2Reindex for newly invited members.
func (mc *MembershipStore) ProcessEntry(seq int64, mm *multimsg.MultiMessage) error {
	if mm.Message == nil {
		return nil
	}
	msg := mm.Message

	if msg.Author().Equal(mc.self) {
		// our own message - all is done already
		level.Debug(mc.logger).Log("msg", "skipping own invite")
		return nil
	}

	cleartext, err := mc.unboxer.DecryptMessage(msg)
	if err != nil {
		return nil // invalid message
	}

	var addMemberMsg private.GroupAddMember
	err = json.Unmarshal(cleartext, &addMemberMsg)
	if err != nil {
		return nil // invalid message
	}

	var groupID refs.MessageRef
	var newMembers []refs.FeedRef
	for _, r := range addMemberMsg.Recps {
		rcp, err := refs.ParseMessageRef(r)
		if err == nil && rcp.Algo() == refs.RefAlgoCloakedGroup {
			groupID = rcp
			continue
		}

		m, err := refs.ParseFeedRef(r)
		if err != nil {
			return nil // invalid message
		}
		newMembers = append(newMembers, m)
	}
	level.Debug(mc.logger).Log("msg", "new members",
		"author", msg.Author().ShortSigil(),
		"group", groupID.ShortSigil(),
		"members", fmt.Sprintf("%v", newMembers),
	)

	idxAddr := storedrefs.Message(groupID)
	badgerKey := append(mc.prefix, []byte(idxAddr)...)

	currentMembers, err := mc.getMembers(badgerKey)
	if err != nil {
		return err
	}

	for _, nm := range newMembers {
		_, indexed := currentMembers[nm.String()]
		if indexed {
			level.Debug(mc.logger).Log("msg", "already indexed",
				"group", groupID.ShortSigil(),
				"who", nm,
			)
			continue
		}

		whoToIndex := nm
		if nm.Equal(mc.self) {
			// if the invite is for us, we need to add the new group key
			cloakedGroupID, err := mc.unboxer.Join(addMemberMsg.GroupKey, addMemberMsg.Root)
			if err != nil {
				return err
			}
			level.Debug(mc.logger).Log("event", "joined group", "id", cloakedGroupID.String())

			// if we are invited, we need to index the sending author
			whoToIndex = msg.Author()
		}
		level.Debug(mc.logger).Log("msg", "reindexing",
			"group", groupID.ShortSigil(),
			"whoToIndex", whoToIndex,
		)
		err = mc.combinedidx.Box2Reindex(whoToIndex)
		if err != nil {
			return err
		}

		// mark as indexed
		currentMembers[whoToIndex.String()] = true
	}

	err = mc.setMembers(badgerKey, currentMembers)
	if err != nil {
		return err
	}

	return nil
}
