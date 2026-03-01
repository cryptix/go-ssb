// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package keys

import (
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/ssbc/go-ssb/repo"
	"github.com/stretchr/testify/require"
)

type opIndexNew struct {
	DB    **badger.DB
	Store **Store
}

func (op opIndexNew) Do(t *testing.T, env interface{}) {
	*op.Store = NewStore(*op.DB, nil)
	require.NotNil(t, *op.Store, "NewStore returned nil")
}

type opIndexGet struct {
	Store  **Store
	Scheme KeyScheme
	ID     ID

	ExpValue  Recipients
	ExpGetErr string
}

func (op opIndexGet) Do(t *testing.T, env interface{}) {
	recps, err := (*op.Store).GetKeys(op.Scheme, op.ID)
	if op.ExpGetErr == "" {
		require.NoError(t, err, "unexpected error on store.GetKeys")
	} else {
		require.EqualError(t, err, op.ExpGetErr, "expected different error on store.GetKeys")
		return
	}

	require.Equal(t, op.ExpValue, recps, "wrong value")
}

type opDBCreate struct {
	Name string

	ExpErr string
	DB     **badger.DB
}

func (op opDBCreate) Do(t *testing.T, env interface{}) {
	var err error

	*(op.DB), err = repo.OpenBadgerDB(op.Name)
	if op.ExpErr == "" {
		require.NoError(t, err, "unexpected error on db create")
	} else {
		require.EqualError(t, err, op.ExpErr, "expected different error on db create")
	}
}

type opDBGet struct {
	DB  **badger.DB
	Key []byte

	Log bool

	ExpValue []byte
	ExpErr   string
}

func (op opDBGet) Do(t *testing.T, env interface{}) {
	(*op.DB).View(func(txn *badger.Txn) error {
		val, err := txn.Get(op.Key)
		if op.ExpErr == "" {
			require.NoError(t, err, "error getting value from db")
		} else {
			require.EqualErrorf(t, err, op.ExpErr, "expected error getting value from db %q but got: %v", op.ExpErr, err)
			return nil
		}
		data, err := val.ValueCopy(nil)
		if err != nil {
			require.NoError(t, err, "did not get value")
			return nil
		}
		if op.Log {
			t.Logf("DB.Get - Key:%x Value:%x Exp:%x", op.Key, data, op.ExpValue)
		}
		require.Equal(t, op.ExpValue, data, "read wrong value from db")
		return nil
	})

}
