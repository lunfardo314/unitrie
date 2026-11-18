package badger_adaptor

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func openTempDB(t *testing.T) *DB {
	opts := badger.DefaultOptions(t.TempDir()).WithLogger(nil)
	db := MustCreateOrOpenBadgerDB(opts.Dir, opts)
	t.Cleanup(func() { _ = db.Close() })
	return New(db)
}

// fill writes n records of equal-size values under one prefix: equal sizes let
// badger reuse an item's buffer in place, which is how a kept slice got overwritten.
func fill(t *testing.T, db *DB, prefix byte, n int) {
	batch := db.BatchedWriter()
	for i := 0; i < n; i++ {
		k := make([]byte, 5)
		k[0] = prefix
		binary.BigEndian.PutUint32(k[1:], uint32(i))
		batch.Set(k, bytes.Repeat([]byte{byte('A' + i)}, 64))
	}
	require.NoError(t, batch.Commit())
}

// Keys and values handed to the callbacks stay valid after the iteration moves on:
// badger reuses its item buffers a few items later, so the adaptor hands out copies.
func TestIterateHandsOutCopies(t *testing.T) {
	db := openTempDB(t)
	const n = 3 * iteratorPrefetchSize
	fill(t, db, 6, n)

	keys := make([][]byte, 0, n)
	values := make([][]byte, 0, n)
	db.Iterator([]byte{6}).Iterate(func(k, v []byte) bool {
		keys = append(keys, k)
		values = append(values, v)
		return true
	})
	keysOnly := make([][]byte, 0, n)
	db.Iterator([]byte{6}).IterateKeys(func(k []byte) bool {
		keysOnly = append(keysOnly, k)
		return true
	})
	require.Len(t, values, n)
	for i := 0; i < n; i++ {
		require.Equal(t, bytes.Repeat([]byte{byte('A' + i)}, 64), values[i], "value %d", i)
		require.EqualValues(t, i, binary.BigEndian.Uint32(keys[i][1:]), "key %d", i)
		require.Equal(t, keys[i], keysOnly[i], "key-only %d", i)
	}
}

// A panic in the callback is raised to the caller, not swallowed as an ended iteration.
func TestIteratePropagatesPanic(t *testing.T) {
	db := openTempDB(t)
	fill(t, db, 6, 5)
	errCallback := errors.New("callback failed")

	visited := 0
	require.Panics(t, func() {
		db.Iterator([]byte{6}).Iterate(func(k, v []byte) bool {
			visited++
			if visited == 2 {
				panic(errCallback)
			}
			return true
		})
	})
	require.Panics(t, func() {
		db.Iterator([]byte{6}).IterateKeys(func(k []byte) bool { panic(errCallback) })
	})
	// stopping early on purpose is not an error
	visited = 0
	require.NotPanics(t, func() {
		db.Iterator([]byte{6}).Iterate(func(k, v []byte) bool { visited++; return false })
	})
	require.Equal(t, 1, visited)
}
