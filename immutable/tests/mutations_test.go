package tests

import (
	"bytes"
	"testing"

	"github.com/lunfardo314/unitrie/common"
	"github.com/lunfardo314/unitrie/immutable"
	"github.com/lunfardo314/unitrie/models/trie_blake2b"
	"github.com/stretchr/testify/require"
)

func TestMutations(t *testing.T) {
	t.Run("1", func(t *testing.T) {
		mut := common.NewMutations()
		require.EqualValues(t, 0, mut.LenSet())
		require.EqualValues(t, 0, mut.LenDel())

		mut.Set([]byte("a"), []byte("1"))
		mut.Set([]byte("ab"), []byte("2"))
		mut.Set([]byte("a"), nil)
		mut.Set([]byte("abc"), []byte("3"))
		require.EqualValues(t, 3, mut.LenSet())
		require.EqualValues(t, 1, mut.LenDel())

		s := common.NewInMemoryKVStore()
		mut.WriteTo(s)
		require.EqualValues(t, 2, s.Len())
		require.EqualValues(t, []byte("2"), s.Get([]byte("ab")))
		require.EqualValues(t, []byte("3"), s.Get([]byte("abc")))
		require.False(t, s.Has([]byte("a")))

		mut = common.NewMutations()
		mut.Set([]byte("abc"), nil)
		mut.Set([]byte("a"), []byte("1"))
		mut.WriteTo(s)
		require.EqualValues(t, 2, s.Len())
		require.False(t, s.Has([]byte("abc")))
		require.EqualValues(t, []byte("1"), s.Get([]byte("a")))
		require.EqualValues(t, []byte("2"), s.Get([]byte("ab")))
	})
	t.Run("2", func(t *testing.T) {
		mut := common.NewMutationsMustNoDoubleBooking()
		mut.Set([]byte("abc"), nil)
		mut.Set([]byte("a"), []byte("1"))
		// idempotent write: same key, same value — should NOT panic
		mut.Set([]byte("a"), []byte("1"))
		// conflicting write: same key, different value — should panic
		common.RequirePanicOrErrorWith(t, func() error {
			mut.Set([]byte("a"), []byte("2"))
			return nil
		}, "conflicting SET mutation")
		mut.Set([]byte("a"), nil)
		common.RequirePanicOrErrorWith(t, func() error {
			mut.Set([]byte("a"), nil)
			return nil
		}, "repetitive DEL mutation")
	})
	t.Run("3", func(t *testing.T) {
		mut1 := common.NewMutationsMustNoDoubleBooking()
		mut1.Set([]byte("abc"), nil)
		mut1.Set([]byte("a"), []byte("1"))
		mut2 := common.NewMutationsMustNoDoubleBooking()
		mut2.Set([]byte("ab"), []byte("3"))
		mut2.Set([]byte("a"), nil)
		mut2.WriteTo(mut1)
	})
	t.Run("4", func(t *testing.T) {
		mut1 := common.NewMutationsMustNoDoubleBooking()
		mut1.Set([]byte("abc"), nil)
		mut1.Set([]byte("a"), []byte("1"))

		mut2 := common.NewMutationsMustNoDoubleBooking()
		mut2.Set([]byte("ab"), []byte("3"))
		mut2.Set([]byte("a"), nil)
		mut2.WriteTo(mut1)
		// second WriteTo: SET is idempotent (same values), but DEL still panics
		common.RequirePanicOrErrorWith(t, func() error {
			mut2.WriteTo(mut1)
			return nil
		}, "repetitive DEL mutation")

		mut3 := common.NewMutationsMustNoDoubleBooking()
		mut3.Set([]byte("abc"), nil)
		common.RequirePanicOrErrorWith(t, func() error {
			mut3.WriteTo(mut1)
			return nil
		}, "repetitive DEL mutation")
		t.Logf("\n%s", mut1.String())
	})
	t.Run("idempotent SET same value", func(t *testing.T) {
		mut := common.NewMutationsMustNoDoubleBooking()
		v := []byte("hello")
		mut.Set([]byte("k"), v)
		// same key, same value — idempotent, no panic
		mut.Set([]byte("k"), v)
		require.EqualValues(t, 1, mut.LenSet())
	})
	t.Run("conflicting SET different value", func(t *testing.T) {
		mut := common.NewMutationsMustNoDoubleBooking()
		mut.Set([]byte("k"), []byte("v1"))
		common.RequirePanicOrErrorWith(t, func() error {
			mut.Set([]byte("k"), []byte("v2"))
			return nil
		}, "conflicting SET mutation")
	})
	t.Run("iterate", func(t *testing.T) {
		mut := common.NewMutationsMustNoDoubleBooking()
		mut.Set([]byte("a"), nil)
		mut.Iterate(func(k []byte, v []byte, wasSet bool) bool {
			require.False(t, wasSet)
			return true
		})
		mut = common.NewMutationsMustNoDoubleBooking()
		mut.Set([]byte("a"), []byte("a"))
		mut.Set([]byte("a"), nil)
		mut.Set([]byte("c"), nil)
		mut.Set([]byte("b"), []byte("b"))
		mut.Iterate(func(k []byte, v []byte, wasSet bool) bool {
			if string(k) == "c" {
				require.False(t, wasSet)
			} else {
				require.True(t, wasSet)
			}
			return true
		})
	})
}

func TestCommitIdenticalLargeValues(t *testing.T) {
	// Regression test: two trie keys with identical large values (>62 bytes)
	// must not panic during Commit. The value partition is content-addressed,
	// so the second write is an idempotent duplicate.
	largeValue := bytes.Repeat([]byte{0xAB}, 100) // >62 bytes, forces value store write

	t.Run("via mutations batch", func(t *testing.T) {
		// This is the exact path that triggers the bug: Commit writes into a
		// MustNoDoubleBooking mutations batch (as the Badger adaptor does).
		store := common.NewInMemoryKVStore()
		m := trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize256)
		root := immutable.MustInitRoot(store, m, []byte("test"))
		tr, err := immutable.NewTrieUpdatable(m, store, root)
		require.NoError(t, err)

		tr.Update([]byte("key1"), largeValue)
		tr.Update([]byte("key2"), largeValue)

		batch := common.NewMutationsMustNoDoubleBooking()
		newRoot := tr.Commit(batch) // must not panic
		batch.WriteTo(store)

		tr2, err := immutable.NewTrieUpdatable(m, store, newRoot)
		require.NoError(t, err)
		require.Equal(t, largeValue, tr2.Get([]byte("key1")))
		require.Equal(t, largeValue, tr2.Get([]byte("key2")))
	})
	t.Run("three identical values arity2", func(t *testing.T) {
		store := common.NewInMemoryKVStore()
		m := trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize160)
		root := immutable.MustInitRoot(store, m, []byte("test"))
		tr, err := immutable.NewTrieUpdatable(m, store, root)
		require.NoError(t, err)

		tr.Update([]byte("a"), largeValue)
		tr.Update([]byte("b"), largeValue)
		tr.Update([]byte("c"), largeValue)

		batch := common.NewMutationsMustNoDoubleBooking()
		newRoot := tr.Commit(batch)
		batch.WriteTo(store)

		tr2, err := immutable.NewTrieUpdatable(m, store, newRoot)
		require.NoError(t, err)
		require.Equal(t, largeValue, tr2.Get([]byte("a")))
		require.Equal(t, largeValue, tr2.Get([]byte("b")))
		require.Equal(t, largeValue, tr2.Get([]byte("c")))
	})
	t.Run("small values embedded", func(t *testing.T) {
		// Small values (<= 62 bytes) are embedded in the commitment, no value store write.
		// This already worked before the fix — just verifying.
		store := common.NewInMemoryKVStore()
		m := trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize256)
		root := immutable.MustInitRoot(store, m, []byte("test"))
		tr, err := immutable.NewTrieUpdatable(m, store, root)
		require.NoError(t, err)

		smallValue := []byte("small")
		tr.Update([]byte("key1"), smallValue)
		tr.Update([]byte("key2"), smallValue)

		batch := common.NewMutationsMustNoDoubleBooking()
		newRoot := tr.Commit(batch)
		batch.WriteTo(store)

		tr2, err := immutable.NewTrieUpdatable(m, store, newRoot)
		require.NoError(t, err)
		require.Equal(t, smallValue, tr2.Get([]byte("key1")))
		require.Equal(t, smallValue, tr2.Get([]byte("key2")))
	})
}
