package tests

import (
	"fmt"
	"testing"

	"github.com/lunfardo314/unitrie/common"
	"github.com/lunfardo314/unitrie/immutable"
	"github.com/lunfardo314/unitrie/models/trie_blake2b"
	"github.com/stretchr/testify/require"
)

// populateTrie creates a committed trie with n entries of the form key_NNNN -> value_NNNN.
// Returns the store and root commitment.
func populateTrie(t *testing.T, m common.CommitmentModel, n int) (common.KVStore, common.VCommitment) {
	t.Helper()
	store := common.NewInMemoryKVStore()
	root := immutable.MustInitRoot(store, m, []byte("identity"))
	tr, err := immutable.NewTrieChained(m, store, root)
	require.NoError(t, err)

	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key_%04d", i)
		v := fmt.Sprintf("value_%04d", i)
		tr.Update([]byte(k), []byte(v))
	}
	tr = tr.CommitChained()
	return store, tr.Root()
}

// TestCacheIterationSmallCache iterates a trie with many more entries than the cache size.
// Verifies that cache eviction does not lose or corrupt any data.
func TestCacheIterationSmallCache(t *testing.T) {
	const numEntries = 2000
	const cacheSize = 50

	m := trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize256)
	store, root := populateTrie(t, m, numEntries)

	tr, err := immutable.NewTrieReader(m, store, root, cacheSize)
	require.NoError(t, err)

	expected := make(map[string]string, numEntries)
	for i := 0; i < numEntries; i++ {
		expected[fmt.Sprintf("key_%04d", i)] = fmt.Sprintf("value_%04d", i)
	}

	count := 0
	tr.Iterate(func(k []byte, v []byte) bool {
		if len(k) == 0 {
			// root identity entry
			return true
		}
		ev, ok := expected[string(k)]
		require.True(t, ok, "unexpected key: %s", string(k))
		require.Equal(t, ev, string(v))
		delete(expected, string(k))
		count++
		return true
	})
	require.Equal(t, numEntries, count, "not all entries were iterated")
	require.Empty(t, expected, "some entries were missed during iteration")
}

// TestCacheDisabled verifies that caching can be fully disabled with clearCacheAtSize=0
// and all entries are still accessible.
func TestCacheDisabled(t *testing.T) {
	const numEntries = 500
	m := trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize256)
	store, root := populateTrie(t, m, numEntries)

	// clearCacheAtSize=0 means no caching
	tr, err := immutable.NewTrieReader(m, store, root, 0)
	require.NoError(t, err)

	count := 0
	tr.Iterate(func(k []byte, v []byte) bool {
		if len(k) == 0 {
			return true
		}
		count++
		return true
	})
	require.Equal(t, numEntries, count)

	// spot-check a few reads
	for i := 0; i < numEntries; i += 50 {
		k := fmt.Sprintf("key_%04d", i)
		v := tr.Get([]byte(k))
		require.Equal(t, fmt.Sprintf("value_%04d", i), string(v))
	}
}

// TestCacheHitsReturnCorrectData verifies that a cache hit returns the same data
// as a cache miss (i.e., the populated cache serves correct values).
func TestCacheHitsReturnCorrectData(t *testing.T) {
	const numEntries = 100
	m := trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize256)
	store, root := populateTrie(t, m, numEntries)

	// large cache so nothing gets evicted
	tr, err := immutable.NewTrieReader(m, store, root, 10000)
	require.NoError(t, err)

	for i := 0; i < numEntries; i++ {
		k := fmt.Sprintf("key_%04d", i)
		expected := fmt.Sprintf("value_%04d", i)

		// first read: cache miss, populates cache
		v1 := tr.Get([]byte(k))
		require.Equal(t, expected, string(v1))

		// second read: cache hit
		v2 := tr.Get([]byte(k))
		require.Equal(t, expected, string(v2))
	}
}

// TestCacheEvictionConsistency verifies that repeated full iterations with a small cache
// always produce the same results (cache eviction is deterministic and correct).
func TestCacheEvictionConsistency(t *testing.T) {
	const numEntries = 500
	const cacheSize = 30

	m := trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize256)
	store, root := populateTrie(t, m, numEntries)

	tr, err := immutable.NewTrieReader(m, store, root, cacheSize)
	require.NoError(t, err)

	// collect keys from first iteration
	var keys1 []string
	tr.IterateKeys(func(k []byte) bool {
		keys1 = append(keys1, string(k))
		return true
	})

	// collect keys from second iteration (cache is partially populated from first)
	var keys2 []string
	tr.IterateKeys(func(k []byte) bool {
		keys2 = append(keys2, string(k))
		return true
	})

	require.Equal(t, keys1, keys2, "iteration order must be deterministic across runs")
}
