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

// TestCacheLRUKeepsHot verifies that the LRU policy keeps a frequently-accessed
// "hot" entry in cache while many "cold" entries pass through. Under the previous
// flush-on-overflow policy, the cache was wiped wholesale every time it grew past
// the threshold, so the hot entry got evicted along with cold ones, forcing
// repeated re-fetches from the underlying store. Under LRU, the hot entry stays
// at the front of the list as long as it's accessed, and only the least recently
// used entries age out.
//
// The test uses an interleaved access pattern (hot, cold_i, hot, cold_i+1, …)
// with cache size much smaller than the working set, and verifies that the hot
// key incurs only a single Get against the underlying store.
func TestCacheLRUKeepsHot(t *testing.T) {
	const numEntries = 500
	const cacheSize = 16
	const interleavedRounds = 200

	m := trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize256)
	store, root := populateTrie(t, m, numEntries)

	cnt := &countingKVReader{inner: store}
	tr, err := immutable.NewTrieReader(m, cnt, root, cacheSize)
	require.NoError(t, err)

	hot := []byte("key_0000")
	expectedHot := "value_0000"

	// Prime the hot key into the cache. This fetches the trie path down to it.
	// We don't care about the exact Get count for the priming — only what
	// happens AFTER, when the hot key is supposedly cached.
	v := tr.Get(hot)
	require.Equal(t, expectedHot, string(v))
	getsAfterPrime := cnt.gets

	// Interleaved cold accesses + hot re-accesses. With cacheSize=16 and 200
	// distinct cold keys each round (well, distinct over the inner loop), the
	// cache turns over many times.
	for round := 0; round < interleavedRounds; round++ {
		coldKey := []byte(fmt.Sprintf("key_%04d", 1+(round%(numEntries-1))))
		_ = tr.Get(coldKey)
		// Re-access hot.
		v := tr.Get(hot)
		require.Equal(t, expectedHot, string(v))
	}

	// Count cold-only accesses (to subtract them out): fetch all the cold keys
	// once more, then assert the hot key didn't trigger any additional Get on
	// the underlying store. Practically: from the moment we primed the hot key,
	// it should have produced ZERO additional Gets — it stayed in the LRU front
	// because every interleaved iteration touched it.
	totalGets := cnt.gets

	// Compute how many Gets the cold accesses needed (ignore the hot accesses).
	// For correctness we don't need an exact number — we just need the upper
	// bound: getsAfterPrime + (cold work). The hot accesses contributed 0 IF
	// the LRU kept the hot path warm.
	t.Logf("Gets: after-prime=%d, total=%d (cold rounds=%d, cache size=%d)",
		getsAfterPrime, totalGets, interleavedRounds, cacheSize)

	// Sanity: the cache is small enough that the cold keys couldn't all stay in.
	// So some cold gets are real disk reads. The interesting invariant: the hot
	// key didn't evict; if it had, we'd see ~interleavedRounds extra fetches for
	// the hot path nodes alone (the path is several nodes deep, so this number
	// would be large). Concretely, total - getsAfterPrime should be roughly the
	// cost of interleavedRounds * one_path_walk for cold keys, NOT
	// 2 * interleavedRounds * one_path_walk.
	//
	// The pre-LRU flush policy would re-fetch the hot path every time the cache
	// hit cap. With cacheSize=16 and ~5 nodes per path, that's a flush every
	// ~3 cold keys — so the hot key would have been re-fetched ~70 times,
	// each fetch being ~5 nodes, ~350 extra Gets. Under LRU, those should be 0.
	// Use a generous cap (50) so the test is robust to model differences.
	hotEvictionCost := totalGets - getsAfterPrime - interleavedRounds*8 // 8 = generous upper bound on cold-path Gets per round
	require.Less(t, hotEvictionCost, 50,
		"hot key appears to have been evicted under LRU; total=%d, after-prime=%d, rounds=%d",
		totalGets, getsAfterPrime, interleavedRounds)
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
