package tests

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"

	"github.com/lunfardo314/unitrie/common"
	"github.com/lunfardo314/unitrie/immutable"
	"github.com/lunfardo314/unitrie/models/trie_blake2b"
	"github.com/stretchr/testify/require"
)

// Regression tests of the trie update engine with the commitment model Proxima
// uses for its ledger state (arity 16, blake2b-192). Every node computing the
// root of a mutation set must get the same root whatever the history behind it
// (order, batching, cache size), or the nodes split.

var engineModel = trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize192, 0)

type engineOp struct{ k, v []byte }

func engineNewStore() (common.KVStore, common.VCommitment) {
	store := common.NewInMemoryKVStore()
	return store, immutable.MustInitRoot(store, engineModel, []byte("identity"))
}

// commitEngineOps applies ops to a fresh updatable over root and commits into store.
func commitEngineOps(t *testing.T, store common.KVStore, root common.VCommitment, ops []engineOp, cacheSize int) common.VCommitment {
	tr, err := immutable.NewTrieUpdatable(engineModel, store, root, cacheSize)
	require.NoError(t, err)
	for _, op := range ops {
		tr.Update(op.k, op.v)
	}
	return tr.Commit(store)
}

// rootOfSet builds the trie of a key/value set from scratch in one batch.
func rootOfSet(t *testing.T, set map[string][]byte) common.VCommitment {
	store, root := engineNewStore()
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	ops := make([]engineOp, 0, len(keys))
	for _, k := range keys {
		ops = append(ops, engineOp{[]byte(k), set[k]})
	}
	return commitEngineOps(t, store, root, ops, 0)
}

// genEngineKey draws keys over a few prefixes from a small alphabet, so shared
// paths, last-nibble differences and keys prefixing other keys are frequent.
func genEngineKey(rnd *rand.Rand) []byte {
	prefixes := [][]byte{{}, {0x00}, {0x01}, {0x10}, {0x01, 0x00}, {0xab, 0xcd}, bytes.Repeat([]byte{0x00}, 5)}
	k := append([]byte{}, prefixes[rnd.Intn(len(prefixes))]...)
	n := rnd.Intn(6)
	if rnd.Intn(10) == 0 {
		n = 20 + rnd.Intn(20) // occasionally long keys, like Proxima's 33-35 byte keys
	}
	for i := 0; i < n; i++ {
		k = append(k, []byte{0x00, 0x01, 0x0f, 0x10, 0xf0, 0xff}[rnd.Intn(6)])
	}
	if len(k) == 0 {
		k = []byte{0x01}
	}
	return k
}

// genEngineValue draws values around the 24-byte hash size and the 62-byte
// content-addressing threshold, from a small alphabet so equal large values
// (one shared value record) are frequent. Nil means delete.
func genEngineValue(rnd *rand.Rand) []byte {
	n := []int{0, 0, 1, 2, 23, 24, 25, 61, 62, 63, 64, 100, 300}[rnd.Intn(13)]
	if n == 0 {
		return nil
	}
	v := bytes.Repeat([]byte{byte(rnd.Intn(3))}, n)
	if rnd.Intn(2) == 0 {
		v[0] ^= byte(rnd.Intn(4))
	}
	return v
}

// Random sequences of updates, deletes (also of absent keys) and prefix deletes,
// cut into batches at random and run with random cache sizes, must give the root
// of the final set built from scratch, and every read path must agree with it.
func TestEngineDifferential(t *testing.T) {
	const seeds = 200
	for seed := 0; seed < seeds; seed++ {
		rnd := rand.New(rand.NewSource(int64(seed)))
		store, root := engineNewStore()
		ref := make(map[string][]byte)
		cacheSize := []int{0, 1, 3, 1000}[rnd.Intn(4)]

		tr, err := immutable.NewTrieUpdatable(engineModel, store, root, cacheSize)
		require.NoError(t, err)
		nOps := 50 + rnd.Intn(400)
		for i := 0; i < nOps; i++ {
			k := genEngineKey(rnd)
			switch r := rnd.Intn(20); {
			case r == 0:
				p := k[:1+rnd.Intn(len(k))]
				deleted := tr.DeletePrefix(p)
				expected := false
				for rk := range ref {
					if bytes.HasPrefix([]byte(rk), p) {
						delete(ref, rk)
						expected = true
					}
				}
				require.Equal(t, expected, deleted, "seed %d: DeletePrefix(%x) result", seed, p)
			case r < 7:
				_, existed := ref[string(k)]
				require.Equal(t, existed, tr.Delete(k), "seed %d: Delete(%x) result", seed, k)
				delete(ref, string(k))
			default:
				v := genEngineValue(rnd)
				tr.Update(k, v)
				if len(v) == 0 {
					delete(ref, string(k))
				} else {
					ref[string(k)] = bytes.Clone(v)
				}
			}
			if rnd.Intn(15) == 0 {
				root = tr.Commit(store)
				tr, err = immutable.NewTrieUpdatable(engineModel, store, root, cacheSize)
				require.NoError(t, err)
			}
		}
		root = tr.Commit(store)
		require.True(t, engineModel.EqualCommitments(root, rootOfSet(t, ref)), "seed %d: root differs from rebuild", seed)

		rdr, err := immutable.NewTrieReader(engineModel, store, root, cacheSize)
		require.NoError(t, err)
		for k, v := range ref {
			require.Equal(t, v, rdr.Get([]byte(k)), "seed %d key %x", seed, k)
		}
		got := make(map[string][]byte)
		rdr.Iterate(func(k, v []byte) bool {
			if len(k) > 0 { // the empty key is the root identity
				got[string(k)] = v
			}
			return true
		})
		require.Equal(t, len(ref), len(got), "seed %d", seed)
		for k, v := range ref {
			require.Equal(t, v, got[k], "seed %d key %x", seed, k)
		}
	}
}

// Deleting a key at which a branch node without a terminal sits reports it absent
// and leaves the root unchanged, through Delete and through Update with nil.
func TestEngineDeleteAbsentAtBranchNode(t *testing.T) {
	store, root := engineNewStore()
	// two keys extending 0x0102: the node at 0x0102 is a branch with no terminal
	root = commitEngineOps(t, store, root, []engineOp{{[]byte{1, 2, 0x03}, []byte("a")}, {[]byte{1, 2, 0x13}, []byte("b")}}, 0)

	tr, err := immutable.NewTrieUpdatable(engineModel, store, root)
	require.NoError(t, err)
	require.False(t, tr.Has([]byte{1, 2}))
	require.False(t, tr.Delete([]byte{1, 2}))
	require.False(t, tr.Update([]byte{1, 2}, nil))
	require.True(t, engineModel.EqualCommitments(root, tr.Commit(store)))
}

// DeletePrefix on a key added in the same batch deletes it, as it does after a commit in between.
func TestEngineDeletePrefixSameBatch(t *testing.T) {
	store, root := engineNewStore()
	root = commitEngineOps(t, store, root, []engineOp{{[]byte{0x10}, []byte("a")}}, 0)

	tr, err := immutable.NewTrieUpdatable(engineModel, store, root)
	require.NoError(t, err)
	tr.Update([]byte{0x20, 0x30, 0x40}, []byte("b"))
	require.True(t, tr.DeletePrefix([]byte{0x20, 0x30}))
	require.True(t, engineModel.EqualCommitments(root, tr.Commit(store)))
}

// DeletePrefix after a split in the same batch does not delete keys outside the prefix:
// the split node's persisted path fragment matches the prefix, its current one does not.
func TestEngineDeletePrefixAfterSplit(t *testing.T) {
	store, root := engineNewStore()
	root = commitEngineOps(t, store, root, []engineOp{{[]byte{0x12, 0x34, 0x56}, []byte("victim")}, {[]byte{0xF0}, []byte("z")}}, 0)

	tr, err := immutable.NewTrieUpdatable(engineModel, store, root)
	require.NoError(t, err)
	tr.Update([]byte{0x12, 0x99}, []byte("other")) // splits the leaf of 0x123456
	require.False(t, tr.DeletePrefix([]byte{0x12, 0x32, 0x34}))
	rdr, err := immutable.NewTrieReader(engineModel, store, tr.Commit(store))
	require.NoError(t, err)
	require.Equal(t, []byte("victim"), rdr.Get([]byte{0x12, 0x34, 0x56}))
}

// A caller writing into a value returned by Get or Iterate cannot change what a
// later commit hashes: short values live in the cached node itself.
func TestEngineReturnedValuesAreCopies(t *testing.T) {
	store, root := engineNewStore()
	k := []byte{0x12, 0x34}
	root = commitEngineOps(t, store, root, []engineOp{{k, []byte("original")}, {[]byte{0xF0}, []byte("z")}}, 0)

	run := func(cacheSize int, scribble bool) common.VCommitment {
		tr, err := immutable.NewTrieUpdatable(engineModel, store, root, cacheSize)
		require.NoError(t, err)
		if scribble {
			copy(tr.Get(k), "MUTATED!")
			tr.Iterate(func(_, v []byte) bool { copy(v, "MUTATED!"); return true })
		}
		tr.Update([]byte{0x12, 0x34, 0x56}, []byte("child")) // the node at k is committed again
		return tr.Commit(store)
	}
	expected := run(0, false)
	require.True(t, engineModel.EqualCommitments(expected, run(1000, true)))
	require.True(t, engineModel.EqualCommitments(expected, run(0, true)))
}

// Concurrent reads through one TrieReader are safe: an LRU cache hit reorders the
// cache list. Meaningful under -race.
func TestEngineConcurrentReaders(t *testing.T) {
	store, root := engineNewStore()
	ops := make([]engineOp, 0, 200)
	for i := 0; i < 200; i++ {
		ops = append(ops, engineOp{[]byte(fmt.Sprintf("key%03d", i)), []byte("v")})
	}
	root = commitEngineOps(t, store, root, ops, 0)
	rdr, err := immutable.NewTrieReader(engineModel, store, root, 50)
	require.NoError(t, err)

	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				if string(rdr.Get([]byte(fmt.Sprintf("key%03d", i%200)))) != "v" {
					panic("wrong value")
				}
			}
		}()
	}
	wg.Wait()
}

// A key of MaxKeyLength bytes commits and reads back; a longer one is refused at
// Update, not by a panic deep in Commit with the batch half built.
func TestEngineMaxKeyLength(t *testing.T) {
	store, root := engineNewStore()
	key := bytes.Repeat([]byte{0xab}, immutable.MaxKeyLength)
	root = commitEngineOps(t, store, root, []engineOp{{key, []byte("v")}, {[]byte{0x01}, []byte("w")}}, 0)
	rdr, err := immutable.NewTrieReader(engineModel, store, root)
	require.NoError(t, err)
	require.Equal(t, []byte("v"), rdr.Get(key))

	tr, err := immutable.NewTrieUpdatable(engineModel, store, root)
	require.NoError(t, err)
	require.Panics(t, func() { tr.Update(append(key, 0), []byte("v")) })
}
