package tests

import (
	"bytes"
	"encoding/hex"
	"math/rand"
	"testing"

	"github.com/lunfardo314/unitrie/common"
	"github.com/lunfardo314/unitrie/immutable"
	"github.com/lunfardo314/unitrie/models/trie_blake2b"
	"github.com/lunfardo314/unitrie/models/trie_blake2b/trie_blake2b_verify"
	"github.com/stretchr/testify/require"
)

// A node's path enters its commitment unhashed and zero-padded when it fits in
// the hash size, so a short key and the same key extended by zero bytes could
// commit identically. A model declaring a minimum key length long enough keeps
// that encoding (it is unambiguous there) and so its roots; any other model
// hashes every path.

// The mode follows from minimum key length, arity and hash size: paths stay
// unhashed only when every key has at least HashSize path units.
func TestMinKeyLengthMode(t *testing.T) {
	cases := []struct {
		arity  common.PathArity
		sz     trie_blake2b.HashSize
		minKey int
		hashed bool
	}{
		{common.PathArity16, trie_blake2b.HashSize192, 12, false}, // Proxima
		{common.PathArity16, trie_blake2b.HashSize192, 11, true},
		{common.PathArity16, trie_blake2b.HashSize192, 0, true},
		{common.PathArity16, trie_blake2b.HashSize256, 12, true},
		{common.PathArity16, trie_blake2b.HashSize256, 16, false},
		{common.PathArity256, trie_blake2b.HashSize192, 23, true},
		{common.PathArity256, trie_blake2b.HashSize192, 24, false},
		{common.PathArity2, trie_blake2b.HashSize192, 2, true},
		{common.PathArity2, trie_blake2b.HashSize192, 3, false},
	}
	for _, c := range cases {
		m := trie_blake2b.New(c.arity, c.sz, c.minKey)
		require.Equal(t, c.hashed, m.HashedPath(), "%s %s min %d", c.arity, c.sz, c.minKey)
		require.Equal(t, c.minKey, m.MinKeyLength())
	}
}

// Roots of the Proxima model (arity 16, 24-byte hashes, minimum key length 12)
// are the ones computed before the minimum existed: captured from that code on a
// Proxima-shaped state, keys of 33-35 bytes, values on both sides of the 62-byte
// threshold, some deleted.
func TestMinKeyLengthKeepsRoots(t *testing.T) {
	m := trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize192, 12)
	require.False(t, m.HashedPath())
	store := common.NewInMemoryKVStore()
	root := immutable.MustInitRoot(store, m, []byte("proxima-like identity"))
	require.Equal(t, "522e10283a049551de5f284fc91f16bb3e8dc0f13f2ea865", hex.EncodeToString(root.Bytes()))

	rnd := rand.New(rand.NewSource(42))
	tr, err := immutable.NewTrieUpdatable(m, store, root)
	require.NoError(t, err)
	var keys [][]byte
	for i := 0; i < 500; i++ {
		k := make([]byte, 33+rnd.Intn(3))
		k[0] = byte(rnd.Intn(3))
		for j := 1; j < len(k); j++ {
			k[j] = byte(rnd.Intn(256))
		}
		if i%7 == 0 && len(keys) > 0 { // share long prefixes with an earlier key
			copy(k[:20], keys[rnd.Intn(len(keys))][:20])
		}
		keys = append(keys, k)
		tr.Update(k, bytes.Repeat([]byte{byte(i)}, 1+rnd.Intn(120)))
	}
	root = tr.Commit(store)
	tr, err = immutable.NewTrieUpdatable(m, store, root)
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		tr.Delete(keys[rnd.Intn(len(keys))])
	}
	root = tr.Commit(store)
	require.Equal(t, "ba11537b10c27bebe4b5b8f57fa289a37410628c3f22364d", hex.EncodeToString(root.Bytes()))
}

// With short keys allowed, a key and the same key extended by a zero byte give
// different roots. Unhashed, they gave the same root.
func TestShortKeysDoNotCollide(t *testing.T) {
	m := trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize192, 0)
	rootOf := func(k []byte) common.VCommitment {
		store := common.NewInMemoryKVStore()
		root := immutable.MustInitRoot(store, m, []byte("identity"))
		tr, err := immutable.NewTrieUpdatable(m, store, root)
		require.NoError(t, err)
		tr.Update(k, []byte("value"))
		return tr.Commit(store)
	}
	for _, k := range [][]byte{{0x12}, {0x12, 0x34}, bytes.Repeat([]byte{0xab}, 10)} {
		require.False(t, m.EqualCommitments(rootOf(k), rootOf(append(bytes.Clone(k), 0))), "key %x", k)
	}
}

// A model refuses keys shorter than its minimum, so it never holds a key its
// path encoding could confuse.
func TestKeysBelowMinimumRefused(t *testing.T) {
	m := trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize192, 12)
	store := common.NewInMemoryKVStore()
	tr, err := immutable.NewTrieUpdatable(m, store, immutable.MustInitRoot(store, m, []byte("identity")))
	require.NoError(t, err)
	require.Panics(t, func() { tr.Update(make([]byte, 11), []byte("v")) })
	require.Panics(t, func() { tr.Delete(make([]byte, 11)) })
	require.NotPanics(t, func() { tr.Update(make([]byte, 12), []byte("v")) })
}

// Proofs carry the path mode. Those of an unhashed model keep their old encoding
// (no flag bit) and stay bound to their key; flipping the flag breaks a proof;
// an unhashed proof about a key too short for the encoding is refused.
func TestProofPathMode(t *testing.T) {
	build := func(minKey int, keys [][]byte) (*trie_blake2b.CommitmentModel, *immutable.TrieReader, []byte) {
		m := trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize192, minKey)
		store := common.NewInMemoryKVStore()
		tr, err := immutable.NewTrieUpdatable(m, store, immutable.MustInitRoot(store, m, []byte("identity")))
		require.NoError(t, err)
		for i, k := range keys {
			tr.Update(k, bytes.Repeat([]byte{byte(i + 1)}, 1+i*7))
		}
		root := tr.Commit(store)
		rdr, err := immutable.NewTrieReader(m, store, root)
		require.NoError(t, err)
		return m, rdr, root.Bytes()
	}

	rnd := rand.New(rand.NewSource(7))
	var long [][]byte
	for i := 0; i < 30; i++ {
		k := make([]byte, 33)
		rnd.Read(k)
		k[0] = 0
		long = append(long, k)
	}
	m, rdr, root := build(12, long)
	for i, k := range long {
		p := m.ProofImmutable(k, rdr)
		require.False(t, p.HashedPath)
		require.Equal(t, byte(trie_blake2b.HashSize192), p.Bytes()[1], "old encoding of the hash size byte")
		require.NoError(t, trie_blake2b_verify.ValidateWithTerminal(p, root, m.CommitToData(rdr.Get(k)).Bytes()))
		// re-labelled with another present key it does not prove that key's value
		other := long[(i+1)%len(long)]
		p.Key = common.UnpackBytes(other, p.PathArity)
		require.Error(t, trie_blake2b_verify.ValidateWithTerminal(p, root, m.CommitToData(rdr.Get(other)).Bytes()))
		// with the flag flipped the commitment does not match
		p = m.ProofImmutable(k, rdr)
		p.HashedPath = true
		require.Error(t, trie_blake2b_verify.Validate(p, root))
	}

	short := [][]byte{{0x12}, {0x12, 0x34}, {0x12, 0x34, 0x00}}
	m, rdr, root = build(0, short)
	for _, k := range short {
		p := m.ProofImmutable(k, rdr)
		require.True(t, p.HashedPath)
		p2, err := trie_blake2b.ProofFromBytes(p.Bytes())
		require.NoError(t, err)
		require.True(t, p2.HashedPath)
		require.NoError(t, trie_blake2b_verify.ValidateWithTerminal(p2, root, m.CommitToData(rdr.Get(k)).Bytes()))
		p2.HashedPath = false
		require.ErrorContains(t, trie_blake2b_verify.Validate(p2, root), "key too short")
	}
}
