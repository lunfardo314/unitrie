package tests

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/lunfardo314/unitrie/common"
	"github.com/lunfardo314/unitrie/immutable"
	"github.com/lunfardo314/unitrie/models/trie_blake2b"
	"github.com/lunfardo314/unitrie/models/trie_blake2b/trie_blake2b_verify"
	"github.com/stretchr/testify/require"
)

// The proof verifier must bind a proof to its key. The child index of the last
// proof element and the key's tail beyond the last path fragment enter no hash,
// so without explicit checks an absence proof could be turned into presence,
// presence into absence, and a proof re-labelled with another key, all against
// an honest root. These tests build tries whose keys prefix each other, so all
// three proof endings occur (terminal, split, extend), and check that whatever a
// tampered proof makes the verifier accept agrees with what the trie holds.

type proofFixture struct {
	m       *trie_blake2b.CommitmentModel
	trr     *immutable.TrieReader
	root    []byte
	present [][]byte
	absent  [][]byte
}

// newProofFixture builds a trie of random short keys, many of them prefixes of
// others, with values on both sides of the 62-byte threshold (value in the
// terminal vs hashed terminal), and lists keys absent from it.
func newProofFixture(t *testing.T, arity common.PathArity, sz trie_blake2b.HashSize, seed int64) *proofFixture {
	rnd := rand.New(rand.NewSource(seed))
	m := trie_blake2b.New(arity, sz, 0)
	store := common.NewInMemoryKVStore()
	root := immutable.MustInitRoot(store, m, []byte("identity"))
	tr, err := immutable.NewTrieUpdatable(m, store, root)
	require.NoError(t, err)

	randKey := func() []byte {
		k := make([]byte, 1+rnd.Intn(4))
		for i := range k {
			k[i] = byte(rnd.Intn(4)) * 0x11 // few distinct bytes: many shared prefixes
		}
		return k
	}
	inTrie := make(map[string]bool)
	for i := 0; i < 60; i++ {
		k := randKey()
		v := bytes.Repeat([]byte{byte(i + 1)}, 1+rnd.Intn(100))
		tr.Update(k, v)
		inTrie[string(k)] = true
	}
	root = tr.Commit(store)
	trr, err := immutable.NewTrieReader(m, store, root)
	require.NoError(t, err)

	f := &proofFixture{m: m, trr: trr, root: root.Bytes()}
	for k := range inTrie {
		f.present = append(f.present, []byte(k))
	}
	for i := 0; i < 200; i++ {
		if k := randKey(); !inTrie[string(k)] {
			f.absent = append(f.absent, k)
		}
	}
	require.NotEmpty(t, f.absent)
	return f
}

// claimOf returns what a validated proof claims: the key and the terminal (nil for absence).
func claimOf(p *trie_blake2b.MerkleProof) (key, terminal []byte) {
	return trie_blake2b_verify.MustKeyWithTerminal(p)
}

// requireTruthful checks that a proof which validates claims exactly what the trie holds for its key.
func (f *proofFixture) requireTruthful(t *testing.T, p *trie_blake2b.MerkleProof, msg string) {
	if trie_blake2b_verify.Validate(p, f.root) != nil {
		return
	}
	key, terminal := claimOf(p)
	packed, err := common.PackUnpackedBytes(key, p.PathArity)
	require.NoError(t, err)
	value := f.trr.Get(packed)
	if len(value) == 0 {
		require.Nil(t, terminal, "%s: validated proof claims presence of an absent key %x", msg, packed)
		return
	}
	require.NotNil(t, terminal, "%s: validated proof claims absence of a present key %x", msg, packed)
	expected, _ := trie_blake2b.CompressToHashSize(f.m.CommitToData(value).Bytes(), p.HashSize)
	require.Equal(t, expected, terminal, "%s: validated proof claims a wrong value for key %x", msg, packed)
}

func forAllModels(t *testing.T, fun func(t *testing.T, f *proofFixture)) {
	for _, arity := range common.AllPathArity {
		for _, sz := range []trie_blake2b.HashSize{trie_blake2b.HashSize192, trie_blake2b.HashSize256} {
			for seed := int64(0); seed < 5; seed++ {
				fun(t, newProofFixture(t, arity, sz, seed))
			}
		}
	}
}

// Honest proofs of present and absent keys validate and claim the truth.
func TestProofHonest(t *testing.T) {
	forAllModels(t, func(t *testing.T, f *proofFixture) {
		for _, k := range f.present {
			p := f.m.ProofImmutable(k, f.trr)
			require.NoError(t, trie_blake2b_verify.ValidateWithTerminal(p, f.root, f.m.CommitToData(f.trr.Get(k)).Bytes()))
			require.False(t, trie_blake2b_verify.IsProofOfAbsence(p))
		}
		for _, k := range f.absent {
			p := f.m.ProofImmutable(k, f.trr)
			require.NoError(t, trie_blake2b_verify.Validate(p, f.root))
			require.True(t, trie_blake2b_verify.IsProofOfAbsence(p))
		}
	})
}

// Every child index on the last element: a proof that still validates must not
// change the answer (absence into presence, presence into absence).
func TestProofLastChildIndexTampering(t *testing.T) {
	forAllModels(t, func(t *testing.T, f *proofFixture) {
		for _, k := range append(append([][]byte{}, f.present...), f.absent...) {
			for idx := 0; idx <= f.m.PathArity().PathCommitmentIndex()+1; idx++ {
				p := f.m.ProofImmutable(k, f.trr)
				p.Path[len(p.Path)-1].ChildIndex = idx
				f.requireTruthful(t, p, "last child index tampered")
			}
		}
	})
}

// A proof re-labelled with any other key must not validate as a claim about that key
// other than the truth.
func TestProofKeyRelabel(t *testing.T) {
	forAllModels(t, func(t *testing.T, f *proofFixture) {
		all := append(append([][]byte{}, f.present...), f.absent...)
		for _, k := range all {
			for _, other := range all {
				p := f.m.ProofImmutable(k, f.trr)
				p.Key = common.UnpackBytes(other, p.PathArity)
				f.requireTruthful(t, p, "key re-labelled")
			}
		}
	})
}

// A child index on an inner element that does not follow the key is rejected.
func TestProofInnerChildIndexMustFollowKey(t *testing.T) {
	f := newProofFixture(t, common.PathArity16, trie_blake2b.HashSize192, 1)
	for _, k := range f.present {
		p := f.m.ProofImmutable(k, f.trr)
		if len(p.Path) < 2 {
			continue
		}
		p.Path[0].ChildIndex = (p.Path[0].ChildIndex + 1) % common.PathArity16.NumChildren()
		require.Error(t, trie_blake2b_verify.Validate(p, f.root))
	}
}

// Malformed proof bytes return an error from ProofFromBytes, never a panic: proofs
// come from untrusted sources. Every truncation and every single-byte change of
// honest proofs is tried, plus the encodings which used to panic in decode16/decode2.
func TestProofFromBytesNoPanic(t *testing.T) {
	forAllModels(t, func(t *testing.T, f *proofFixture) {
		for _, k := range append(f.present[:5], f.absent[:5]...) {
			data := f.m.ProofImmutable(k, f.trr).Bytes()
			for i := 0; i < len(data); i++ {
				require.NotPanics(t, func() { _, _ = trie_blake2b.ProofFromBytes(data[:i]) })
				for _, b := range []byte{0x00, 0x01, 0x07, 0x80, 0xff, data[i] ^ 0x01} {
					mutated := bytes.Clone(data)
					mutated[i] = b
					require.NotPanics(t, func() {
						if p, err := trie_blake2b.ProofFromBytes(mutated); err == nil {
							_ = trie_blake2b_verify.Validate(p, f.root)
						}
					})
				}
			}
		}
	})
	for _, arity := range []byte{byte(common.PathArity16), byte(common.PathArity2)} {
		// arity, hash size, key: 1-byte encoding claiming padding with no data byte
		_, err := trie_blake2b.ProofFromBytes([]byte{arity, byte(trie_blake2b.HashSize192), 0x01, 0x00, 0x01})
		require.Error(t, err)
	}
	_, err := trie_blake2b.ProofFromBytes([]byte{0x05, byte(trie_blake2b.HashSize192), 0x00, 0x00, 0x00, 0x00})
	require.Error(t, err, "unknown arity")
}

// A proof has one encoding: undefined element flag bits are rejected.
func TestProofUndefinedFlagsRejected(t *testing.T) {
	f := newProofFixture(t, common.PathArity16, trie_blake2b.HashSize192, 2)
	p := f.m.ProofImmutable(f.present[0], f.trr)
	data := p.Bytes()
	p2, err := trie_blake2b.ProofFromBytes(data)
	require.NoError(t, err)
	require.Equal(t, data, p2.Bytes())

	// the flags byte of the first element follows arity, hash size, key and path count
	// and the element's own path fragment and child index
	encodedKey, err := common.EncodeUnpackedBytes(p.Key, p.PathArity)
	require.NoError(t, err)
	encodedFragment, err := common.EncodeUnpackedBytes(p.Path[0].PathFragment, p.PathArity)
	require.NoError(t, err)
	flagsPos := 2 + 2 + len(encodedKey) + 2 + 2 + len(encodedFragment) + 2
	mutated := bytes.Clone(data)
	mutated[flagsPos] |= 0x04
	_, err = trie_blake2b.ProofFromBytes(mutated)
	require.Error(t, err)
}
