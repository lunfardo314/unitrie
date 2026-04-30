package tests

import (
	"bytes"
	"strings"
	"testing"

	"github.com/lunfardo314/unitrie/common"
	"github.com/lunfardo314/unitrie/immutable"
	"github.com/lunfardo314/unitrie/models/trie_blake2b"
	"github.com/lunfardo314/unitrie/models/trie_blake2b/trie_blake2b_verify"
	"github.com/stretchr/testify/require"
)

func TestHashSize192String(t *testing.T) {
	require.Equal(t, "HashSize(192)", trie_blake2b.HashSize192.String())
}

func TestHashSize192IncludedInAllHashSize(t *testing.T) {
	found := false
	for _, hs := range trie_blake2b.AllHashSize {
		if hs == trie_blake2b.HashSize192 {
			found = true
			break
		}
	}
	require.True(t, found, "HashSize192 must appear in trie_blake2b.AllHashSize")
}

func TestBlake2b192Distinct(t *testing.T) {
	data := []byte("the quick brown fox jumps over the lazy dog")
	h160, _ := trie_blake2b.CompressToHashSize(data, trie_blake2b.HashSize160)
	h192, _ := trie_blake2b.CompressToHashSize(data, trie_blake2b.HashSize192)
	h256, _ := trie_blake2b.CompressToHashSize(data, trie_blake2b.HashSize256)
	require.Len(t, h160, 20)
	require.Len(t, h192, 24)
	require.Len(t, h256, 32)
	require.NotEqual(t, h160, h192[:20])
	require.NotEqual(t, h256[:24], h192)
}

func TestHashSize192Description(t *testing.T) {
	m := trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize192)
	require.Contains(t, m.Description(), "HashSize(192)")
	require.Contains(t, m.ShortName(), "192")
}

func TestHashSize192CompressShortValueIsCopy(t *testing.T) {
	short := []byte("short value")
	out, valueInCommitment := trie_blake2b.CompressToHashSize(short, trie_blake2b.HashSize192)
	require.True(t, valueInCommitment)
	require.Equal(t, short, out)

	exactly24 := bytes.Repeat([]byte{0xAB}, 24)
	out, valueInCommitment = trie_blake2b.CompressToHashSize(exactly24, trie_blake2b.HashSize192)
	require.True(t, valueInCommitment)
	require.Equal(t, exactly24, out)

	long := bytes.Repeat([]byte{0xAB}, 25)
	out, valueInCommitment = trie_blake2b.CompressToHashSize(long, trie_blake2b.HashSize192)
	require.False(t, valueInCommitment)
	require.Len(t, out, 24)
}

func TestHashSize192RootCommitmentLength(t *testing.T) {
	m := trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize192)
	store := common.NewInMemoryKVStore()
	root := immutable.MustInitRoot(store, m, []byte("identity"))
	require.Len(t, root.Bytes(), 24)
}

func TestHashSize192BasicOps(t *testing.T) {
	runForArity := func(arity common.PathArity) {
		t.Run(arity.String(), func(t *testing.T) {
			m := trie_blake2b.New(arity, trie_blake2b.HashSize192)
			store := common.NewInMemoryKVStore()
			root := immutable.MustInitRoot(store, m, []byte("identity-192"))
			tr, err := immutable.NewTrieChained(m, store, root)
			require.NoError(t, err)

			data := map[string]string{
				"a":     "alpha",
				"ab":    "alphabet",
				"abc":   "alphabetic",
				"bcd":   strings.Repeat("Z", 200),
				"klmnt": "k-thing",
			}
			for k, v := range data {
				existed := tr.Update([]byte(k), []byte(v))
				require.False(t, existed)
			}
			tr = tr.CommitChained()

			for k, v := range data {
				require.Equal(t, []byte(v), tr.Get([]byte(k)))
			}

			existed := tr.Delete([]byte("ab"))
			require.True(t, existed)
			tr = tr.CommitChained()
			require.Nil(t, tr.Get([]byte("ab")))
			require.Equal(t, []byte("alpha"), tr.Get([]byte("a")))
		})
	}
	runForArity(common.PathArity256)
	runForArity(common.PathArity16)
	runForArity(common.PathArity2)
}

func TestHashSize192Determinism(t *testing.T) {
	runForArity := func(arity common.PathArity) {
		t.Run(arity.String(), func(t *testing.T) {
			data := map[string]string{
				"a":     "alpha",
				"ab":    "alphabet",
				"abc":   "alphabetic",
				"bcd":   strings.Repeat("Z", 200),
				"klmnt": "k-thing",
			}
			build := func() common.VCommitment {
				m := trie_blake2b.New(arity, trie_blake2b.HashSize192)
				store := common.NewInMemoryKVStore()
				root := immutable.MustInitRoot(store, m, []byte("identity-192"))
				tr, err := immutable.NewTrieChained(m, store, root)
				require.NoError(t, err)
				for k, v := range data {
					tr.Update([]byte(k), []byte(v))
				}
				tr = tr.CommitChained()
				return tr.Root()
			}
			r1 := build()
			r2 := build()
			m := trie_blake2b.New(arity, trie_blake2b.HashSize192)
			require.True(t, m.EqualCommitments(r1, r2))
		})
	}
	runForArity(common.PathArity256)
	runForArity(common.PathArity16)
	runForArity(common.PathArity2)
}

func TestHashSize192ProofOfInclusionAndAbsence(t *testing.T) {
	scenario := []string{"a", "ab", "abc", "bcd/" + strings.Repeat("Z", 200), "klmnt", "x", "y"}
	runForArity := func(arity common.PathArity) {
		t.Run(arity.String(), func(t *testing.T) {
			m := trie_blake2b.New(arity, trie_blake2b.HashSize192)
			store := common.NewInMemoryKVStore()
			rootInitial := immutable.MustInitRoot(store, m, []byte("idididididid"))
			tr, err := immutable.NewTrieChained(m, store, rootInitial)
			require.NoError(t, err)

			tr, checklist := runUpdateScenario(tr, scenario)
			root := tr.Root()
			require.Len(t, root.Bytes(), 24)

			trr, err := immutable.NewTrieReader(m, store, root)
			require.NoError(t, err)

			// inclusion proofs for present keys
			for k, v := range checklist {
				p := m.ProofImmutable([]byte(k), trr)
				require.Equal(t, trie_blake2b.HashSize192, p.HashSize)
				require.NoError(t, trie_blake2b_verify.Validate(p, root.Bytes()))

				if len(v) > 0 {
					cID := m.CommitToData([]byte(v))
					require.NoError(t, trie_blake2b_verify.ValidateWithTerminal(p, root.Bytes(), cID.Bytes()))
				} else {
					require.True(t, trie_blake2b_verify.IsProofOfAbsence(p))
				}
			}

			// proof of absence for an unrelated key
			p := m.ProofImmutable([]byte("definitely-missing-key"), trr)
			require.NoError(t, trie_blake2b_verify.Validate(p, root.Bytes()))
			require.True(t, trie_blake2b_verify.IsProofOfAbsence(p))

			// a wrong root must not validate
			badRoot := bytes.Clone(root.Bytes())
			badRoot[0] ^= 0xFF
			p = m.ProofImmutable([]byte("a"), trr)
			require.Error(t, trie_blake2b_verify.Validate(p, badRoot))
		})
	}
	runForArity(common.PathArity256)
	runForArity(common.PathArity16)
	runForArity(common.PathArity2)
}

func TestHashSize192ProofRoundTrip(t *testing.T) {
	m := trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize192)
	store := common.NewInMemoryKVStore()
	rootInitial := immutable.MustInitRoot(store, m, []byte("idididididid"))
	tr, err := immutable.NewTrieChained(m, store, rootInitial)
	require.NoError(t, err)
	tr.Update([]byte("a"), []byte("alpha"))
	tr.Update([]byte("ab"), []byte(strings.Repeat("X", 100)))
	tr = tr.CommitChained()
	root := tr.Root()
	trr, err := immutable.NewTrieReader(m, store, root)
	require.NoError(t, err)

	p := m.ProofImmutable([]byte("ab"), trr)
	encoded := p.Bytes()
	decoded, err := trie_blake2b.ProofFromBytes(encoded)
	require.NoError(t, err)
	require.Equal(t, trie_blake2b.HashSize192, decoded.HashSize)
	require.NoError(t, trie_blake2b_verify.Validate(decoded, root.Bytes()))
}
