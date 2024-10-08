package tests

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/lunfardo314/unitrie/common"
	"github.com/lunfardo314/unitrie/immutable"
	"github.com/lunfardo314/unitrie/models/trie_blake2b"
	"github.com/lunfardo314/unitrie/models/trie_kzg_bn256"
	"github.com/stretchr/testify/require"
)

func TestKeyExistence(t *testing.T) {
	store := common.NewInMemoryKVStore()
	m := trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize160)

	root := immutable.MustInitRoot(store, m, []byte("identity"))
	tr, err := immutable.NewTrieChained(m, store, root)
	require.NoError(t, err)
	existed := tr.Update([]byte("a"), []byte("a"))
	require.False(t, existed)
	existed = tr.Update([]byte("b"), []byte("b"))
	require.False(t, existed)
	tr = tr.CommitChained()

	existed = tr.Update([]byte("b"), []byte("bbb"))
	require.True(t, existed)
	tr = tr.CommitChained()

	existed = tr.Delete([]byte("c"))
	require.False(t, existed)
	existed = tr.Delete([]byte("b"))
	require.True(t, existed)

}

func TestDeletedKey(t *testing.T) {
	store := common.NewInMemoryKVStore()
	m := trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize160)

	var root0 common.VCommitment
	{
		root0 = immutable.MustInitRoot(store, m, []byte("identity"))
	}

	var root1 common.VCommitment
	{
		tr, err := immutable.NewTrieChained(m, store, root0)
		require.NoError(t, err)
		tr.Update([]byte("a"), []byte("a"))
		tr.Update([]byte("b"), []byte("b"))
		tr = tr.CommitChained()
		root1 = tr.Root()
	}

	var root2 common.VCommitment
	{
		tr, err := immutable.NewTrieChained(m, store, root1)
		require.NoError(t, err)
		deleted := tr.Update([]byte("a"), nil)
		require.True(t, deleted)
		tr.Update([]byte("b"), []byte("bb"))
		tr.Update([]byte("c"), []byte("c"))
		tr = tr.CommitChained()
		root2 = tr.Root()

		require.Nil(t, tr.Get([]byte("a")))
		deleted = tr.Update([]byte("a"), nil)
		require.False(t, deleted)

		deleted = tr.Delete([]byte("a"))
		require.False(t, deleted)
	}

	state, err := immutable.NewTrieReader(m, store, root2)
	require.NoError(t, err)
	require.Nil(t, state.Get([]byte("a")))
}

func TestCreateTrie(t *testing.T) {
	runTest := func(m common.CommitmentModel) {
		t.Run("not init-"+m.ShortName(), func(t *testing.T) {
			_, err := immutable.NewTrieUpdatable(m, common.NewInMemoryKVStore(), nil)
			common.RequireErrorWith(t, err, "does not exist")
		})
		t.Run("wrong init-"+m.ShortName(), func(t *testing.T) {
			store := common.NewInMemoryKVStore()
			common.RequirePanicOrErrorWith(t, func() error {
				immutable.MustInitRoot(store, m, nil)
				return nil
			}, "identity of the root cannot be empty")
		})
		t.Run("ok init-"+m.ShortName(), func(t *testing.T) {
			store := common.NewInMemoryKVStore()
			const identity1 = "abc"
			const identity2 = "abcabc"

			rootC1 := immutable.MustInitRoot(store, m, []byte(identity1))
			require.NotNil(t, rootC1)
			t.Logf("initial root commitment with id '%s': %s", identity1, rootC1)

			rootC2 := immutable.MustInitRoot(store, m, []byte(identity2))
			require.NotNil(t, rootC2)
			t.Logf("initial root commitment with id '%s': %s", identity2, rootC2)

			require.False(t, m.EqualCommitments(rootC1, rootC2))
		})
		t.Run("ok init long id-"+m.ShortName(), func(t *testing.T) {
			store := common.NewInMemoryKVStore()
			identity := strings.Repeat("abc", 50)

			rootC1 := immutable.MustInitRoot(store, m, []byte(identity))
			require.NotNil(t, rootC1)
			t.Logf("initial root commitment with id '%s': %s", identity, rootC1)
		})
		t.Run("update 1"+m.ShortName(), func(t *testing.T) {
			store := common.NewInMemoryKVStore()
			const (
				identity = "idIDidIDidID"
				key      = "key"
				value    = "value"
			)

			rootInitial := immutable.MustInitRoot(store, m, []byte(identity))
			require.NotNil(t, rootInitial)
			t.Logf("initial root commitment with id '%s': %s", identity, rootInitial)

			tr, err := immutable.NewTrieChained(m, store, rootInitial)
			require.NoError(t, err)

			v := tr.GetStr("")
			require.EqualValues(t, identity, v)

			tr.UpdateStr(key, value)
			tr = tr.CommitChained()
			rootCnext := tr.Root()
			t.Logf("initial root commitment: %s", rootInitial)
			t.Logf("next root commitment: %s", rootCnext)

			trInit, err := immutable.NewTrieReader(m, store, rootInitial)
			require.NoError(t, err)
			v = trInit.GetStr("")
			require.EqualValues(t, identity, v)

			v = tr.GetStr(key)
			require.EqualValues(t, value, v)

			require.True(t, tr.HasStr(key))
		})
		t.Run("update 2 long value"+m.ShortName(), func(t *testing.T) {
			store := common.NewInMemoryKVStore()
			const (
				identity = "idIDidIDidID"
				key      = "key"
				value    = "value"
			)

			rootInitial := immutable.MustInitRoot(store, m, []byte(identity))
			require.NotNil(t, rootInitial)
			t.Logf("initial root commitment with id '%s': %s", identity, rootInitial)

			tr, err := immutable.NewTrieChained(m, store, rootInitial)
			require.NoError(t, err)

			v := tr.GetStr("")
			require.EqualValues(t, identity, v)

			tr.UpdateStr(key, strings.Repeat(value, 500))
			tr = tr.CommitChained()
			rootCnext := tr.Root()
			t.Logf("initial root commitment: %s", rootInitial)
			t.Logf("next root commitment: %s", rootCnext)

			require.True(t, m.EqualCommitments(rootCnext, tr.Root()))

			v = tr.GetStr("")
			require.EqualValues(t, identity, v)

			v = tr.GetStr(key)
			require.EqualValues(t, strings.Repeat(value, 500), v)

			require.True(t, tr.HasStr(key))
		})
	}
	runTest(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256))
	runTest(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize160))
	runTest(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize256))
	runTest(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize160))
	runTest(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize256))
	runTest(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize160))
	runTest(trie_kzg_bn256.New())
}

func TestBaseUpdate(t *testing.T) {
	const identity = "idIDidIDidID"

	runTest := func(m common.CommitmentModel, data []string) {
		t.Run("update many", func(t *testing.T) {
			store := common.NewInMemoryKVStore()
			rootInitial := immutable.MustInitRoot(store, m, []byte(identity))
			require.NotNil(t, rootInitial)
			t.Logf("initial root commitment with id '%s': %s", identity, rootInitial)

			tr, err := immutable.NewTrieChained(m, store, rootInitial)
			require.NoError(t, err)

			//data = data[:2]
			for _, key := range data {
				value := strings.Repeat(key, 5)
				fmt.Printf("+++ update key='%s', value='%s'\n", key, value)
				tr.UpdateStr(key, value)
			}
			tr = tr.CommitChained()
			rootNext := tr.Root()
			t.Logf("after commit: %s", rootNext)

			for _, key := range data {
				v := tr.GetStr(key)
				require.EqualValues(t, strings.Repeat(key, 5), v)
			}
		})
	}
	data := []string{"ab", "acd", "a", "dba", "abc", "abd", "abcdafgh", "aaaaaaaaaaaaaaaa", "klmnt"}

	runTest(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256), []string{"a", "ab"})
	runTest(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256), []string{"ab", "acb"})
	runTest(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256), []string{"abc", "a"})
	runTest(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256), data)
	runTest(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize160), data)
	runTest(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize256), data)
	runTest(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize160), data)
	runTest(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize256), data)
	runTest(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize160), data)
	runTest(trie_kzg_bn256.New(), data)
}

var traceScenarios = false

func runUpdateScenario(trie *immutable.TrieChained, scenario []string) (*immutable.TrieChained, map[string]string) {
	checklist := make(map[string]string)
	uncommitted := false
	var ret common.VCommitment
	for _, cmd := range scenario {
		if len(cmd) == 0 {
			continue
		}
		if cmd == "*" {
			trie = trie.CommitChained()
			if traceScenarios {
				fmt.Printf("+++ commit. Root: '%s'\n", ret)
			}
			uncommitted = false
			continue
		}
		var key, value []byte
		before, after, found := strings.Cut(cmd, "/")
		if found {
			if len(before) == 0 {
				continue // key must not be empty
			}
			key = []byte(before)
			if len(after) > 0 {
				value = []byte(after)
			}
		} else {
			key = []byte(cmd)
			value = []byte(cmd)
		}
		trie.Update(key, value)
		checklist[string(key)] = string(value)
		uncommitted = true
		if traceScenarios {
			if len(value) > 0 {
				fmt.Printf("SET '%s' -> '%s'\n", string(key), string(value))
			} else {
				fmt.Printf("DEL '%s'\n", string(key))
			}
		}
	}
	if uncommitted {
		trie = trie.CommitChained()
		if traceScenarios {
			fmt.Printf("+++ commit. Root: '%s'\n", ret)
		}
	}
	if traceScenarios {
		fmt.Printf("+++ return root: '%s'\n", ret)
	}
	return trie, checklist
}

func checkResult(t *testing.T, trie *immutable.TrieReader, checklist map[string]string) {
	keys := make([]string, 0)
	for k := range checklist {
		keys = append(keys, k)
	}
	//sort.Strings(keys)

	for _, key := range keys {
		expectedValue := checklist[key]
		v := trie.GetStr(key)
		if traceScenarios {
			if len(v) > 0 {
				fmt.Printf("FOUND '%s': '%s' (expected '%s')\n", key, v, expectedValue)
			} else {
				fmt.Printf("NOT FOUND '%s' (expected '%s')\n", key, func() string {
					if len(expectedValue) > 0 {
						return "FOUND"
					} else {
						return "NOT FOUND"
					}
				}())
			}
		}
		require.EqualValues(t, expectedValue, v)
	}
}

func TestBaseScenarios(t *testing.T) {
	const identity = "idIDidIDidID"

	tf := func(m common.CommitmentModel, data []string) func(t *testing.T) {
		return func(t *testing.T) {
			store := common.NewInMemoryKVStore()
			rootInitial := immutable.MustInitRoot(store, m, []byte(identity))
			require.NotNil(t, rootInitial)
			t.Logf("initial root commitment with id '%s': %s", identity, rootInitial)

			tr, err := immutable.NewTrieChained(m, store, rootInitial)
			require.NoError(t, err)

			var checklist map[string]string
			tr, checklist = runUpdateScenario(tr, data)
			checkResult(t, tr.TrieReader, checklist)
		}
	}
	data1 := []string{"ab", "acd", "-a", "-ab", "abc", "abd", "abcdafgh", "-acd", "aaaaaaaaaaaaaaaa", "klmnt"}

	t.Run("1-1", tf(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256), []string{"a", "a/"}))
	t.Run("1-2", tf(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256), []string{"a", "*", "a/"}))
	t.Run("1-3", tf(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256), []string{"a", "b", "*", "b/", "a/"}))
	t.Run("1-4", tf(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256), []string{"a", "b", "*", "a/", "b/"}))
	t.Run("1-5", tf(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256), []string{"a", "b", "*", "a/", "b/bb", "c"}))
	t.Run("1-6", tf(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256), []string{"a", "b", "*", "a/", "b/bb", "c"}))
	t.Run("1-7", tf(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256), []string{"a", "b", "*", "a/", "b", "c"}))
	t.Run("1-8", tf(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256), []string{"acb/", "*", "acb/bca", "acb/123"}))
	t.Run("1-9", tf(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256), []string{"abc", "a", "abc/", "a/"}))
	t.Run("1-10", tf(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256), []string{"abc", "a", "a/", "abc/", "klmn"}))

	t.Run("5", tf(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256), data1))
	t.Run("6", tf(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize160), data1))
	t.Run("7", tf(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize256), data1))
	t.Run("8", tf(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize160), data1))
	t.Run("9", tf(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize256), data1))
	t.Run("10", tf(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize160), data1))
	t.Run("11", tf(trie_kzg_bn256.New(), data1))

	t.Run("12", tf(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256), []string{"a", "ab", "-a"}))

	t.Run("s1-1", tf(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256), []string{"a", "ab", "a/"}))
	t.Run("s1-2", tf(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize256), []string{"a", "ab", "a/"}))
	t.Run("s1-3", tf(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize256), []string{"a", "ab", "a/"}))

	data2 := []string{"a", "ab", "abc", "abcd", "abcde", "-abd", "-a"}
	t.Run("14", tf(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256), data2))
	t.Run("15", tf(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256), data2))
	t.Run("16", tf(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize160), data2))
	t.Run("17", tf(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize256), data2))
	t.Run("18", tf(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize160), data2))
	t.Run("19", tf(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize256), data2))
	t.Run("20", tf(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize160), data2))
	t.Run("21", tf(trie_kzg_bn256.New(), data2))

	data3 := []string{"a", "ab", "abc", "abcd", "abcde", "-abcde", "-abcd", "-abc", "-ab", "-a"}
	t.Run("14", tf(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256), data3))
	t.Run("15", tf(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256), data3))
	t.Run("16", tf(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize160), data3))
	t.Run("17", tf(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize256), data3))
	t.Run("18", tf(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize160), data3))
	t.Run("19", tf(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize256), data3))
	t.Run("20", tf(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize160), data3))
	t.Run("21", tf(trie_kzg_bn256.New(), data3))

	data4 := genRnd3()
	name := "update-many-"
	t.Run(name+"1", tf(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256), data4))
	t.Run(name+"2", tf(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize160), data4))
	t.Run(name+"3", tf(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize256), data4))
	t.Run(name+"4", tf(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize160), data4))
	t.Run(name+"5", tf(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize256), data4))
	t.Run(name+"6", tf(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize160), data4))
	t.Run(name+"7", tf(trie_kzg_bn256.New(), data3))

	traceScenarios = true
	data5 := []string{"0", "1/0", "\x10/0"}
	name = "reproduce1-"
	t.Run(name+"1", tf(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256), data5))
	t.Run(name+"2", tf(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize160), data5))
	t.Run(name+"3", tf(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize256), data5))
	t.Run(name+"4", tf(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize160), data5))
	t.Run(name+"5", tf(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize256), data5))
	t.Run(name+"6", tf(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize160), data5))
	t.Run(name+"7", tf(trie_kzg_bn256.New(), data3))
}

func TestDeletionLoop(t *testing.T) {
	runTest := func(m common.CommitmentModel, initScenario, scenario []string) {
		store := common.NewInMemoryKVStore()
		beginRoot := immutable.MustInitRoot(store, m, []byte("ididididid"))
		tr, err := immutable.NewTrieChained(m, store, beginRoot)
		require.NoError(t, err)
		t.Logf("TestDeletionLoop: model: '%s', init='%s', scenario='%s'", m.ShortName(), initScenario, scenario)
		tr, _ = runUpdateScenario(tr, initScenario)
		beginRoot = tr.Root()
		tr, _ = runUpdateScenario(tr, scenario)
		endRoot := tr.Root()
		require.True(t, tr.Model().EqualCommitments(beginRoot, endRoot))
	}
	runAll := func(init, sc []string) {
		runTest(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256), init, sc)
		runTest(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize160), init, sc)
		runTest(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize256), init, sc)
		runTest(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize160), init, sc)
		traceScenarios = true
		runTest(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize256), init, sc)
		traceScenarios = false
		runTest(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize160), init, sc)
		runTest(trie_kzg_bn256.New(), init, sc)
	}
	runAll([]string{"a"}, []string{"1", "*", "1/"})
	runAll([]string{"a", "ab", "abc"}, []string{"ac", "*", "ac/"})
	runAll([]string{"a", "ab", "abc"}, []string{"ac", "ac/"})
	runAll([]string{}, []string{"a", "a/"})
	runAll([]string{"a", "ab", "abc"}, []string{"a/", "a"})
	runAll([]string{}, []string{"a", "a/"})
	runAll([]string{"a"}, []string{"a/", "a"})
	runAll([]string{"a"}, []string{"b", "b/"})
	runAll([]string{"a"}, []string{"b", "*", "b/"})
	runAll([]string{"a", "bc"}, []string{"1", "*", "2", "*", "3", "1/", "2/", "3/"})
}

func TestDeterminism(t *testing.T) {
	const identity = "idIDidIDidID"

	tf := func(m common.CommitmentModel, scenario1, scenario2 []string) func(t *testing.T) {
		return func(t *testing.T) {
			fmt.Printf("--------- scenario1: %v\n", scenario1)
			store1 := common.NewInMemoryKVStore()
			initRoot1 := immutable.MustInitRoot(store1, m, []byte(identity))

			tr1, err := immutable.NewTrieChained(m, store1, initRoot1)
			require.NoError(t, err)

			var checklist1 map[string]string
			tr1, checklist1 = runUpdateScenario(tr1, scenario1)
			root1 := tr1.Root()
			checkResult(t, tr1.TrieReader, checklist1)

			fmt.Printf("--------- scenario2: %v\n", scenario2)
			store2 := common.NewInMemoryKVStore()
			initRoot2 := immutable.MustInitRoot(store2, m, []byte(identity))

			tr2, err := immutable.NewTrieChained(m, store2, initRoot2)
			require.NoError(t, err)

			var checklist2 map[string]string
			tr2, checklist2 = runUpdateScenario(tr2, scenario2)
			root2 := tr2.Root()
			checkResult(t, tr2.TrieReader, checklist2)

			require.True(t, m.EqualCommitments(root1, root2))
		}
	}
	{
		s1 := []string{"a", "ab"}
		s2 := []string{"ab", "a"}
		name := "order-simple-"
		t.Run(name+"1", tf(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256), s1, s2))
		t.Run(name+"2", tf(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256), s1, s2))
		t.Run(name+"3", tf(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize160), s1, s2))
		t.Run(name+"4", tf(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize256), s1, s2))
		t.Run(name+"5", tf(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize160), s1, s2))
		t.Run(name+"6", tf(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize256), s1, s2))
		t.Run(name+"7", tf(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize160), s1, s2))
		t.Run(name+"8", tf(trie_kzg_bn256.New(), s1, s2))
	}
	{
		s1 := genRnd3()[:50]
		s2 := reverse(s1)
		name := "order-reverse-many-"
		t.Run(name+"1", tf(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256), s1, s2))
		t.Run(name+"2", tf(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256), s1, s2))
		t.Run(name+"3", tf(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize160), s1, s2))
		t.Run(name+"4", tf(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize256), s1, s2))
		t.Run(name+"5", tf(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize160), s1, s2))
		t.Run(name+"6", tf(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize256), s1, s2))
		t.Run(name+"7", tf(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize160), s1, s2))
		t.Run(name+"8", tf(trie_kzg_bn256.New(), s1, s2))
	}
	{
		s1 := []string{"a", "ab"}
		s2 := []string{"a", "*", "ab"}
		name := "commit-simple-"
		t.Run(name+"1", tf(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256), s1, s2))
		t.Run(name+"2", tf(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256), s1, s2))
		t.Run(name+"3", tf(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize160), s1, s2))
		t.Run(name+"4", tf(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize256), s1, s2))
		t.Run(name+"5", tf(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize160), s1, s2))
		t.Run(name+"6", tf(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize256), s1, s2))
		t.Run(name+"7", tf(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize160), s1, s2))
		t.Run(name+"kzg", tf(trie_kzg_bn256.New(), s1, s2)) // failing because of KZG commitment model cryptography bug
	}
}

func TestIterate(t *testing.T) {
	iterTest := func(m common.CommitmentModel, scenario []string) func(t *testing.T) {
		return func(t *testing.T) {
			store := common.NewInMemoryKVStore()
			rootInitial := immutable.MustInitRoot(store, m, []byte("identity"))
			require.NotNil(t, rootInitial)
			t.Logf("initial root commitment with id '%s': %s", "identity", rootInitial)

			tr, err := immutable.NewTrieChained(m, store, rootInitial)
			require.NoError(t, err)

			var checklist map[string]string
			tr, checklist = runUpdateScenario(tr, scenario)
			root := tr.Root()
			checkResult(t, tr.TrieReader, checklist)

			trr, err := immutable.NewTrieReader(m, store, root, 0)
			require.NoError(t, err)
			var iteratedKeys1 [][]byte
			trr.Iterate(func(k []byte, v []byte) bool {
				if traceScenarios {
					fmt.Printf("---- iter --- '%s': '%s'\n", string(k), string(v))
				}
				if len(k) != 0 {
					vCheck := checklist[string(k)]
					require.True(t, len(v) > 0)
					require.EqualValues(t, []byte(vCheck), v)
				} else {
					require.EqualValues(t, []byte("identity"), v)
				}
				iteratedKeys1 = append(iteratedKeys1, k)
				return true
			})

			// assert that iteration order is deterministic
			var iteratedKeys2 [][]byte
			trr.IterateKeys(func(k []byte) bool {
				iteratedKeys2 = append(iteratedKeys2, k)
				return true
			})
			require.EqualValues(t, iteratedKeys1, iteratedKeys2)
		}
	}
	{
		name := "iterate-one-"
		scenario := []string{"a"}
		t.Run(name+"1", iterTest(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256), scenario))
		t.Run(name+"2", iterTest(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize160), scenario))
		t.Run(name+"3", iterTest(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize256), scenario))
		t.Run(name+"4", iterTest(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize160), scenario))
		t.Run(name+"5", iterTest(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize256), scenario))
		t.Run(name+"6", iterTest(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize160), scenario))
		t.Run(name+"7", iterTest(trie_kzg_bn256.New(), scenario))
	}
	{
		name := "iterate-"
		scenario := []string{"a", "b", "c", "*", "a/"}
		t.Run(name+"1", iterTest(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256), scenario))
		t.Run(name+"2", iterTest(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize160), scenario))
		t.Run(name+"3", iterTest(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize256), scenario))
		t.Run(name+"4", iterTest(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize160), scenario))
		t.Run(name+"5", iterTest(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize256), scenario))
		t.Run(name+"6", iterTest(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize160), scenario))
		t.Run(name+"7", iterTest(trie_kzg_bn256.New(), scenario))
	}
	{
		name := "iterate-big-"
		scenario := genRnd3()
		t.Run(name+"1", iterTest(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256), scenario))
		t.Run(name+"2", iterTest(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize160), scenario))
		t.Run(name+"3", iterTest(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize256), scenario))
		t.Run(name+"4", iterTest(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize160), scenario))
		t.Run(name+"5", iterTest(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize256), scenario))
		t.Run(name+"6", iterTest(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize160), scenario))
		t.Run(name+"7", iterTest(trie_kzg_bn256.New(), scenario))
	}
}

func TestIteratePrefix(t *testing.T) {
	iterTest := func(m common.CommitmentModel, scenario []string, prefix string) func(t *testing.T) {
		return func(t *testing.T) {
			store := common.NewInMemoryKVStore()
			rootInitial := immutable.MustInitRoot(store, m, []byte("identity"))
			require.NotNil(t, rootInitial)
			t.Logf("initial root commitment with id '%s': %s", "identity", rootInitial)

			tr, err := immutable.NewTrieChained(m, store, rootInitial)
			require.NoError(t, err)

			tr, _ = runUpdateScenario(tr, scenario)
			root := tr.Root()

			trr, err := immutable.NewTrieReader(m, store, root, 0)
			require.NoError(t, err)

			countIter := 0
			trr.Iterator([]byte(prefix)).Iterate(func(k []byte, v []byte) bool {
				if traceScenarios {
					fmt.Printf("---- iter --- '%s': '%s'\n", string(k), string(v))
				}
				if string(v) != "identity" {
					countIter++
				}
				require.True(t, strings.HasPrefix(string(k), prefix))
				return true
			})
			countOrig := 0
			for _, s := range scenario {
				if strings.HasPrefix(s, prefix) {
					countOrig++
				}
			}
			require.EqualValues(t, countOrig, countIter)
		}
	}
	{
		name := "iterate-ab"
		scenario := []string{"a", "ab", "c", "cd", "abcd", "klmn", "aaa", "abra", "111"}
		prefix := "ab"
		traceScenarios = true
		t.Run(name+"1", iterTest(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256), scenario, prefix))
		traceScenarios = false
		t.Run(name+"2", iterTest(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize160), scenario, prefix))
		t.Run(name+"3", iterTest(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize256), scenario, prefix))
		t.Run(name+"4", iterTest(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize160), scenario, prefix))
		t.Run(name+"5", iterTest(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize256), scenario, prefix))
		t.Run(name+"6", iterTest(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize160), scenario, prefix))
		t.Run(name+"7", iterTest(trie_kzg_bn256.New(), scenario, prefix))
	}
	{
		name := "iterate-a"
		scenario := []string{"a", "ab", "c", "cd", "abcd", "klmn", "aaa", "abra", "111", "baba", "ababa"}
		prefix := "a"
		t.Run(name+"1", iterTest(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256), scenario, prefix))
		t.Run(name+"2", iterTest(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize160), scenario, prefix))
		t.Run(name+"3", iterTest(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize256), scenario, prefix))
		t.Run(name+"4", iterTest(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize160), scenario, prefix))
		t.Run(name+"5", iterTest(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize256), scenario, prefix))
		t.Run(name+"6", iterTest(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize160), scenario, prefix))
		t.Run(name+"7", iterTest(trie_kzg_bn256.New(), scenario, prefix))
	}
	{
		name := "iterate-empty"
		scenario := []string{"a", "ab", "c", "cd", "abcd", "klmn", "aaa", "abra", "111", "baba", "ababa"}
		prefix := ""
		t.Run(name+"1", iterTest(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256), scenario, prefix))
		t.Run(name+"2", iterTest(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize160), scenario, prefix))
		t.Run(name+"3", iterTest(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize256), scenario, prefix))
		t.Run(name+"4", iterTest(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize160), scenario, prefix))
		t.Run(name+"5", iterTest(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize256), scenario, prefix))
		t.Run(name+"6", iterTest(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize160), scenario, prefix))
		t.Run(name+"7", iterTest(trie_kzg_bn256.New(), scenario, prefix))
	}
	{
		name := "iterate-none"
		scenario := []string{"a", "ab", "c", "cd", "abcd", "klmn", "aaa", "abra", "111", "baba", "ababa"}
		prefix := "---"
		t.Run(name+"1", iterTest(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256), scenario, prefix))
		t.Run(name+"2", iterTest(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize160), scenario, prefix))
		t.Run(name+"3", iterTest(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize256), scenario, prefix))
		t.Run(name+"4", iterTest(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize160), scenario, prefix))
		t.Run(name+"5", iterTest(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize256), scenario, prefix))
		t.Run(name+"6", iterTest(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize160), scenario, prefix))
		t.Run(name+"7", iterTest(trie_kzg_bn256.New(), scenario, prefix))
	}
}

func TestDeletePrefix(t *testing.T) {
	iterTest := func(m common.CommitmentModel, scenario []string, prefix string) func(t *testing.T) {
		return func(t *testing.T) {
			store := common.NewInMemoryKVStore()
			rootInitial := immutable.MustInitRoot(store, m, []byte("identity"))
			require.NotNil(t, rootInitial)
			t.Logf("initial root commitment with id '%s': %s", "identity", rootInitial)

			tr, err := immutable.NewTrieChained(m, store, rootInitial)
			require.NoError(t, err)

			tr, _ = runUpdateScenario(tr, scenario)

			deleted := tr.DeletePrefix([]byte(prefix))
			tr = tr.CommitChained()

			tr.Iterator([]byte(prefix)).Iterate(func(k []byte, v []byte) bool {
				if traceScenarios {
					fmt.Printf("---- iter --- '%s': '%s'\n", string(k), string(v))
				}
				if len(k) == 0 {
					require.EqualValues(t, "identity", string(v))
					return true
				}
				if deleted && len(prefix) != 0 {
					require.False(t, strings.HasPrefix(string(k), prefix))
				}
				return true
			})
		}
	}
	{
		name := "delete-ab"
		scenario := []string{"a", "ab", "c", "cd", "abcd", "klmn", "aaa", "abra", "111"}
		prefix := "ab"
		t.Run(name+"1", iterTest(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256), scenario, prefix))
		t.Run(name+"2", iterTest(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize160), scenario, prefix))
		t.Run(name+"3", iterTest(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize256), scenario, prefix))
		t.Run(name+"4", iterTest(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize160), scenario, prefix))
		t.Run(name+"5", iterTest(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize256), scenario, prefix))
		t.Run(name+"6", iterTest(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize160), scenario, prefix))
		t.Run(name+"7", iterTest(trie_kzg_bn256.New(), scenario, prefix))
	}
	{
		name := "delete-a"
		scenario := []string{"a", "ab", "c", "cd", "abcd", "klmn", "aaa", "abra", "111", "baba", "ababa"}
		prefix := "a"
		t.Run(name+"1", iterTest(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256), scenario, prefix))
		t.Run(name+"2", iterTest(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize160), scenario, prefix))
		t.Run(name+"3", iterTest(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize256), scenario, prefix))
		t.Run(name+"4", iterTest(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize160), scenario, prefix))
		t.Run(name+"5", iterTest(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize256), scenario, prefix))
		t.Run(name+"6", iterTest(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize160), scenario, prefix))
		t.Run(name+"7", iterTest(trie_kzg_bn256.New(), scenario, prefix))
	}
	{
		name := "delete-root"
		scenario := []string{"a", "ab", "c", "cd", "abcd", "klmn", "aaa", "abra", "111", "baba", "ababa"}
		prefix := ""
		t.Run(name+"1", iterTest(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256), scenario, prefix))
		t.Run(name+"2", iterTest(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize160), scenario, prefix))
		t.Run(name+"3", iterTest(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize256), scenario, prefix))
		t.Run(name+"4", iterTest(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize160), scenario, prefix))
		t.Run(name+"5", iterTest(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize256), scenario, prefix))
		t.Run(name+"6", iterTest(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize160), scenario, prefix))
		t.Run(name+"7", iterTest(trie_kzg_bn256.New(), scenario, prefix))
	}
	{
		name := "delete-none"
		scenario := []string{"a", "ab", "c", "cd", "abcd", "klmn", "aaa", "abra", "111", "baba", "ababa"}
		prefix := "---"
		t.Run(name+"1", iterTest(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256), scenario, prefix))
		t.Run(name+"2", iterTest(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize160), scenario, prefix))
		t.Run(name+"3", iterTest(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize256), scenario, prefix))
		t.Run(name+"4", iterTest(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize160), scenario, prefix))
		t.Run(name+"5", iterTest(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize256), scenario, prefix))
		t.Run(name+"6", iterTest(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize160), scenario, prefix))
		t.Run(name+"7", iterTest(trie_kzg_bn256.New(), scenario, prefix))
	}
}

func TestHasWithPrefix(t *testing.T) {
	runTest := func(m common.CommitmentModel) {
		store := common.NewInMemoryKVStore()
		initRoot := immutable.MustInitRoot(store, m, []byte("idididid"))
		tr, err := immutable.NewTrieChained(m, store, initRoot)
		require.NoError(t, err)

		scenario := []string{"a", "ab", "a1", "b", "bcd"}

		var res map[string]string
		tr, res = runUpdateScenario(tr, scenario)
		root := tr.Root()

		checkResult(t, tr.TrieReader, res)

		trCheck, err := immutable.NewTrieUpdatable(m, store, root)
		require.NoError(t, err)

		require.True(t, common.HasWithPrefix(trCheck, []byte("")))
		require.True(t, common.HasWithPrefix(trCheck, []byte("a")))
		require.True(t, common.HasWithPrefix(trCheck, []byte("ab")))
		require.True(t, common.HasWithPrefix(trCheck, []byte("a1")))
		require.True(t, common.HasWithPrefix(trCheck, []byte("b")))
		require.True(t, common.HasWithPrefix(trCheck, []byte("bc")))
		require.False(t, common.HasWithPrefix(trCheck, []byte("ac")))
		require.False(t, common.HasWithPrefix(trCheck, []byte("c")))
		require.False(t, common.HasWithPrefix(trCheck, []byte("1")))
		require.False(t, common.HasWithPrefix(trCheck, []byte("a12")))
	}
	runTest(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256))
	runTest(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize160))
	runTest(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize256))
	runTest(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize160))
	runTest(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize256))
	runTest(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize160))
	runTest(trie_kzg_bn256.New())
}

const letters = "abcdefghijklmnop"

func genRnd3() []string {
	ret := make([]string, 0, len(letters)*len(letters)*len(letters))
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := range letters {
		for j := range letters {
			for k := range letters {
				s := string([]byte{letters[i], letters[j], letters[k]})
				s = s + s + s + s
				r1 := rnd.Intn(len(s))
				r2 := rnd.Intn(len(s))
				if r2 < r1 {
					r1, r2 = r2, r1
				}
				ret = append(ret, s[r1:r2])
			}
		}
	}
	return ret
}

func reverse(orig []string) []string {
	ret := make([]string, 0, len(orig))
	for i := len(orig) - 1; i >= 0; i-- {
		ret = append(ret, orig[i])
	}
	return ret
}

func TestSnapshot1(t *testing.T) {
	runTest := func(m common.CommitmentModel, data []string) {
		store1 := common.NewInMemoryKVStore()
		initRoot1 := immutable.MustInitRoot(store1, m, []byte("idididid"))
		tr1, err := immutable.NewTrieChained(m, store1, initRoot1)
		require.NoError(t, err)

		tr1, _ = runUpdateScenario(tr1, data)
		root1 := tr1.Root()

		storeData := common.NewInMemoryKVStore()
		tr1.SnapshotData(storeData)

		store2 := common.NewInMemoryKVStore()
		initRoot2 := immutable.MustInitRoot(store2, m, []byte("idididid"))
		tr2, err := immutable.NewTrieChained(m, store2, initRoot2)
		require.NoError(t, err)

		storeData.Iterate(func(k, v []byte) bool {
			if len(k) != 0 {
				tr2.Update(k, v)
			}
			return true
		})
		tr2 = tr2.CommitChained()
		root2 := tr2.Root()

		require.True(t, m.EqualCommitments(root1, root2))
	}
	{
		data := []string{"a", "ab", "abc", "1", "2", "3", "11"}
		runTest(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256), data)
		runTest(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize160), data)
		runTest(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize256), data)
		runTest(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize160), data)
		runTest(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize256), data)
		runTest(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize160), data)
		runTest(trie_kzg_bn256.New(), data)
	}
	{
		data := genRnd3()
		runTest(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256), data)
		runTest(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize160), data)
		runTest(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize256), data)
		runTest(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize160), data)
		runTest(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize256), data)
		runTest(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize160), data)
		runTest(trie_kzg_bn256.New(), data)
	}
}

func TestSnapshot2(t *testing.T) {
	runTest := func(m common.CommitmentModel, data []string) {
		store1 := common.NewInMemoryKVStore()
		initRoot1 := immutable.MustInitRoot(store1, m, []byte("idididid"))
		tr1, err := immutable.NewTrieChained(m, store1, initRoot1)
		require.NoError(t, err)

		tr1, _ = runUpdateScenario(tr1, data)
		root1 := tr1.Root()

		store2 := common.NewInMemoryKVStore()
		tr1.Snapshot(store2)

		tr2, err := immutable.NewTrieChained(m, store2, root1)
		require.NoError(t, err)

		sc1 := []string{"@", "#$%%^", "____++++", "~~~~~"}
		sc2 := []string{"@", "#$%%^", "*", "____++++", "~~~~~"}
		tr1, _ = runUpdateScenario(tr1, sc1)
		r1 := tr1.Root()

		tr2, _ = runUpdateScenario(tr2, sc2)
		r2 := tr2.Root()
		require.True(t, m.EqualCommitments(r1, r2))
	}
	{
		data := []string{"a", "ab", "abc", "1", "2", "3", "11"}
		runTest(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256), data)
		runTest(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize160), data)
		runTest(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize256), data)
		runTest(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize160), data)
		runTest(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize256), data)
		runTest(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize160), data)
		runTest(trie_kzg_bn256.New(), data)
	}
	{
		data := genRnd3()
		runTest(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256), data)
		runTest(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize160), data)
		runTest(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize256), data)
		runTest(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize160), data)
		runTest(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize256), data)
		runTest(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize160), data)
		runTest(trie_kzg_bn256.New(), data)
	}
}

func TestDontChangeIdentity(t *testing.T) {
	runTest := func(m common.CommitmentModel) {
		db := common.NewInMemoryKVStore()
		root1 := func() common.VCommitment {
			id1 := []byte("id1")
			return immutable.MustInitRoot(db, m, id1)
		}()

		err := common.CatchPanicOrError(func() error {
			tr, err := immutable.NewTrieUpdatable(m, db, root1)
			require.NoError(t, err)
			tr.Update([]byte(""), []byte("new identity"))
			tr.Commit(db)
			return nil
		})
		common.RequireErrorWith(t, err, "identity of the state can't be changed")
	}
	runTest(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize256))
	runTest(trie_blake2b.New(common.PathArity256, trie_blake2b.HashSize160))
	runTest(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize256))
	runTest(trie_blake2b.New(common.PathArity16, trie_blake2b.HashSize160))
	runTest(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize256))
	runTest(trie_blake2b.New(common.PathArity2, trie_blake2b.HashSize160))
	runTest(trie_kzg_bn256.New())
}
