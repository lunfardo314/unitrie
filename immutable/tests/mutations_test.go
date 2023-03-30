package tests

import (
	"testing"

	"github.com/lunfardo314/unitrie/common"
	"github.com/stretchr/testify/require"
)

func TestMutations(t *testing.T) {
	mut := common.NewMutations()
	require.EqualValues(t, 0, mut.Len())

	mut.Set([]byte("a"), []byte("1"))
	mut.Set([]byte("ab"), []byte("2"))
	mut.Set([]byte("a"), nil)
	mut.Set([]byte("abc"), []byte("3"))
	require.EqualValues(t, 4, mut.Len())

	s := common.NewInMemoryKVStore()
	mut.Write(s)
	require.EqualValues(t, 2, s.Len())
	require.EqualValues(t, []byte("2"), s.Get([]byte("ab")))
	require.EqualValues(t, []byte("3"), s.Get([]byte("abc")))
	require.False(t, s.Has([]byte("a")))

	mut = common.NewMutations()
	mut.Set([]byte("abc"), nil)
	mut.Set([]byte("a"), []byte("1"))
	mut.Write(s)
	require.EqualValues(t, 2, s.Len())
	require.False(t, s.Has([]byte("abc")))
	require.EqualValues(t, []byte("1"), s.Get([]byte("a")))
	require.EqualValues(t, []byte("2"), s.Get([]byte("ab")))
}
