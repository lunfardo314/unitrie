package tests

import (
	"testing"

	"github.com/lunfardo314/unitrie/common"
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
		require.EqualValues(t, 2, mut.LenSet())
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
		common.RequirePanicOrErrorWith(t, func() error {
			mut.Set([]byte("a"), []byte("1"))
			return nil
		}, "repetitive SET mutation")
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
		common.RequirePanicOrErrorWith(t, func() error {
			mut2.WriteTo(mut1)
			return nil
		}, "repetitive SET mutation")

		mut3 := common.NewMutationsMustNoDoubleBooking()
		mut3.Set([]byte("abc"), nil)
		common.RequirePanicOrErrorWith(t, func() error {
			mut3.WriteTo(mut1)
			return nil
		}, "repetitive DEL mutation")
		t.Logf("\n%s", mut1.String())
	})
}
