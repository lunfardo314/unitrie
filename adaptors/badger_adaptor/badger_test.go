package badger_adaptor

import (
	"errors"
	"fmt"
	"testing"

	"github.com/lunfardo314/unitrie/common"
	"github.com/stretchr/testify/require"
)

const dbPath = "./tmpDB"

func TestBasic(t *testing.T) {
	db := MustCreateOrOpenBadgerDB(dbPath)
	defer db.Close()

	data := []string{"a", "ab", "1", "klmn"}
	a := New(db)

	for _, k := range data {
		a.Set([]byte(k), []byte(k+k))
	}

	count := 0
	a.Iterator(nil).Iterate(func(k, v []byte) bool {
		fmt.Printf("%d : '%s' - '%s'\n", count, string(k), string(v))
		count++
		return true
	})
	fmt.Printf("------ with prefix 'a'\n")

	a.Iterator([]byte("a")).Iterate(func(k, v []byte) bool {
		fmt.Printf("%d : '%s' - '%s'\n", count, string(k), string(v))
		count++
		return true
	})

	for _, k := range data {
		require.True(t, a.Has([]byte(k)))
		require.False(t, a.Has([]byte(k+k+k)))
		v := a.Get([]byte(k))
		require.EqualValues(t, k+k, string(v))
	}
}

func TestClose(t *testing.T) {
	db := MustCreateOrOpenBadgerDB(dbPath)
	a := New(db)
	a.Set([]byte("kuku"), []byte("mumu"))
	err := a.Close()
	require.NoError(t, err)

	err = common.CatchPanicOrError(func() error {
		a.Get([]byte("kuku"))
		return nil
	})
	require.True(t, errors.Is(err, common.ErrDBUnavailable))

	err = common.CatchPanicOrError(func() error {
		a.Set([]byte("kuku"), []byte("zzz"))
		return nil
	})
	require.True(t, errors.Is(common.ErrDBUnavailable, err))
}
