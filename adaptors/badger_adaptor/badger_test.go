package badger_adaptor

import (
	"fmt"
	"testing"

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
