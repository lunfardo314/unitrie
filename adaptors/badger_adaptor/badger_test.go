package badger_adaptor

import (
	"fmt"
	"testing"
)

const dbPath = "./tmpDB"

func TestBasic(t *testing.T) {
	db := MustCreateOrOpenBadgerDB(dbPath)
	defer db.Close()

	a := New(db)

	for _, k := range []string{"a", "ab", "1", "klmn"} {
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

}
