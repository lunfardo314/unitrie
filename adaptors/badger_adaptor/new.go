package badger_adaptor

import (
	"os"

	"github.com/dgraph-io/badger/v4"
	"github.com/lunfardo314/unitrie/common"
)

func createDirectoryIfNeeded(dir string) error {
	_, err := os.Stat(dir)
	if os.IsNotExist(err) {
		return os.MkdirAll(dir, 0700)
	}
	return err
}

func MustCreateOrOpenBadgerDB(dir string, opt ...badger.Options) *badger.DB {
	err := createDirectoryIfNeeded(dir)
	common.AssertNoError(err)
	var opts badger.Options
	if len(opt) == 0 {
		opts = badger.DefaultOptions(dir)
	} else {
		opts = opt[0]
	}
	opts.Logger = nil
	db, err := badger.Open(opts)
	common.AssertNoError(err)
	return db
}

func New(db *badger.DB) *DB {
	return &DB{db: db}
}
