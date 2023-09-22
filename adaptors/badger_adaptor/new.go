package badger_adaptor

import (
	"fmt"
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

// MustCreateOrOpenBadgerDB opens existing DB or creates new empty
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
	return &DB{DB: db}
}

// OpenBadgerDB opens existing Badger DB
func OpenBadgerDB(dir string, opt ...badger.Options) (*badger.DB, error) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return nil, fmt.Errorf("'%s' does not exist, can't open DB", dir)
	}
	var opts badger.Options
	if len(opt) == 0 {
		opts = badger.DefaultOptions(dir)
	} else {
		opts = opt[0]
	}
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return db, err
}
