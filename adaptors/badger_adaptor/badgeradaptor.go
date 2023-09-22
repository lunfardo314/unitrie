package badger_adaptor

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
	"github.com/lunfardo314/unitrie/common"
)

type (
	DB struct {
		*badger.DB
		closed atomic.Bool
	}

	badgerAdaptorBatch struct {
		db  *DB
		mut *common.Mutations
	}

	badgerAdaptorIterator struct {
		db     *DB
		prefix []byte
	}
)

func (a *DB) Close() error {
	a.closed.Store(true)
	return a.DB.Close()
}

// KVReader

func (a *DB) Get(key []byte) []byte {
	if a.closed.Load() {
		return nil
	}
	var ret []byte
	err := a.DB.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		ret, err = item.ValueCopy(nil)
		return err
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil
	}
	common.AssertNoError(err)
	return ret
}

func (a *DB) Has(key []byte) bool {
	if a.closed.Load() {
		return false
	}
	err := a.DB.View(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		return err
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return false
	}
	common.AssertNoError(err)
	return true
}

// KVWriter

func (a *DB) Set(key, value []byte) {
	if a.closed.Load() {
		return
	}
	err := a.DB.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
	common.AssertNoError(err)
}

// BatchedUpdatable

func (a *DB) BatchedWriter() common.KVBatchedWriter {
	return &badgerAdaptorBatch{
		db:  a,
		mut: common.NewMutationsMustNoDoubleBooking(),
	}
}

// KVBatchedWriter

func (b *badgerAdaptorBatch) Set(key, value []byte) {
	b.mut.Set(key, value)
}

func (b *badgerAdaptorBatch) Commit() error {
	return b.db.Update(func(txn *badger.Txn) error {
		if b.db.closed.Load() {
			return fmt.Errorf("database is closed")
		}
		var err error
		b.mut.Iterate(func(k []byte, v []byte, _ bool) bool {
			if len(v) > 0 {
				err = txn.Set(k, v)
			} else {
				err = txn.Delete(k)
			}
			return err == nil
		})
		if b.db.closed.Load() {
			return fmt.Errorf("database is closed")
		}
		return err
	})
}

// Traversable

func (a *DB) Iterator(prefix []byte) common.KVIterator {
	return &badgerAdaptorIterator{
		db:     a,
		prefix: prefix,
	}
}

// KVIterator

const iteratorPrefetchSize = 10

func (it *badgerAdaptorIterator) Iterate(fun func(k []byte, v []byte) bool) {
	err := it.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = iteratorPrefetchSize

		dbIt := txn.NewIterator(opts)
		defer dbIt.Close()

		exit := false
		for dbIt.Seek(it.prefix); !exit && dbIt.ValidForPrefix(it.prefix); dbIt.Next() {
			err := dbIt.Item().Value(func(val []byte) error {
				exit = !fun(dbIt.Item().Key(), val)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if !it.db.closed.Load() {
		common.AssertNoError(err)
	}
}

func (it *badgerAdaptorIterator) IterateKeys(fun func(k []byte) bool) {
	err := it.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = iteratorPrefetchSize

		dbIt := txn.NewIterator(opts)
		defer dbIt.Close()

		for dbIt.Rewind(); dbIt.ValidForPrefix(it.prefix); dbIt.Next() {
			if !fun(dbIt.Item().Key()) {
				return nil
			}
		}
		return nil
	})
	if !it.db.closed.Load() {
		common.AssertNoError(err)
	}
}
