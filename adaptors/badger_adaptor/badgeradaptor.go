package badger_adaptor

import (
	"errors"

	"github.com/dgraph-io/badger/v4"
	"github.com/lunfardo314/unitrie/common"
)

type (
	DB struct {
		*badger.DB
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

// KVReader

func (a *DB) Get(key []byte) []byte {
	var ret []byte
	err := common.CatchPanicOrError(func() error {
		return a.DB.View(func(txn *badger.Txn) error {
			item, err := txn.Get(key)
			if err != nil {
				return err
			}
			ret, err = item.ValueCopy(nil)
			return err
		})
	})
	switch {
	case errors.Is(err, badger.ErrKeyNotFound):
		return nil
	case errors.Is(err, badger.ErrDBClosed):
		panic(common.ErrDBUnavailable)
	default:
		common.AssertNoError(err)
	}
	return ret
}

func (a *DB) Has(key []byte) bool {
	err := common.CatchPanicOrError(func() error {
		return a.DB.View(func(txn *badger.Txn) error {
			_, err := txn.Get(key)
			return err
		})
	})
	switch {
	case errors.Is(err, badger.ErrKeyNotFound):
		return false
	case errors.Is(err, badger.ErrDBClosed):
		panic(common.ErrDBUnavailable)
	default:
		common.AssertNoError(err)
	}
	return true
}

// KVWriter

func (a *DB) Set(key, value []byte) {
	err := a.DB.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
	if errors.Is(err, badger.ErrDBClosed) {
		panic(common.ErrDBUnavailable)
	}
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
	err := common.CatchPanicOrError(func() error {
		return b.db.Update(func(txn *badger.Txn) error {
			var err error
			b.mut.Iterate(func(k []byte, v []byte, _ bool) bool {
				if len(v) > 0 {
					err = txn.Set(k, v)
				} else {
					err = txn.Delete(k)
				}
				return err == nil
			})
			return err
		})
	})
	if errors.Is(err, badger.ErrDBClosed) {
		err = common.ErrDBUnavailable
	}
	return err
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

// Iterate and IterateKeys hand the callback copies: badger's own key and value
// slices are valid only until the iterator moves on and are then reused, so a
// callback keeping one would see it overwritten by a later item.
func (it *badgerAdaptorIterator) Iterate(fun func(k []byte, v []byte) bool) {
	err := common.CatchPanicOrError(func() error {
		return it.db.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchSize = iteratorPrefetchSize

			dbIt := txn.NewIterator(opts)
			defer dbIt.Close()

			for dbIt.Seek(it.prefix); dbIt.ValidForPrefix(it.prefix); dbIt.Next() {
				item := dbIt.Item()
				v, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}
				if !fun(item.KeyCopy(nil), v) {
					return nil
				}
			}
			return nil
		})
	})
	mustIterationSucceed(err)
}

// mustIterationSucceed raises what the iteration met, like Get and Has do: an
// error or a panic in the callback must not end the iteration as if it were complete.
func mustIterationSucceed(err error) {
	if errors.Is(err, badger.ErrDBClosed) {
		panic(common.ErrDBUnavailable)
	}
	common.AssertNoError(err)
}

func (it *badgerAdaptorIterator) IterateKeys(fun func(k []byte) bool) {
	err := common.CatchPanicOrError(func() error {
		return it.db.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchSize = iteratorPrefetchSize
			opts.PrefetchValues = false

			dbIt := txn.NewIterator(opts)
			defer dbIt.Close()

			for dbIt.Seek(it.prefix); dbIt.ValidForPrefix(it.prefix); dbIt.Next() {
				if !fun(dbIt.Item().KeyCopy(nil)) {
					return nil
				}
			}
			return nil
		})
	})
	mustIterationSucceed(err)
}
