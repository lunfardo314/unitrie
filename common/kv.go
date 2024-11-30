package common

//----------------------------------------------------------------------------
// generic abstraction interfaces of key/value storage

type (
	// KVReader is a key/value reader
	KVReader interface {
		// Get retrieves value by key. Returned nil means absence of the key
		Get(key []byte) []byte
		// Has checks presence of the key in the key/value store
		Has(key []byte) bool // for performance
	}

	// KVWriter is a key/value writer
	KVWriter interface {
		// Set writes new or updates existing key with the value.
		// value == nil means deletion of the key from the store
		Set(key, value []byte)
	}

	// KVIteratorBase is an interface to iterate through the collection of key/value pairs, probably with duplicate keys.
	// Order of iteration is NON-DETERMINISTIC in general
	KVIteratorBase interface {
		Iterate(func(k, v []byte) bool)
	}

	// KVIterator normally implements iteration over k/v collection with unique keys
	KVIterator interface {
		KVIteratorBase
		IterateKeys(func(k []byte) bool)
	}

	// KVBatchedWriter collects Mutations in the buffer via Set-s to KVWriter and then flushes (applies) it atomically to DB with Commit
	// KVBatchedWriter implementation should be deterministic: the sequence of Set-s to KWWriter exactly determines
	// the sequence, how key/value pairs in the database are updated or deleted (with value == nil)
	KVBatchedWriter interface {
		KVWriter
		Commit() error
	}

	// KVStore is a compound interface for reading and writing
	KVStore interface {
		KVReader
		KVWriter
	}

	KVTraversableReader interface {
		KVReader
		Traversable
	}

	// BatchedUpdatable is a KVStore equipped with the batched update capability. You can only update
	// BatchedUpdatable in atomic batches
	BatchedUpdatable interface {
		BatchedWriter() KVBatchedWriter
	}

	// Traversable is an interface which provides with partial iterators
	Traversable interface {
		Iterator(prefix []byte) KVIterator
	}
)

// CopyAll flushes KVIterator to KVWriter. It is up to the iterator correctly stop iterating
func CopyAll(dst KVWriter, src KVIterator) {
	src.Iterate(func(k, v []byte) bool {
		dst.Set(k, v)
		return true
	})
}

func HasWithPrefix(r Traversable, prefix []byte) bool {
	ret := false
	r.Iterator(prefix).IterateKeys(func(_ []byte) bool {
		ret = true
		return false
	})
	return ret
}
