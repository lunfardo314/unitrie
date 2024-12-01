package common

import "sync"

// ---------------reader partition

type ReaderPartition struct {
	r      KVReader
	prefix byte
}

var (
	_                   KVReader = &ReaderPartition{}
	readerPartitionPool sync.Pool
)

func (p *ReaderPartition) Get(key []byte) []byte {
	return p.r.Get(Concat(p.prefix, key))
}

func (p *ReaderPartition) Has(key []byte) bool {
	return p.r.Has(Concat(p.prefix, key))
}

func MakeReaderPartition(r KVReader, prefix byte) *ReaderPartition {
	var ret *ReaderPartition
	s := readerPartitionPool.Get()
	if s == nil {
		ret = new(ReaderPartition)
	} else {
		ret = s.(*ReaderPartition)
	}
	*ret = ReaderPartition{
		prefix: prefix,
		r:      r,
	}
	return ret
}

func (p *ReaderPartition) Dispose() {
	p.r = nil
	readerPartitionPool.Put(p)
}

// ---------------- traversable reader partition

type TraversableReaderPartition struct {
	r      KVTraversableReader
	prefix byte
}

var (
	_                              KVTraversableReader = &TraversableReaderPartition{}
	traversableReaderPartitionPool sync.Pool
)

func (p *TraversableReaderPartition) Get(key []byte) (ret []byte) {
	UseConcatBytes(func(cat []byte) {
		ret = p.r.Get(cat)
	}, []byte{p.prefix}, key)
	return
}

func (p *TraversableReaderPartition) Has(key []byte) (ret bool) {
	UseConcatBytes(func(cat []byte) {
		ret = p.r.Has(cat)
	}, []byte{p.prefix}, key)
	return
}

func (p *TraversableReaderPartition) Iterator(iterPrefix []byte) KVIterator {
	return p.r.Iterator(Concat(p.prefix, iterPrefix))
}

func MakeTraversableReaderPartition(r KVTraversableReader, p byte) *TraversableReaderPartition {
	var ret *TraversableReaderPartition
	s := traversableReaderPartitionPool.Get()
	if s == nil {
		ret = new(TraversableReaderPartition)
	} else {
		ret = s.(*TraversableReaderPartition)
	}
	*ret = TraversableReaderPartition{
		prefix: p,
		r:      r,
	}
	return ret
}

func (p *TraversableReaderPartition) Dispose() {
	p.r = nil
	traversableReaderPartitionPool.Put(p)
}

// -------------------- writer partition

var (
	_                   KVWriter = &WriterPartition{}
	writerPartitionPool sync.Pool
)

type WriterPartition struct {
	w      KVWriter
	prefix byte
}

func (w *WriterPartition) Set(key, value []byte) {
	w.w.Set(Concat(w.prefix, key), value)
}

func MakeWriterPartition(w KVWriter, p byte) *WriterPartition {
	var ret *WriterPartition
	s := writerPartitionPool.Get()
	if s == nil {
		ret = new(WriterPartition)
	} else {
		ret = s.(*WriterPartition)
	}
	*ret = WriterPartition{
		prefix: p,
		w:      w,
	}
	return ret
}

func (w *WriterPartition) Dispose() {
	w.w = nil
	writerPartitionPool.Put(w)
}
