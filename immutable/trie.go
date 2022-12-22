package immutable

import (
	"fmt"

	"github.com/lunfardo314/unitrie/common"
)

// TrieUpdatable is an updatable trie implemented on top of the unpackedKey/value store. It is virtualized and optimized by caching of the
// trie update operation and keeping consistent trie in the cache
type TrieUpdatable struct {
	*TrieReader
	mutatedRoot *bufferedNode
}

// TrieReader direct read-only access to trie
type TrieReader struct {
	nodeStore      *NodeStore
	persistentRoot common.VCommitment
}

func NewTrieUpdatable(m common.CommitmentModel, store common.KVReader, root common.VCommitment, clearCacheAtSize ...int) (*TrieUpdatable, error) {
	trieReader, rootNodeData, err := newTrieReader(m, store, root, clearCacheAtSize...)
	if err != nil {
		return nil, err
	}
	return &TrieUpdatable{
		TrieReader:  trieReader,
		mutatedRoot: newBufferedNode(rootNodeData, nil),
	}, nil
}

func NewTrieReader(m common.CommitmentModel, store common.KVReader, root common.VCommitment, clearCacheAtSize ...int) (*TrieReader, error) {
	ret, _, err := newTrieReader(m, store, root, clearCacheAtSize...)
	return ret, err
}

func newTrieReader(m common.CommitmentModel, store common.KVReader, root common.VCommitment, clearCacheAtSize ...int) (*TrieReader, *common.NodeData, error) {
	s := openImmutableNodeStore(store, m, clearCacheAtSize...)
	rootNodeData, ok := s.FetchNodeData(root)
	if !ok {
		return nil, nil, fmt.Errorf("root commitment '%s' does not exist", root)
	}
	return &TrieReader{
		nodeStore:      s,
		persistentRoot: root.Clone(),
	}, rootNodeData, nil
}

func (tr *TrieReader) Root() common.VCommitment {
	return tr.persistentRoot
}

func (tr *TrieReader) Model() common.CommitmentModel {
	return tr.nodeStore.m
}

func (tr *TrieReader) PathArity() common.PathArity {
	return tr.nodeStore.m.PathArity()
}

func (tr *TrieReader) ClearCache() {
	tr.nodeStore.clearCache()
}

// Commit calculates a new mutatedRoot commitment value from the cache, commits all mutations
// and writes it into the store.
// The nodes and values are written into separate partitions
// The buffered nodes are garbage collected, except the mutated ones
// The new root becomes current
func (tr *TrieUpdatable) Commit(store common.KVWriter) common.VCommitment {
	triePartition := common.MakeWriterPartition(store, PartitionTrieNodes)
	valuePartition := common.MakeWriterPartition(store, PartitionValues)

	tr.mutatedRoot.commitNode(triePartition, valuePartition, tr.Model())
	// set uncommitted children in the root to empty -> the GC will collect the whole tree of buffered nodes
	tr.mutatedRoot.uncommittedChildren = make(map[byte]*bufferedNode)

	tr.persistentRoot = tr.mutatedRoot.nodeData.Commitment.Clone()

	tr.ClearCache()
	rootNodeData, ok := tr.nodeStore.FetchNodeData(tr.persistentRoot)
	common.Assert(ok, "Commit: inconsistency")
	tr.mutatedRoot = newBufferedNode(rootNodeData, nil) // the previous mutated tree will be GC-ed
	return tr.persistentRoot
}

func (tr *TrieUpdatable) Persist(db common.KVBatchedWriter) (common.VCommitment, error) {
	ret := tr.Commit(db)
	if err := db.Commit(); err != nil {
		return nil, err
	}
	return ret, nil
}

func (tr *TrieUpdatable) newTerminalNode(triePath, pathFragment, value []byte) *bufferedNode {
	ret := newBufferedNode(nil, triePath)
	ret.setPathFragment(pathFragment)
	ret.setValue(value, tr.Model())
	return ret
}
