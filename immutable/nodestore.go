package immutable

import (
	"container/list"
	"encoding/hex"

	"github.com/lunfardo314/unitrie/common"
)

// NodeStore immutable node store.
//
// The cache is bounded LRU: at most clearCacheAtSize entries; on overflow the
// least-recently-used entry is evicted (one at a time). Hot trie nodes —
// top-of-trie, branch points near the LRB — therefore survive across queries
// rather than being wiped together with cold entries on every threshold hit.
// The previous flush-on-overflow policy did the opposite, killing locality for
// any workload that re-queries the same trie root (e.g. PrunableTxIDsAtSlot
// running on the same baseline across consecutive branch commits).
//
// clearCacheAtSize == 0 disables caching entirely (no allocations).
type NodeStore struct {
	m          common.CommitmentModel
	trieStore  common.KVReader
	valueStore common.KVReader

	cacheCapacity int
	cacheList     *list.List               // front = MRU, back = LRU; nil iff caching disabled
	cacheMap      map[string]*list.Element // dbKey -> element holding *cacheEntry; nil iff caching disabled
}

// cacheEntry is what NodeStore.cacheList nodes carry.
type cacheEntry struct {
	key  string
	data *common.NodeData
}

const defaultClearCacheEveryGets = 1000

const (
	PartitionTrieNodes = byte(iota)
	PartitionValues
	PartitionOther
)

// MustInitRoot initializes new empty root with the given identity
// stores identity in the value store if it does not fit the commitment
// assigns state index 0
func MustInitRoot(store common.KVWriter, m common.CommitmentModel, identity []byte) common.VCommitment {
	common.Assertf(len(identity) > 0, "MustInitRoot: identity of the root cannot be empty")
	// create a node with the commitment to the identity as terminal for the root
	rootNodeData := common.NewNodeData()
	n := newBufferedNode(rootNodeData, nil)
	n.setValue(identity, m)

	trieStore := common.MakeWriterPartition(store, PartitionTrieNodes)
	valueStore := common.MakeWriterPartition(store, PartitionValues)
	n.commitNode(trieStore, valueStore, m)

	return n.nodeData.Commitment.Clone()
}

func openImmutableNodeStore(store common.KVReader, model common.CommitmentModel, clearCacheAtSize ...int) *NodeStore {
	ret := &NodeStore{
		m:             model,
		trieStore:     common.MakeReaderPartition(store, PartitionTrieNodes),
		valueStore:    common.MakeReaderPartition(store, PartitionValues),
		cacheCapacity: defaultClearCacheEveryGets,
	}
	if len(clearCacheAtSize) > 0 {
		ret.cacheCapacity = clearCacheAtSize[0]
	}
	if ret.cacheCapacity > 0 {
		ret.cacheList = list.New()
		ret.cacheMap = make(map[string]*list.Element, ret.cacheCapacity)
	}
	return ret
}

func (ns *NodeStore) FetchNodeData(nodeCommitment common.VCommitment) (*common.NodeData, bool) {
	dbKey := common.AsKey(nodeCommitment)
	if ns.cacheCapacity > 0 {
		if elem, inCache := ns.cacheMap[string(dbKey)]; inCache {
			ns.cacheList.MoveToFront(elem)
			return elem.Value.(*cacheEntry).data, true
		}
	}
	nodeBin := ns.trieStore.Get(dbKey)
	if len(nodeBin) == 0 {
		return nil, false
	}
	noValueStore := func(_ []byte) ([]byte, error) {
		panic("internal inconsistency: all terminal commitments must be stored in the trie node")
	}
	ret, err := common.NodeDataFromBytes(ns.m, nodeBin, ns.m.PathArity(), noValueStore)
	common.Assertf(err == nil, "NodeStore::FetchNodeData err: '%v' nodeBin: '%s', commitment: %s, arity: %s",
		err, func() string { return hex.EncodeToString(nodeBin) }, nodeCommitment, ns.m.PathArity())
	ret.Commitment = nodeCommitment

	if ns.cacheCapacity > 0 {
		entry := &cacheEntry{key: string(dbKey), data: ret}
		elem := ns.cacheList.PushFront(entry)
		ns.cacheMap[entry.key] = elem
		// evict LRU until within capacity
		for ns.cacheList.Len() > ns.cacheCapacity {
			oldest := ns.cacheList.Back()
			ns.cacheList.Remove(oldest)
			delete(ns.cacheMap, oldest.Value.(*cacheEntry).key)
		}
	}
	return ret, true
}

func (ns *NodeStore) MustFetchNodeData(nodeCommitment common.VCommitment) *common.NodeData {
	ret, ok := ns.FetchNodeData(nodeCommitment)
	common.Assertf(ok, "NodeStore::MustFetchNodeData: cannot find node data: commitment: '%s'", func() string { return nodeCommitment.String() })
	return ret
}

func (ns *NodeStore) FetchChild(n *common.NodeData, childIdx byte, trieKey []byte) (*common.NodeData, []byte) {
	c, childFound := n.ChildCommitments[childIdx]
	if !childFound {
		return nil, nil
	}
	common.Assertf(!common.IsNil(c), "immutable::FetchChild: unexpected nil commitment")
	childTriePath := common.Concat(trieKey, n.PathFragment, childIdx)

	ret, ok := ns.FetchNodeData(c)
	common.Assertf(ok, "immutable::FetchChild: failed to fetch node. trieKey: '%s', childIndex: %d",
		func() string { return hex.EncodeToString(trieKey) }, childIdx)
	return ret, childTriePath
}

func (ns *NodeStore) clearCache() {
	if ns.cacheList != nil {
		ns.cacheList.Init()
	}
	if ns.cacheMap != nil {
		clear(ns.cacheMap)
	}
}
