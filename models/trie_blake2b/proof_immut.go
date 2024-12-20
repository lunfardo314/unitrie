package trie_blake2b

import (
	"github.com/lunfardo314/unitrie/common"
	"github.com/lunfardo314/unitrie/immutable"
)

// ProofImmutable converts generic proof path of the immutable trie implementation to the Merkle proof path
func (m *CommitmentModel) ProofImmutable(key []byte, tr *immutable.TrieReader) *MerkleProof {
	unpackedKey := common.UnpackBytes(key, tr.PathArity())
	nodePath, ending := tr.NodePath(unpackedKey)
	ret := &MerkleProof{
		PathArity: tr.PathArity(),
		HashSize:  m.hashSize,
		Key:       unpackedKey,
		Path:      make([]*MerkleProofElement, len(nodePath)),
	}
	for i, e := range nodePath {
		elem := &MerkleProofElement{
			PathFragment: e.NodeData.PathFragment,
			Children:     make(map[byte][]byte),
			Terminal:     nil,
			ChildIndex:   int(e.ChildIndex),
		}
		if !common.IsNil(e.NodeData.Terminal) {
			elem.Terminal, _ = CompressToHashSize(e.NodeData.Terminal.Bytes(), m.hashSize)
		}
		isLast := i == len(nodePath)-1
		for childIndex, childCommitment := range e.NodeData.ChildCommitments {
			if !isLast && childIndex == e.ChildIndex {
				// commitment to the next child is not included, it must be calculated by the verifier
				continue
			}
			elem.Children[childIndex] = childCommitment.(vectorCommitment)
		}
		ret.Path[i] = elem
	}
	common.Assertf(len(ret.Path) > 0, "len(ret.Path)")
	last := ret.Path[len(ret.Path)-1]
	switch ending {
	case common.EndingTerminal:
		last.ChildIndex = m.arity.TerminalCommitmentIndex()
	case common.EndingExtend, common.EndingSplit:
		last.ChildIndex = m.arity.PathCommitmentIndex()
	default:
		panic("wrong ending code")
	}
	return ret
}
