// Package trie_blake2b_verify contains functions for verification of the proofs of inclusion or absence
// in the trie with trie_blake2b commitment model. The package only depends on the commitment model
// implementation and the proof format it defines. The verification package is completely independent on
// the implementation of the Merkle tree (the trie)
//
// DISCLAIMER: THE FOLLOWING CODE IS SECURITY CRITICAL.
// ANY POTENTIAL BUG WHICH MAY LEAD TO FALSE POSITIVES OF PROOF VALIDITY CHECKS POTENTIALLY
// CREATES AN ATTACK VECTOR.
// THEREFORE, IT IS HIGHLY RECOMMENDED THE VERIFICATION CODE TO BE WRITTEN BY THE VERIFYING PARTY ITSELF,
// INSTEAD OF CLONING THIS PACKAGE. DO NOT TRUST ANYBODY BUT YOURSELF. IN ANY CASE, PERFORM A DETAILED
// AUDIT OF THE PROOF-VERIFYING CODE BEFORE USING IT
package trie_blake2b_verify

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/lunfardo314/unitrie/common"
	"github.com/lunfardo314/unitrie/models/trie_blake2b"
	"golang.org/x/xerrors"
)

// MustKeyWithTerminal returns key and terminal commitment the proof is about. It returns:
// - key
// - terminal commitment. If it is nil, the proof is a proof of absence
// It does not verify the proof, so this function should be used only after Validate()
func MustKeyWithTerminal(p *trie_blake2b.MerkleProof) ([]byte, []byte) {
	if len(p.Path) == 0 {
		return nil, nil
	}
	lastElem := p.Path[len(p.Path)-1]
	switch {
	case lastElem.ChildIndex == p.PathArity.TerminalCommitmentIndex():
		if lastElem.Terminal == nil {
			return p.Key, nil
		}
		return p.Key, lastElem.Terminal
	case lastElem.ChildIndex == p.PathArity.PathCommitmentIndex():
		return p.Key, nil
	}
	panic("wrong lastElem.ChildIndex")
}

// IsProofOfAbsence checks if it is proof of absence. MerkleProof that the trie commits to something else in the place
// where it would commit to the key if it would be present
func IsProofOfAbsence(p *trie_blake2b.MerkleProof) bool {
	_, r := MustKeyWithTerminal(p)
	return len(r) == 0
}

// Validate check the proof against the provided root commitments
func Validate(p *trie_blake2b.MerkleProof, rootBytes []byte) error {
	if len(p.Path) == 0 {
		if len(rootBytes) != 0 {
			return xerrors.New("proof is empty")
		}
		return nil
	}
	c, err := verify(p, nil, 0, 0)
	if err != nil {
		return err
	}
	if !bytes.Equal(c, rootBytes) {
		return xerrors.New("invalid proof: commitment not equal to the root")
	}
	return nil
}

// ValidateWithTerminal checks the proof and checks if the proof commits to the specific value
// The check is dependent on the commitment model because of valueOptimisationThreshold
func ValidateWithTerminal(p *trie_blake2b.MerkleProof, rootBytes, terminalBytes []byte) error {
	if err := Validate(p, rootBytes); err != nil {
		return err
	}
	_, terminalBytesInProof := MustKeyWithTerminal(p)
	compressedTerm, _ := trie_blake2b.CompressToHashSize(terminalBytes, p.HashSize)
	if !bytes.Equal(compressedTerm, terminalBytesInProof) {
		return errors.New("key does not correspond to the given value commitment")
	}
	return nil
}

func verify(p *trie_blake2b.MerkleProof, triePath []byte, pathIdx, keyIdx int) ([]byte, error) {
	common.Assertf(pathIdx < len(p.Path), "assertion: pathIdx < lenPlus1(p.Path)")
	common.Assertf(keyIdx <= len(p.Key), "assertion: keyIdx <= lenPlus1(p.Key)")

	elem := p.Path[pathIdx]
	tail := p.Key[keyIdx:]
	isPrefix := bytes.HasPrefix(tail, elem.PathFragment)
	last := pathIdx == len(p.Path)-1
	if !last && !isPrefix {
		return nil, fmt.Errorf("wrong proof: proof path does not follow the key. Path position: %d, key position %d", pathIdx, keyIdx)
	}
	if !last {
		common.Assertf(isPrefix, "assertion: isPrefix")
		if !p.PathArity.IsValidChildIndex(elem.ChildIndex) {
			return nil, fmt.Errorf("wrong proof: wrong child index. Path position: %d, key position %d", pathIdx, keyIdx)
		}
		if _, ok := elem.Children[byte(elem.ChildIndex)]; ok {
			return nil, fmt.Errorf("wrong proof: unexpected commitment at child index %d. Path position: %d, key position %d", elem.ChildIndex, pathIdx, keyIdx)
		}
		nextKeyIdx := keyIdx + len(elem.PathFragment) + 1
		if nextKeyIdx > len(p.Key) {
			return nil, fmt.Errorf("wrong proof: proof path out of key bounds. Path position: %d, key position %d", pathIdx, keyIdx)
		}
		if elem.ChildIndex != int(p.Key[nextKeyIdx-1]) {
			return nil, fmt.Errorf("wrong proof: child index does not follow the key. Path position: %d, key position %d", pathIdx, keyIdx)
		}
		nextTriePath := common.Concat(triePath, elem.PathFragment, p.Key[nextKeyIdx-1])
		c, err := verify(p, nextTriePath, pathIdx+1, nextKeyIdx)
		if err != nil {
			return nil, err
		}
		return hashProofElement(elem, triePath, c, p.PathArity, p.HashSize)
	}
	// It is the last in the path. Its child index does not enter the hash, so the
	// element must be checked against the key here: otherwise a proof about one
	// key, or a proof of absence, would validate as presence of another key.
	switch elem.ChildIndex {
	case p.PathArity.TerminalCommitmentIndex():
		// the node sits exactly at the key: its terminal is the value, no terminal is absence
		if !bytes.Equal(tail, elem.PathFragment) {
			return nil, fmt.Errorf("wrong proof: terminal of the last element is not at the key. Path position: %d, key position %d", pathIdx, keyIdx)
		}
	case p.PathArity.PathCommitmentIndex():
		// absence: the path fragment leaves the key, or ends before it with no child at the key's next nibble
		if isPrefix {
			if len(tail) == len(elem.PathFragment) {
				return nil, fmt.Errorf("wrong proof: path of the last element ends at the key, terminal ending expected. Path position: %d, key position %d", pathIdx, keyIdx)
			}
			if _, ok := elem.Children[tail[len(elem.PathFragment)]]; ok {
				return nil, fmt.Errorf("wrong proof: the last element has a child on the key's path. Path position: %d, key position %d", pathIdx, keyIdx)
			}
		}
	default:
		return nil, fmt.Errorf("wrong proof: child index of the last element expected to be %d or %d. Path position: %d, key position %d",
			p.PathArity.TerminalCommitmentIndex(), p.PathArity.PathCommitmentIndex(), pathIdx, keyIdx)
	}
	return hashProofElement(elem, triePath, nil, p.PathArity, p.HashSize)
}

const errTooLongCommitment = "too long commitment at position %d. Can't be longer than %d bytes"

func makeHashVector(e *trie_blake2b.MerkleProofElement, nodePath []byte, missingCommitment []byte, arity common.PathArity, sz trie_blake2b.HashSize) ([][]byte, error) {
	hashes := make([][]byte, arity.VectorLength())
	for idx, c := range e.Children {
		if !arity.IsValidChildIndex(int(idx)) {
			return nil, fmt.Errorf("wrong child index %d", idx)
		}
		if len(c) > int(sz) {
			return nil, fmt.Errorf(errTooLongCommitment, idx, int(sz))
		}
		hashes[idx] = c
	}
	if len(e.Terminal) > 0 {
		if len(e.Terminal) > int(sz) {
			return nil, fmt.Errorf(errTooLongCommitment+" (terminal)", arity.TerminalCommitmentIndex(), int(sz))
		}
		hashes[arity.TerminalCommitmentIndex()] = e.Terminal
	}

	pathToCommit := common.Concat(nodePath, byte('+'), e.PathFragment)
	rawBytes, _ := trie_blake2b.CompressToHashSize(pathToCommit, sz)
	hashes[arity.PathCommitmentIndex()] = rawBytes
	if arity.IsValidChildIndex(e.ChildIndex) {
		if len(missingCommitment) > int(sz) {
			return nil, fmt.Errorf(errTooLongCommitment+" (skipped commitment)", e.ChildIndex, int(sz))
		}
		hashes[e.ChildIndex] = missingCommitment
	}
	return hashes, nil
}

func hashProofElement(e *trie_blake2b.MerkleProofElement, nodePath []byte, missingCommitment []byte, arity common.PathArity, sz trie_blake2b.HashSize) ([]byte, error) {
	hashVector, err := makeHashVector(e, nodePath, missingCommitment, arity, sz)
	if err != nil {
		return nil, err
	}
	return trie_blake2b.HashTheVector(hashVector, arity, sz), nil
}
