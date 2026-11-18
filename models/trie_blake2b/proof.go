package trie_blake2b

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"slices"

	"github.com/lunfardo314/unitrie/common"
)

// MerkleProof blake2b model specific proof of inclusion
type MerkleProof struct {
	PathArity common.PathArity
	HashSize  HashSize
	// HashedPath: the trie hashes every node path (see New). Encoded as the top bit
	// of the hash size byte, so a proof of a model keeping unhashed short paths has
	// the same bytes as before the flag existed.
	HashedPath bool
	Key        []byte
	Path       []*MerkleProofElement
}

const hashedPathFlag = 0x80

type MerkleProofElement struct {
	PathFragment []byte
	Children     map[byte][]byte
	Terminal     []byte
	ChildIndex   int
}

func ProofFromBytes(data []byte) (*MerkleProof, error) {
	ret := &MerkleProof{}
	rdr := bytes.NewReader(data)
	if err := ret.Read(rdr); err != nil {
		return nil, err
	}
	if rdr.Len() != 0 {
		return nil, common.ErrNotAllBytesConsumed
	}
	return ret, nil
}

func (p *MerkleProof) Bytes() []byte {
	return common.MustBytes(p)
}

func (p *MerkleProof) Write(w io.Writer) error {
	var err error
	if err = common.WriteByte(w, byte(p.PathArity)); err != nil {
		return err
	}
	hashSizeByte := byte(p.HashSize)
	if p.HashedPath {
		hashSizeByte |= hashedPathFlag
	}
	if err = common.WriteByte(w, hashSizeByte); err != nil {
		return err
	}
	encodedKey, err := common.EncodeUnpackedBytes(p.Key, p.PathArity)
	if err != nil {
		return err
	}
	if err = common.WriteBytes16(w, encodedKey); err != nil {
		return err
	}
	if err = common.WriteUint16(w, uint16(len(p.Path))); err != nil {
		return err
	}
	for _, e := range p.Path {
		if err = e.Write(w, p.PathArity, p.HashSize); err != nil {
			return err
		}
	}
	return nil
}

func (p *MerkleProof) Read(r io.Reader) error {
	b, err := common.ReadByte(r)
	if err != nil {
		return err
	}
	p.PathArity = common.PathArity(b)
	if !slices.Contains(common.AllPathArity, p.PathArity) {
		return common.ErrWrongArity
	}

	b, err = common.ReadByte(r)
	if err != nil {
		return err
	}
	p.HashedPath = b&hashedPathFlag != 0
	p.HashSize = HashSize(b &^ hashedPathFlag)
	if p.HashSize != HashSize256 && p.HashSize != HashSize192 && p.HashSize != HashSize160 {
		return errors.New("wrong hash size")
	}

	var encodedKey []byte
	if encodedKey, err = common.ReadBytes16(r); err != nil {
		return err
	}
	if p.Key, err = common.DecodeToUnpackedBytes(encodedKey, p.PathArity); err != nil {
		return err
	}
	var size uint16
	if err = common.ReadUint16(r, &size); err != nil {
		return err
	}
	p.Path = make([]*MerkleProofElement, size)
	for i := range p.Path {
		p.Path[i] = &MerkleProofElement{}
		if err = p.Path[i].Read(r, p.PathArity, p.HashSize); err != nil {
			return err
		}
	}
	return nil
}

const (
	hasTerminalValueFlag = 0x01
	hasChildrenFlag      = 0x02
)

func (e *MerkleProofElement) Write(w io.Writer, arity common.PathArity, sz HashSize) error {
	encodedPathFragment, err := common.EncodeUnpackedBytes(e.PathFragment, arity)
	if err != nil {
		return err
	}
	if err = common.WriteBytes16(w, encodedPathFragment); err != nil {
		return err
	}
	if err = common.WriteUint16(w, uint16(e.ChildIndex)); err != nil {
		return err
	}
	var smallFlags byte
	if e.Terminal != nil {
		smallFlags = hasTerminalValueFlag
	}
	// compress children flags 32 bytes (if any)
	var flags [32]byte
	for i := range e.Children {
		flags[i/8] |= 0x1 << (i % 8)
		smallFlags |= hasChildrenFlag
	}
	if err := common.WriteByte(w, smallFlags); err != nil {
		return err
	}
	// write terminal commitment if any
	if smallFlags&hasTerminalValueFlag != 0 {
		if err = common.WriteBytes8(w, e.Terminal); err != nil {
			return err
		}
	}
	// write child commitments if any
	if smallFlags&hasChildrenFlag != 0 {
		if _, err = w.Write(flags[:]); err != nil {
			return err
		}
		for i := 0; i < arity.VectorLength(); i++ {
			child, ok := e.Children[uint8(i)]
			if !ok {
				continue
			}
			if len(child) != int(sz) {
				return fmt.Errorf("wrong data size. Expected %s, got %d", sz.String(), len(child))
			}
			if _, err = w.Write(child); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *MerkleProofElement) Read(r io.Reader, arity common.PathArity, sz HashSize) error {
	var err error
	var encodedPathFragment []byte
	if encodedPathFragment, err = common.ReadBytes16(r); err != nil {
		return err
	}
	if e.PathFragment, err = common.DecodeToUnpackedBytes(encodedPathFragment, arity); err != nil {
		return err
	}
	var idx uint16
	if err := common.ReadUint16(r, &idx); err != nil {
		return err
	}
	e.ChildIndex = int(idx)
	var smallFlags byte
	if smallFlags, err = common.ReadByte(r); err != nil {
		return err
	}
	// every accepted proof has exactly one encoding: no undefined flag bits,
	// no empty terminal, no child flags beyond the arity, no empty child set
	if smallFlags&^(hasTerminalValueFlag|hasChildrenFlag) != 0 {
		return errors.New("wrong proof element flags")
	}
	if smallFlags&hasTerminalValueFlag != 0 {
		if e.Terminal, err = common.ReadBytes8(r); err != nil {
			return err
		}
		if len(e.Terminal) == 0 {
			return errors.New("empty terminal commitment in proof element")
		}
	} else {
		e.Terminal = nil
	}
	e.Children = make(map[byte][]byte)
	if smallFlags&hasChildrenFlag != 0 {
		var flags [32]byte
		if _, err = io.ReadFull(r, flags[:]); err != nil {
			return err
		}
		for i := 0; i < len(flags)*8; i++ {
			if flags[i/8]&(0x1<<(i%8)) == 0 {
				continue
			}
			if i >= arity.NumChildren() {
				return errors.New("child flag beyond the arity in proof element")
			}
			ib := uint8(i)
			e.Children[ib] = make([]byte, sz)
			if _, err = io.ReadFull(r, e.Children[ib]); err != nil {
				return err
			}
		}
		if len(e.Children) == 0 {
			return errors.New("children flag set with no children in proof element")
		}
	}
	return nil
}
