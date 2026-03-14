# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`unitrie` is a Go package implementing committed radix tries (sparse Merkle trees) with pluggable cryptographic commitment models. The trie logic is fully decoupled from the commitment scheme — the same trie code works with hash-based commitments (Blake2b) and polynomial commitments (KZG/BN256).

## Build and Test Commands

```bash
# Run all tests
go test ./...

# Run tests for a specific package
go test ./immutable/tests/
go test ./common/
go test ./models/trie_blake2b/
go test ./adaptors/badger_adaptor/

# Run a single test
go test ./immutable/tests/ -run TestName

# Run tests with verbose output
go test -v ./...
```

No Makefile or CI configuration exists. Standard `go test` is used throughout.

## Architecture

### Key Abstractions

**CommitmentModel** (`common/model.go`) — The central abstraction. Defines how trie nodes are committed cryptographically. Configures `PathArity` (2=binary, 16=hex/Patricia, 256=byte trie) and provides functions to compute terminal and vector commitments.

**KV Storage** (`common/kv.go`) — Interfaces (`KVReader`, `KVWriter`, `KVBatchedWriter`, `KVStore`, `Traversable`) abstract all storage. In-memory implementation in `common/kvimpl.go` for testing; Badger DB adaptor in `adaptors/badger_adaptor/` for production.

**Data Partitioning** (`common/partition.go`) — The trie store is split into two key-space partitions using prefix bytes: `PartitionTrieNodes` (0x00) for serialized trie nodes, and `PartitionValues` (0x01) for large values (>62 bytes). Values in the value partition are content-addressed (keyed by their commitment hash).

### Trie Implementation (`immutable/`)

Three trie types with increasing capability:
- `TrieReader` — read-only access to a committed trie
- `TrieUpdatable` — adds mutation support via in-memory buffered nodes
- `TrieChained` — persists mutations back to the same store on commit

The commit flow (`trie.go` → `node.go:commitNode()`) recursively walks buffered nodes, writing trie node data to the trie partition and large values to the value partition.

**Mutations** (`common/mutations.go`) — Tracks SET/DEL operations for batch writes. `NewMutationsMustNoDoubleBooking()` creates a mutation tracker that panics on duplicate key writes — used by the Badger adaptor to enforce write consistency.

### Commitment Models (`models/`)

- **`trie_blake2b/`** — Blake2b hash-based commitments with configurable hash size (160/192/256 bits). Includes Merkle proof generation and verification.
- **`trie_kzg_bn256/`** — KZG polynomial commitments over BN256 curve using `go.dedis.ch/kyber/v3`. Precomputed trusted setup in `setup_data.go`.

### Node Serialization

`common/nodedata.go` defines `NodeData` — the serialized form of a trie node containing path fragments, child commitments, and terminal data. Uses compact binary encoding with compression flags.

`common/encode.go` handles key encoding/decoding between packed bytes and unpacked representations for different path arities (nibble-expansion for arity 16, bit-expansion for arity 2).
