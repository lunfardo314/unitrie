# Fix: unitrie NodeStore cache causes OOM on full trie iteration

## Problem

When Proxima saves a snapshot, it iterates the entire trie (~171K records, many more trie nodes).
The `NodeStore` in `immutable/nodestore.go` has a node cache that contributes to memory spikes
(575MB → 5.6GB on an 8GB machine), causing OOM kills.

## What to fix

File: `immutable/nodestore.go`, struct `NodeStore`, method `FetchNodeData`.

### Bug 1: cache is never populated

`FetchNodeData` checks the cache and clears it at threshold, but **never inserts fetched nodes into the cache**.
The line `ns.cache[string(dbKey)] = ret` is missing after the fetch-from-store path.
This makes the cache entirely useless — it's always empty.

**Fix**: add `ns.cache[string(dbKey)] = ret` before the return at line 80.

### Bug 2: cache clearing doesn't help GC

When the cache exceeds `clearCacheAtSize`, the code does:
```go
ns.cache = make(map[string]*common.NodeData)
```

This creates a new empty map, but the old map (with all its `*NodeData` pointers and string keys)
only becomes garbage after this assignment. During fast iteration, multiple generations of old maps
can pile up before GC runs, causing memory spikes.

**Fix**: use `clear(ns.cache)` (Go 1.21+) instead of `make(...)`. This:
- Zeroes all entries in-place (keys and value pointers become GC-eligible immediately)
- Reuses the underlying map memory (no new allocation)
- Prevents pileup of old map shells waiting for GC

```go
if len(ns.cache) > ns.clearCacheAtSize {
    clear(ns.cache)  // instead of: ns.cache = make(map[string]*common.NodeData)
}
```

Same fix in `clearCache()` method (line 103-105).

### Bug 3: `clearCacheAtSize=0` should fully disable caching

When caller passes `clearCacheAtSize=0`, the intent is "no cache at all."
Currently this works (the `if ns.clearCacheAtSize > 0` guard skips cache logic), but the
constructor still allocates an empty map at line 48. Minor: skip map allocation when disabled.

## Testing

- Existing unit tests must pass
- Add a test that iterates a trie with 10K+ entries using `clearCacheAtSize=100` and
  verifies all entries are returned correctly (cache eviction doesn't lose data)
- Optionally: benchmark memory usage of full iteration with/without cache clearing

## Context

Proxima snapshot code (`multistate/snapshot.go:61`) calls `NewReadable(state, root)` without
`clearCacheAtSize`, getting the default of 1000. After this unitrie fix, Proxima should also
pass a small value (e.g., 100) for snapshot iteration to keep the cache tight.
