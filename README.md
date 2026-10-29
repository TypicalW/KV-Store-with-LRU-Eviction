

A concurrent in-memory key-value store written in Go, featuring LRU eviction, interchangeable concurrency strategies, and optional write-ahead logging for recovery across restarts.

## Overview

`kvstore` is a lightweight key-value storage library designed to explore the engineering trade-offs behind cache design, concurrent access, and persistence.

It combines a hash map and doubly linked list for efficient LRU operations, provides multiple synchronization strategies for concurrent workloads, and supports an optional append-only JSON Lines write-ahead log (WAL) to reconstruct state after a restart.

The project emphasizes correctness, explicit concurrency trade-offs, standard-library implementations, and measurable performance rather than relying on external caching libraries.

## Features

- **O(1) LRU operations:** Hash-map lookups and doubly linked list updates support efficient reads, writes, deletions, and eviction.
- **Interchangeable concurrency strategies:** Choose a mutex-protected cache, a sharded cache, or a cache with buffered recency updates.
- **Configurable persistence:** Enable a WAL to recover key-value mutations after a restart.
- **WAL recovery:** Replay valid records and recover from incomplete or malformed log tails.
- **Configurable synchronization:** Select synchronous writes, periodic synchronization, or no automatic synchronization.
- **Concurrency testing:** Exercise cache strategies under concurrent reads, writes, and deletions.
- **Performance benchmarks:** Compare cache strategies under different read/write ratios, key distributions, and CPU settings.
- **Technical documentation:** Review the design rationale, implementation walkthrough, benchmark methodology, and known limitations.

## Architecture

The library separates the storage API, cache implementation, eviction policy, and persistence layer.

```text
                    Application
                         |
                         v
                    Store API
                  Get / Set / Delete
                         |
              +----------+----------+
              |                     |
              v                     v
         Cache Strategy         Optional WAL
              |                     |
      +-------+--------+            v
      |       |        |        Append-only
    Mutex  Sharded  Buffered    JSONL records
      |       |        |
      +-------+--------+
              |
              v
       LRU Storage Engine
        Map + Doubly Linked List
```

### LRU eviction

The underlying LRU engine combines:

- **Hash map:** Maps keys to their corresponding list nodes.
- **Doubly linked list:** Maintains recency order and enables constant-time node removal and movement.
- **Sentinel nodes:** Simplify insertion and deletion by avoiding special cases at list boundaries.

When the cache reaches capacity, the least recently used entry is evicted.

### Concurrency strategies

The library provides three strategies through a common cache interface.

| Strategy | Implementation | Trade-off |
|---|---|---|
| `Mutex` | One mutex protects the LRU engine | Simple synchronization and exact LRU ordering |
| `Sharded(n)` | Partitions keys across independently locked caches | Reduces contention across shards but sacrifices global LRU ordering |
| `Buffered` | Uses read locks and asynchronous recency promotion | Reduces read-side lock contention at the cost of approximate LRU ordering |

The default strategy is `Mutex`. The alternatives allow workloads with different contention patterns to be evaluated without changing the public storage API.

### Write-ahead logging

When WAL is enabled, `Set` and `Delete` operations are appended to the log before the corresponding in-memory mutation.

On initialization, the store replays valid records to reconstruct the cache. An incomplete or malformed trailing record causes recovery to truncate the log to its last valid record.

Three synchronization policies are available:

- `SyncAlways`: Synchronizes the log after each append.
- `SyncInterval(d)`: Synchronizes periodically at the specified interval.
- `SyncNever`: Does not automatically synchronize each append.

Reads are not persisted. Consequently, recovery reconstructs state from mutations, not the historical access order of reads.

## Requirements

- Go 1.22 or later
- Git

The implementation uses the Go standard library and does not require third-party Go modules.

## Installation

```bash
go get github.com/TypicalW/kvstore
```

## Quick start

Create a store with a capacity of 1,000 entries and use it as an in-memory key-value cache.

```go
package main

import (
	"fmt"
	"log"

	"github.com/TypicalW/kvstore"
)

func main() {
	store, err := kvstore.New(1000)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	if err := store.Set("language", "Go"); err != nil {
		log.Fatal(err)
	}

	value, ok := store.Get("language")
	if ok {
		fmt.Println(value)
	}

	fmt.Println("Entries:", store.Len())
}
```

Output:

```text
Go
Entries: 1
```

## Configuration

### Select a concurrency strategy

```go
store, err := kvstore.New(
	1000,
	kvstore.WithStrategy(kvstore.Sharded(16)),
)
if err != nil {
	log.Fatal(err)
}
defer store.Close()
```

Available strategies:

```go
kvstore.Mutex
kvstore.Sharded(16)
kvstore.Buffered
```

For sharding, the argument specifies the number of independent cache shards.

### Enable persistence

```go
store, err := kvstore.New(
	1000,
	kvstore.WithWAL("store.wal"),
	kvstore.WithSyncPolicy(kvstore.SyncAlways),
)
if err != nil {
	log.Fatal(err)
}
defer store.Close()
```

To reduce synchronization frequency:

```go
kvstore.WithSyncPolicy(
	kvstore.SyncInterval(100 * time.Millisecond),
)
```

Or disable automatic synchronization:

```go
kvstore.WithSyncPolicy(kvstore.SyncNever)
```

Add the `time` package import when using `SyncInterval`.

When a WAL is configured, the library replays its records during initialization. Reopening a store with the same WAL path restores the state represented by its recorded mutations.

## API reference

| Method | Description |
|---|---|
| `New(capacity, opts...)` | Creates a store with the specified capacity and optional configuration |
| `Get(key)` | Retrieves a value and reports whether the key exists |
| `Set(key, value)` | Inserts or updates a value |
| `Delete(key)` | Deletes a key |
| `Len()` | Returns the number of entries currently stored |
| `Close()` | Closes the store and its associated resources |

`Set` and `Delete` return errors because WAL operations can fail. `Get` returns `(value, found)`, while `Len` returns the current entry count.

## Project structure

```text
kvstore/
├── store.go                # Public API and configuration
├── cache.go                # Cache interface and mutex strategy
├── lru.go                  # Map and doubly linked list
├── mutex_cache.go          # Mutex-protected cache
├── sharded_cache.go        # Sharded cache implementation
├── buffered_cache.go       # Buffered recency promotion
├── wal.go                  # Write-ahead log and recovery
├── store_test.go           # Public API and persistence tests
├── lru_test.go             # LRU behavior tests
├── wal_test.go             # WAL and recovery tests
├── concurrency_test.go     # Concurrent-access tests
├── bench_test.go           # Performance benchmarks
├── bench/
│   └── raw.txt             # Raw benchmark output
├── docs/
│   ├── DESIGN.md           # Design decisions and trade-offs
│   ├── WALKTHROUGH.md      # Implementation walkthrough
│   └── BENCHMARKS.md       # Measurements and methodology
├── go.mod
└── README.md
```

## Testing

Run the complete test suite:

```bash
go test ./...
```

Run tests with the race detector:

```bash
go test -race -count=1 ./...
```

Run static analysis:

```bash
go vet ./...
```

The test suite covers LRU ordering and eviction, cache capacity, concurrent operations, persistence across restarts, malformed WAL records, incomplete log recovery, and constructor validation.

## Benchmarking

Run the cache and WAL benchmark suites:

```bash
go test -run '^$' \
  -bench 'Benchmark(Caches|WAL)' \
  -benchmem \
  -cpu 1,2,4 \
  -benchtime=100ms \
  ./...
```

The benchmarks use parallel workloads with:

- 95% reads and 5% writes.
- 50% reads and 50% writes.
- Uniform and Zipf-distributed keys.
- Multiple sharding configurations.
- Different CPU settings.
- Different WAL synchronization policies.

Recorded measurements and the test environment are documented in [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md). Raw output is available in [`bench/raw.txt`](bench/raw.txt).

Benchmark results depend on the execution environment and workload. No single concurrency strategy is assumed to be universally fastest.

## Design documentation

For a deeper look at the implementation:

- [Design decisions and trade-offs](docs/DESIGN.md)
- [Implementation walkthrough](docs/WALKTHROUGH.md)
- [Benchmark methodology and results](docs/BENCHMARKS.md)

These documents cover data structures, locking, sharding, asynchronous recency updates, WAL ordering, recovery, and performance measurements.

## Limitations

This project is an educational storage library and performance experiment, not a production database.

Current limitations include:

- WAL files grow indefinitely; compaction and snapshots are not implemented.
- There is no TTL or automatic expiration.
- Sharded caches do not maintain a single global LRU order.
- Buffered cache recency is approximate, and promotion requests may be dropped when the queue is full.
- WAL writes are serialized.
- WAL records are stored as plaintext JSON without encryption.
- Historical read activity is not restored during WAL replay.

These limitations are documented to make the implementation's guarantees and trade-offs explicit.

## Future improvements

Potential extensions include:

- WAL compaction and snapshot-based recovery.
- TTL and expiration policies.
- Additional benchmark workloads and reproducible performance comparisons.
- Continuous integration for tests, race detection, and static analysis.
- A network-facing API for remote access.


