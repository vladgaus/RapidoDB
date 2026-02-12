# Changelog

All notable changes to RapidoDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Versioning Scheme

RapidoDB follows a milestone-based versioning during development:

| Version | Milestone |
|:--------|:----------|
| 0.1.x | SkipList MemTable |
| 0.2.x | Write-Ahead Log (WAL) |
| 0.3.x | SSTable Writer, SSTable Reader |
| 0.4.x | Bloom Filters |
| 0.5.x | Basic LSM Engine |
| 0.6.x | Leveled Compaction |
| 0.7.x | Tiered and FIFO Compaction |
| 0.8.x | MVCC & Snapshots |
| 0.9.x | Manifest & Recovery |
| 0.10.x | Iterators & Range Scans |
| 0.11.x | TCP Server (Memcached Protocol) |
| 0.12.x | Benchmarks & Monitoring |
| 1.0.0 | Production Ready |

---

## [Unreleased]

### Planned (Steps 16-25)
- Step 16: Health Checks & Liveness Probes
- Step 17: Graceful Shutdown
- Step 18: Rate Limiting
- Step 19: Prometheus Metrics
- Step 20: Structured Logging
- Step 21: Distributed Tracing
- Step 22: Admin API
- Step 23: Backup & Restore
- Step 24: Import/Export
- Step 25: CLI Tool

---

## [v0.15.0] - 2025-02-12

### Changed
- **License changed from MIT to BSL 1.1** (Business Source License)
  - Free for internal use, SaaS backends, startups
  - Converts to Apache 2.0 after 4 years
  - Commercial license required for Database-as-a-Service offerings

### Added - All 15 Core Steps Completed
- SkipList MemTable (`pkg/memtable`)
- Write-Ahead Log (`pkg/wal`)
- SSTable Writer/Reader (`pkg/sstable`)
- Bloom Filters (`pkg/bloom`)
- LSM Engine (`pkg/lsm`)
- Leveled Compaction (`pkg/compaction/leveled`)
- Tiered Compaction (`pkg/compaction/tiered`)
- FIFO Compaction (`pkg/compaction/fifo`)
- MVCC & Snapshots (`pkg/mvcc`)
- Manifest & Recovery (`pkg/manifest`)
- Iterators (`pkg/iterator`)
- TCP Server with Memcached protocol (`pkg/server`)
- Benchmark framework (`pkg/benchmark`)

### Performance
- Sequential writes: ~100K ops/sec
- Random writes: ~87K ops/sec
- Sequential reads: ~1.45M ops/sec
- Random reads: ~1.45M ops/sec
- Mixed read/write (80/20): ~374K ops/sec

---

## [v0.1.0] - 2025-02-03

### Added

#### Project Foundation
- Go module setup with Go 1.22+
- Comprehensive project structure following Go best practices
- MIT License

#### Configuration System (`pkg/config`)
- JSON-based configuration with sensible defaults
- Support for all compaction strategies: Leveled, Tiered, FIFO
- Configurable MemTable, WAL, SSTable, Bloom filter settings
- Server configuration for TCP connections
- Configuration validation

#### Core Types (`pkg/types`)
- `Entry` type for key-value storage with MVCC sequence numbers
- `EntryType` enum (Put, Delete)
- `InternalKey` for internal ordering (userkey + seqnum + type)
- `KeyRange` for SSTable metadata and compaction
- `FileMetadata` for SSTable file tracking
- Core interfaces:
  - `Storage` - main KV operations
  - `SnapshotStorage` - MVCC snapshot support
  - `MemTable` - in-memory table interface
  - `SSTableWriter/Reader` - SSTable I/O
  - `WALWriter/Reader` - Write-Ahead Log
  - `Compactor` - compaction strategy interface
  - `Iterator` - range scan interface

#### Error Handling (`pkg/errors`)
- Sentinel errors for common conditions (KeyNotFound, DBClosed, Corruption, etc.)
- Specialized error types:
  - `IOError` with operation context
  - `CorruptionError` with file/offset details
  - `CompactionError` with level info
  - `RecoveryError` with phase info
- Key/value validation utilities
- Error wrapping helpers

#### Binary Encoding (`internal/encoding`)
- Big-Endian encoding for cross-platform compatibility
- Fixed-size integer encoding (uint16, uint32, uint64)
- Variable-length integer encoding (varint)
- Entry serialization/deserialization
- InternalKey encoding/decoding
- CRC32 checksum (Castagnoli polynomial)

#### Utilities (`internal/utils`)
- File path generation (SSTable, WAL, Manifest)
- File type detection
- Atomic file writes
- Directory sync for durability
- File number generator (atomic)
- Byte formatting utilities

#### Test Infrastructure (`tests/testutil`)
- Temporary directory/file helpers
- Random and sequential key/value generators
- Entry and tombstone factories
- Assertion utilities

#### CLI Applications
- Server binary (`cmd/server`) with:
  - Configuration file support
  - Command-line flag overrides
  - Graceful shutdown handling
  - ASCII banner
- Benchmark tool (`cmd/bench`) placeholder with:
  - Multiple benchmark modes planned
  - Configurable parameters

#### Build System
- Makefile with targets:
  - `build`, `test`, `bench`, `lint`
  - `test-race`, `test-cover`
  - `profile-cpu`, `profile-mem`
  - `clean`, `install`, `run`

#### Documentation
- Comprehensive README with:
  - Architecture diagrams
  - LSM-Tree concepts
  - Configuration examples
  - Implementation roadmap
- Example configuration file

#### CI/CD
- GitHub Actions workflow for CI (test, lint, build)
- GitHub Actions workflow for releases (multi-platform builds)

### Technical Details

- **Entry Format**: Type(1) + KeyLen(4) + ValLen(4) + SeqNum(8) + Key + Value
- **InternalKey Order**: UserKey (asc) → SeqNum (desc) → Type (desc)
- **Checksum**: CRC32 with Castagnoli polynomial (same as RocksDB)

### Benchmarks (Initial)

```
BenchmarkEncodeEntry     13M ops    89ns/op    144B/op
BenchmarkDecodeEntry      8M ops   144ns/op    192B/op
BenchmarkChecksum         6M ops   194ns/op      0B/op
BenchmarkInternalKeyCompare  263M ops  4.6ns/op   0B/op
```

---

## Release Links

[Unreleased]: https://github.com/vladgaus/RapidoDB/compare/v0.1.0...HEAD
[v0.1.0]: https://github.com/vladgaus/RapidoDB/releases/tag/v0.1.0
