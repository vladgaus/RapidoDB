# Changelog

All notable changes to RapidoDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Versioning Scheme

RapidoDB follows a milestone-based versioning during development:

| Version | Milestone |
|:--------|:----------|
| 0.1.x | Project scaffold, types, encoding |
| 0.2.x | SkipList MemTable |
| 0.3.x | Write-Ahead Log (WAL) |
| 0.4.x | SSTable Writer |
| 0.5.x | SSTable Reader |
| 0.6.x | Bloom Filters |
| 0.7.x | Basic LSM Engine |
| 0.8.x | Leveled Compaction |
| 0.9.x | Tiered Compaction |
| 0.10.x | FIFO Compaction |
| 0.11.x | MVCC & Snapshots |
| 0.12.x | Manifest & Recovery |
| 0.13.x | Iterators & Range Scans |
| 0.14.x | TCP Server (Memcached Protocol) |
| 0.15.x | Benchmarks & Monitoring |
| 1.0.0 | Production Ready |

---

## [Unreleased]

### Planned
- Step 2: SkipList MemTable implementation

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

[Unreleased]: https://github.com/rapidodb/rapidodb/compare/v0.1.0...HEAD
[v0.1.0]: https://github.com/rapidodb/rapidodb/releases/tag/v0.1.0
