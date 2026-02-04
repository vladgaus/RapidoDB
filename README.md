# RapidoDB

![Go Version](https://img.shields.io/badge/Go-1.23+-00ADD8?style=flat&logo=go)
![License](https://img.shields.io/badge/License-MIT-blue.svg)
![Status](https://img.shields.io/badge/Status-Educational-yellow.svg)

A high-performance, persistent Key-Value storage engine based on **Log-Structured Merge-tree (LSM-Tree)** architecture. Built for learning and understanding how modern databases like RocksDB, LevelDB, and PostgreSQL implement their storage layers.

```
╦═╗┌─┐┌─┐┬┌┬┐┌─┐╔╦╗╔╗ 
╠╦╝├─┤├─┘│ │││ │ ║║╠╩╗
╩╚═┴ ┴┴  ┴─┴┘└─┘═╩╝╚═╝
```

## 🎯 Project Goals

This project is designed as an **educational deep-dive** into storage engine internals. Goals include:

1. **Understanding LSM-Tree Architecture** - MemTable, SSTable, WAL, Compaction
2. **Implementing Multiple Compaction Strategies** - Leveled, Tiered (Universal), FIFO
3. **Exploring Performance Trade-offs** - Write amplification, read amplification, space amplification
4. **Building Production-Quality Code** - Tests, benchmarks, clean architecture

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      RapidoDB Architecture                      │
├─────────────────────────────────────────────────────────────────┤
│  Client API (Get/Put/Delete/Scan)                               │
│       ↓                                                         │
│  ┌─────────────┐     ┌─────────────┐                            │
│  │  MemTable   │ ←── │    WAL      │  (durability)              │
│  │ (SkipList)  │     │  (append)   │                            │
│  └─────────────┘     └─────────────┘                            │
│       ↓ flush                                                   │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                    SSTable Levels                       │    │
│  │  L0: [SST][SST][SST] (unsorted, may overlap)            │    │
│  │  L1: [SST][SST][SST][SST] (sorted, non-overlapping)     │    │
│  │  L2: [SST][SST][SST][SST][SST][SST][SST][SST]           │    │
│  │  ...                                                    │    │
│  └─────────────────────────────────────────────────────────┘    │
│       ↑                                                         │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐              │
│  │   Bloom     │  │   Block     │  │  Compaction │              │
│  │  Filters    │  │   Cache     │  │  Scheduler  │              │
│  └─────────────┘  └─────────────┘  └─────────────┘              │
└─────────────────────────────────────────────────────────────────┘
```

## 📚 Key Concepts

### LSM-Tree Basics

| Component | Purpose |
|:----------|:--------|
| **MemTable** | In-memory sorted buffer for writes (SkipList) |
| **WAL** | Write-Ahead Log for durability |
| **SSTable** | Sorted String Table - immutable on-disk files |
| **Compaction** | Background merging to reduce read amplification |
| **Bloom Filter** | Probabilistic filter to avoid unnecessary disk reads |

### Compaction Strategies

| Strategy | Write Amp | Read Amp | Space Amp | Best For |
|:---------|:----------|:---------|:----------|:---------|
| **Leveled** | High | Low | Low | Read-heavy workloads |
| **Tiered** | Low | High | Medium | Write-heavy workloads |
| **FIFO** | Minimal | Medium | Low | Time-series, caches |

## 🚀 Getting Started

### Prerequisites

- Go 1.23 or higher
- Make (optional, for convenience)

### Building

```bash
# Clone the repository
git clone https://github.com/rapidodb/rapidodb.git
cd rapidodb

# Build all binaries
make build

# Or build manually
go build -o build/rapidodb-server ./cmd/server
go build -o build/rapidodb-bench ./cmd/bench
```

### Running

```bash
# Run the server with default configuration
./build/rapidodb-server

# Run with custom config
./build/rapidodb-server -config=config.yaml

# Run with command-line overrides
./build/rapidodb-server -data-dir=/data/rapidodb -port=11211
```

### Testing

```bash
# Run all tests
make test

# Run with race detector
make test-race

# Run with coverage report
make test-cover

# Run benchmarks
make bench
```

## 📁 Project Structure

```
rapidodb/
├── cmd/
│   ├── server/         # TCP server entry point
│   └── bench/          # Benchmark tool
├── pkg/
│   ├── config/         # Configuration management
│   ├── types/          # Core types and interfaces
│   ├── errors/         # Custom error types
│   ├── memtable/       # MemTable implementations
│   ├── wal/            # Write-Ahead Log
│   ├── sstable/        # SSTable format
│   ├── bloom/          # Bloom filters
│   ├── lsm/            # LSM engine core
│   ├── compaction/     # Compaction strategies
│   ├── mvcc/           # MVCC support
│   ├── iterator/       # Iterators
│   └── server/         # TCP server
├── internal/
│   ├── encoding/       # Binary encoding utilities
│   └── utils/          # General utilities
├── tests/
│   ├── unit/           # Unit tests
│   ├── integration/    # Integration tests
│   └── benchmark/      # Benchmark tests
├── go.mod
├── Makefile
└── README.md
```

## 📋 Implementation Roadmap

| Step | Component | Status | Description |
|:----:|:----------|:------:|:------------|
| 1 | Project Scaffold | ✅ | Basic structure, config, types |
| 2 | SkipList MemTable | ✅ | In-memory sorted data structure |
| 3 | Write-Ahead Log | ✅ | Durability layer |
| 4 | SSTable Writer | ⏳ | Immutable file format |
| 5 | SSTable Reader | ⏳ | Read with sparse index |
| 6 | Bloom Filters | ⏳ | Fast negative lookups |
| 7 | Basic LSM Engine | ⏳ | Combine MemTable + SSTable |
| 8 | Leveled Compaction | ⏳ | RocksDB-style compaction |
| 9 | Tiered Compaction | ⏳ | Universal compaction |
| 10 | FIFO Compaction | ⏳ | Time-based eviction |
| 11 | MVCC & Snapshots | ⏳ | Multi-version concurrency |
| 12 | Manifest & Recovery | ⏳ | Crash recovery |
| 13 | Iterators | ⏳ | Range scans |
| 14 | TCP Server | ⏳ | Memcached protocol |
| 15 | Benchmarks | ⏳ | Performance testing |

## 🔧 Configuration

RapidoDB uses YAML configuration. Example:

```yaml
data_dir: ./rapidodb_data

memtable:
  max_size: 67108864  # 64MB
  max_memtables: 4
  type: skiplist

wal:
  enabled: true
  sync_on_write: false
  max_size: 134217728  # 128MB

sstable:
  block_size: 4096
  sparse_index_interval: 16
  compression: none

compaction:
  strategy: leveled  # leveled, tiered, or fifo
  max_background_compactions: 4
  leveled:
    num_levels: 7
    l0_compaction_trigger: 4
    base_level_size: 268435456  # 256MB
    level_size_multiplier: 10

bloom_filter:
  enabled: true
  bits_per_key: 10

server:
  host: 0.0.0.0
  port: 11211
  max_connections: 1000
```

## 📊 Performance (Target)

| Operation | Throughput | Latency (p99) |
|:----------|:-----------|:--------------|
| Sequential Write | 400K ops/sec | < 50µs |
| Random Write | 300K ops/sec | < 100µs |
| Sequential Read | 500K ops/sec | < 30µs |
| Random Read | 200K ops/sec | < 200µs |

*Benchmarks run on NVMe SSD with 16 cores*

## 📖 Learning Resources

### Papers
- [The Log-Structured Merge-Tree (O'Neil et al.)](https://www.cs.umb.edu/~poneil/lsmtree.pdf)
- [Dostoevsky: Better Space-Time Trade-Offs for LSM-Tree](https://nivdayan.github.io/dostoevsky.pdf)
- [WiscKey: Separating Keys from Values](https://www.usenix.org/conference/fast16/technical-sessions/presentation/lu)

### Documentation
- [RocksDB Wiki](https://github.com/facebook/rocksdb/wiki)
- [LevelDB Implementation Notes](https://github.com/google/leveldb/blob/main/doc/impl.md)

### Courses
- [CMU 15-445 Database Systems](https://15445.courses.cs.cmu.edu/)
- [MIT 6.824 Distributed Systems](https://pdos.csail.mit.edu/6.824/)

## ⚠️ Disclaimer

This project is developed for **educational and research purposes** to explore LSM-tree internals and storage engine design.

**Not Production Ready:**
- Focus is on clarity over optimization
- Some edge cases may not be handled
- No production-level testing

## 📄 License

MIT License - see [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

Inspired by:
- [RocksDB](https://github.com/facebook/rocksdb) - Facebook's LSM-based storage engine
- [LevelDB](https://github.com/google/leveldb) - Google's original LSM implementation
- [BadgerDB](https://github.com/dgraph-io/badger) - Fast key-value store in Go
- [Mini-LSM](https://github.com/skyzh/mini-lsm) - Educational LSM implementation

---

*Built with ❤️ for learning*
