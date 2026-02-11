# RapidoDB

![Go Version](https://img.shields.io/badge/Go-1.22+-00ADD8?style=flat&logo=go)
![License](https://img.shields.io/badge/License-MIT-blue.svg)
![Status](https://img.shields.io/badge/Status-Educational-yellow.svg)

A high-performance, persistent Key-Value storage engine based on **Log-Structured Merge-tree (LSM-Tree)** architecture. Built for learning and understanding how modern databases like RocksDB, LevelDB, and PostgreSQL implement their storage layers.

```
╦═╗┌─┐┌─┐┬┌┬┐┌─┐╔╦╗╔╗ 
╠╦╝├─┤├─┘│ │││ │ ║║╠╩╗
╩╚═┴ ┴┴  ┴─┴┘└─┘═╩╝╚═╝
```

**Author:** Vladimir Sinica

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

- Go 1.22 or higher
- Make (optional, for convenience)
- Linux/macOS (Windows may work but untested)

### Building

```bash
# Clone the repository
git clone https://github.com/vladgaus/RapidoDB.git
cd RapidoDB

# Build all binaries
make build

# Or build manually
go build -o build/rapidodb-server ./cmd/server
go build -o build/rapidodb-bench ./cmd/bench
```

### Running the Server

```bash
# Run with default configuration
./build/rapidodb-server

# Run with custom data directory and port
./build/rapidodb-server --data-dir=/data/rapidodb --port=11211

# Run with config file
./build/rapidodb-server --config=config.example.yaml
```

### Testing

```bash
# Run all tests
make test

# Run with verbose output
make test-verbose

# Run with race detector
make test-race

# Run benchmarks
make bench
```

## 📁 Project Structure

```
RapidoDB/
├── cmd/
│   ├── server/              # TCP server entry point
│   │   └── main.go
│   └── bench/               # Benchmark tool
│       └── main.go
├── pkg/
│   ├── benchmark/           # Benchmark framework
│   │   ├── runner.go        # Benchmark runner
│   │   ├── stats.go         # Statistics collection
│   │   ├── tcp.go           # TCP benchmarks
│   │   └── workload.go      # Workload definitions
│   ├── bloom/               # Bloom filters
│   │   └── bloom.go
│   ├── compaction/          # Compaction strategies
│   │   ├── compaction.go    # Base types
│   │   ├── compactor.go     # Background compactor
│   │   ├── level_manager.go # Level management
│   │   ├── merge_iter.go    # Merge iterator
│   │   ├── leveled/         # Leveled compaction
│   │   ├── tiered/          # Tiered (universal) compaction
│   │   └── fifo/            # FIFO compaction
│   ├── config/              # Configuration management
│   │   └── config.go
│   ├── errors/              # Custom error types
│   │   └── errors.go
│   ├── iterator/            # Iterator implementations
│   │   ├── iterator.go      # Base interfaces
│   │   ├── merge.go         # Merge iterator
│   │   ├── bounded.go       # Bounded/prefix iterators
│   │   └── adapter.go       # Iterator adapters
│   ├── lsm/                 # LSM engine core
│   │   ├── engine.go        # Main engine
│   │   ├── open.go          # Open/recovery
│   │   ├── read.go          # Read path
│   │   └── write.go         # Write path
│   ├── manifest/            # Manifest & recovery
│   │   ├── manifest.go      # Manifest file
│   │   ├── version_edit.go  # Version edits
│   │   └── version_set.go   # Version management
│   ├── memtable/            # MemTable implementations
│   │   ├── memtable.go      # MemTable wrapper
│   │   └── skiplist.go      # SkipList implementation
│   ├── mvcc/                # MVCC support
│   │   └── snapshot.go      # Snapshot management
│   ├── server/              # TCP server
│   │   ├── server.go        # Server core
│   │   ├── connection.go    # Connection handler
│   │   └── protocol.go      # Memcached protocol
│   ├── sstable/             # SSTable format
│   │   ├── format.go        # File format
│   │   ├── writer.go        # SSTable writer
│   │   ├── reader.go        # SSTable reader
│   │   └── block.go         # Block handling
│   ├── types/               # Core types
│   │   ├── entry.go         # Key-value entry
│   │   └── interfaces.go    # Common interfaces
│   └── wal/                 # Write-Ahead Log
│       ├── manager.go       # WAL manager
│       ├── writer.go        # WAL writer
│       ├── reader.go        # WAL reader
│       └── record.go        # Record format
├── internal/
│   ├── encoding/            # Binary encoding utilities
│   │   └── encoding.go
│   └── utils/               # General utilities
│       └── utils.go
├── tests/
│   ├── benchmark/           # Benchmark tests
│   └── testutil/            # Test utilities
├── build/                   # Compiled binaries
├── config.example.yaml      # Example configuration
├── go.mod
├── Makefile
├── LICENSE
└── README.md
```

## 📋 Implementation Status

| Step | Component | Status | Description |
|:----:|:----------|:------:|:------------|
| 1 | Project Scaffold | ✅ | Basic structure, config, types |
| 2 | SkipList MemTable | ✅ | In-memory sorted data structure |
| 3 | Write-Ahead Log | ✅ | Durability layer |
| 4 | SSTable Writer | ✅ | Immutable file format |
| 5 | SSTable Reader | ✅ | Read with sparse index |
| 6 | Bloom Filters | ✅ | Fast negative lookups |
| 7 | Basic LSM Engine | ✅ | Combine MemTable + SSTable |
| 8 | Leveled Compaction | ✅ | RocksDB-style compaction |
| 9 | Tiered Compaction | ✅ | Universal compaction |
| 10 | FIFO Compaction | ✅ | Time-based eviction |
| 11 | MVCC & Snapshots | ✅ | Multi-version concurrency |
| 12 | Manifest & Recovery | ✅ | Crash recovery |
| 13 | Iterators | ✅ | Range scans, prefix scans |
| 14 | TCP Server | ✅ | Memcached protocol |
| 15 | Benchmarks | ✅ | Performance testing |

## 🔌 Memcached Protocol

RapidoDB supports the Memcached text protocol, allowing you to use any standard memcached client:

```bash
# Start server
./build/rapidodb-server --data-dir ./data --port 11211

# SET a value (use printf, not echo -e)
printf "set mykey 0 0 5\r\nhello\r\n" | nc localhost 11211
# STORED

# GET a value
printf "get mykey\r\n" | nc localhost 11211
# VALUE mykey 0 5
# hello
# END

# DELETE a value
printf "delete mykey\r\n" | nc localhost 11211
# DELETED

# INCREMENT a counter
printf "set counter 0 0 1\r\n5\r\n" | nc localhost 11211
printf "incr counter 3\r\n" | nc localhost 11211
# 8

# GET stats
printf "stats\r\n" | nc localhost 11211
```

**Supported commands:** `get`, `gets`, `set`, `add`, `replace`, `delete`, `incr`, `decr`, `stats`, `version`, `quit`

## ⚙️ Compaction Strategies

RapidoDB supports three compaction strategies. Choose based on your workload:

### 1. Leveled Compaction (Default)

Best for **read-heavy** workloads with good space efficiency.

```yaml
# config.yaml
compaction:
  strategy: leveled
  leveled:
    num_levels: 7
    l0_compaction_trigger: 4
    base_level_size: 268435456  # 256MB
    level_size_multiplier: 10
```

```bash
./build/rapidodb-server --config=config.yaml
```

### 2. Tiered Compaction

Best for **write-heavy** workloads with lower write amplification.

```yaml
# config-tiered.yaml
compaction:
  strategy: tiered
  tiered:
    min_sstables_to_merge: 4
    max_sstables_to_merge: 32
    size_ratio: 4
```

### 3. FIFO Compaction

Best for **time-series data** or caches where old data can be discarded.

```yaml
# config-fifo.yaml
compaction:
  strategy: fifo
  fifo:
    max_table_files_size: 1073741824  # 1GB total
    ttl_seconds: 86400                 # 24 hours
```

## 📊 Benchmark Tool

```bash
# Run all embedded benchmarks
./build/rapidodb-bench --mode all --num 100000

# Run specific benchmark with options
./build/rapidodb-bench --mode fillrandom --num 100000 --workers 4 --value-size 1024

# TCP benchmarks (start server first)
./build/rapidodb-server --data-dir ./data --port 11211 &
./build/rapidodb-bench --mode tcp-get --server 127.0.0.1:11211 --num 100000
```

**Available modes:** `fillseq`, `fillrandom`, `readseq`, `readrandom`, `readwrite`, `scan`, `delete`, `tcp-set`, `tcp-get`, `tcp-mixed`, `all`

### Performance Results

Benchmark results on a single core (your results will vary based on hardware):

| Workload | Throughput | Avg Latency | P99 Latency | MB/s |
|:---------|:-----------|:------------|:------------|:-----|
| fillseq | ~100K ops/sec | 9 µs | 25 µs | 11 MB/s |
| fillrandom | ~100K ops/sec | 9 µs | 22 µs | 11 MB/s |
| readseq | ~2M ops/sec | 0.15 µs | 0.3 µs | 227 MB/s |
| readrandom | ~1.8M ops/sec | 0.2 µs | 0.4 µs | 197 MB/s |
| readwrite (80/20) | ~350K ops/sec | 2.4 µs | 18 µs | 31 MB/s |

## 🔄 Comparing with LevelDB/RocksDB

To compare RapidoDB with production databases:

### Install LevelDB benchmark tool

```bash
# Build LevelDB with benchmarks
git clone https://github.com/google/leveldb.git
cd leveldb
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j$(nproc)

# Run LevelDB benchmark
./db_bench --benchmarks=fillseq,fillrandom,readseq,readrandom \
           --num=100000 --value_size=100
```

### Install RocksDB benchmark tool

```bash
# Build RocksDB with db_bench
git clone https://github.com/facebook/rocksdb.git
cd rocksdb
make db_bench -j$(nproc)

# Run RocksDB benchmark
./db_bench --benchmarks=fillseq,fillrandom,readseq,readrandom \
           --num=100000 --value_size=100
```

### Run RapidoDB benchmark

```bash
./build/rapidodb-bench --mode all --num 100000 --value-size 100
```

### Expected Comparison

| Database | fillrandom | readrandom | Notes |
|:---------|:-----------|:-----------|:------|
| RapidoDB | ~100K ops/s | ~1.8M ops/s | Limited Production, single-threaded |
| LevelDB | ~200K ops/s | ~500K ops/s | Production, optimized C++ |
| RocksDB | ~400K ops/s | ~800K ops/s | Production, highly optimized |

## 🖥️ Deployment Guide

### Deploy on Linux Server (from scratch)

```bash
# 1. Connect to your server
ssh root@your-server-ip

# 2. Update system
apt update && apt upgrade -y

# 3. Install Go (if not installed)
wget https://go.dev/dl/go1.22.0.linux-amd64.tar.gz
rm -rf /usr/local/go && tar -C /usr/local -xzf go1.22.0.linux-amd64.tar.gz
echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc
source ~/.bashrc
go version  # Verify installation

# 4. Install Git and clone RapidoDB
apt install -y git
git clone https://github.com/vladgaus/RapidoDB.git
cd RapidoDB

# 5. Build
make build

# 6. Create data directory
mkdir -p /var/lib/rapidodb

# 7. Run server (foreground for testing)
./build/rapidodb-server --data-dir=/var/lib/rapidodb --host=0.0.0.0 --port=11211

# 8. Test from another terminal
printf "set test 0 0 5\r\nhello\r\n" | nc localhost 11211
printf "get test\r\n" | nc localhost 11211
```

### Run as Systemd Service

```bash
# Create service file
cat > /etc/systemd/system/rapidodb.service << 'EOF'
[Unit]
Description=RapidoDB Key-Value Store
After=network.target

[Service]
Type=simple
User=root
ExecStart=/root/RapidoDB/build/rapidodb-server --data-dir=/var/lib/rapidodb --host=0.0.0.0 --port=11211
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

# Enable and start
systemctl daemon-reload
systemctl enable rapidodb
systemctl start rapidodb

# Check status
systemctl status rapidodb

# View logs
journalctl -u rapidodb -f
```

### Connect from Client Applications

```python
# Python example using pymemcache
from pymemcache.client import base

client = base.Client(('your-server-ip', 11211))
client.set('user:1', '{"name": "John", "age": 30}')
result = client.get('user:1')
print(result)  # b'{"name": "John", "age": 30}'
```

```go
// Go example using gomemcache
import "github.com/bradfitz/gomemcache/memcache"

mc := memcache.New("your-server-ip:11211")
mc.Set(&memcache.Item{Key: "user:1", Value: []byte(`{"name": "John"}`)})
item, _ := mc.Get("user:1")
fmt.Println(string(item.Value))
```

## 🎯 Use Cases

### ✅ Good For (OLTP-style workloads)

| Use Case | Why |
|:---------|:----|
| **Session Storage** | Fast reads/writes, simple key-value access |
| **Caching Layer** | Low-latency lookups, TTL support (FIFO) |
| **User Profiles** | Read-heavy, simple get/set operations |
| **Feature Flags** | Fast lookups, infrequent writes |
| **Rate Limiting** | Counter operations (incr/decr) |
| **Leaderboards** | Fast writes, range scans |
| **Real-time Analytics Counters** | High write throughput |

**Example: Session Storage**
```bash
# Store session
printf "set session:abc123 0 3600 45\r\n{\"user_id\":1,\"logged_in\":true,\"role\":\"admin\"}\r\n" | nc localhost 11211

# Retrieve session
printf "get session:abc123\r\n" | nc localhost 11211
```

**Example: Rate Limiting**
```bash
# Initialize counter
printf "set ratelimit:user:1 0 60 1\r\n0\r\n" | nc localhost 11211

# Increment on each request
printf "incr ratelimit:user:1 1\r\n" | nc localhost 11211
# Returns current count, reject if > threshold
```

### ❌ Not Ideal For (OLAP-style workloads)

| Use Case | Why Not | Alternative |
|:---------|:--------|:------------|
| **Complex Queries** | No SQL, no joins | PostgreSQL, MySQL |
| **Aggregations** | No SUM/AVG/GROUP BY | ClickHouse, TimescaleDB |
| **Full-text Search** | No text indexing | Elasticsearch |
| **Graph Relationships** | No graph traversal | Neo4j, DGraph |
| **Large Documents** | 1MB value limit | MongoDB, S3 |
| **Transactions** | No multi-key ACID | PostgreSQL, CockroachDB |

### 📊 Workload Patterns

```
                    RapidoDB Sweet Spot
                           ↓
Write-Heavy ←────────────────────────────→ Read-Heavy
     │                                          │
     │    ┌───────────────────────────────┐     │
     │    │                               │     │
     │    │   ✅ Sessions, Caching        │     │
     │    │   ✅ Counters, Rate Limits    │     │
     │    │   ✅ User Profiles            │     │
     │    │   ✅ Feature Flags            │     │
     │    │                               │     │
     │    └───────────────────────────────┘     │
     │                                          │
  Tiered                                    Leveled
  Strategy                                  Strategy
```

## 🔧 Configuration Reference

```yaml
# config.example.yaml - Full configuration reference

data_dir: ./rapidodb_data

memtable:
  max_size: 67108864      # 64MB - Size before flush
  max_memtables: 4        # Max immutable memtables
  type: skiplist          # Only skiplist supported

wal:
  enabled: true           # Disable for pure cache mode
  sync_on_write: false    # true = safer but slower
  max_size: 134217728     # 128MB per WAL file

sstable:
  block_size: 4096        # 4KB blocks
  sparse_index_interval: 16
  compression: none       # Compression not yet implemented

compaction:
  strategy: leveled       # leveled, tiered, or fifo
  max_background_compactions: 4
  
  leveled:
    num_levels: 7
    l0_compaction_trigger: 4
    l0_stop_writes_trigger: 12
    base_level_size: 268435456
    level_size_multiplier: 10
  
  tiered:
    min_sstables_to_merge: 4
    max_sstables_to_merge: 32
    size_ratio: 4
  
  fifo:
    max_table_files_size: 1073741824
    ttl_seconds: 0        # 0 = no TTL

bloom_filter:
  enabled: true
  bits_per_key: 10        # ~1% false positive rate

server:
  host: 127.0.0.1
  port: 11211
  max_connections: 1000
  read_timeout: 30s
  write_timeout: 30s
```

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
- Limited production-level testing
- Single-node only (no replication)

## 📄 License

MIT License - see [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

Inspired by:
- [RocksDB](https://github.com/facebook/rocksdb) - Facebook's LSM-based storage engine
- [LevelDB](https://github.com/google/leveldb) - Google's original LSM implementation
- [BadgerDB](https://github.com/dgraph-io/badger) - Fast key-value store in Go
- [Mini-LSM](https://github.com/skyzh/mini-lsm) - Educational LSM implementation

---

*Built with ❤️ for learning by Vladimir Sinica*
