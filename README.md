# RapidoDB

<p align="center">
  <strong>🚀 Fast. Light. Zero Dependencies.</strong>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Go-1.22+-00ADD8?style=flat&logo=go" alt="Go Version">
  <img src="https://img.shields.io/badge/License-BSL_1.1-blue.svg" alt="License">
  <img src="https://img.shields.io/badge/Dependencies-0-success.svg" alt="Zero Dependencies">
  <img src="https://img.shields.io/badge/Binary-4MB-green.svg" alt="Binary Size">
</p>

```
╦═╗┌─┐┌─┐┬┌┬┐┌─┐╔╦╗╔╗ 
╠╦╝├─┤├─┘│ │││ │ ║║╠╩╗
╩╚═┴ ┴┴  ┴─┴┘└─┘═╩╝╚═╝
```

<p align="center">
  A high-performance, embeddable Key-Value storage engine built on <strong>LSM-Tree</strong> architecture.<br>
  <em>Built for speed. Designed for simplicity. Ready for production.</em>
</p>

<p align="center">
  <strong>Created by <a href="https://github.com/vladgaus">Vladimir Sinica</a></strong>
</p>

---

## ⚡ Why RapidoDB?

| Feature | RapidoDB | LevelDB | RocksDB |
|:--------|:--------:|:-------:|:-------:|
| **Language** | Go | C++ | C++ |
| **Dependencies** | **0** | 2 | 20+ |
| **Binary Size** | **4 MB** | 1.5 MB | 15+ MB |
| **Build Time** | **< 5 sec** | Minutes | 10+ min |
| **Learn in** | **1 day** | 1 week | 2+ weeks |
| **Writes/sec** | 100K | 200K | 400K |
| **Reads/sec** | 1.5M | 3M | 3M |

**RapidoDB is 1.5-2x slower than LevelDB, but offers:**
- ✅ **Zero dependencies** — pure Go standard library
- ✅ **Tiny binary** — 4MB complete server
- ✅ **5 second build** — from clone to running
- ✅ **Drop-in ready** — Memcached protocol support
- ✅ **Multiple strategies** — Leveled, Tiered, FIFO compaction
- ✅ **MVCC snapshots** — consistent point-in-time reads

> **For 95% of applications, 100K writes/sec and 1.5M reads/sec is MORE than enough.**

---

## 🎯 Project Goals

This project implements a **production-grade storage engine** with focus on:

1. **LSM-Tree Architecture** — MemTable, SSTable, WAL, Compaction
2. **Multiple Compaction Strategies** — Leveled, Tiered (Universal), FIFO
3. **Performance Trade-offs** — Configurable write/read/space amplification
4. **Production-Quality Code** — Comprehensive tests, benchmarks, clean architecture

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
```

### Supported Commands

| Command | Syntax | Description |
|:--------|:-------|:------------|
| `get` | `get <key>` | Retrieve value |
| `gets` | `gets <key>` | Retrieve with CAS token |
| `set` | `set <key> <flags> <exptime> <bytes>` | Store value |
| `add` | `add <key> <flags> <exptime> <bytes>` | Store if not exists |
| `replace` | `replace <key> <flags> <exptime> <bytes>` | Store if exists |
| `append` | `append <key> <flags> <exptime> <bytes>` | Append to existing |
| `prepend` | `prepend <key> <flags> <exptime> <bytes>` | Prepend to existing |
| `cas` | `cas <key> <flags> <exptime> <bytes> <cas>` | Compare-and-swap |
| `delete` | `delete <key>` | Remove key |
| `incr` | `incr <key> <value>` | Increment numeric value |
| `decr` | `decr <key> <value>` | Decrement numeric value |
| `touch` | `touch <key> <exptime>` | Update expiration |
| `stats` | `stats` | Server statistics |
| `flush_all` | `flush_all` | Clear all data |
| `version` | `version` | Server version |
| `quit` | `quit` | Close connection |

### Using with Client Libraries

**Python (pymemcache)**
```python
from pymemcache.client import base

client = base.Client(('localhost', 11211))
client.set('user:1', '{"name": "John", "age": 30}')
result = client.get('user:1')
print(result)  # b'{"name": "John", "age": 30}'
```

**Go (gomemcache)**
```go
import "github.com/bradfitz/gomemcache/memcache"

mc := memcache.New("localhost:11211")
mc.Set(&memcache.Item{Key: "user:1", Value: []byte(`{"name": "John"}`)})
item, _ := mc.Get("user:1")
fmt.Println(string(item.Value))
```

**Node.js (memcached)**
```javascript
const Memcached = require('memcached');
const client = new Memcached('localhost:11211');

client.set('user:1', '{"name": "John"}', 3600, (err) => {
    client.get('user:1', (err, data) => {
        console.log(data);
    });
});
```

## 📊 Benchmarks

Run benchmarks using the built-in tool:

```bash
# Build benchmark tool
make bench-tool

# Run all benchmarks
./build/rapidodb-bench --mode all --num 100000

# Specific benchmarks
./build/rapidodb-bench --mode fillseq --num 100000
./build/rapidodb-bench --mode fillrandom --num 100000
./build/rapidodb-bench --mode readrandom --num 100000
./build/rapidodb-bench --mode readseq --num 100000
./build/rapidodb-bench --mode scan --num 100000
```

### Performance Results

Tested on standard cloud VM (4 vCPU, 8GB RAM, NVMe SSD):

| Workload | Ops/sec | Avg Latency | P99 Latency | Throughput |
|:---------|--------:|------------:|------------:|-----------:|
| **fillseq** | 100,834 | 9.4 µs | 39 µs | 11 MB/s |
| **fillrandom** | 87,619 | 10.9 µs | 50 µs | 10 MB/s |
| **readseq** | 1,445,363 | 0.31 µs | 0.76 µs | 160 MB/s |
| **readrandom** | 1,454,217 | 0.36 µs | 0.66 µs | 161 MB/s |
| **mixed (80/20)** | 374,028 | 2.3 µs | 19 µs | 33 MB/s |

### Comparison with LevelDB & RocksDB

| Metric | RapidoDB | LevelDB | RocksDB |
|:-------|:--------:|:-------:|:-------:|
| Random Writes | ~100K/s | ~200K/s | ~400K/s |
| Random Reads | ~1.5M/s | ~3M/s | ~3M/s |
| Build Time | 5 sec | 2 min | 10+ min |
| Dependencies | 0 | 2 | 20+ |
| Binary Size | 4 MB | 1.5 MB | 15+ MB |
| Language | Go | C++ | C++ |

**RapidoDB trades some raw speed for developer productivity and operational simplicity.**

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

### Docker Deployment

```bash
# Build image
docker build -t rapidodb .

# Run container
docker run -p 11211:11211 -v rapidodb-data:/data rapidodb
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

## 💼 Pricing Plans (Coming Soon)

| Plan | Price | Features |
|:-----|------:|:---------|
| **Community** | Free | Full engine, self-hosted, community support |
| **Pro** | $29/mo | Priority email support, early access to updates |
| **Team** | $99/mo | 5 instances, monitoring dashboard, Slack support |
| **Business** | $299/mo | Unlimited instances, 99.9% SLA, phone support |
| **Enterprise** | Custom | On-premise, custom SLA, training, white-label |

### RapidoDB Cloud (Planned)

| Tier | RAM | Storage | Price |
|:-----|----:|--------:|------:|
| **Starter** | 512MB | 10GB | $9/mo |
| **Growth** | 2GB | 50GB | $29/mo |
| **Scale** | 8GB | 200GB | $99/mo |
| **Pro** | 32GB | 1TB | $299/mo |

## 📄 License

**Business Source License 1.1** — See [LICENSE](LICENSE) file for details.

- ✅ Free for internal use
- ✅ Free for SaaS backends  
- ✅ Free for startups & enterprises
- ❌ Cannot offer as Database-as-a-Service
- 🔄 Converts to Apache 2.0 after 4 years

## 🙏 Acknowledgments

Inspired by:
- [RocksDB](https://github.com/facebook/rocksdb) - Facebook's LSM-based storage engine
- [LevelDB](https://github.com/google/leveldb) - Google's original LSM implementation
- [BadgerDB](https://github.com/dgraph-io/badger) - Fast key-value store in Go
- [Mini-LSM](https://github.com/skyzh/mini-lsm) - Educational LSM implementation

---

<p align="center">
  <strong>Built with ❤️ by <a href="https://github.com/vladgaus">Vladimir Sinica</a></strong><br>
  <em>Fast. Light. Zero Dependencies.</em>
</p>
