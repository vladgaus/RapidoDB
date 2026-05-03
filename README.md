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

### Performance Comparison

| Feature | RapidoDB | BadgerDB | LevelDB | RocksDB |
|:--------|:--------:|:--------:|:-------:|:-------:|
| **Language** | Go | Go | C++ | C++ |
| **Storage** | Disk (LSM) | Disk (LSM) | Disk (LSM) | Disk (LSM) |
| **Dependencies** | **0** | ~10 | 2 | 20+ |
| **Binary Size** | **4 MB** | 8+ MB | 1.5 MB | 15+ MB |
| **Build Time** | **< 5 sec** | 30 sec | Minutes | 10+ min |
| **CGO Required** | **No** | No | Yes | Yes |
| **Server Mode** | **Yes** | No | No | No |
| **Memcached Protocol** | **Yes** | No | No | No |

> **Note**: All databases above are disk-based LSM-tree implementations. RapidoDB uniquely offers built-in server mode with Memcached protocol.

### RapidoDB vs BadgerDB

| Aspect | RapidoDB | BadgerDB |
|:-------|:--------:|:--------:|
| **Language** | Pure Go | Pure Go |
| **Dependencies** | **0** | ~10 |
| **CGO Required** | **No** | No |
| **Server Mode** | **Yes (built-in)** | No |
| **Protocol** | Memcached | Library only |
| **Architecture** | Traditional LSM | WiscKey (SSD-optimized) |
| **Maturity** | New | 7+ years |
| **GitHub Stars** | New | 14K+ |

**Choose RapidoDB when:**
- ✅ You need a standalone server (not just a library)
- ✅ You want zero dependencies
- ✅ You need Memcached protocol compatibility
- ✅ You prefer simpler architecture
- ✅ You want easy deployment (single binary)

**Choose BadgerDB when:**
- ✅ You need a mature, battle-tested library
- ✅ You're building Go applications that embed the DB
- ✅ You need WiscKey's SSD-optimized architecture
- ✅ You have an active community and support

### Key Benefits

**RapidoDB offers:**
- ✅ **Zero dependencies** — pure Go standard library
- ✅ **Tiny binary** — 4MB complete server
- ✅ **5 second build** — from clone to running
- ✅ **Drop-in ready** — Memcached protocol support
- ✅ **Multiple strategies** — Leveled, Tiered, FIFO compaction
- ✅ **MVCC snapshots** — consistent point-in-time reads
- ✅ **Unlimited data size** — not limited by RAM

> **For 95% of applications, 300K writes/sec and 1.4M reads/sec is MORE than enough.**

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
go build -o build/rapidodb-cli ./cmd/cli
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
│   ├── bench/               # Benchmark tool
│   │   └── main.go
│   └── cli/                 # Command-line interface
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
│   ├── health/              # Health checks & probes
│   │   ├── health.go        # Health checker core
│   │   ├── checker.go       # Individual health checkers
│   │   └── http.go          # HTTP health endpoints
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
│   ├── metrics/             # Prometheus metrics
│   │   ├── metrics.go       # Registry & core types
│   │   ├── counter.go       # Counter metric
│   │   ├── gauge.go         # Gauge metric
│   │   ├── histogram.go     # Histogram metric
│   │   └── collector.go     # RapidoDB collector
│   ├── logging/             # Structured logging
│   │   ├── logging.go       # Logger core (slog-based)
│   │   ├── rotation.go      # Log rotation
│   │   └── request_id.go    # Request ID generation
│   ├── mvcc/                # MVCC support
│   │   └── snapshot.go      # Snapshot management
│   ├── ratelimit/           # Rate limiting
│   │   ├── ratelimit.go     # Core types & interfaces
│   │   ├── token_bucket.go  # Token bucket algorithm
│   │   ├── sliding_window.go # Sliding window algorithm
│   │   └── client_limiter.go # Per-client rate limiting
│   ├── server/              # TCP server
│   │   ├── server.go        # Server core
│   │   ├── connection.go    # Connection handler
│   │   └── protocol.go      # Memcached protocol
│   ├── shutdown/            # Graceful shutdown
│   │   ├── shutdown.go      # Shutdown coordinator
│   │   └── drainer.go       # Request drainer
│   ├── tracing/             # Distributed tracing
│   │   ├── tracing.go       # Span and trace IDs
│   │   ├── tracer.go        # Tracer implementation
│   │   ├── sampler.go       # Sampling strategies
│   │   ├── exporter.go      # Jaeger/Zipkin exporters
│   │   └── propagation.go   # W3C/B3 context propagation
│   ├── admin/               # Admin HTTP API
│   │   └── admin.go         # Endpoints for operations
│   ├── backup/              # Backup & Restore
│   │   ├── backup.go        # Backup manager
│   │   └── backend.go       # Storage backends (local, S3)
│   ├── importer/            # Import/Export
│   │   ├── importer.go      # CSV/JSON import/export
│   │   └── sstable_import.go # SSTable direct import
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
| 16 | Health Checks | ✅ | Kubernetes-native probes |
| 17 | Graceful Shutdown | ✅ | Signal handling, drain, flush |
| 18 | Rate Limiting | ✅ | Token bucket, per-client limits |
| 19 | Prometheus Metrics | ✅ | GET /metrics endpoint |
| 20 | Structured Logging | ✅ | JSON/text, levels, rotation |
| 21 | Distributed Tracing | ✅ | OpenTelemetry, Jaeger/Zipkin |
| 22 | Admin API | ✅ | HTTP endpoints for operations |
| 23 | Backup/Restore | ✅ | Hot backups, incremental, S3 |
| 24 | Import/Export | ✅ | CSV, JSON, SSTable, streaming |
| 25 | CLI Tool | ✅ | Interactive management |

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

## 🏥 Health Check API

RapidoDB includes a built-in HTTP health check server for monitoring and Kubernetes integration.

```bash
# Start server (health server runs on port 8080 by default)
./build/rapidodb-server --data-dir ./data

# Or specify a custom health port
./build/rapidodb-server --data-dir ./data --health-port 9090

# Disable health server
./build/rapidodb-server --data-dir ./data --no-health
```

### Health Endpoints

| Endpoint | Description | Use Case |
|:---------|:------------|:---------|
| `GET /health` | Full health report with all checks | Monitoring dashboards |
| `GET /health/live` | Liveness probe (is process alive?) | Kubernetes liveness |
| `GET /health/ready` | Readiness probe (ready for traffic?) | Kubernetes readiness |

### Example Requests

```bash
# Full health status
curl http://localhost:8080/health
# {
#   "status": "healthy",
#   "uptime": "24h30m0s",
#   "version": "RapidoDB/0.15.0",
#   "checks": [
#     {"name": "memory", "status": "healthy", "message": "Heap: 45.2 MB used"},
#     {"name": "disk", "status": "healthy", "message": "25.3% used (150.2 GB free)"},
#     {"name": "engine", "status": "healthy", "message": "Engine is running"},
#     {"name": "server", "status": "healthy", "message": "42 active connections"}
#   ]
# }

# Kubernetes liveness probe
curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/health/live
# 200

# Kubernetes readiness probe  
curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/health/ready
# 200

# Detailed liveness response
curl http://localhost:8080/health/live
# {"status": "live", "uptime": "24h30m0s"}

# Detailed readiness response
curl http://localhost:8080/health/ready
# {"status": "ready", "version": "RapidoDB/0.15.0"}
```

### Health Checks Included

| Check | Monitors | Degraded | Unhealthy |
|:------|:---------|:---------|:----------|
| **Memory** | Heap usage, GC pressure | GC rate > 10/s | — |
| **Disk** | Data directory free space | > 80% used | > 95% or < 100MB |
| **Engine** | LSM engine state | — | Engine closed |
| **Server** | Active connections | > 80% of max | > 95% of max |

### Kubernetes Integration

```yaml
apiVersion: apps/v1
kind: Deployment
spec:
  template:
    spec:
      containers:
      - name: rapidodb
        image: rapidodb:latest
        ports:
        - containerPort: 11211  # Memcached protocol
        - containerPort: 8080   # Health checks
        livenessProbe:
          httpGet:
            path: /health/live
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /health/ready
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 5
```

### Health Configuration

```json
{
  "health": {
    "enabled": true,
    "host": "0.0.0.0",
    "port": 8080,
    "disk_warning_percent": 80,
    "disk_critical_percent": 95,
    "memory_max_heap_mb": 0
  }
}
```

## 🛑 Graceful Shutdown

RapidoDB supports graceful shutdown to ensure zero data loss when stopping the server.

### Signal Handling

```bash
# Send SIGTERM (graceful shutdown)
kill -TERM <pid>

# Send SIGINT (Ctrl+C in terminal)
kill -INT <pid>

# Or simply press Ctrl+C when running in foreground
```

### Shutdown Sequence

When a shutdown signal is received, RapidoDB executes the following steps in order:

1. **Mark as not ready** — Health `/health/ready` returns 503, Kubernetes stops routing traffic
2. **Close health server** — Stop accepting health check requests
3. **Drain connections** — Wait for in-flight requests to complete (configurable timeout)
4. **Flush MemTable** — Write any unflushed data to SSTables
5. **Sync WAL** — Ensure all writes are persisted to disk
6. **Stop compactor** — Wait for any running compaction to complete
7. **Close files** — Clean up all open file handles

### Shutdown Configuration

```json
{
  "shutdown": {
    "timeout": "30s",
    "drain_timeout": "10s"
  }
}
```

| Setting | Default | Description |
|:--------|:--------|:------------|
| `timeout` | 30s | Maximum time for entire shutdown process |
| `drain_timeout` | 10s | Time to wait for in-flight requests to complete |

### Example Output

```
^C
Received signal: interrupt
Starting graceful shutdown...
[shutdown] Initiating graceful shutdown: received signal: interrupt
[shutdown] Running shutdown hook: mark-not-ready
  → Marked as not ready
[shutdown] Running shutdown hook: health-server
  → Health server closed
[shutdown] Running shutdown hook: tcp-server
  → Draining 3 active connections...
  → TCP server closed
[shutdown] Running shutdown hook: storage-engine
  → Flushing MemTable and syncing WAL...
  → Storage engine closed
[shutdown] Shutdown complete in 245ms

Shutdown completed successfully in 245ms
Goodbye!
```

### Kubernetes Graceful Shutdown

For Kubernetes deployments, configure the `terminationGracePeriodSeconds` to match your shutdown timeout:

```yaml
spec:
  terminationGracePeriodSeconds: 35  # timeout + buffer
  containers:
  - name: rapidodb
    lifecycle:
      preStop:
        exec:
          command: ["sleep", "5"]  # Allow time for endpoint removal
```

## 🚦 Rate Limiting

RapidoDB includes built-in rate limiting to protect against abuse and ensure fair resource usage.

### Features

- **Token Bucket Algorithm** — Smooth rate limiting with burst support
- **Sliding Window Algorithm** — Precise rate limiting over time windows
- **Per-Client Limits** — Separate limits for each client IP
- **Global Limits** — Server-wide throughput protection
- **Backpressure Signaling** — Retry-after hints for clients

### Configuration

```json
{
  "rate_limit": {
    "enabled": true,
    "algorithm": "token_bucket",
    "global": {
      "enabled": true,
      "rate": 10000,
      "burst": 10000
    },
    "per_client": {
      "enabled": true,
      "rate": 100,
      "burst": 100,
      "max_idle_time": "5m"
    }
  }
}
```

| Setting | Default | Description |
|:--------|:--------|:------------|
| `algorithm` | token_bucket | Rate limiting algorithm (token_bucket or sliding_window) |
| `global.rate` | 10000 | Server-wide requests per second |
| `global.burst` | 10000 | Maximum burst size for server |
| `per_client.rate` | 100 | Per-client requests per second |
| `per_client.burst` | 100 | Maximum burst size per client |
| `per_client.max_idle_time` | 5m | Cleanup idle client entries after this duration |

### Rate Limit Response

When rate limited, clients receive a `SERVER_ERROR` response with retry-after hint:

```
SERVER_ERROR RATE_LIMITED retry_after=10ms
```

### Algorithms

**Token Bucket** (default)
- Tokens added at constant rate
- Allows bursts up to bucket capacity
- Best for smooth traffic with occasional spikes

**Sliding Window**
- Tracks requests in sliding time windows
- More precise rate limiting
- Better for strict rate enforcement

### Programmatic Access

```go
// Check rate limit status
stats := server.RateLimitStats()
fmt.Printf("Active clients: %d\n", stats.ActiveClients)
fmt.Printf("Global limited: %d\n", stats.GlobalLimited)
fmt.Printf("Client limited: %d\n", stats.ClientLimited)
```

## 📈 Prometheus Metrics

RapidoDB exposes metrics in Prometheus text format for monitoring and alerting.

### Endpoint

```bash
# Start server (metrics server runs on port 9090 by default)
./build/rapidodb-server --data-dir ./data

# Fetch metrics
curl http://localhost:9090/metrics
```

### Available Metrics

| Metric | Type | Description |
|:-------|:-----|:------------|
| `rapidodb_writes_total` | counter | Total write operations |
| `rapidodb_reads_total` | counter | Total read operations |
| `rapidodb_deletes_total` | counter | Total delete operations |
| `rapidodb_write_latency_seconds` | histogram | Write latency distribution |
| `rapidodb_read_latency_seconds` | histogram | Read latency distribution |
| `rapidodb_memtable_size_bytes` | gauge | Current MemTable size |
| `rapidodb_sstable_count` | gauge | Number of SSTable files |
| `rapidodb_wal_size_bytes` | gauge | Current WAL size |
| `rapidodb_disk_usage_bytes` | gauge | Total disk usage |
| `rapidodb_compaction_duration_seconds` | histogram | Compaction duration |
| `rapidodb_bloom_filter_hits_total` | counter | Bloom filter hits |
| `rapidodb_bloom_filter_misses_total` | counter | Bloom filter misses |
| `rapidodb_active_connections` | gauge | Current active connections |
| `rapidodb_connections_total` | counter | Total connections |
| `rapidodb_rate_limited_total` | counter | Rate limited requests |
| `rapidodb_goroutines` | gauge | Number of goroutines |
| `rapidodb_heap_alloc_bytes` | gauge | Heap memory allocated |
| `rapidodb_info` | gauge | Version information |

### Example Output

```
# HELP rapidodb_writes_total Total number of write operations
# TYPE rapidodb_writes_total counter
rapidodb_writes_total 1542

# HELP rapidodb_read_latency_seconds Read operation latency in seconds
# TYPE rapidodb_read_latency_seconds histogram
rapidodb_read_latency_seconds_bucket{le="0.0001"} 892
rapidodb_read_latency_seconds_bucket{le="0.001"} 1456
rapidodb_read_latency_seconds_bucket{le="+Inf"} 1542
rapidodb_read_latency_seconds_sum 0.234
rapidodb_read_latency_seconds_count 1542

# HELP rapidodb_active_connections Number of active client connections
# TYPE rapidodb_active_connections gauge
rapidodb_active_connections 5

# HELP rapidodb_info RapidoDB version information
# TYPE rapidodb_info gauge
rapidodb_info{version="0.16.0"} 1
```

### Configuration

```json
{
  "metrics": {
    "enabled": true,
    "host": "0.0.0.0",
    "port": 9090
  }
}
```

### Grafana Integration

Import the RapidoDB dashboard or create custom panels:

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'rapidodb'
    static_configs:
      - targets: ['localhost:9090']
    scrape_interval: 15s
```

## 📝 Structured Logging

RapidoDB uses structured logging with JSON output for easy parsing by log aggregators.

### Features

| Feature | Description |
|:--------|:------------|
| **JSON Format** | Machine-readable log output |
| **Text Format** | Human-readable alternative |
| **Log Levels** | debug, info, warn, error |
| **Request IDs** | Trace requests across logs |
| **Log Rotation** | Automatic file rotation |
| **Compression** | Gzip old log files |

### Example Output

```json
{"time":"2024-01-15T10:30:00Z","level":"INFO","msg":"server started","component":"rapidodb","addr":"0.0.0.0:11211"}
{"time":"2024-01-15T10:30:01Z","level":"INFO","msg":"request completed","request_id":"a1b2c3d4-00000001","operation":"SET","duration_ms":0.45}
{"time":"2024-01-15T10:30:02Z","level":"WARN","msg":"rate limited","client":"192.168.1.100","retry_after_ms":100}
```

### Configuration

```json
{
  "logging": {
    "level": "info",
    "format": "json",
    "output": "stdout",
    "add_source": false,
    "file": {
      "max_size": 100,
      "max_backups": 5,
      "max_age": 0,
      "compress": false
    }
  }
}
```

| Setting | Default | Description |
|:--------|:--------|:------------|
| `level` | `info` | Minimum log level (debug, info, warn, error) |
| `format` | `json` | Output format (json, text) |
| `output` | `stdout` | Output destination (stdout, stderr, or file path) |
| `add_source` | `false` | Include source file and line in logs |
| `file.max_size` | `100` | Max file size in MB before rotation |
| `file.max_backups` | `5` | Number of rotated files to keep |
| `file.max_age` | `0` | Max age in days (0 = no limit) |
| `file.compress` | `false` | Gzip compress rotated files |

### Log to File with Rotation

```json
{
  "logging": {
    "level": "debug",
    "format": "json",
    "output": "/var/log/rapidodb/rapidodb.log",
    "file": {
      "max_size": 100,
      "max_backups": 10,
      "max_age": 30,
      "compress": true
    }
  }
}
```

### Programmatic Usage

```go
import "github.com/vladgaus/RapidoDB/pkg/logging"

// Create logger
logger := logging.New(logging.Options{
    Level:  logging.LevelInfo,
    Format: logging.FormatJSON,
    Output: os.Stdout,
})

// Log with structured fields
logger.Info("operation completed",
    "key", "mykey",
    "duration_ms", 1.5,
    "bytes", 1024,
)

// Create request-scoped logger
reqID := logging.GenerateRequestID()
reqLogger := logging.NewRequestLogger(logger, reqID)
reqLogger.Start("GET")
// ... do work ...
reqLogger.Success("GET", "status", 200)
```

## 🔍 Distributed Tracing

RapidoDB provides distributed tracing compatible with OpenTelemetry, Jaeger, and Zipkin.

### Features

| Feature | Description |
|:--------|:------------|
| **OpenTelemetry Compatible** | W3C Trace Context propagation |
| **Jaeger Export** | HTTP/Thrift export to Jaeger |
| **Zipkin Export** | HTTP v2 API export to Zipkin |
| **B3 Propagation** | Zipkin B3 header support |
| **Sampling** | Always, Never, Ratio, Rate-limited |
| **Zero Dependencies** | No external libraries required |

### Example Output (JSON)

```json
{
  "trace_id": "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6",
  "span_id": "1234567890abcdef",
  "parent_span_id": "abcdef1234567890",
  "name": "rapidodb.get",
  "kind": "SERVER",
  "start_time": "2024-01-15T10:30:00.123456789Z",
  "duration_ms": 0.45,
  "status": "OK",
  "attributes": {
    "service.name": "rapidodb",
    "db.key": "mykey",
    "db.bytes.read": 1024
  }
}
```

### Configuration

```go
import "github.com/vladgaus/RapidoDB/pkg/tracing"

// Create tracer with Jaeger export
tracer := tracing.NewTracer(tracing.TracerOptions{
    ServiceName: "rapidodb",
    Exporter:    tracing.NewJaegerExporter(tracing.JaegerExporterOptions{
        Endpoint: "http://localhost:14268/api/traces",
    }),
    Sampler: tracing.RatioSampler(0.1), // Sample 10%
})

// Create span for operation
ctx, span := tracer.Start(ctx, tracing.SpanGet,
    tracing.WithSpanKind(tracing.SpanKindServer),
)
defer span.End()

// Add attributes
span.SetAttributes(
    tracing.String("db.key", key),
    tracing.Int("db.bytes.read", len(value)),
)

// Record errors
if err != nil {
    span.RecordError(err)
}
```

### Trace Context Propagation

```go
// W3C Trace Context
propagator := tracing.NewTraceContextPropagator()

// Inject into outgoing request
carrier := make(tracing.MapCarrier)
propagator.Inject(ctx, carrier)
// carrier["traceparent"] = "00-{trace_id}-{span_id}-01"

// Extract from incoming request
ctx = propagator.Extract(ctx, carrier)
```

### Sampling Strategies

```go
// Always sample (development)
tracing.AlwaysSample()

// Never sample (disable tracing)
tracing.NeverSample()

// Sample 10% of traces
tracing.RatioSampler(0.1)

// Limit to 100 traces/second
tracing.RateLimitedSampler(100)
```

### Export to Jaeger

```bash
# Start Jaeger
docker run -d --name jaeger \
  -p 14268:14268 \
  -p 16686:16686 \
  jaegertracing/all-in-one

# View traces at http://localhost:16686
```

### Export to Zipkin

```bash
# Start Zipkin
docker run -d --name zipkin \
  -p 9411:9411 \
  openzipkin/zipkin

# View traces at http://localhost:9411
```

## 🔧 Admin API

RapidoDB provides an HTTP Admin API for operational tasks.

### Endpoints

| Endpoint | Method | Description |
|:---------|:-------|:------------|
| `/admin/compact` | POST | Trigger manual compaction |
| `/admin/flush` | POST | Force MemTable flush to disk |
| `/admin/sstables` | GET | List all SSTables with sizes |
| `/admin/levels` | GET | Level statistics |
| `/admin/config` | POST | Hot reload configuration |
| `/admin/properties` | GET | Database properties |
| `/admin/range` | DELETE | Delete key range |
| `/admin/stats` | GET | Operation statistics |
| `/admin/import/csv` | POST | Import from CSV file |
| `/admin/import/json` | POST | Import from JSON Lines file |
| `/admin/export/csv` | POST | Export to CSV file |
| `/admin/export/json` | POST | Export to JSON Lines file |
| `/admin/import/stats` | GET | Import/Export statistics |

### Configuration

```json
{
  "admin": {
    "enabled": true,
    "host": "127.0.0.1",
    "port": 9091,
    "auth_token": "your-secret-token"
  }
}
```

```bash
# Start server with enabled api
./build/rapidodb-server --data-dir ./data --config config.json
```

### Example Usage

```bash
# Trigger compaction
curl -X POST http://localhost:9091/admin/compact

# Force flush
curl -X POST http://localhost:9091/admin/flush

# List SSTables
curl http://localhost:9091/admin/sstables

# Get level stats
curl http://localhost:9091/admin/levels

# Get database properties
curl http://localhost:9091/admin/properties

# Get operation statistics
curl http://localhost:9091/admin/stats

# Delete key range (maintenance)
curl -X DELETE http://localhost:9091/admin/range \
  -H "Content-Type: application/json" \
  -d '{"start_key": "temp:", "end_key": "temp:~"}'

# Import from CSV file
curl -X POST http://localhost:9091/admin/import/csv \
  -H "Content-Type: application/json" \
  -d '{"path": "/data/import.csv", "has_header": true, "skip_errors": true}'

# Import from JSON Lines file
curl -X POST http://localhost:9091/admin/import/json \
  -H "Content-Type: application/json" \
  -d '{"path": "/data/import.jsonl", "key_prefix": "imported:"}'

# Export to CSV file
curl -X POST http://localhost:9091/admin/export/csv \
  -H "Content-Type: application/json" \
  -d '{"path": "/data/export.csv", "key_prefix": "user:", "include_header": true}'

# Export to JSON Lines file
curl -X POST http://localhost:9091/admin/export/json \
  -H "Content-Type: application/json" \
  -d '{"path": "/data/export.jsonl", "limit": 10000}'

# Get import/export statistics
curl http://localhost:9091/admin/import/stats
```

### With Authentication

```bash
# If auth_token is configured
curl -H "Authorization: Bearer your-secret-token" \
  http://localhost:9091/admin/stats
```

### Example Responses

**GET /admin/levels**
```json
{
  "success": true,
  "data": {
    "num_levels": 7,
    "total_size": 104857600,
    "total_size_hr": "100.0 MB",
    "total_files": 15,
    "levels": [
      {"level": 0, "num_files": 3, "size": 12582912, "size_hr": "12.0 MB"},
      {"level": 1, "num_files": 5, "size": 52428800, "size_hr": "50.0 MB"}
    ]
  }
}
```

**GET /admin/stats**
```json
{
  "success": true,
  "data": {
    "total_reads": 1000000,
    "total_writes": 500000,
    "total_deletes": 10000,
    "bytes_read": 104857600,
    "bytes_written": 52428800,
    "cache_hits": 950000,
    "cache_misses": 50000,
    "hit_rate": "95.00%"
  }
}
```

## 💾 Backup & Restore

RapidoDB provides hot backup functionality with support for full and incremental backups.

### Features

| Feature | Description |
|:--------|:------------|
| **Hot Backup** | Backup while DB is running |
| **Incremental** | Only backup changed files |
| **Point-in-Time** | Restore to specific sequence number |
| **Checksums** | SHA256 verification |
| **Multiple Backends** | Local filesystem, S3 (stub) |

### Configuration

```json
{
  "backup": {
    "enabled": true,
    "backend": "./backups",
    "compression": false
  }
}
```

### API Endpoints

| Endpoint | Method | Description |
|:---------|:-------|:------------|
| `/admin/backup` | POST | Create new backup |
| `/admin/backup` | GET | Get backup info (with ?id=) |
| `/admin/backup` | DELETE | Delete backup (with ?id=) |
| `/admin/backup/list` | GET | List all backups |
| `/admin/backup/restore` | POST | Restore a backup |

### Example Usage

```bash
# Create full backup
curl -X POST http://localhost:9091/admin/backup \
  -H "Content-Type: application/json" \
  -d '{"type": "full"}'

# Create incremental backup
curl -X POST http://localhost:9091/admin/backup \
  -H "Content-Type: application/json" \
  -d '{"type": "incremental"}'

# List backups
curl http://localhost:9091/admin/backup/list

# Get specific backup
curl "http://localhost:9091/admin/backup?id=backup-20240115-120000-123456"

# Restore backup
curl -X POST http://localhost:9091/admin/backup/restore \
  -H "Content-Type: application/json" \
  -d '{
    "backup_id": "backup-20240115-120000-123456",
    "target_dir": "/data/restored",
    "verify": true
  }'

# Delete backup
curl -X DELETE "http://localhost:9091/admin/backup?id=backup-20240115-120000-123456"
```

### Programmatic Usage

```go
import "github.com/vladgaus/RapidoDB/pkg/backup"

// Create backup manager
backend := backup.NewLocalBackend("./backups")
manager := backup.NewManager(backup.ManagerOptions{
    Engine:  engine,
    Backend: backend,
})

// Create full backup
info, err := manager.CreateBackup(ctx, backup.BackupOptions{
    Type: backup.BackupTypeFull,
})
fmt.Printf("Backup created: %s (%d files, %d bytes)\n",
    info.ID, info.FileCount, info.TotalSize)

// Create incremental backup
incr, err := manager.CreateBackup(ctx, backup.BackupOptions{
    Type: backup.BackupTypeIncremental,
})

// Restore
err = manager.Restore(ctx, backup.RestoreOptions{
    BackupID:  info.ID,
    TargetDir: "/data/restored",
    Verify:    true,
})

// List backups
backups, _ := manager.ListBackups(ctx)
for _, b := range backups {
    fmt.Printf("%s: %s (%s)\n", b.ID, b.Type, b.Status)
}
```

### Example Responses

**POST /admin/backup**
```json
{
  "success": true,
  "data": {
    "backup_id": "backup-20240115-120000-123456",
    "type": "full",
    "status": "completed",
    "start_time": "2024-01-15T12:00:00Z",
    "sequence_number": 12345,
    "total_size": 104857600,
    "total_size_hr": "100.0 MB",
    "file_count": 25
  }
}
```

**GET /admin/backup/list**
```json
{
  "success": true,
  "data": {
    "backups": [
      {
        "id": "backup-20240115-120000-123456",
        "type": "full",
        "status": "completed",
        "total_size": 104857600,
        "file_count": 25
      },
      {
        "id": "backup-20240115-140000-789012",
        "type": "incremental",
        "status": "completed",
        "parent_id": "backup-20240115-120000-123456",
        "total_size": 1048576,
        "file_count": 3
      }
    ],
    "count": 2
  }
}
```

## 📥 Import/Export

RapidoDB provides bulk import and export functionality for data migration and ETL workflows.

### Supported Formats

| Format | Import | Export | Description |
|:-------|:------:|:------:|:------------|
| CSV | ✅ | ✅ | `key,value` format with configurable columns |
| JSON Lines | ✅ | ✅ | `{"key":"...","value":"..."}` per line |
| SSTable | ✅ | - | Direct SSTable file import (fastest) |

### CSV Import/Export

```go
import "github.com/vladgaus/RapidoDB/pkg/importer"

imp := importer.New(importer.Options{Engine: engine})

// Import CSV
stats, err := imp.ImportCSV(ctx, "data.csv", importer.CSVOptions{
    HasHeader:   true,
    KeyColumn:   0,
    ValueColumn: 1,
    Delimiter:   ',',
    KeyPrefix:   "import:",
    BatchSize:   1000,
    SkipErrors:  true,
})
fmt.Printf("Imported %d records in %v\n", stats.RecordsImported, stats.Duration)

// Export CSV
stats, err := imp.ExportCSV(ctx, "export.csv", importer.ExportOptions{
    KeyPrefix:     "user:",
    IncludeHeader: true,
    Limit:         10000,
})
```

### JSON Lines Import/Export

```go
// Import JSON Lines
stats, err := imp.ImportJSON(ctx, "data.jsonl", importer.JSONOptions{
    KeyField:   "key",
    ValueField: "value",
    KeyPrefix:  "json:",
    SkipErrors: true,
})

// Export JSON Lines
stats, err := imp.ExportJSON(ctx, "export.jsonl", importer.ExportOptions{
    StartKey: "a",
    EndKey:   "z",
})
```

### Streaming Import (Large Datasets)

```go
// For very large datasets, use streaming import
stream := imp.NewStreamImporter(10000) // batch size

for record := range records {
    stream.Write([]byte(record.Key), []byte(record.Value))
}
stream.Flush()

stats := stream.Stats()
fmt.Printf("Streamed %d records\n", stats.RecordsImported)
```

### SSTable Direct Import (Fastest)

```go
import "github.com/vladgaus/RapidoDB/pkg/importer"

// Import existing SSTable files directly
sstImporter := importer.NewSSTableImporter(importer.SSTableImportOptions{
    DataDir: "/data/rapidodb",
})

// Import single file
stats, err := sstImporter.ImportSSTable(ctx, "/backup/000001.sst")

// Import directory
stats, err := sstImporter.ImportDirectory(ctx, "/backup/sst/")
fmt.Printf("Imported %d files (%d bytes)\n", stats.FilesImported, stats.TotalSize)
```

### Building SSTables Externally

```go
// Create SSTable files outside the database for bulk loading
builder, err := importer.NewSSTableBuilder("output.sst", importer.SSTableBuilderOptions{
    BlockSize: 4096,
})

// Add entries IN SORTED ORDER
builder.Add([]byte("key1"), []byte("value1"))
builder.Add([]byte("key2"), []byte("value2"))
builder.Add([]byte("key3"), []byte("value3"))

builder.Finish()
builder.Close()

stats := builder.Stats()
fmt.Printf("Built SSTable: %d entries, %d bytes\n", stats.EntryCount, stats.DataSize)
```

### Import Statistics

```go
type ImportStats struct {
    RecordsTotal     int64         // Total records processed
    RecordsImported  int64         // Successfully imported
    RecordsFailed    int64         // Failed records
    RecordsSkipped   int64         // Skipped records
    BytesRead        int64         // Bytes read from source
    BytesWritten     int64         // Bytes written to DB
    Duration         time.Duration // Total duration
    RecordsPerSecond float64       // Throughput
}
```

### Performance Tips

| Tip | Impact |
|:----|:-------|
| Use SSTable import for large datasets | 10x faster than record-by-record |
| Increase BatchSize | Reduces flush overhead |
| Pre-sort data | Enables SSTable builder usage |
| Use streaming for memory efficiency | Constant memory usage |
| Enable SkipErrors for dirty data | Prevents abort on bad records |

## 🖥️ CLI Tool (rapidodb-cli)

RapidoDB includes a command-line interface for managing and interacting with the database.

### Installation

```bash
# Build from source
make cli

# Or build directly
go build -o rapidodb-cli ./cmd/cli/
```

### Basic Usage

```bash
# Get a value
rapidodb-cli get mykey

# Set a value (with optional TTL in seconds)
rapidodb-cli set user:1 '{"name":"John"}' 3600

# Delete a key
rapidodb-cli delete mykey

# Ping server
rapidodb-cli ping

# View stats
rapidodb-cli stats
```

### Connection Options

```bash
# Connect to remote server
rapidodb-cli -h 192.168.1.10 -p 11211 get mykey

# With custom admin port
rapidodb-cli --host=db.example.com --admin-port=9091 info

# With timeout
rapidodb-cli --timeout=10s stats
```

### Admin Commands

```bash
# Server info
rapidodb-cli info

# Trigger compaction
rapidodb-cli compact

# Flush memtable
rapidodb-cli flush

# View level stats
rapidodb-cli levels

# List SSTables
rapidodb-cli sstables
```

### Backup Commands

```bash
# Create full backup
rapidodb-cli backup --type=full

# List backups
rapidodb-cli backups

# Restore backup
rapidodb-cli restore backup-20240101-120000 /restore/path
```

### Import/Export Commands

```bash
# Import CSV
rapidodb-cli import-csv /data/import.csv --prefix=imported: --header

# Import JSON Lines
rapidodb-cli import-json /data/import.jsonl --prefix=data:

# Export to CSV
rapidodb-cli export-csv /data/export.csv --prefix=user: --limit=10000

# Export to JSON Lines
rapidodb-cli export-json /data/export.jsonl
```

### Interactive Mode

```bash
rapidodb-cli -i
# Or just run without arguments
rapidodb-cli
```

Interactive session example:
```
RapidoDB CLI - Interactive Mode
Type 'help' for commands, 'exit' to quit

rapidodb (disconnected)> connect localhost:11211
PONG from localhost:11211 (RapidoDB 0.23.0) - 1.2ms

rapidodb [localhost:11211]> set greeting "Hello, World!"
OK

rapidodb [localhost:11211]> get greeting
Hello, World!

rapidodb [localhost:11211]> stats
pid:                      12345
uptime:                   3600
cmd_get:                  10000
cmd_set:                  5000

rapidodb [localhost:11211]> exit
Bye!
```

### All Commands Reference

| Command | Description |
|:--------|:------------|
| `get <key>` | Get value by key |
| `set <key> <value> [ttl]` | Set key-value pair |
| `delete <key>` | Delete a key |
| `stats` | Show memcached stats |
| `ping` | Ping server |
| `info` | Show server properties |
| `backup [--type=TYPE]` | Create backup |
| `backups` | List backups |
| `restore <id> <dir>` | Restore backup |
| `compact` | Trigger compaction |
| `flush` | Flush memtable |
| `levels` | Show level stats |
| `sstables` | List SSTables |
| `import-csv <file>` | Import from CSV |
| `import-json <file>` | Import from JSON Lines |
| `export-csv <file>` | Export to CSV |
| `export-json <file>` | Export to JSON Lines |

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

Tested on GitHub Actions (ubuntu-latest, 4 vCPU):

| Workload | Ops/sec | Avg Latency | P99 Latency | Throughput |
|:---------|--------:|------------:|------------:|-----------:|
| **fillseq** | 330,000 | 3.0 µs | 15 µs | 37 MB/s |
| **fillrandom** | 286,000 | 3.5 µs | 20 µs | 32 MB/s |
| **readseq** | 1,940,000 | 0.5 µs | 2 µs | 215 MB/s |
| **readrandom** | 1,410,000 | 0.7 µs | 3 µs | 156 MB/s |

### Comparison with BadgerDB, LevelDB & RocksDB

The four-way comparison (RapidoDB vs BadgerDB vs LevelDB vs RocksDB) runs
in two ways:

```bash
# Locally via Docker (no system installs needed)
make docker-bench

# Or in CI — runs automatically on every release tag and weekly
# See .github/workflows/benchmark.yml
```

RapidoDB itself stays dependency-free: BadgerDB, LevelDB, and RocksDB are
each installed in their own Docker container (`docker/Dockerfile.badger`,
`Dockerfile.leveldb`, `Dockerfile.rocksdb`). The main `go.mod` never
imports any of them.

| Metric | RapidoDB | BadgerDB | LevelDB | RocksDB |
|:-------|:--------:|:--------:|:-------:|:-------:|
| fillseq | 330K/s | ~280K/s | 450K/s | 270K/s |
| fillrandom | 286K/s | ~250K/s | 410K/s | 200K/s |
| readseq | 1.9M/s | ~1.5M/s | 8.5M/s | 4.5M/s |
| readrandom | 1.4M/s | ~1.0M/s | 4.5M/s | 640K/s |
| Dependencies | **0** | ~10 | 2 | 20+ |
| CGO Required | **No** | No | Yes | Yes |
| Server Mode | **Yes** | No | No | No |
| Binary Size | **4 MB** | 8+ MB | 1.5 MB | 15+ MB |

_Note: BadgerDB numbers are approximate. Run `make run-compare` for exact results on your hardware._

**Key Insights:**
- 🏆 **RapidoDB beats RocksDB** on write throughput (330K vs 270K)
- 🏆 **RapidoDB beats BadgerDB** on random reads (~40% faster)
- ✅ **LevelDB has fastest sequential reads** but requires C++ toolchain
- ✅ **RapidoDB is the only Go database with built-in server mode**

**RapidoDB's unique advantages:**
- Zero dependencies (BadgerDB has ~10)
- Built-in TCP server with Memcached protocol
- Works with ANY language (PHP, Python, Node.js, etc.)
- Single binary deployment

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
