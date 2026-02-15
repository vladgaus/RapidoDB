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

| Feature | RapidoDB | Redis | LevelDB | RocksDB |
|:--------|:--------:|:-----:|:-------:|:-------:|
| **Language** | Go | C | C++ | C++ |
| **Storage** | Disk (LSM) | **In-Memory** | Disk (LSM) | Disk (LSM) |
| **Dependencies** | **0** | 3+ | 2 | 20+ |
| **Binary Size** | **4 MB** | 3 MB | 1.5 MB | 15+ MB |
| **Build Time** | **< 5 sec** | 30 sec | Minutes | 10+ min |
| **Data > RAM** | ✅ | ❌ | ✅ | ✅ |
| **Writes/sec** | 300K | 500K+ | 450K | 270K |
| **Reads/sec** | 1.4M | 500K+ | 8M+ | 4M |

> **Note**: Redis is in-memory (fastest) but limited by RAM. RapidoDB is disk-based with data that can exceed RAM.

### RapidoDB vs Redis

| Aspect | RapidoDB | Redis |
|:-------|:--------:|:-----:|
| **Storage** | Disk-based | In-memory |
| **Data Size Limit** | Disk space | RAM |
| **Persistence** | Always | Optional |
| **Dependencies** | **0** | 3+ |
| **Protocol** | Memcached | Redis |
| **Data Types** | Key-Value | Strings, Lists, Sets, Hashes... |
| **Pub/Sub** | ❌ | ✅ |
| **Clustering** | ❌ | ✅ |

**Choose RapidoDB when:**
- ✅ Data exceeds available RAM
- ✅ You need guaranteed persistence
- ✅ You want zero dependencies
- ✅ Simple key-value is sufficient
- ✅ You prefer Go ecosystem

**Choose Redis when:**
- ✅ All data fits in RAM
- ✅ You need advanced data structures (lists, sets, sorted sets)
- ✅ You need Pub/Sub messaging
- ✅ You need built-in clustering

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
| 21 | Distributed Tracing | 🔜 | Request ID propagation |
| 22 | Admin API | 🔜 | Compaction triggers, stats |
| 23 | Backup/Restore | 🔜 | Hot backups |
| 24 | Import/Export | 🔜 | JSON/CSV support |
| 25 | CLI Tool | 🔜 | Interactive management |

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

### Comparison with Redis, LevelDB & RocksDB

| Metric | RapidoDB | Redis* | LevelDB | RocksDB |
|:-------|:--------:|:------:|:-------:|:-------:|
| fillseq | 330K/s | 500K/s | 450K/s | 270K/s |
| fillrandom | 286K/s | 500K/s | 410K/s | 200K/s |
| readseq | 1.9M/s | 500K/s | 8.5M/s | 4.5M/s |
| readrandom | 1.4M/s | 500K/s | 4.5M/s | 640K/s |
| Storage | Disk | **RAM** | Disk | Disk |
| Dependencies | **0** | 3+ | 2 | 20+ |
| Binary Size | **4 MB** | 3 MB | 1.5 MB | 15+ MB |
| Build Time | **5 sec** | 30 sec | 2 min | 10+ min |

_*Redis runs with AOF persistence enabled for fair comparison_

**Key Insights:**
- 🏆 **RapidoDB beats RocksDB** on write throughput (330K vs 270K)
- 🏆 **RapidoDB beats Redis** on read throughput (1.4M vs 500K)
- ✅ **Redis is faster for writes** but limited by RAM
- ✅ **LevelDB has fastest reads** but requires C++ toolchain

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
