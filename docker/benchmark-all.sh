#!/bin/bash
# Benchmark Comparison Script
# Compares RapidoDB vs Redis vs LevelDB vs RocksDB

set -e

NUM_OPS=${NUM_OPS:-100000}
VALUE_SIZE=${VALUE_SIZE:-100}
OUTPUT_DIR=${OUTPUT_DIR:-/benchmarks/results}

mkdir -p "$OUTPUT_DIR"

echo "╔═══════════════════════════════════════════════════════════════════════╗"
echo "║            DATABASE BENCHMARK COMPARISON                              ║"
echo "║                                                                       ║"
echo "║  Operations: $NUM_OPS                                                 ║"
echo "║  Value Size: $VALUE_SIZE bytes                                        ║"
echo "║  Databases:  RapidoDB, Redis, LevelDB, RocksDB                        ║"
echo "╚═══════════════════════════════════════════════════════════════════════╝"
echo ""

# ============================================================================
# RapidoDB Benchmark
# ============================================================================
echo "┌─────────────────────────────────────────────────────────────────────┐"
echo "│  1/4  Running RapidoDB Benchmark...                                 │"
echo "└─────────────────────────────────────────────────────────────────────┘"

rm -rf /tmp/rapidodb_bench
rapidodb-bench --mode all --num "$NUM_OPS" --value-size "$VALUE_SIZE" --data-dir /tmp/rapidodb_bench 2>&1 | tee "$OUTPUT_DIR/rapidodb.txt"
rm -rf /tmp/rapidodb_bench

echo ""
sleep 2

# ============================================================================
# Redis Benchmark (with AOF persistence for fair comparison)
# ============================================================================
echo "┌─────────────────────────────────────────────────────────────────────┐"
echo "│  2/4  Running Redis Benchmark (with AOF persistence)...             │"
echo "└─────────────────────────────────────────────────────────────────────┘"

# Start Redis with AOF persistence
redis-server --daemonize yes --appendonly yes --appendfsync everysec --dir /tmp
sleep 2

# Check Redis is running
redis-cli ping

echo "=== Redis SET (write) benchmark ===" | tee "$OUTPUT_DIR/redis.txt"
redis-benchmark -t set -n "$NUM_OPS" -d "$VALUE_SIZE" -P 1 --csv 2>&1 | tee -a "$OUTPUT_DIR/redis.txt"

echo "" >> "$OUTPUT_DIR/redis.txt"
echo "=== Redis GET (read) benchmark ===" >> "$OUTPUT_DIR/redis.txt"
redis-benchmark -t get -n "$NUM_OPS" -d "$VALUE_SIZE" -P 1 --csv 2>&1 | tee -a "$OUTPUT_DIR/redis.txt"

# Stop Redis
redis-cli shutdown nosave 2>/dev/null || true
rm -rf /tmp/appendonly.aof /tmp/dump.rdb

echo ""
sleep 2

# ============================================================================
# LevelDB Benchmark
# ============================================================================
echo "┌─────────────────────────────────────────────────────────────────────┐"
echo "│  3/4  Running LevelDB Benchmark...                                  │"
echo "└─────────────────────────────────────────────────────────────────────┘"

rm -rf /tmp/leveldb_bench
leveldb_bench \
    --benchmarks=fillseq,fillrandom,readseq,readrandom \
    --num="$NUM_OPS" \
    --value_size="$VALUE_SIZE" \
    --db=/tmp/leveldb_bench \
    2>&1 | tee "$OUTPUT_DIR/leveldb.txt"
rm -rf /tmp/leveldb_bench

echo ""
sleep 2

# ============================================================================
# RocksDB Benchmark
# ============================================================================
echo "┌─────────────────────────────────────────────────────────────────────┐"
echo "│  4/4  Running RocksDB Benchmark...                                  │"
echo "└─────────────────────────────────────────────────────────────────────┘"

rm -rf /tmp/rocksdb_bench
rocksdb_bench \
    --benchmarks=fillseq,fillrandom,readseq,readrandom \
    --num="$NUM_OPS" \
    --value_size="$VALUE_SIZE" \
    --db=/tmp/rocksdb_bench \
    2>&1 | tee "$OUTPUT_DIR/rocksdb.txt"
rm -rf /tmp/rocksdb_bench

echo ""

# ============================================================================
# Parse and Display Summary
# ============================================================================
echo "╔═══════════════════════════════════════════════════════════════════════╗"
echo "║                      BENCHMARK COMPARISON SUMMARY                     ║"
echo "╠═══════════════════════════════════════════════════════════════════════╣"
echo ""

# Function to extract ops/sec
extract_rapidodb() {
    grep "$1" "$OUTPUT_DIR/rapidodb.txt" | grep -oP '\d+(?=\s+\d+\.\d+\s+\d+\.\d+\s+\d+\.\d+ ║)' | head -1 || echo "N/A"
}

extract_leveldb() {
    local micros=$(grep "^$1" "$OUTPUT_DIR/leveldb.txt" | awk '{print $3}' | head -1)
    if [ -n "$micros" ] && [ "$micros" != "0" ]; then
        echo "scale=0; 1000000 / $micros" | bc 2>/dev/null || echo "N/A"
    else
        echo "N/A"
    fi
}

extract_rocksdb() {
    grep "^$1" "$OUTPUT_DIR/rocksdb.txt" | awk '{print $5}' | head -1 || echo "N/A"
}

extract_redis_set() {
    grep '"SET"' "$OUTPUT_DIR/redis.txt" | head -1 | cut -d',' -f2 | tr -d '"' || echo "N/A"
}

extract_redis_get() {
    grep '"GET"' "$OUTPUT_DIR/redis.txt" | head -1 | cut -d',' -f2 | tr -d '"' || echo "N/A"
}

# Extract values
RAPIDODB_FILLSEQ=$(extract_rapidodb "fillseq")
RAPIDODB_FILLRANDOM=$(extract_rapidodb "fillrandom")
RAPIDODB_READSEQ=$(extract_rapidodb "readseq")
RAPIDODB_READRANDOM=$(extract_rapidodb "readrandom")

REDIS_WRITE=$(extract_redis_set)
REDIS_READ=$(extract_redis_get)

LEVELDB_FILLSEQ=$(extract_leveldb "fillseq")
LEVELDB_FILLRANDOM=$(extract_leveldb "fillrandom")
LEVELDB_READSEQ=$(extract_leveldb "readseq")
LEVELDB_READRANDOM=$(extract_leveldb "readrandom")

ROCKSDB_FILLSEQ=$(extract_rocksdb "fillseq")
ROCKSDB_FILLRANDOM=$(extract_rocksdb "fillrandom")
ROCKSDB_READSEQ=$(extract_rocksdb "readseq")
ROCKSDB_READRANDOM=$(extract_rocksdb "readrandom")

# Print comparison table
echo "┌─────────────┬─────────────┬─────────────┬─────────────┬─────────────┐"
echo "│  Workload   │  RapidoDB   │   Redis*    │  LevelDB    │  RocksDB    │"
echo "├─────────────┼─────────────┼─────────────┼─────────────┼─────────────┤"
printf "│ fillseq     │ %11s │ %11s │ %11s │ %11s │\n" "$RAPIDODB_FILLSEQ" "$REDIS_WRITE" "$LEVELDB_FILLSEQ" "$ROCKSDB_FILLSEQ"
printf "│ fillrandom  │ %11s │ %11s │ %11s │ %11s │\n" "$RAPIDODB_FILLRANDOM" "$REDIS_WRITE" "$LEVELDB_FILLRANDOM" "$ROCKSDB_FILLRANDOM"
printf "│ readseq     │ %11s │ %11s │ %11s │ %11s │\n" "$RAPIDODB_READSEQ" "$REDIS_READ" "$LEVELDB_READSEQ" "$ROCKSDB_READSEQ"
printf "│ readrandom  │ %11s │ %11s │ %11s │ %11s │\n" "$RAPIDODB_READRANDOM" "$REDIS_READ" "$LEVELDB_READRANDOM" "$ROCKSDB_READRANDOM"
echo "└─────────────┴─────────────┴─────────────┴─────────────┴─────────────┘"
echo ""
echo "* Redis runs with AOF persistence (appendfsync everysec) for fair comparison"
echo ""

echo "┌─────────────────────────────────────────────────────────────────────┐"
echo "│  Database Comparison                                                │"
echo "├─────────────────────────────────────────────────────────────────────┤"
echo "│  RapidoDB: Go, 0 deps, disk-based, data can exceed RAM              │"
echo "│  Redis:    C, in-memory, fastest but limited by RAM                 │"
echo "│  LevelDB:  C++, 2 deps, disk-based, original LSM implementation     │"
echo "│  RocksDB:  C++, 20+ deps, disk-based, highly optimized              │"
echo "└─────────────────────────────────────────────────────────────────────┘"

echo ""
echo "╚═══════════════════════════════════════════════════════════════════════╝"
echo ""
echo "Full results saved to: $OUTPUT_DIR/"
echo "  - rapidodb.txt"
echo "  - redis.txt"
echo "  - leveldb.txt"
echo "  - rocksdb.txt"

# Save summary
cat > "$OUTPUT_DIR/summary.md" << EOF
# Benchmark Summary

**Date:** $(date -u '+%Y-%m-%d %H:%M:%S UTC')
**Operations:** $NUM_OPS
**Value Size:** $VALUE_SIZE bytes

## Results (ops/sec)

| Workload | RapidoDB | Redis* | LevelDB | RocksDB |
|:---------|:---------|:-------|:--------|:--------|
| fillseq | $RAPIDODB_FILLSEQ | $REDIS_WRITE | $LEVELDB_FILLSEQ | $ROCKSDB_FILLSEQ |
| fillrandom | $RAPIDODB_FILLRANDOM | $REDIS_WRITE | $LEVELDB_FILLRANDOM | $ROCKSDB_FILLRANDOM |
| readseq | $RAPIDODB_READSEQ | $REDIS_READ | $LEVELDB_READSEQ | $ROCKSDB_READSEQ |
| readrandom | $RAPIDODB_READRANDOM | $REDIS_READ | $LEVELDB_READRANDOM | $ROCKSDB_READRANDOM |

_*Redis with AOF persistence (appendfsync everysec)_

## Notes

- **RapidoDB**: Zero dependencies, 4MB binary, pure Go, data can exceed RAM
- **Redis**: In-memory (fastest but limited by RAM)
- **LevelDB**: Original LSM implementation by Google
- **RocksDB**: Highly optimized, 20+ dependencies
EOF

echo "Summary saved to: $OUTPUT_DIR/summary.md"
