#!/bin/bash
# Benchmark Comparison Script
# Compares RapidoDB vs BadgerDB vs LevelDB vs RocksDB

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
echo "║  Databases:  RapidoDB, BadgerDB, LevelDB, RocksDB                     ║"
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
# BadgerDB Benchmark
# ============================================================================
echo "┌─────────────────────────────────────────────────────────────────────┐"
echo "│  2/4  Running BadgerDB Benchmark...                                 │"
echo "└─────────────────────────────────────────────────────────────────────┘"

# BadgerDB ships its own `badger benchmark` CLI (installed via `go install`).
# CLI uses --keys-mil (millions of keys) and --val-size, not --key-count.
rm -rf /tmp/badger_bench
mkdir -p /tmp/badger_bench

# Convert NUM_OPS to millions for --keys-mil
KEYS_MIL=$(awk "BEGIN { printf \"%.6f\", $NUM_OPS / 1000000 }")

echo "=== BadgerDB write benchmark ===" | tee "$OUTPUT_DIR/badger.txt"
badger benchmark write \
    --dir=/tmp/badger_bench \
    --keys-mil="$KEYS_MIL" \
    --val-size="$VALUE_SIZE" \
    2>&1 | tee -a "$OUTPUT_DIR/badger.txt"

echo "" >> "$OUTPUT_DIR/badger.txt"
echo "=== BadgerDB read benchmark ===" >> "$OUTPUT_DIR/badger.txt"
badger benchmark read \
    --dir=/tmp/badger_bench \
    -d=10s \
    2>&1 | tee -a "$OUTPUT_DIR/badger.txt"

rm -rf /tmp/badger_bench

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

extract_badger() {
    # BadgerDB CLI prints lines like "200000 keys/sec" or "speed=20000/s".
    # Split output by phase (write vs read), pull last "/sec" or "/s" number.
    local section_file="/tmp/badger-section.txt"
    case "$1" in
        fillseq|fillrandom)
            awk '/=== BadgerDB write/,/=== BadgerDB read/' "$OUTPUT_DIR/badger.txt" > "$section_file"
            ;;
        readseq|readrandom)
            awk '/=== BadgerDB read/,EOF' "$OUTPUT_DIR/badger.txt" > "$section_file"
            ;;
        *)
            echo "N/A"; return ;;
    esac

    local ops=$(grep -oE '[0-9]+(\.[0-9]+)?[[:space:]]*(keys|writes|reads)?/(sec|s)\b' "$section_file" \
        | grep -oE '^[0-9]+' \
        | tail -1)
    rm -f "$section_file"
    [ -z "$ops" ] && echo "N/A" || echo "$ops"
}

# Extract values
RAPIDODB_FILLSEQ=$(extract_rapidodb "fillseq")
RAPIDODB_FILLRANDOM=$(extract_rapidodb "fillrandom")
RAPIDODB_READSEQ=$(extract_rapidodb "readseq")
RAPIDODB_READRANDOM=$(extract_rapidodb "readrandom")

BADGER_FILLSEQ=$(extract_badger "fillseq")
BADGER_FILLRANDOM=$(extract_badger "fillrandom")
BADGER_READSEQ=$(extract_badger "readseq")
BADGER_READRANDOM=$(extract_badger "readrandom")

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
echo "│  Workload   │  RapidoDB   │  BadgerDB   │  LevelDB    │  RocksDB    │"
echo "├─────────────┼─────────────┼─────────────┼─────────────┼─────────────┤"
printf "│ fillseq     │ %11s │ %11s │ %11s │ %11s │\n" "$RAPIDODB_FILLSEQ" "$BADGER_FILLSEQ" "$LEVELDB_FILLSEQ" "$ROCKSDB_FILLSEQ"
printf "│ fillrandom  │ %11s │ %11s │ %11s │ %11s │\n" "$RAPIDODB_FILLRANDOM" "$BADGER_FILLRANDOM" "$LEVELDB_FILLRANDOM" "$ROCKSDB_FILLRANDOM"
printf "│ readseq     │ %11s │ %11s │ %11s │ %11s │\n" "$RAPIDODB_READSEQ" "$BADGER_READSEQ" "$LEVELDB_READSEQ" "$ROCKSDB_READSEQ"
printf "│ readrandom  │ %11s │ %11s │ %11s │ %11s │\n" "$RAPIDODB_READRANDOM" "$BADGER_READRANDOM" "$LEVELDB_READRANDOM" "$ROCKSDB_READRANDOM"
echo "└─────────────┴─────────────┴─────────────┴─────────────┴─────────────┘"
echo ""

echo "┌─────────────────────────────────────────────────────────────────────┐"
echo "│  Database Comparison                                                │"
echo "├─────────────────────────────────────────────────────────────────────┤"
echo "│  RapidoDB: Pure Go, 0 deps, built-in server, Memcached protocol     │"
echo "│  BadgerDB: Pure Go, ~10 deps, embedded only, WiscKey architecture   │"
echo "│  LevelDB:  C++, 2 deps, embedded, original LSM implementation       │"
echo "│  RocksDB:  C++, 20+ deps, embedded, highly optimized                │"
echo "└─────────────────────────────────────────────────────────────────────┘"

echo ""
echo "╚═══════════════════════════════════════════════════════════════════════╝"
echo ""
echo "Full results saved to: $OUTPUT_DIR/"
echo "  - rapidodb.txt"
echo "  - badger.txt"
echo "  - leveldb.txt"
echo "  - rocksdb.txt"

# Save summary
cat > "$OUTPUT_DIR/summary.md" << EOF
# Benchmark Summary

**Date:** $(date -u '+%Y-%m-%d %H:%M:%S UTC')
**Operations:** $NUM_OPS
**Value Size:** $VALUE_SIZE bytes

## Results (ops/sec)

| Workload | RapidoDB | BadgerDB | LevelDB | RocksDB |
|:---------|:---------|:---------|:--------|:--------|
| fillseq | $RAPIDODB_FILLSEQ | $BADGER_FILLSEQ | $LEVELDB_FILLSEQ | $ROCKSDB_FILLSEQ |
| fillrandom | $RAPIDODB_FILLRANDOM | $BADGER_FILLRANDOM | $LEVELDB_FILLRANDOM | $ROCKSDB_FILLRANDOM |
| readseq | $RAPIDODB_READSEQ | $BADGER_READSEQ | $LEVELDB_READSEQ | $ROCKSDB_READSEQ |
| readrandom | $RAPIDODB_READRANDOM | $BADGER_READRANDOM | $LEVELDB_READRANDOM | $ROCKSDB_READRANDOM |

## Database Comparison

| Feature | RapidoDB | BadgerDB | LevelDB | RocksDB |
|:--------|:---------|:---------|:--------|:--------|
| Language | Pure Go | Pure Go | C++ | C++ |
| Dependencies | **0** | ~10 | 2 | 20+ |
| CGO Required | No | No | Yes | Yes |
| Server Mode | **Yes** | No | No | No |
| Architecture | LSM-Tree | WiscKey | LSM-Tree | LSM-Tree |
EOF

echo "Summary saved to: $OUTPUT_DIR/summary.md"
