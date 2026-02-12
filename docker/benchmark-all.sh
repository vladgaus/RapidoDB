#!/bin/bash
# Benchmark Comparison Script
# Compares RapidoDB vs LevelDB vs RocksDB

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
echo "╚═══════════════════════════════════════════════════════════════════════╝"
echo ""

# ============================================================================
# RapidoDB Benchmark
# ============================================================================
echo "┌─────────────────────────────────────────────────────────────────────┐"
echo "│  1/3  Running RapidoDB Benchmark...                                 │"
echo "└─────────────────────────────────────────────────────────────────────┘"

rm -rf /tmp/rapidodb_bench
rapidodb-bench --mode all --num "$NUM_OPS" --value-size "$VALUE_SIZE" --data-dir /tmp/rapidodb_bench 2>&1 | tee "$OUTPUT_DIR/rapidodb.txt"
rm -rf /tmp/rapidodb_bench

echo ""
sleep 2

# ============================================================================
# LevelDB Benchmark
# ============================================================================
echo "┌─────────────────────────────────────────────────────────────────────┐"
echo "│  2/3  Running LevelDB Benchmark...                                  │"
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
echo "│  3/3  Running RocksDB Benchmark...                                  │"
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
# Summary
# ============================================================================
echo "╔═══════════════════════════════════════════════════════════════════════╗"
echo "║                      BENCHMARK COMPARISON SUMMARY                     ║"
echo "╠═══════════════════════════════════════════════════════════════════════╣"
echo ""

echo "┌─────────────────────────────────────────────────────────────────────┐"
echo "│  RapidoDB Results                                                   │"
echo "└─────────────────────────────────────────────────────────────────────┘"
grep -E "(fillseq|fillrandom|readseq|readrandom|Workload|ops/sec)" "$OUTPUT_DIR/rapidodb.txt" 2>/dev/null | head -20 || echo "  See full output in $OUTPUT_DIR/rapidodb.txt"

echo ""
echo "┌─────────────────────────────────────────────────────────────────────┐"
echo "│  LevelDB Results                                                    │"
echo "└─────────────────────────────────────────────────────────────────────┘"
grep -E "^(fillseq|fillrandom|readseq|readrandom)" "$OUTPUT_DIR/leveldb.txt" 2>/dev/null || echo "  See full output in $OUTPUT_DIR/leveldb.txt"

echo ""
echo "┌─────────────────────────────────────────────────────────────────────┐"
echo "│  RocksDB Results                                                    │"
echo "└─────────────────────────────────────────────────────────────────────┘"
grep -E "^(fillseq|fillrandom|readseq|readrandom)" "$OUTPUT_DIR/rocksdb.txt" 2>/dev/null || echo "  See full output in $OUTPUT_DIR/rocksdb.txt"

echo ""
echo "╚═══════════════════════════════════════════════════════════════════════╝"
echo ""
echo "Full results saved to: $OUTPUT_DIR/"
echo "  - rapidodb.txt"
echo "  - leveldb.txt"
echo "  - rocksdb.txt"
