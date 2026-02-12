#!/bin/bash
# benchmark-comparison.sh
# Compare RapidoDB vs RocksDB vs LevelDB performance
#
# Usage:
#   ./benchmark-comparison.sh all      # Run all benchmarks
#   ./benchmark-comparison.sh rapidodb # Run only RapidoDB
#   ./benchmark-comparison.sh rocksdb  # Run only RocksDB
#   ./benchmark-comparison.sh leveldb  # Run only LevelDB

set -e

# Configuration
NUM_OPS=${NUM_OPS:-100000}
VALUE_SIZE=${VALUE_SIZE:-100}
KEY_SIZE=${KEY_SIZE:-16}

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_header() {
    echo ""
    echo -e "${BLUE}╔═══════════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║${NC}                    $1"
    echo -e "${BLUE}╚═══════════════════════════════════════════════════════════════════════╝${NC}"
    echo ""
}

print_separator() {
    echo ""
    echo -e "${YELLOW}───────────────────────────────────────────────────────────────────────${NC}"
    echo ""
}

run_rapidodb() {
    print_header "RapidoDB Benchmark"
    echo "Operations: $NUM_OPS"
    echo "Value Size: $VALUE_SIZE bytes"
    echo ""
    
    rm -rf /data/rapidodb
    mkdir -p /data/rapidodb
    
    rapidodb-bench \
        --mode all \
        --num $NUM_OPS \
        --value-size $VALUE_SIZE \
        --key-size $KEY_SIZE \
        --data-dir /data/rapidodb
}

run_rocksdb() {
    print_header "RocksDB Benchmark"
    echo "Operations: $NUM_OPS"
    echo "Value Size: $VALUE_SIZE bytes"
    echo ""
    
    rm -rf /data/rocksdb
    mkdir -p /data/rocksdb
    
    rocksdb-bench \
        --benchmarks=fillseq,fillrandom,readseq,readrandom,readwhilewriting \
        --num=$NUM_OPS \
        --value_size=$VALUE_SIZE \
        --key_size=$KEY_SIZE \
        --threads=1 \
        --db=/data/rocksdb \
        --use_existing_db=0
}

run_leveldb() {
    print_header "LevelDB Benchmark"
    echo "Operations: $NUM_OPS"
    echo "Value Size: $VALUE_SIZE bytes"
    echo ""
    
    rm -rf /data/leveldb
    mkdir -p /data/leveldb
    
    leveldb-bench \
        --benchmarks=fillseq,fillrandom,readseq,readrandom \
        --num=$NUM_OPS \
        --value_size=$VALUE_SIZE \
        --key_size=$KEY_SIZE \
        --db=/data/leveldb
}

print_summary() {
    print_header "Benchmark Comparison Summary"
    
    echo -e "${GREEN}Benchmark completed!${NC}"
    echo ""
    echo "Key metrics to compare:"
    echo "  - fillseq:    Sequential write throughput (ops/sec, MB/s)"
    echo "  - fillrandom: Random write throughput"
    echo "  - readseq:    Sequential read throughput"
    echo "  - readrandom: Random read throughput"
    echo ""
    echo "Note: RapidoDB is an educational project focused on code clarity."
    echo "      RocksDB/LevelDB are production-optimized C++ implementations."
    echo ""
}

# Main
case "${1:-all}" in
    rapidodb)
        run_rapidodb
        ;;
    rocksdb)
        run_rocksdb
        ;;
    leveldb)
        run_leveldb
        ;;
    all)
        echo -e "${GREEN}"
        echo "╔═══════════════════════════════════════════════════════════════════════╗"
        echo "║         Database Benchmark Comparison Tool                            ║"
        echo "║         RapidoDB vs RocksDB vs LevelDB                               ║"
        echo "╚═══════════════════════════════════════════════════════════════════════╝"
        echo -e "${NC}"
        echo ""
        echo "Configuration:"
        echo "  Operations:  $NUM_OPS"
        echo "  Value Size:  $VALUE_SIZE bytes"
        echo "  Key Size:    $KEY_SIZE bytes"
        echo ""
        
        run_rapidodb
        print_separator
        
        run_leveldb
        print_separator
        
        run_rocksdb
        print_separator
        
        print_summary
        ;;
    *)
        echo "Usage: $0 {all|rapidodb|rocksdb|leveldb}"
        echo ""
        echo "Environment variables:"
        echo "  NUM_OPS=100000     Number of operations"
        echo "  VALUE_SIZE=100     Value size in bytes"
        echo "  KEY_SIZE=16        Key size in bytes"
        exit 1
        ;;
esac
