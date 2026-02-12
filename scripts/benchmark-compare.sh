#!/bin/bash
# Quick Benchmark Comparison Script
# Usage: ./scripts/benchmark-compare.sh [num_ops] [value_size]

NUM_OPS=${1:-100000}
VALUE_SIZE=${2:-100}

echo "╔═══════════════════════════════════════════════════════════════════════╗"
echo "║            RapidoDB Benchmark Comparison                              ║"
echo "║                                                                       ║"
echo "║  This script runs benchmarks using Docker.                            ║"
echo "║  No installation required - everything runs in containers.            ║"
echo "╚═══════════════════════════════════════════════════════════════════════╝"
echo ""
echo "Parameters:"
echo "  Operations: $NUM_OPS"
echo "  Value Size: $VALUE_SIZE bytes"
echo ""

# Check if Docker is available
if ! command -v docker &> /dev/null; then
    echo "Error: Docker is not installed or not in PATH"
    echo "Please install Docker: https://docs.docker.com/get-docker/"
    exit 1
fi

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    echo "Error: docker-compose is not installed"
    echo "Please install docker-compose: https://docs.docker.com/compose/install/"
    exit 1
fi

# Create results directory
mkdir -p benchmark-results

echo "Building benchmark container (this may take 15-20 minutes on first run)..."
echo ""

# Use docker compose (v2) or docker-compose (v1)
if docker compose version &> /dev/null 2>&1; then
    COMPOSE_CMD="docker compose"
else
    COMPOSE_CMD="docker-compose"
fi

# Build and run
$COMPOSE_CMD build benchmark
$COMPOSE_CMD run -e NUM_OPS=$NUM_OPS -e VALUE_SIZE=$VALUE_SIZE benchmark

echo ""
echo "Results saved to ./benchmark-results/"
echo "  - rapidodb.txt"
echo "  - leveldb.txt"  
echo "  - rocksdb.txt"
