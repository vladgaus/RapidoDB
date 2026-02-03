# RapidoDB Makefile
# High-Performance LSM-Tree Key-Value Store

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod
GOFMT=$(GOCMD) fmt
GOVET=$(GOCMD) vet

# Binary names
SERVER_BINARY=rapidodb-server
BENCH_BINARY=rapidodb-bench

# Directories
BUILD_DIR=./build
CMD_DIR=./cmd
PKG_DIR=./pkg
INTERNAL_DIR=./internal

# Flags
LDFLAGS=-ldflags "-s -w"
RACE=-race
COVER=-cover

# Build version info
VERSION?=$(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
BUILD_TIME=$(shell date -u '+%Y-%m-%d_%H:%M:%S')
COMMIT=$(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")

LDFLAGS=-ldflags "-s -w -X main.Version=$(VERSION) -X main.BuildTime=$(BUILD_TIME) -X main.Commit=$(COMMIT)"

.PHONY: all build clean test bench fmt vet lint deps help server bench-tool

# Default target
all: clean deps fmt vet test build

# Build all binaries
build: server bench-tool
	@echo "Build complete!"

# Build server binary
server:
	@echo "Building RapidoDB server..."
	@mkdir -p $(BUILD_DIR)
	$(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(SERVER_BINARY) $(CMD_DIR)/server/main.go

# Build benchmark tool
bench-tool:
	@echo "Building benchmark tool..."
	@mkdir -p $(BUILD_DIR)
	$(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(BENCH_BINARY) $(CMD_DIR)/bench/main.go

# Run all tests
test:
	@echo "Running tests..."
	$(GOTEST) -v ./...

# Run tests with race detector
test-race:
	@echo "Running tests with race detector..."
	$(GOTEST) $(RACE) -v ./...

# Run tests with coverage
test-cover:
	@echo "Running tests with coverage..."
	$(GOTEST) $(COVER) -coverprofile=coverage.out ./...
	$(GOCMD) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

# Run unit tests only
test-unit:
	@echo "Running unit tests..."
	$(GOTEST) -v ./tests/unit/...

# Run integration tests only
test-integration:
	@echo "Running integration tests..."
	$(GOTEST) -v ./tests/integration/...

# Run benchmarks
bench:
	@echo "Running benchmarks..."
	$(GOTEST) -bench=. -benchmem -run=^$$ ./tests/benchmark/...

# Run specific benchmark
bench-%:
	@echo "Running benchmark $*..."
	$(GOTEST) -bench=$* -benchmem -run=^$$ ./tests/benchmark/...

# Format code
fmt:
	@echo "Formatting code..."
	$(GOFMT) ./...

# Vet code
vet:
	@echo "Vetting code..."
	$(GOVET) ./...

# Run linter (requires golangci-lint)
lint:
	@echo "Linting code..."
	@which golangci-lint > /dev/null || (echo "Installing golangci-lint..." && go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest)
	golangci-lint run ./...

# Download dependencies
deps:
	@echo "Downloading dependencies..."
	$(GOMOD) download
	$(GOMOD) tidy

# Clean build artifacts
clean:
	@echo "Cleaning..."
	@rm -rf $(BUILD_DIR)
	@rm -f coverage.out coverage.html
	@rm -rf rapidodb_data
	@rm -rf testdata

# Install binaries to GOPATH/bin
install: build
	@echo "Installing binaries..."
	@cp $(BUILD_DIR)/$(SERVER_BINARY) $(GOPATH)/bin/
	@cp $(BUILD_DIR)/$(BENCH_BINARY) $(GOPATH)/bin/

# Run the server
run: server
	@echo "Starting RapidoDB server..."
	$(BUILD_DIR)/$(SERVER_BINARY)

# Run with race detector (for development)
run-race:
	@echo "Starting RapidoDB server with race detector..."
	$(GOBUILD) $(RACE) -o $(BUILD_DIR)/$(SERVER_BINARY) $(CMD_DIR)/server/main.go
	$(BUILD_DIR)/$(SERVER_BINARY)

# Generate documentation
docs:
	@echo "Generating documentation..."
	@which godoc > /dev/null || (echo "Installing godoc..." && go install golang.org/x/tools/cmd/godoc@latest)
	@echo "Documentation server starting at http://localhost:6060/pkg/github.com/rapidodb/rapidodb/"
	godoc -http=:6060

# Profile CPU
profile-cpu:
	@echo "Running CPU profile..."
	$(GOTEST) -cpuprofile=cpu.prof -bench=. ./tests/benchmark/...
	$(GOCMD) tool pprof -http=:8080 cpu.prof

# Profile memory
profile-mem:
	@echo "Running memory profile..."
	$(GOTEST) -memprofile=mem.prof -bench=. ./tests/benchmark/...
	$(GOCMD) tool pprof -http=:8080 mem.prof

# Quick development check
check: fmt vet test
	@echo "All checks passed!"

# Help
help:
	@echo "RapidoDB - High-Performance LSM-Tree Key-Value Store"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@echo "  all          - Clean, format, vet, test, and build (default)"
	@echo "  build        - Build all binaries"
	@echo "  server       - Build server binary"
	@echo "  bench-tool   - Build benchmark tool"
	@echo "  test         - Run all tests"
	@echo "  test-race    - Run tests with race detector"
	@echo "  test-cover   - Run tests with coverage report"
	@echo "  test-unit    - Run unit tests only"
	@echo "  test-integration - Run integration tests only"
	@echo "  bench        - Run benchmarks"
	@echo "  bench-<name> - Run specific benchmark"
	@echo "  fmt          - Format code"
	@echo "  vet          - Vet code"
	@echo "  lint         - Run linter"
	@echo "  deps         - Download dependencies"
	@echo "  clean        - Clean build artifacts"
	@echo "  install      - Install binaries to GOPATH/bin"
	@echo "  run          - Run the server"
	@echo "  run-race     - Run server with race detector"
	@echo "  docs         - Start documentation server"
	@echo "  profile-cpu  - Run CPU profiling"
	@echo "  profile-mem  - Run memory profiling"
	@echo "  check        - Quick development check"
	@echo "  help         - Show this help"
