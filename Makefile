# RapidoDB Makefile
# High-Performance LSM-Tree Key-Value Store
# Author: Vladimir Sinica

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOMOD=$(GOCMD) mod
GOFMT=$(GOCMD) fmt
GOVET=$(GOCMD) vet

# Binary names
SERVER_BINARY=rapidodb-server
BENCH_BINARY=rapidodb-bench

# Directories
BUILD_DIR=./build
CMD_DIR=./cmd

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

# Run tests with verbose output
test-verbose:
	@echo "Running tests (verbose)..."
	$(GOTEST) -v -count=1 ./...

# Run tests with race detector
test-race:
	@echo "Running tests with race detector..."
	$(GOTEST) -race -v ./...

# Run tests with coverage
test-cover:
	@echo "Running tests with coverage..."
	$(GOTEST) -cover -coverprofile=coverage.out ./...
	$(GOCMD) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

# Run Go benchmarks (not rapidodb-bench tool)
bench:
	@echo "Running Go benchmarks..."
	$(GOTEST) -bench=. -benchmem -run=^$$ ./...

# Run specific benchmark pattern
bench-%:
	@echo "Running benchmark $*..."
	$(GOTEST) -bench=$* -benchmem -run=^$$ ./...

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
	@rm -rf benchmark_data

# Install binaries to GOPATH/bin
install: build
	@echo "Installing binaries..."
	@cp $(BUILD_DIR)/$(SERVER_BINARY) $(GOPATH)/bin/ 2>/dev/null || echo "GOPATH not set, skipping install"
	@cp $(BUILD_DIR)/$(BENCH_BINARY) $(GOPATH)/bin/ 2>/dev/null || true

# Run the server
run: server
	@echo "Starting RapidoDB server..."
	$(BUILD_DIR)/$(SERVER_BINARY)

# Run with race detector (for development)
run-race:
	@echo "Starting RapidoDB server with race detector..."
	$(GOBUILD) -race -o $(BUILD_DIR)/$(SERVER_BINARY)-race $(CMD_DIR)/server/main.go
	$(BUILD_DIR)/$(SERVER_BINARY)-race

# Quick development check
check: fmt vet test
	@echo "All checks passed!"

# Run rapidodb-bench tool
run-bench: bench-tool
	@echo "Running RapidoDB benchmark tool..."
	$(BUILD_DIR)/$(BENCH_BINARY) --mode all --num 10000

# Help
help:
	@echo "RapidoDB - High-Performance LSM-Tree Key-Value Store"
	@echo "Author: Vladimir Sinica"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Build targets:"
	@echo "  build        - Build all binaries"
	@echo "  server       - Build server binary only"
	@echo "  bench-tool   - Build benchmark tool only"
	@echo "  clean        - Clean build artifacts"
	@echo "  install      - Install binaries to GOPATH/bin"
	@echo ""
	@echo "Test targets:"
	@echo "  test         - Run all tests"
	@echo "  test-verbose - Run tests with verbose output"
	@echo "  test-race    - Run tests with race detector"
	@echo "  test-cover   - Run tests with coverage report"
	@echo "  bench        - Run Go benchmarks"
	@echo ""
	@echo "Code quality:"
	@echo "  fmt          - Format code"
	@echo "  vet          - Vet code"
	@echo "  lint         - Run golangci-lint"
	@echo "  check        - Run fmt, vet, and test"
	@echo ""
	@echo "Run targets:"
	@echo "  run          - Run the server"
	@echo "  run-race     - Run server with race detector"
	@echo "  run-bench    - Run benchmark tool"
	@echo ""
	@echo "Other:"
	@echo "  deps         - Download dependencies"
	@echo "  all          - Clean, format, vet, test, and build"
	@echo "  help         - Show this help"
