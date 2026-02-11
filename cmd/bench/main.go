// RapidoDB Benchmark Tool
// Performance testing for RapidoDB storage engine
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/vladgaus/RapidoDB/pkg/benchmark"
)

// Build-time variables
var (
	Version   = "dev"
	BuildTime = "unknown"
	Commit    = "unknown"
)

func main() {
	// Command-line flags
	mode := flag.String("mode", "fillrandom", "Benchmark mode: fillseq, fillrandom, readseq, readrandom, readwrite, scan, delete, tcp-set, tcp-get, tcp-mixed, all")
	numOps := flag.Int("num", 100000, "Number of operations")
	keySize := flag.Int("key-size", 16, "Key size in bytes")
	valueSize := flag.Int("value-size", 100, "Value size in bytes")
	workers := flag.Int("workers", 1, "Number of concurrent workers")
	readPercent := flag.Int("read-percent", 80, "Read percentage for readwrite mode (0-100)")
	dataDir := flag.String("data-dir", "./benchmark_data", "Data directory")
	serverAddr := flag.String("server", "127.0.0.1:11211", "Server address for TCP benchmarks")
	showVersion := flag.Bool("version", false, "Show version information")
	flag.Parse()

	if *showVersion {
		fmt.Printf("RapidoDB Benchmark Tool %s\n", Version)
		fmt.Printf("Build Time: %s\n", BuildTime)
		fmt.Printf("Commit: %s\n", Commit)
		os.Exit(0)
	}

	printBanner()

	// Validate mode
	validModes := map[string]bool{
		"fillseq": true, "fillrandom": true,
		"readseq": true, "readrandom": true,
		"readwrite": true, "scan": true,
		"delete": true, "all": true,
		"tcp-set": true, "tcp-get": true, "tcp-mixed": true,
	}
	if !validModes[*mode] {
		fmt.Fprintf(os.Stderr, "Invalid mode: %s\n", *mode)
		fmt.Fprintf(os.Stderr, "Valid modes: fillseq, fillrandom, readseq, readrandom, readwrite, scan, delete, tcp-set, tcp-get, tcp-mixed, all\n")
		os.Exit(1)
	}

	// Handle TCP benchmarks
	if *mode == "tcp-set" || *mode == "tcp-get" || *mode == "tcp-mixed" {
		tcpCfg := benchmark.TCPBenchConfig{
			Addr:        *serverAddr,
			NumOps:      *numOps,
			KeySize:     *keySize,
			ValueSize:   *valueSize,
			Workers:     *workers,
			ReadPercent: *readPercent,
			NumKeys:     *numOps,
		}

		fmt.Println("TCP Benchmark Configuration:")
		fmt.Printf("  Server:      %s\n", tcpCfg.Addr)
		fmt.Printf("  Operations:  %d\n", tcpCfg.NumOps)
		fmt.Printf("  Key Size:    %d bytes\n", tcpCfg.KeySize)
		fmt.Printf("  Value Size:  %d bytes\n", tcpCfg.ValueSize)
		fmt.Printf("  Workers:     %d\n", tcpCfg.Workers)

		result, err := benchmark.RunTCPBenchmark(tcpCfg, benchmark.TCPWorkloadType(*mode))
		if err != nil {
			fmt.Fprintf(os.Stderr, "TCP benchmark failed: %v\n", err)
			os.Exit(1)
		}

		benchmark.PrintResult(result)
		fmt.Println("\nDone!")
		return
	}

	// Run all benchmarks
	if *mode == "all" {
		fmt.Println("Running all benchmarks...")
		if err := benchmark.RunAll(*dataDir, *numOps, *keySize, *valueSize, *workers); err != nil {
			fmt.Fprintf(os.Stderr, "Benchmark failed: %v\n", err)
			os.Exit(1)
		}
		return
	}

	// Create config
	cfg := benchmark.Config{
		Workload:    benchmark.WorkloadType(*mode),
		NumOps:      *numOps,
		KeySize:     *keySize,
		ValueSize:   *valueSize,
		Workers:     *workers,
		ReadPercent: *readPercent,
		NumKeys:     *numOps, // Pre-populate same number of keys
		DataDir:     *dataDir,
	}

	// Create and run benchmark
	runner := benchmark.NewRunner(cfg)
	runner.PrintConfig()

	result, err := runner.Run()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Benchmark failed: %v\n", err)
		os.Exit(1)
	}

	benchmark.PrintResult(result)

	// Cleanup
	fmt.Println("\nCleaning up...")
	_ = os.RemoveAll(*dataDir)
	fmt.Println("Done!")
}

func printBanner() {
	banner := `
╔═══════════════════════════════════════════════════════╗
║           RapidoDB Benchmark Tool                     ║
║   High-Performance LSM-Tree Key-Value Store           ║
╚═══════════════════════════════════════════════════════╝
`
	fmt.Println(banner)
	fmt.Printf("Version: %s\n", Version)
}
