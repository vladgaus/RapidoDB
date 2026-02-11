package benchmark

import (
	"fmt"
	"os"
	"time"

	"github.com/rapidodb/rapidodb/pkg/lsm"
)

// Runner executes benchmarks.
type Runner struct {
	cfg    Config
	engine *lsm.Engine
}

// NewRunner creates a benchmark runner.
func NewRunner(cfg Config) *Runner {
	return &Runner{
		cfg: cfg,
	}
}

// Run executes the benchmark.
func (r *Runner) Run() (*Result, error) {
	// Create data directory if needed
	if err := os.MkdirAll(r.cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data dir: %w", err)
	}

	// Open engine
	opts := lsm.DefaultOptions(r.cfg.DataDir)
	opts.MemTableSize = 64 * 1024 * 1024 // 64MB for benchmarks

	engine, err := lsm.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open engine: %w", err)
	}
	defer func() { _ = engine.Close() }()
	r.engine = engine

	// Get workload
	workload := GetWorkload(r.cfg.Workload)

	fmt.Printf("\n=== Running %s benchmark ===\n\n", workload.Name())

	// Setup
	fmt.Println("Setting up...")
	if err := workload.Setup(engine, r.cfg); err != nil {
		return nil, fmt.Errorf("setup failed: %w", err)
	}

	// Create stats
	stats := NewStats()

	// Create progress reporter
	reporter := NewProgressReporter(stats, time.Second)
	reporter.Start(func(ops uint64, elapsed time.Duration) {
		opsPerSec := float64(ops) / elapsed.Seconds()
		fmt.Printf("\r  Progress: %d ops (%.0f ops/sec)    ", ops, opsPerSec)
	})

	// Run benchmark
	fmt.Println("Running benchmark...")
	stats.Start()
	if err := workload.Run(engine, r.cfg, stats); err != nil {
		reporter.Stop()
		return nil, fmt.Errorf("benchmark failed: %w", err)
	}
	stats.Stop()
	reporter.Stop()

	fmt.Println() // Clear progress line

	// Compute results
	result := stats.Compute()
	return &result, nil
}

// PrintConfig prints the benchmark configuration.
func (r *Runner) PrintConfig() {
	fmt.Println("Configuration:")
	fmt.Printf("  Workload:    %s\n", r.cfg.Workload)
	fmt.Printf("  Operations:  %d\n", r.cfg.NumOps)
	fmt.Printf("  Key Size:    %d bytes\n", r.cfg.KeySize)
	fmt.Printf("  Value Size:  %d bytes\n", r.cfg.ValueSize)
	fmt.Printf("  Workers:     %d\n", r.cfg.Workers)
	if r.cfg.Workload == WorkloadReadWrite {
		fmt.Printf("  Read %%:      %d%%\n", r.cfg.ReadPercent)
	}
	if r.cfg.Workload == WorkloadReadSeq || r.cfg.Workload == WorkloadReadRandom || r.cfg.Workload == WorkloadReadWrite {
		fmt.Printf("  Pre-pop:     %d keys\n", r.cfg.NumKeys)
	}
	fmt.Printf("  Data Dir:    %s\n", r.cfg.DataDir)
}

// PrintResult prints benchmark results.
func PrintResult(r *Result) {
	fmt.Println("\n╔═══════════════════════════════════════════════════════╗")
	fmt.Println("║                  Benchmark Results                    ║")
	fmt.Println("╠═══════════════════════════════════════════════════════╣")

	// Operations
	fmt.Printf("║  %-20s %15d %-15s ║\n", "Total Operations", r.TotalOps, "ops")
	fmt.Printf("║  %-20s %15d %-15s ║\n", "Errors", r.TotalErrs, "")
	fmt.Printf("║  %-20s %15.2f %-15s ║\n", "Duration", r.Duration.Seconds(), "seconds")
	fmt.Printf("║  %-20s %15.0f %-15s ║\n", "Throughput", r.OpsPerSec, "ops/sec")

	fmt.Println("╠═══════════════════════════════════════════════════════╣")

	// Latency
	fmt.Printf("║  %-20s %15.2f %-15s ║\n", "Latency (avg)", r.LatencyAvg, "µs")
	fmt.Printf("║  %-20s %15.2f %-15s ║\n", "Latency (min)", r.LatencyMin, "µs")
	fmt.Printf("║  %-20s %15.2f %-15s ║\n", "Latency (max)", r.LatencyMax, "µs")
	fmt.Printf("║  %-20s %15.2f %-15s ║\n", "Latency (p50)", r.LatencyP50, "µs")
	fmt.Printf("║  %-20s %15.2f %-15s ║\n", "Latency (p90)", r.LatencyP90, "µs")
	fmt.Printf("║  %-20s %15.2f %-15s ║\n", "Latency (p99)", r.LatencyP99, "µs")
	fmt.Printf("║  %-20s %15.2f %-15s ║\n", "Latency (p99.9)", r.LatencyP999, "µs")

	fmt.Println("╠═══════════════════════════════════════════════════════╣")

	// Throughput
	if r.BytesWritten > 0 {
		fmt.Printf("║  %-20s %15s %-15s ║\n", "Data Written", formatBytes(r.BytesWritten), "")
		fmt.Printf("║  %-20s %15.2f %-15s ║\n", "Write Throughput", r.WriteMBPS, "MB/s")
	}
	if r.BytesRead > 0 {
		fmt.Printf("║  %-20s %15s %-15s ║\n", "Data Read", formatBytes(r.BytesRead), "")
		fmt.Printf("║  %-20s %15.2f %-15s ║\n", "Read Throughput", r.ReadMBPS, "MB/s")
	}

	fmt.Println("╚═══════════════════════════════════════════════════════╝")
}

// formatBytes formats bytes as human-readable string.
func formatBytes(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

// RunAll runs multiple benchmark types and prints comparison.
func RunAll(dataDir string, numOps int, keySize, valueSize, workers int) error {
	workloads := []WorkloadType{
		WorkloadFillSeq,
		WorkloadFillRandom,
		WorkloadReadSeq,
		WorkloadReadRandom,
		WorkloadScan,
		WorkloadReadWrite,
	}

	results := make(map[WorkloadType]*Result)

	for _, wl := range workloads {
		// Clean data dir between runs
		_ = os.RemoveAll(dataDir)

		cfg := Config{
			Workload:    wl,
			NumOps:      numOps,
			KeySize:     keySize,
			ValueSize:   valueSize,
			Workers:     workers,
			ReadPercent: 80,
			NumKeys:     numOps,
			DataDir:     dataDir,
		}

		runner := NewRunner(cfg)
		result, err := runner.Run()
		if err != nil {
			return fmt.Errorf("%s failed: %w", wl, err)
		}
		results[wl] = result
	}

	// Print comparison table
	PrintComparison(results)

	return nil
}

// PrintComparison prints a comparison of multiple benchmark results.
func PrintComparison(results map[WorkloadType]*Result) {
	fmt.Println("\n╔═══════════════════════════════════════════════════════════════════════╗")
	fmt.Println("║                        Benchmark Comparison                           ║")
	fmt.Println("╠═══════════════════════════════════════════════════════════════════════╣")
	fmt.Printf("║  %-15s %12s %12s %12s %12s ║\n", "Workload", "Ops/sec", "Avg (µs)", "P99 (µs)", "MB/s")
	fmt.Println("╠═══════════════════════════════════════════════════════════════════════╣")

	order := []WorkloadType{
		WorkloadFillSeq,
		WorkloadFillRandom,
		WorkloadReadSeq,
		WorkloadReadRandom,
		WorkloadScan,
		WorkloadReadWrite,
	}

	for _, wl := range order {
		if r, ok := results[wl]; ok {
			mbps := r.WriteMBPS
			if r.ReadMBPS > mbps {
				mbps = r.ReadMBPS
			}
			fmt.Printf("║  %-15s %12.0f %12.2f %12.2f %12.2f ║\n",
				wl, r.OpsPerSec, r.LatencyAvg, r.LatencyP99, mbps)
		}
	}

	fmt.Println("╚═══════════════════════════════════════════════════════════════════════╝")
}
