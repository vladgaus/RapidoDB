package benchmark

import (
	"os"
	"testing"
	"time"
)

func TestStats(t *testing.T) {
	stats := NewStats()
	stats.Start()

	// Record some operations
	for i := 0; i < 100; i++ {
		stats.RecordOp(time.Microsecond * time.Duration(i+1))
	}

	stats.RecordBytes(1000, 2000)
	stats.RecordError()

	stats.Stop()

	result := stats.Compute()

	if result.TotalOps != 100 {
		t.Errorf("TotalOps = %d, want 100", result.TotalOps)
	}

	if result.TotalErrs != 1 {
		t.Errorf("TotalErrs = %d, want 1", result.TotalErrs)
	}

	if result.BytesRead != 1000 {
		t.Errorf("BytesRead = %d, want 1000", result.BytesRead)
	}

	if result.BytesWritten != 2000 {
		t.Errorf("BytesWritten = %d, want 2000", result.BytesWritten)
	}

	// Check latency percentiles are reasonable
	if result.LatencyMin <= 0 {
		t.Error("LatencyMin should be > 0")
	}

	if result.LatencyMax <= 0 {
		t.Error("LatencyMax should be > 0")
	}

	if result.LatencyP50 <= 0 {
		t.Error("LatencyP50 should be > 0")
	}

	if result.LatencyP99 <= 0 {
		t.Error("LatencyP99 should be > 0")
	}
}

func TestStatsMerge(t *testing.T) {
	stats1 := NewStats()
	stats2 := NewStats()

	stats1.RecordOp(time.Microsecond)
	stats1.RecordOp(time.Microsecond)

	stats2.RecordOp(time.Microsecond)
	stats2.RecordBytes(100, 200)

	stats1.Merge(stats2)

	if stats1.ops.Load() != 3 {
		t.Errorf("merged ops = %d, want 3", stats1.ops.Load())
	}

	if stats1.bytesRead.Load() != 100 {
		t.Errorf("merged bytesRead = %d, want 100", stats1.bytesRead.Load())
	}
}

func TestKeyGenerator(t *testing.T) {
	gen := NewKeyGenerator("test", 20)

	// Sequential keys
	k1 := gen.Sequential()
	k2 := gen.Sequential()
	k3 := gen.Sequential()

	if string(k1) == string(k2) {
		t.Error("sequential keys should be different")
	}

	if len(k1) != 20 || len(k2) != 20 || len(k3) != 20 {
		t.Error("key length should match keySize")
	}

	// Random keys
	r1 := gen.Random(1000000)
	r2 := gen.Random(1000000)

	if len(r1) != 20 || len(r2) != 20 {
		t.Error("random key length should match keySize")
	}
}

func TestValueGenerator(t *testing.T) {
	gen := NewValueGenerator(100)
	val := gen.Generate()

	if len(val) != 100 {
		t.Errorf("value length = %d, want 100", len(val))
	}

	// Should be repeatable
	val2 := gen.Generate()
	if string(val) != string(val2) {
		t.Error("generated values should be consistent")
	}
}

func TestGetWorkload(t *testing.T) {
	tests := []struct {
		wtype WorkloadType
		name  string
	}{
		{WorkloadFillSeq, "fillseq"},
		{WorkloadFillRandom, "fillrandom"},
		{WorkloadReadSeq, "readseq"},
		{WorkloadReadRandom, "readrandom"},
		{WorkloadReadWrite, "readwrite"},
		{WorkloadScan, "scan"},
		{WorkloadDelete, "delete"},
	}

	for _, tt := range tests {
		w := GetWorkload(tt.wtype)
		if w.Name() != tt.name {
			t.Errorf("GetWorkload(%s).Name() = %s, want %s", tt.wtype, w.Name(), tt.name)
		}
	}
}

func TestRunnerFillSeq(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping benchmark test in short mode")
	}

	dir, err := os.MkdirTemp("", "rapidodb-bench-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cfg := Config{
		Workload:  WorkloadFillSeq,
		NumOps:    1000,
		KeySize:   16,
		ValueSize: 100,
		Workers:   1,
		DataDir:   dir,
	}

	runner := NewRunner(cfg)
	result, err := runner.Run()
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	if result.TotalOps != 1000 {
		t.Errorf("TotalOps = %d, want 1000", result.TotalOps)
	}

	if result.OpsPerSec <= 0 {
		t.Error("OpsPerSec should be > 0")
	}

	if result.BytesWritten == 0 {
		t.Error("BytesWritten should be > 0")
	}
}

func TestRunnerFillRandom(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping benchmark test in short mode")
	}

	dir, err := os.MkdirTemp("", "rapidodb-bench-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cfg := Config{
		Workload:  WorkloadFillRandom,
		NumOps:    1000,
		KeySize:   16,
		ValueSize: 100,
		Workers:   2,
		DataDir:   dir,
	}

	runner := NewRunner(cfg)
	result, err := runner.Run()
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	if result.TotalOps < 900 { // Allow for rounding with workers
		t.Errorf("TotalOps = %d, want ~1000", result.TotalOps)
	}
}

func TestRunnerReadRandom(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping benchmark test in short mode")
	}

	dir, err := os.MkdirTemp("", "rapidodb-bench-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cfg := Config{
		Workload:  WorkloadReadRandom,
		NumOps:    1000,
		KeySize:   16,
		ValueSize: 100,
		Workers:   1,
		NumKeys:   500, // Pre-populate 500 keys
		DataDir:   dir,
	}

	runner := NewRunner(cfg)
	result, err := runner.Run()
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	if result.TotalOps != 1000 {
		t.Errorf("TotalOps = %d, want 1000", result.TotalOps)
	}

	if result.BytesRead == 0 {
		t.Error("BytesRead should be > 0")
	}
}

func TestRunnerScan(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping benchmark test in short mode")
	}

	dir, err := os.MkdirTemp("", "rapidodb-bench-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cfg := Config{
		Workload:  WorkloadScan,
		NumOps:    10,
		KeySize:   16,
		ValueSize: 100,
		Workers:   1,
		NumKeys:   1000, // Pre-populate 1000 keys for better scan range
		DataDir:   dir,
	}

	runner := NewRunner(cfg)
	result, err := runner.Run()
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	if result.TotalOps != 10 {
		t.Errorf("TotalOps = %d, want 10", result.TotalOps)
	}

	// Scan reads data from iterator
	if result.OpsPerSec <= 0 {
		t.Error("OpsPerSec should be > 0")
	}
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		bytes uint64
		want  string
	}{
		{0, "0 B"},
		{100, "100 B"},
		{1024, "1.0 KB"},
		{1536, "1.5 KB"},
		{1048576, "1.0 MB"},
		{1073741824, "1.0 GB"},
	}

	for _, tt := range tests {
		got := formatBytes(tt.bytes)
		if got != tt.want {
			t.Errorf("formatBytes(%d) = %s, want %s", tt.bytes, got, tt.want)
		}
	}
}

func TestProgressReporter(t *testing.T) {
	stats := NewStats()
	stats.RecordOp(time.Microsecond)

	reporter := NewProgressReporter(stats, 50*time.Millisecond)

	var called int
	reporter.Start(func(ops uint64, elapsed time.Duration) {
		called++
	})

	time.Sleep(150 * time.Millisecond)
	reporter.Stop()

	if called < 2 {
		t.Errorf("callback called %d times, want >= 2", called)
	}
}

func BenchmarkStatsRecordOp(b *testing.B) {
	stats := NewStats()
	latency := time.Microsecond

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stats.RecordOp(latency)
	}
}

func BenchmarkKeyGeneratorSequential(b *testing.B) {
	gen := NewKeyGenerator("key", 16)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		gen.Sequential()
	}
}

func BenchmarkKeyGeneratorRandom(b *testing.B) {
	gen := NewKeyGenerator("key", 16)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		gen.Random(1000000)
	}
}

func TestTCPBenchConfig(t *testing.T) {
	cfg := DefaultTCPBenchConfig()

	if cfg.Addr != "127.0.0.1:11211" {
		t.Errorf("Addr = %s, want 127.0.0.1:11211", cfg.Addr)
	}

	if cfg.NumOps != 100000 {
		t.Errorf("NumOps = %d, want 100000", cfg.NumOps)
	}

	if cfg.Workers != 1 {
		t.Errorf("Workers = %d, want 1", cfg.Workers)
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Workload != WorkloadFillRandom {
		t.Errorf("Workload = %s, want fillrandom", cfg.Workload)
	}

	if cfg.NumOps != 1000000 {
		t.Errorf("NumOps = %d, want 1000000", cfg.NumOps)
	}
}
