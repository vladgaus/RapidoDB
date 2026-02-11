// Package benchmark provides performance benchmarking for RapidoDB.
package benchmark

import (
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Stats collects benchmark statistics.
type Stats struct {

	// Operation counts
	ops       atomic.Uint64
	errors    atomic.Uint64
	bytesRead atomic.Uint64
	bytesWrit atomic.Uint64

	// Latency tracking (in nanoseconds)
	latencies []int64
	latencyMu sync.Mutex

	// Timing
	startTime time.Time
	endTime   time.Time
}

// NewStats creates a new stats collector.
func NewStats() *Stats {
	return &Stats{
		latencies: make([]int64, 0, 100000),
	}
}

// Start marks the start of the benchmark.
func (s *Stats) Start() {
	s.startTime = time.Now()
}

// Stop marks the end of the benchmark.
func (s *Stats) Stop() {
	s.endTime = time.Now()
}

// RecordOp records a successful operation with latency.
func (s *Stats) RecordOp(latency time.Duration) {
	s.ops.Add(1)

	s.latencyMu.Lock()
	s.latencies = append(s.latencies, int64(latency))
	s.latencyMu.Unlock()
}

// RecordError records a failed operation.
func (s *Stats) RecordError() {
	s.errors.Add(1)
}

// RecordBytes records bytes read/written.
func (s *Stats) RecordBytes(read, written int64) {
	if read > 0 {
		s.bytesRead.Add(uint64(read))
	}
	if written > 0 {
		s.bytesWrit.Add(uint64(written))
	}
}

// Result contains computed benchmark results.
type Result struct {
	// Operation stats
	TotalOps  uint64
	TotalErrs uint64
	Duration  time.Duration
	OpsPerSec float64

	// Latency stats (in microseconds)
	LatencyAvg  float64
	LatencyMin  float64
	LatencyMax  float64
	LatencyP50  float64
	LatencyP90  float64
	LatencyP99  float64
	LatencyP999 float64

	// Throughput
	BytesRead    uint64
	BytesWritten uint64
	ReadMBPS     float64
	WriteMBPS    float64
}

// Compute calculates final benchmark results.
func (s *Stats) Compute() Result {
	duration := s.endTime.Sub(s.startTime)
	ops := s.ops.Load()
	errs := s.errors.Load()
	bytesR := s.bytesRead.Load()
	bytesW := s.bytesWrit.Load()

	result := Result{
		TotalOps:     ops,
		TotalErrs:    errs,
		Duration:     duration,
		BytesRead:    bytesR,
		BytesWritten: bytesW,
	}

	// Calculate ops/sec
	if duration > 0 {
		result.OpsPerSec = float64(ops) / duration.Seconds()
		result.ReadMBPS = float64(bytesR) / duration.Seconds() / (1024 * 1024)
		result.WriteMBPS = float64(bytesW) / duration.Seconds() / (1024 * 1024)
	}

	// Calculate latency percentiles
	s.latencyMu.Lock()
	latencies := make([]int64, len(s.latencies))
	copy(latencies, s.latencies)
	s.latencyMu.Unlock()

	if len(latencies) > 0 {
		sort.Slice(latencies, func(i, j int) bool {
			return latencies[i] < latencies[j]
		})

		// Convert to microseconds for display
		toMicro := func(ns int64) float64 {
			return float64(ns) / 1000.0
		}

		result.LatencyMin = toMicro(latencies[0])
		result.LatencyMax = toMicro(latencies[len(latencies)-1])
		result.LatencyAvg = toMicro(average(latencies))
		result.LatencyP50 = toMicro(percentile(latencies, 50))
		result.LatencyP90 = toMicro(percentile(latencies, 90))
		result.LatencyP99 = toMicro(percentile(latencies, 99))
		result.LatencyP999 = toMicro(percentile(latencies, 99.9))
	}

	return result
}

// Merge combines stats from another collector.
func (s *Stats) Merge(other *Stats) {
	s.ops.Add(other.ops.Load())
	s.errors.Add(other.errors.Load())
	s.bytesRead.Add(other.bytesRead.Load())
	s.bytesWrit.Add(other.bytesWrit.Load())

	s.latencyMu.Lock()
	other.latencyMu.Lock()
	s.latencies = append(s.latencies, other.latencies...)
	other.latencyMu.Unlock()
	s.latencyMu.Unlock()
}

// Helper functions

func average(values []int64) int64 {
	if len(values) == 0 {
		return 0
	}
	var sum int64
	for _, v := range values {
		sum += v
	}
	return sum / int64(len(values))
}

func percentile(sorted []int64, p float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(float64(len(sorted))*p/100.0)) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// ProgressReporter reports benchmark progress periodically.
type ProgressReporter struct {
	stats    *Stats
	interval time.Duration
	stopCh   chan struct{}
	doneCh   chan struct{}
}

// NewProgressReporter creates a progress reporter.
func NewProgressReporter(stats *Stats, interval time.Duration) *ProgressReporter {
	return &ProgressReporter{
		stats:    stats,
		interval: interval,
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
}

// Start begins periodic progress reporting.
func (p *ProgressReporter) Start(callback func(ops uint64, elapsed time.Duration)) {
	go func() {
		defer close(p.doneCh)
		ticker := time.NewTicker(p.interval)
		defer ticker.Stop()

		startTime := time.Now()
		for {
			select {
			case <-ticker.C:
				callback(p.stats.ops.Load(), time.Since(startTime))
			case <-p.stopCh:
				return
			}
		}
	}()
}

// Stop stops the progress reporter.
func (p *ProgressReporter) Stop() {
	close(p.stopCh)
	<-p.doneCh
}
