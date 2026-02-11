package benchmark

import (
	"bufio"
	"context"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"
)

// TCPBenchConfig configures TCP benchmarks.
type TCPBenchConfig struct {
	// Server address
	Addr string

	// Workload
	NumOps      int
	KeySize     int
	ValueSize   int
	Workers     int
	ReadPercent int // For mixed workload

	// Pre-population
	NumKeys int
}

// DefaultTCPBenchConfig returns default TCP benchmark config.
func DefaultTCPBenchConfig() TCPBenchConfig {
	return TCPBenchConfig{
		Addr:        "127.0.0.1:11211",
		NumOps:      100000,
		KeySize:     16,
		ValueSize:   100,
		Workers:     1,
		ReadPercent: 80,
		NumKeys:     100000,
	}
}

// TCPBenchRunner runs TCP benchmarks.
type TCPBenchRunner struct {
	cfg   TCPBenchConfig
	conns []net.Conn
}

// NewTCPBenchRunner creates a TCP benchmark runner.
func NewTCPBenchRunner(cfg TCPBenchConfig) *TCPBenchRunner {
	return &TCPBenchRunner{cfg: cfg}
}

// Connect establishes connections to the server.
func (r *TCPBenchRunner) Connect() error {
	r.conns = make([]net.Conn, r.cfg.Workers)

	dialer := &net.Dialer{
		Timeout: 5 * time.Second,
	}

	for i := 0; i < r.cfg.Workers; i++ {
		conn, err := dialer.DialContext(context.Background(), "tcp", r.cfg.Addr)
		if err != nil {
			// Close already opened connections
			for j := 0; j < i; j++ {
				_ = r.conns[j].Close()
			}
			return fmt.Errorf("failed to connect: %w", err)
		}
		r.conns[i] = conn
	}

	return nil
}

// Close closes all connections.
func (r *TCPBenchRunner) Close() {
	for _, conn := range r.conns {
		if conn != nil {
			_ = conn.Close()
		}
	}
}

// Populate pre-populates data for read benchmarks.
func (r *TCPBenchRunner) Populate(stats *Stats) error {
	if len(r.conns) == 0 {
		return fmt.Errorf("not connected")
	}

	conn := r.conns[0]
	reader := bufio.NewReader(conn)
	keyGen := NewKeyGenerator("key", r.cfg.KeySize)
	valGen := NewValueGenerator(r.cfg.ValueSize)
	value := valGen.Generate()

	fmt.Printf("Pre-populating %d keys via TCP...\n", r.cfg.NumKeys)

	for i := 0; i < r.cfg.NumKeys; i++ {
		key := keyGen.Sequential()

		// Send set command
		cmd := fmt.Sprintf("set %s 0 0 %d\r\n%s\r\n", key, len(value), value)
		_, err := conn.Write([]byte(cmd))
		if err != nil {
			return fmt.Errorf("write failed: %w", err)
		}

		// Read response
		resp, err := reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("read failed: %w", err)
		}
		if !strings.Contains(resp, "STORED") {
			return fmt.Errorf("unexpected response: %s", resp)
		}

		if (i+1)%10000 == 0 {
			fmt.Printf("  %d keys written\n", i+1)
		}
	}

	fmt.Println("Pre-population complete")
	return nil
}

// RunSet runs a SET benchmark.
func (r *TCPBenchRunner) RunSet(stats *Stats) error {
	valGen := NewValueGenerator(r.cfg.ValueSize)
	value := valGen.Generate()
	opsPerWorker := r.cfg.NumOps / r.cfg.Workers

	var wg sync.WaitGroup
	for i := 0; i < r.cfg.Workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			conn := r.conns[workerID]
			reader := bufio.NewReader(conn)
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
			keyGen := NewKeyGenerator("key", r.cfg.KeySize)

			for j := 0; j < opsPerWorker; j++ {
				n := uint64(rng.Int63n(int64(r.cfg.NumOps)))
				key := keyGen.formatKey(n)

				cmd := fmt.Sprintf("set %s 0 0 %d\r\n%s\r\n", key, len(value), value)

				start := time.Now()
				_, err := conn.Write([]byte(cmd))
				if err != nil {
					stats.RecordError()
					continue
				}

				resp, err := reader.ReadString('\n')
				latency := time.Since(start)

				if err != nil || !strings.Contains(resp, "STORED") {
					stats.RecordError()
				} else {
					stats.RecordOp(latency)
					stats.RecordBytes(0, int64(len(key)+len(value)))
				}
			}
		}(i)
	}

	wg.Wait()
	return nil
}

// RunGet runs a GET benchmark.
func (r *TCPBenchRunner) RunGet(stats *Stats) error {
	opsPerWorker := r.cfg.NumOps / r.cfg.Workers

	var wg sync.WaitGroup
	for i := 0; i < r.cfg.Workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			conn := r.conns[workerID]
			reader := bufio.NewReader(conn)
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
			keyGen := NewKeyGenerator("key", r.cfg.KeySize)

			for j := 0; j < opsPerWorker; j++ {
				n := uint64(rng.Int63n(int64(r.cfg.NumKeys)))
				key := keyGen.formatKey(n)

				cmd := fmt.Sprintf("get %s\r\n", key)

				start := time.Now()
				_, err := conn.Write([]byte(cmd))
				if err != nil {
					stats.RecordError()
					continue
				}

				// Read response (VALUE line + data + END)
				var bytesRead int64
				for {
					line, err := reader.ReadString('\n')
					if err != nil {
						stats.RecordError()
						break
					}
					bytesRead += int64(len(line))
					if strings.HasPrefix(line, "END") {
						break
					}
				}
				latency := time.Since(start)

				stats.RecordOp(latency)
				stats.RecordBytes(bytesRead, 0)
			}
		}(i)
	}

	wg.Wait()
	return nil
}

// RunMixed runs a mixed read/write benchmark.
func (r *TCPBenchRunner) RunMixed(stats *Stats) error {
	valGen := NewValueGenerator(r.cfg.ValueSize)
	value := valGen.Generate()
	opsPerWorker := r.cfg.NumOps / r.cfg.Workers

	var wg sync.WaitGroup
	for i := 0; i < r.cfg.Workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			conn := r.conns[workerID]
			reader := bufio.NewReader(conn)
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
			keyGen := NewKeyGenerator("key", r.cfg.KeySize)

			for j := 0; j < opsPerWorker; j++ {
				n := uint64(rng.Int63n(int64(r.cfg.NumKeys)))
				key := keyGen.formatKey(n)
				isRead := rng.Intn(100) < r.cfg.ReadPercent

				var cmd string
				if isRead {
					cmd = fmt.Sprintf("get %s\r\n", key)
				} else {
					cmd = fmt.Sprintf("set %s 0 0 %d\r\n%s\r\n", key, len(value), value)
				}

				start := time.Now()
				_, err := conn.Write([]byte(cmd))
				if err != nil {
					stats.RecordError()
					continue
				}

				var bytesRead, bytesWritten int64
				if isRead {
					// Read GET response
					for {
						line, err := reader.ReadString('\n')
						if err != nil {
							stats.RecordError()
							break
						}
						bytesRead += int64(len(line))
						if strings.HasPrefix(line, "END") {
							break
						}
					}
				} else {
					// Read SET response
					resp, err := reader.ReadString('\n')
					if err != nil || !strings.Contains(resp, "STORED") {
						stats.RecordError()
						continue
					}
					bytesWritten = int64(len(key) + len(value))
				}
				latency := time.Since(start)

				stats.RecordOp(latency)
				stats.RecordBytes(bytesRead, bytesWritten)
			}
		}(i)
	}

	wg.Wait()
	return nil
}

// TCPWorkloadType defines TCP benchmark types.
type TCPWorkloadType string

const (
	TCPWorkloadSet   TCPWorkloadType = "tcp-set"
	TCPWorkloadGet   TCPWorkloadType = "tcp-get"
	TCPWorkloadMixed TCPWorkloadType = "tcp-mixed"
)

// RunTCPBenchmark runs a complete TCP benchmark.
func RunTCPBenchmark(cfg TCPBenchConfig, workload TCPWorkloadType) (*Result, error) {
	runner := NewTCPBenchRunner(cfg)

	// Connect
	fmt.Println("Connecting to server...")
	if err := runner.Connect(); err != nil {
		return nil, err
	}
	defer runner.Close()

	stats := NewStats()

	// For GET and mixed workloads, populate first
	if workload == TCPWorkloadGet || workload == TCPWorkloadMixed {
		if err := runner.Populate(stats); err != nil {
			return nil, err
		}
		stats = NewStats() // Reset stats after population
	}

	// Create progress reporter
	reporter := NewProgressReporter(stats, time.Second)
	reporter.Start(func(ops uint64, elapsed time.Duration) {
		opsPerSec := float64(ops) / elapsed.Seconds()
		fmt.Printf("\r  Progress: %d ops (%.0f ops/sec)    ", ops, opsPerSec)
	})

	// Run benchmark
	fmt.Printf("\nRunning %s benchmark...\n", workload)
	stats.Start()

	var err error
	switch workload {
	case TCPWorkloadSet:
		err = runner.RunSet(stats)
	case TCPWorkloadGet:
		err = runner.RunGet(stats)
	case TCPWorkloadMixed:
		err = runner.RunMixed(stats)
	}

	stats.Stop()
	reporter.Stop()
	fmt.Println()

	if err != nil {
		return nil, err
	}

	result := stats.Compute()
	return &result, nil
}
