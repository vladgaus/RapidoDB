package benchmark

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/rapidodb/rapidodb/pkg/lsm"
)

// WorkloadType defines the type of benchmark workload.
type WorkloadType string

const (
	WorkloadFillSeq    WorkloadType = "fillseq"
	WorkloadFillRandom WorkloadType = "fillrandom"
	WorkloadReadSeq    WorkloadType = "readseq"
	WorkloadReadRandom WorkloadType = "readrandom"
	WorkloadReadWrite  WorkloadType = "readwrite"
	WorkloadScan       WorkloadType = "scan"
	WorkloadDelete     WorkloadType = "delete"
)

// Config configures a benchmark run.
type Config struct {
	// Workload type
	Workload WorkloadType

	// Number of operations
	NumOps int

	// Key/value sizes
	KeySize   int
	ValueSize int

	// Concurrency
	Workers int

	// For mixed workloads
	ReadPercent int // Percentage of reads (0-100)

	// Data directory for embedded mode
	DataDir string

	// For pre-population
	NumKeys int // Number of keys to pre-populate for read benchmarks
}

// DefaultConfig returns default benchmark configuration.
func DefaultConfig() Config {
	return Config{
		Workload:    WorkloadFillRandom,
		NumOps:      1000000,
		KeySize:     16,
		ValueSize:   100,
		Workers:     1,
		ReadPercent: 80,
		NumKeys:     1000000,
	}
}

// Workload defines a benchmark workload.
type Workload interface {
	// Name returns the workload name.
	Name() string

	// Setup prepares the workload (e.g., pre-populate data).
	Setup(engine *lsm.Engine, cfg Config) error

	// Run executes the workload.
	Run(engine *lsm.Engine, cfg Config, stats *Stats) error
}

// KeyGenerator generates keys for benchmarks.
type KeyGenerator struct {
	prefix  string
	keySize int
	counter uint64
	mu      sync.Mutex
	rng     *rand.Rand
}

// NewKeyGenerator creates a key generator.
func NewKeyGenerator(prefix string, keySize int) *KeyGenerator {
	return &KeyGenerator{
		prefix:  prefix,
		keySize: keySize,
		rng:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Sequential returns the next sequential key.
func (g *KeyGenerator) Sequential() []byte {
	g.mu.Lock()
	n := g.counter
	g.counter++
	g.mu.Unlock()

	return g.formatKey(n)
}

// Random returns a random key in range [0, max).
func (g *KeyGenerator) Random(max uint64) []byte {
	g.mu.Lock()
	n := uint64(g.rng.Int63n(int64(max)))
	g.mu.Unlock()

	return g.formatKey(n)
}

// formatKey formats a key with the given number.
func (g *KeyGenerator) formatKey(n uint64) []byte {
	key := fmt.Sprintf("%s%016d", g.prefix, n)
	if len(key) > g.keySize {
		key = key[:g.keySize]
	}
	return []byte(key)
}

// ValueGenerator generates values for benchmarks.
type ValueGenerator struct {
	size int
	buf  []byte
}

// NewValueGenerator creates a value generator.
func NewValueGenerator(size int) *ValueGenerator {
	buf := make([]byte, size)
	for i := range buf {
		buf[i] = byte('a' + (i % 26))
	}
	return &ValueGenerator{
		size: size,
		buf:  buf,
	}
}

// Generate returns a value of configured size.
func (g *ValueGenerator) Generate() []byte {
	return g.buf
}

// FillSeqWorkload writes keys sequentially.
type FillSeqWorkload struct{}

func (w *FillSeqWorkload) Name() string { return "fillseq" }

func (w *FillSeqWorkload) Setup(engine *lsm.Engine, cfg Config) error {
	return nil // No setup needed
}

func (w *FillSeqWorkload) Run(engine *lsm.Engine, cfg Config, stats *Stats) error {
	keyGen := NewKeyGenerator("key", cfg.KeySize)
	valGen := NewValueGenerator(cfg.ValueSize)
	value := valGen.Generate()

	opsPerWorker := cfg.NumOps / cfg.Workers

	var wg sync.WaitGroup
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for j := 0; j < opsPerWorker; j++ {
				key := keyGen.Sequential()

				start := time.Now()
				err := engine.Put(key, value)
				latency := time.Since(start)

				if err != nil {
					stats.RecordError()
				} else {
					stats.RecordOp(latency)
					stats.RecordBytes(0, int64(len(key)+len(value)))
				}
			}
		}()
	}

	wg.Wait()
	return nil
}

// FillRandomWorkload writes keys in random order.
type FillRandomWorkload struct{}

func (w *FillRandomWorkload) Name() string { return "fillrandom" }

func (w *FillRandomWorkload) Setup(engine *lsm.Engine, cfg Config) error {
	return nil
}

func (w *FillRandomWorkload) Run(engine *lsm.Engine, cfg Config, stats *Stats) error {
	valGen := NewValueGenerator(cfg.ValueSize)
	value := valGen.Generate()

	opsPerWorker := cfg.NumOps / cfg.Workers

	var wg sync.WaitGroup
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
			keyGen := NewKeyGenerator("key", cfg.KeySize)

			for j := 0; j < opsPerWorker; j++ {
				n := uint64(rng.Int63n(int64(cfg.NumOps)))
				key := keyGen.formatKey(n)

				start := time.Now()
				err := engine.Put(key, value)
				latency := time.Since(start)

				if err != nil {
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

// ReadSeqWorkload reads keys sequentially.
type ReadSeqWorkload struct{}

func (w *ReadSeqWorkload) Name() string { return "readseq" }

func (w *ReadSeqWorkload) Setup(engine *lsm.Engine, cfg Config) error {
	// Pre-populate data
	keyGen := NewKeyGenerator("key", cfg.KeySize)
	valGen := NewValueGenerator(cfg.ValueSize)
	value := valGen.Generate()

	fmt.Printf("Pre-populating %d keys...\n", cfg.NumKeys)
	for i := 0; i < cfg.NumKeys; i++ {
		key := keyGen.Sequential()
		if err := engine.Put(key, value); err != nil {
			return fmt.Errorf("setup failed: %w", err)
		}
		if (i+1)%100000 == 0 {
			fmt.Printf("  %d keys written\n", i+1)
		}
	}
	fmt.Println("Pre-population complete")

	return nil
}

func (w *ReadSeqWorkload) Run(engine *lsm.Engine, cfg Config, stats *Stats) error {
	keyGen := NewKeyGenerator("key", cfg.KeySize)
	opsPerWorker := cfg.NumOps / cfg.Workers

	var wg sync.WaitGroup
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for j := 0; j < opsPerWorker; j++ {
				n := uint64(j % cfg.NumKeys)
				key := keyGen.formatKey(n)

				start := time.Now()
				val, err := engine.Get(key)
				latency := time.Since(start)

				if err != nil {
					stats.RecordError()
				} else {
					stats.RecordOp(latency)
					stats.RecordBytes(int64(len(key)+len(val)), 0)
				}
			}
		}()
	}

	wg.Wait()
	return nil
}

// ReadRandomWorkload reads keys in random order.
type ReadRandomWorkload struct{}

func (w *ReadRandomWorkload) Name() string { return "readrandom" }

func (w *ReadRandomWorkload) Setup(engine *lsm.Engine, cfg Config) error {
	// Pre-populate data
	keyGen := NewKeyGenerator("key", cfg.KeySize)
	valGen := NewValueGenerator(cfg.ValueSize)
	value := valGen.Generate()

	fmt.Printf("Pre-populating %d keys...\n", cfg.NumKeys)
	for i := 0; i < cfg.NumKeys; i++ {
		key := keyGen.Sequential()
		if err := engine.Put(key, value); err != nil {
			return fmt.Errorf("setup failed: %w", err)
		}
		if (i+1)%100000 == 0 {
			fmt.Printf("  %d keys written\n", i+1)
		}
	}
	fmt.Println("Pre-population complete")

	return nil
}

func (w *ReadRandomWorkload) Run(engine *lsm.Engine, cfg Config, stats *Stats) error {
	keyGen := NewKeyGenerator("key", cfg.KeySize)
	opsPerWorker := cfg.NumOps / cfg.Workers

	var wg sync.WaitGroup
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

			for j := 0; j < opsPerWorker; j++ {
				n := uint64(rng.Int63n(int64(cfg.NumKeys)))
				key := keyGen.formatKey(n)

				start := time.Now()
				val, err := engine.Get(key)
				latency := time.Since(start)

				if err != nil {
					stats.RecordError()
				} else {
					stats.RecordOp(latency)
					stats.RecordBytes(int64(len(key)+len(val)), 0)
				}
			}
		}(i)
	}

	wg.Wait()
	return nil
}

// ReadWriteWorkload performs mixed read/write operations.
type ReadWriteWorkload struct{}

func (w *ReadWriteWorkload) Name() string { return "readwrite" }

func (w *ReadWriteWorkload) Setup(engine *lsm.Engine, cfg Config) error {
	// Pre-populate data
	keyGen := NewKeyGenerator("key", cfg.KeySize)
	valGen := NewValueGenerator(cfg.ValueSize)
	value := valGen.Generate()

	fmt.Printf("Pre-populating %d keys...\n", cfg.NumKeys)
	for i := 0; i < cfg.NumKeys; i++ {
		key := keyGen.Sequential()
		if err := engine.Put(key, value); err != nil {
			return fmt.Errorf("setup failed: %w", err)
		}
		if (i+1)%100000 == 0 {
			fmt.Printf("  %d keys written\n", i+1)
		}
	}
	fmt.Println("Pre-population complete")

	return nil
}

func (w *ReadWriteWorkload) Run(engine *lsm.Engine, cfg Config, stats *Stats) error {
	keyGen := NewKeyGenerator("key", cfg.KeySize)
	valGen := NewValueGenerator(cfg.ValueSize)
	value := valGen.Generate()
	opsPerWorker := cfg.NumOps / cfg.Workers

	var wg sync.WaitGroup
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

			for j := 0; j < opsPerWorker; j++ {
				n := uint64(rng.Int63n(int64(cfg.NumKeys)))
				key := keyGen.formatKey(n)

				isRead := rng.Intn(100) < cfg.ReadPercent

				start := time.Now()
				var err error
				var bytesR, bytesW int64

				if isRead {
					var val []byte
					val, err = engine.Get(key)
					bytesR = int64(len(key) + len(val))
				} else {
					err = engine.Put(key, value)
					bytesW = int64(len(key) + len(value))
				}

				latency := time.Since(start)

				if err != nil {
					stats.RecordError()
				} else {
					stats.RecordOp(latency)
					stats.RecordBytes(bytesR, bytesW)
				}
			}
		}(i)
	}

	wg.Wait()
	return nil
}

// ScanWorkload performs range scans.
type ScanWorkload struct{}

func (w *ScanWorkload) Name() string { return "scan" }

func (w *ScanWorkload) Setup(engine *lsm.Engine, cfg Config) error {
	// Pre-populate data
	keyGen := NewKeyGenerator("key", cfg.KeySize)
	valGen := NewValueGenerator(cfg.ValueSize)
	value := valGen.Generate()

	fmt.Printf("Pre-populating %d keys...\n", cfg.NumKeys)
	for i := 0; i < cfg.NumKeys; i++ {
		key := keyGen.Sequential()
		if err := engine.Put(key, value); err != nil {
			return fmt.Errorf("setup failed: %w", err)
		}
		if (i+1)%100000 == 0 {
			fmt.Printf("  %d keys written\n", i+1)
		}
	}
	fmt.Println("Pre-population complete")

	return nil
}

func (w *ScanWorkload) Run(engine *lsm.Engine, cfg Config, stats *Stats) error {
	scanSize := 100 // Scan 100 keys per operation
	if cfg.NumKeys < scanSize*2 {
		scanSize = cfg.NumKeys / 2
	}
	if scanSize < 1 {
		scanSize = 1
	}
	opsPerWorker := cfg.NumOps / cfg.Workers

	var wg sync.WaitGroup
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
			keyGen := NewKeyGenerator("key", cfg.KeySize)

			maxStart := cfg.NumKeys - scanSize
			if maxStart < 1 {
				maxStart = 1
			}

			for j := 0; j < opsPerWorker; j++ {
				// Pick a random start key
				startN := uint64(rng.Int63n(int64(maxStart)))
				startKey := keyGen.formatKey(startN)
				endKey := keyGen.formatKey(startN + uint64(scanSize))

				start := time.Now()
				iter := engine.Scan(startKey, endKey)
				count := 0
				var bytesR int64
				for iter.SeekToFirst(); iter.Valid(); iter.Next() {
					count++
					bytesR += int64(len(iter.Key()) + len(iter.Value()))
				}
				_ = iter.Close()
				latency := time.Since(start)

				stats.RecordOp(latency)
				stats.RecordBytes(bytesR, 0)
			}
		}(i)
	}

	wg.Wait()
	return nil
}

// DeleteWorkload deletes keys randomly.
type DeleteWorkload struct{}

func (w *DeleteWorkload) Name() string { return "delete" }

func (w *DeleteWorkload) Setup(engine *lsm.Engine, cfg Config) error {
	// Pre-populate data
	keyGen := NewKeyGenerator("key", cfg.KeySize)
	valGen := NewValueGenerator(cfg.ValueSize)
	value := valGen.Generate()

	fmt.Printf("Pre-populating %d keys...\n", cfg.NumKeys)
	for i := 0; i < cfg.NumKeys; i++ {
		key := keyGen.Sequential()
		if err := engine.Put(key, value); err != nil {
			return fmt.Errorf("setup failed: %w", err)
		}
		if (i+1)%100000 == 0 {
			fmt.Printf("  %d keys written\n", i+1)
		}
	}
	fmt.Println("Pre-population complete")

	return nil
}

func (w *DeleteWorkload) Run(engine *lsm.Engine, cfg Config, stats *Stats) error {
	keyGen := NewKeyGenerator("key", cfg.KeySize)
	opsPerWorker := cfg.NumOps / cfg.Workers

	var wg sync.WaitGroup
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

			for j := 0; j < opsPerWorker; j++ {
				n := uint64(rng.Int63n(int64(cfg.NumKeys)))
				key := keyGen.formatKey(n)

				start := time.Now()
				err := engine.Delete(key)
				latency := time.Since(start)

				if err != nil {
					stats.RecordError()
				} else {
					stats.RecordOp(latency)
				}
			}
		}(i)
	}

	wg.Wait()
	return nil
}

// GetWorkload returns a workload by type.
func GetWorkload(t WorkloadType) Workload {
	switch t {
	case WorkloadFillSeq:
		return &FillSeqWorkload{}
	case WorkloadFillRandom:
		return &FillRandomWorkload{}
	case WorkloadReadSeq:
		return &ReadSeqWorkload{}
	case WorkloadReadRandom:
		return &ReadRandomWorkload{}
	case WorkloadReadWrite:
		return &ReadWriteWorkload{}
	case WorkloadScan:
		return &ScanWorkload{}
	case WorkloadDelete:
		return &DeleteWorkload{}
	default:
		return &FillRandomWorkload{}
	}
}
