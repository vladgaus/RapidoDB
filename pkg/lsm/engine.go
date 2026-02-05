// Package lsm provides the LSM-tree storage engine for RapidoDB.
//
// The LSM (Log-Structured Merge) tree is a data structure optimized for
// write-heavy workloads. Writes go to an in-memory MemTable, which is
// periodically flushed to immutable SSTables on disk. Reads check the
// MemTable first, then SSTables from newest to oldest.
//
// Architecture:
//
//	┌─────────────────────────────────────────────────────┐
//	│                    LSM Engine                        │
//	├─────────────────────────────────────────────────────┤
//	│  ┌─────────────┐  ┌─────────────┐                   │
//	│  │  MemTable   │  │    WAL      │  (durability)     │
//	│  │  (active)   │  │             │                   │
//	│  └─────────────┘  └─────────────┘                   │
//	│         │                                            │
//	│         ▼                                            │
//	│  ┌─────────────┐                                    │
//	│  │ Immutable   │  (waiting for flush)               │
//	│  │ MemTables   │                                    │
//	│  └─────────────┘                                    │
//	│         │                                            │
//	│         ▼                                            │
//	│  ┌─────────────────────────────────────────────┐    │
//	│  │              SSTables (L0)                  │    │
//	│  ├─────────────────────────────────────────────┤    │
//	│  │              SSTables (L1)                  │    │
//	│  ├─────────────────────────────────────────────┤    │
//	│  │              SSTables (L2+)                 │    │
//	│  └─────────────────────────────────────────────┘    │
//	└─────────────────────────────────────────────────────┘
//
// Write Path:
//  1. Write to WAL (durability)
//  2. Write to MemTable
//  3. When MemTable is full, make it immutable
//  4. Flush immutable MemTable to SSTable (L0)
//  5. Compact SSTables as needed
//
// Read Path:
//  1. Check active MemTable
//  2. Check immutable MemTables (newest first)
//  3. Check SSTables L0 (newest first)
//  4. Check SSTables L1+ (binary search)
package lsm

import (
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/rapidodb/rapidodb/pkg/compaction"
	"github.com/rapidodb/rapidodb/pkg/memtable"
	"github.com/rapidodb/rapidodb/pkg/wal"
)

// Engine is the LSM-tree storage engine.
type Engine struct {
	mu sync.RWMutex

	// Configuration
	opts Options

	// Data directory paths
	walDir string
	sstDir string

	// Active MemTable for writes
	memTable *memtable.MemTable

	// Immutable MemTables waiting to be flushed
	// Ordered from newest to oldest
	immutableMemTables []*memtable.MemTable

	// Write-Ahead Log for durability
	walManager *wal.Manager

	// Level manager for SSTables (from compaction package)
	levels *compaction.LevelManager

	// Compactor with pluggable strategy
	compactor *compaction.Compactor

	// Sequence number for MVCC
	seqNum uint64

	// File number generator for SSTables
	nextFileNum atomic.Uint64

	// Background workers
	flushChan   chan *flushTask
	closeChan   chan struct{}
	closeWg     sync.WaitGroup
	flushWg     sync.WaitGroup
	flushResult chan error

	// State
	closed atomic.Bool
}

// flushTask represents a MemTable to be flushed.
type flushTask struct {
	mem       *memtable.MemTable
	walFileID uint64
}

// CompactionStrategy specifies the compaction algorithm.
type CompactionStrategy = compaction.StrategyType

const (
	// CompactionLeveled uses level-based compaction (default).
	CompactionLeveled = compaction.StrategyLeveled

	// CompactionTiered uses size-tiered compaction.
	CompactionTiered = compaction.StrategyTiered

	// CompactionFIFO uses first-in-first-out compaction.
	CompactionFIFO = compaction.StrategyFIFO
)

// Options configures the LSM engine.
type Options struct {
	// Dir is the data directory for all files
	Dir string

	// MemTableSize is the maximum size of a MemTable before flushing
	// Default: 4MB
	MemTableSize int64

	// MaxMemTables is the maximum number of immutable MemTables
	// If exceeded, writes will stall until flush completes
	// Default: 4
	MaxMemTables int

	// WAL options
	WALSyncOnWrite bool  // Sync WAL on every write (slower but safer)
	WALMaxFileSize int64 // Maximum WAL file size before rotation

	// SSTable options
	BlockSize       int // Data block size (default: 4KB)
	BloomBitsPerKey int // Bloom filter bits per key (default: 10)

	// Background options
	MaxBackgroundFlushes     int // Max concurrent flush operations (default: 1)
	MaxBackgroundCompactions int // Max concurrent compaction operations (default: 1)

	// Compaction strategy
	CompactionStrategy CompactionStrategy // leveled, tiered, or fifo (default: leveled)

	// L0 options
	L0CompactionTrigger int // Number of L0 files to trigger compaction (default: 4)
	L0StopWritesTrigger int // Number of L0 files to stop writes (default: 12)

	// Compaction options
	NumLevels                  int     // Number of levels (default: 7)
	MaxBytesForLevelBase       int64   // Target size for L1 (default: 64MB)
	MaxBytesForLevelMultiplier float64 // Size ratio between levels (default: 10)
	TargetFileSizeBase         int64   // Target file size for L1 (default: 4MB)
}

// DefaultOptions returns sensible default options.
func DefaultOptions(dir string) Options {
	return Options{
		Dir:                        dir,
		MemTableSize:               4 * 1024 * 1024, // 4MB
		MaxMemTables:               4,
		WALSyncOnWrite:             false,
		WALMaxFileSize:             64 * 1024 * 1024, // 64MB
		BlockSize:                  4 * 1024,         // 4KB
		BloomBitsPerKey:            10,
		MaxBackgroundFlushes:       1,
		MaxBackgroundCompactions:   1,
		CompactionStrategy:         CompactionLeveled,
		L0CompactionTrigger:        4,
		L0StopWritesTrigger:        12,
		NumLevels:                  7,
		MaxBytesForLevelBase:       64 * 1024 * 1024, // 64MB
		MaxBytesForLevelMultiplier: 10,
		TargetFileSizeBase:         4 * 1024 * 1024, // 4MB
	}
}

// validateOptions validates and fills in defaults for options.
func (opts *Options) validate() error {
	if opts.Dir == "" {
		return ErrInvalidOptions
	}
	if opts.MemTableSize <= 0 {
		opts.MemTableSize = 4 * 1024 * 1024
	}
	if opts.MaxMemTables <= 0 {
		opts.MaxMemTables = 4
	}
	if opts.WALMaxFileSize <= 0 {
		opts.WALMaxFileSize = 64 * 1024 * 1024
	}
	if opts.BlockSize <= 0 {
		opts.BlockSize = 4 * 1024
	}
	if opts.BloomBitsPerKey <= 0 {
		opts.BloomBitsPerKey = 10
	}
	if opts.MaxBackgroundFlushes <= 0 {
		opts.MaxBackgroundFlushes = 1
	}
	if opts.MaxBackgroundCompactions <= 0 {
		opts.MaxBackgroundCompactions = 1
	}
	if opts.CompactionStrategy == "" {
		opts.CompactionStrategy = CompactionLeveled
	}
	if opts.L0CompactionTrigger <= 0 {
		opts.L0CompactionTrigger = 4
	}
	if opts.L0StopWritesTrigger <= 0 {
		opts.L0StopWritesTrigger = 12
	}
	if opts.NumLevels <= 0 {
		opts.NumLevels = 7
	}
	if opts.MaxBytesForLevelBase <= 0 {
		opts.MaxBytesForLevelBase = 64 * 1024 * 1024
	}
	if opts.MaxBytesForLevelMultiplier <= 0 {
		opts.MaxBytesForLevelMultiplier = 10
	}
	if opts.TargetFileSizeBase <= 0 {
		opts.TargetFileSizeBase = 4 * 1024 * 1024
	}
	return nil
}

// sstPath returns the path for an SSTable file.
func (e *Engine) sstPath(fileNum uint64) string {
	return filepath.Join(e.sstDir, sstFileName(fileNum))
}

// sstFileName returns the filename for an SSTable.
func sstFileName(fileNum uint64) string {
	return filepath.Join("", formatFileNum(fileNum)+".sst")
}

// formatFileNum formats a file number with leading zeros.
func formatFileNum(num uint64) string {
	return fmt.Sprintf("%06d", num)
}

// allocateFileNum allocates a new unique file number.
func (e *Engine) allocateFileNum() uint64 {
	return e.nextFileNum.Add(1)
}
