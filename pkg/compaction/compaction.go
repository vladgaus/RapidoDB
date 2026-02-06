// Package compaction provides compaction strategies for the LSM-tree.
//
// Compaction is the process of merging SSTables to:
// - Reduce read amplification (fewer files to check)
// - Reduce space amplification (remove old versions and tombstones)
// - Maintain sorted order within levels
//
// Supported strategies:
// - Leveled: Traditional level-based compaction (RocksDB default)
// - Tiered: Size-tiered compaction (Cassandra style) - Step 9
// - FIFO: First-in-first-out for time-series data - Step 10
//
// Architecture:
//
//	┌─────────────────────────────────────────────────────────┐
//	│                   Compaction System                      │
//	├─────────────────────────────────────────────────────────┤
//	│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐     │
//	│  │   Leveled   │  │   Tiered    │  │    FIFO     │     │
//	│  │  Strategy   │  │  Strategy   │  │  Strategy   │     │
//	│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘     │
//	│         │                │                │             │
//	│         └────────────────┼────────────────┘             │
//	│                          │                              │
//	│                  ┌───────▼───────┐                      │
//	│                  │   Strategy    │ (interface)          │
//	│                  │   Interface   │                      │
//	│                  └───────┬───────┘                      │
//	│                          │                              │
//	│                  ┌───────▼───────┐                      │
//	│                  │   Compactor   │                      │
//	│                  │   (Runner)    │                      │
//	│                  └───────┬───────┘                      │
//	│                          │                              │
//	│                  ┌───────▼───────┐                      │
//	│                  │ LevelManager  │                      │
//	│                  │  (Shared)     │                      │
//	│                  └───────────────┘                      │
//	└─────────────────────────────────────────────────────────┘
package compaction

import (
	"github.com/rapidodb/rapidodb/pkg/sstable"
	"github.com/rapidodb/rapidodb/pkg/types"
)

// StrategyType identifies a compaction strategy.
type StrategyType string

const (
	// StrategyLeveled uses level-based compaction.
	// Files are organized into levels, with each level ~10x larger than previous.
	// Good for: General workloads, balanced read/write performance.
	StrategyLeveled StrategyType = "leveled"

	// StrategyTiered uses size-tiered compaction.
	// Files of similar size are grouped and merged together.
	// Good for: Write-heavy workloads, time-series data.
	StrategyTiered StrategyType = "tiered"

	// StrategyFIFO uses first-in-first-out compaction.
	// Oldest files are simply deleted when space limit is reached.
	// Good for: TTL-based data, logs, metrics with retention policy.
	StrategyFIFO StrategyType = "fifo"
)

// Strategy defines the interface for compaction strategies.
type Strategy interface {
	// Name returns the strategy name.
	Name() StrategyType

	// PickCompaction selects files for the next compaction.
	// Returns nil if no compaction is needed.
	PickCompaction(levels *LevelManager) *Task

	// ShouldTriggerCompaction checks if compaction should run.
	ShouldTriggerCompaction(levels *LevelManager) bool

	// ShouldStallWrites checks if writes should be stalled.
	ShouldStallWrites(levels *LevelManager) bool
}

// Task represents a compaction job to execute.
type Task struct {
	// Level is the source level being compacted from
	Level int

	// TargetLevel is the destination level
	TargetLevel int

	// Inputs are files to compact from Level
	Inputs []*FileMetadata

	// Overlapping are files from TargetLevel that overlap with Inputs
	Overlapping []*FileMetadata

	// Priority determines scheduling order (higher = more urgent)
	Priority float64

	// IsManual indicates if this was triggered manually
	IsManual bool
}

// FileMetadata contains metadata about an SSTable file.
type FileMetadata struct {
	FileNum  uint64 // Unique file identifier
	Level    int    // Level in the LSM tree (0-6)
	Size     int64  // File size in bytes
	MinKey   []byte // Smallest key in file
	MaxKey   []byte // Largest key in file
	MinSeq   uint64 // Smallest sequence number
	MaxSeq   uint64 // Largest sequence number
	NumKeys  int64  // Number of keys (including tombstones)
	Creating bool   // True if file is being created (not yet visible)
}

// Clone creates a deep copy of FileMetadata.
func (f *FileMetadata) Clone() *FileMetadata {
	clone := *f
	clone.MinKey = append([]byte{}, f.MinKey...)
	clone.MaxKey = append([]byte{}, f.MaxKey...)
	return &clone
}

// LevelStats contains statistics for a single level.
type LevelStats struct {
	Level      int
	NumFiles   int
	Size       int64
	TargetSize int64
	Score      float64 // Size/TargetSize for L1+, NumFiles/Trigger for L0
}

// Stats contains overall compaction statistics.
type Stats struct {
	CompactionsRun int64
	BytesRead      int64
	BytesWritten   int64
	FilesRead      int64
	FilesWritten   int64
}

// Config holds compaction configuration.
type Config struct {
	// Strategy is the compaction strategy to use
	Strategy StrategyType

	// MaxBackgroundCompactions is the max concurrent compaction jobs
	MaxBackgroundCompactions int

	// Leveled compaction settings
	NumLevels           int
	L0CompactionTrigger int
	L0StopWritesTrigger int
	BaseLevelSize       int64
	LevelSizeMultiplier float64
	TargetFileSizeBase  int64

	// Tiered compaction settings (Step 9)
	MinMergeWidth      int
	MaxMergeWidth      int
	SizeRatio          float64
	MaxSizeAmplification float64

	// FIFO compaction settings (Step 10)
	MaxTableFilesSize int64
	TTLSeconds        int64
}

// DefaultConfig returns default compaction configuration.
func DefaultConfig() Config {
	return Config{
		Strategy:                 StrategyLeveled,
		MaxBackgroundCompactions: 1,
		NumLevels:                7,
		L0CompactionTrigger:      4,
		L0StopWritesTrigger:      12,
		BaseLevelSize:            64 * 1024 * 1024, // 64MB
		LevelSizeMultiplier:      10,
		TargetFileSizeBase:       4 * 1024 * 1024, // 4MB
		MinMergeWidth:            4,
		MaxMergeWidth:            32,
		SizeRatio:                1.0,
		MaxSizeAmplification:     200,
		MaxTableFilesSize:        1024 * 1024 * 1024, // 1GB
		TTLSeconds:               0,                   // No TTL by default
	}
}

// Callbacks provides hooks for the compaction system.
type Callbacks struct {
	// AllocateFileNum allocates a new unique file number
	AllocateFileNum func() uint64

	// OnCompactionComplete is called when a compaction finishes
	OnCompactionComplete func(stats Stats)
}

// InputIterator wraps an SSTable iterator with metadata.
type InputIterator struct {
	Iterator *sstable.Iterator
	Level    int
	FileNum  uint64
}

// OutputWriter provides methods to write compaction output.
type OutputWriter interface {
	// Add writes an entry to the current output file
	Add(entry *types.Entry) error

	// ShouldSplit returns true if a new output file should be started
	ShouldSplit(currentSize int64, key []byte) bool

	// FinishFile finishes the current output file
	FinishFile() (*FileMetadata, error)

	// StartFile starts a new output file
	StartFile() error

	// Abort aborts the current output
	Abort()
}
