// Package leveled implements level-based compaction strategy.
//
// Leveled compaction organizes SSTables into multiple levels (L0-L6).
// Each level (except L0) contains non-overlapping files sorted by key.
// Files are compacted from level N to level N+1 when level N exceeds
// its size target.
//
// Characteristics:
// - Predictable read amplification (at most one file per level for point lookups)
// - Higher write amplification (files may be rewritten multiple times)
// - Good space amplification (old versions are cleaned up)
//
// This is similar to LevelDB/RocksDB's default compaction strategy.
//
// Level sizes (with 10x multiplier):
//
//	L0: (by file count, not size)
//	L1: 64MB
//	L2: 640MB
//	L3: 6.4GB
//	L4: 64GB
//	L5: 640GB
//	L6: 6.4TB
package leveled

import (
	"bytes"

	"github.com/vladgaus/RapidoDB/pkg/compaction"
)

// Strategy implements leveled compaction.
type Strategy struct {
	config Config
}

// Config configures leveled compaction.
type Config struct {
	// NumLevels is the number of levels (default: 7)
	NumLevels int

	// L0CompactionTrigger triggers compaction when L0 has this many files
	L0CompactionTrigger int

	// L0StopWritesTrigger stalls writes when L0 has this many files
	L0StopWritesTrigger int

	// BaseLevelSize is the target size for L1 in bytes
	BaseLevelSize int64

	// LevelSizeMultiplier is the size ratio between adjacent levels
	LevelSizeMultiplier float64

	// TargetFileSizeBase is the target size for output files at L1
	TargetFileSizeBase int64

	// TargetFileSizeMultiplier increases file size for higher levels
	TargetFileSizeMultiplier float64
}

// DefaultConfig returns default leveled compaction configuration.
func DefaultConfig() Config {
	return Config{
		NumLevels:                7,
		L0CompactionTrigger:      4,
		L0StopWritesTrigger:      12,
		BaseLevelSize:            64 * 1024 * 1024, // 64MB
		LevelSizeMultiplier:      10,
		TargetFileSizeBase:       4 * 1024 * 1024, // 4MB
		TargetFileSizeMultiplier: 1,
	}
}

// New creates a new leveled compaction strategy.
func New(config Config) *Strategy {
	if config.NumLevels <= 0 {
		config.NumLevels = 7
	}
	if config.L0CompactionTrigger <= 0 {
		config.L0CompactionTrigger = 4
	}
	if config.L0StopWritesTrigger <= 0 {
		config.L0StopWritesTrigger = 12
	}
	if config.BaseLevelSize <= 0 {
		config.BaseLevelSize = 64 * 1024 * 1024
	}
	if config.LevelSizeMultiplier <= 0 {
		config.LevelSizeMultiplier = 10
	}
	if config.TargetFileSizeBase <= 0 {
		config.TargetFileSizeBase = 4 * 1024 * 1024
	}
	if config.TargetFileSizeMultiplier <= 0 {
		config.TargetFileSizeMultiplier = 1
	}

	return &Strategy{config: config}
}

// Name returns the strategy name.
func (s *Strategy) Name() compaction.StrategyType {
	return compaction.StrategyLeveled
}

// PickCompaction selects the next compaction to perform.
// Returns nil if no compaction is needed.
func (s *Strategy) PickCompaction(levels *compaction.LevelManager) *compaction.Task {
	// Priority 1: Compact L0 if it has too many files
	if task := s.pickL0Compaction(levels); task != nil {
		return task
	}

	// Priority 2: Compact levels that exceed their size target
	if task := s.pickLevelCompaction(levels); task != nil {
		return task
	}

	return nil
}

// pickL0Compaction picks an L0 compaction if needed.
func (s *Strategy) pickL0Compaction(levels *compaction.LevelManager) *compaction.Task {
	l0Files := levels.LevelFiles(0)
	if len(l0Files) < s.config.L0CompactionTrigger {
		return nil
	}

	// Find the key range covered by all L0 files
	var minKey, maxKey []byte
	for _, f := range l0Files {
		if minKey == nil || bytes.Compare(f.MinKey, minKey) < 0 {
			minKey = f.MinKey
		}
		if maxKey == nil || bytes.Compare(f.MaxKey, maxKey) > 0 {
			maxKey = f.MaxKey
		}
	}

	// Find overlapping L1 files
	l1Overlapping := levels.OverlappingFiles(1, minKey, maxKey)

	return &compaction.Task{
		Level:       0,
		TargetLevel: 1,
		Inputs:      l0Files,
		Overlapping: l1Overlapping,
		Priority:    float64(len(l0Files)) / float64(s.config.L0CompactionTrigger),
	}
}

// pickLevelCompaction picks a level compaction based on size scores.
func (s *Strategy) pickLevelCompaction(levels *compaction.LevelManager) *compaction.Task {
	bestLevel := -1
	var bestScore float64

	// Check each level (L1 to L5, compacting to next level)
	for level := 1; level < s.config.NumLevels-1; level++ {
		size := levels.LevelSize(level)
		target := s.targetLevelSize(level)
		if target == 0 {
			continue
		}

		score := float64(size) / float64(target)
		if score > 1.0 && score > bestScore {
			bestLevel = level
			bestScore = score
		}
	}

	if bestLevel < 0 {
		return nil
	}

	// Pick a file from the level to compact
	files := levels.LevelFiles(bestLevel)
	if len(files) == 0 {
		return nil
	}

	// Pick the file with the largest size (or oldest, or round-robin)
	// For simplicity, pick the first file
	inputFile := files[0]

	// Find overlapping files in the next level
	overlapping := levels.OverlappingFiles(bestLevel+1, inputFile.MinKey, inputFile.MaxKey)

	return &compaction.Task{
		Level:       bestLevel,
		TargetLevel: bestLevel + 1,
		Inputs:      []*compaction.FileMetadata{inputFile},
		Overlapping: overlapping,
		Priority:    bestScore,
	}
}

// targetLevelSize returns the target size for a level.
func (s *Strategy) targetLevelSize(level int) int64 {
	if level == 0 {
		return 0 // L0 uses file count, not size
	}
	size := s.config.BaseLevelSize
	for i := 1; i < level; i++ {
		size = int64(float64(size) * s.config.LevelSizeMultiplier)
	}
	return size
}

// TargetFileSize returns the target output file size for a level.
func (s *Strategy) TargetFileSize(level int) int64 {
	size := s.config.TargetFileSizeBase
	for i := 1; i < level; i++ {
		size = int64(float64(size) * s.config.TargetFileSizeMultiplier)
	}
	return size
}

// ShouldTriggerCompaction checks if compaction should run.
func (s *Strategy) ShouldTriggerCompaction(levels *compaction.LevelManager) bool {
	// Check L0 file count
	if levels.NumFiles(0) >= s.config.L0CompactionTrigger {
		return true
	}

	// Check level sizes
	for level := 1; level < s.config.NumLevels-1; level++ {
		size := levels.LevelSize(level)
		target := s.targetLevelSize(level)
		if target > 0 && size > target {
			return true
		}
	}

	return false
}

// ShouldStallWrites checks if writes should be stalled.
func (s *Strategy) ShouldStallWrites(levels *compaction.LevelManager) bool {
	return levels.NumFiles(0) >= s.config.L0StopWritesTrigger
}

// Config returns the strategy configuration.
func (s *Strategy) Config() Config {
	return s.config
}
