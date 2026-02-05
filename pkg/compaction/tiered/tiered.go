// Package tiered implements size-tiered compaction strategy.
//
// Size-tiered compaction groups SSTables of similar size together
// and merges them when enough files accumulate.
//
// Characteristics:
// - Lower write amplification than leveled
// - Higher space amplification (may keep multiple versions longer)
// - Less predictable read performance (files may overlap)
//
// Good for:
// - Write-heavy workloads
// - Time-series data
// - Workloads with few updates/deletes
//
// TODO: Implement in Step 9
package tiered

import (
	"github.com/rapidodb/rapidodb/pkg/compaction"
)

// Strategy implements size-tiered compaction.
type Strategy struct {
	config Config
}

// Config configures tiered compaction.
type Config struct {
	// MinMergeWidth is minimum number of files to merge
	MinMergeWidth int

	// MaxMergeWidth is maximum number of files to merge
	MaxMergeWidth int

	// SizeRatio determines when files are "similar" size
	SizeRatio float64

	// MaxSizeAmplificationPercent limits space amplification
	MaxSizeAmplificationPercent float64
}

// DefaultConfig returns default tiered compaction configuration.
func DefaultConfig() Config {
	return Config{
		MinMergeWidth:               4,
		MaxMergeWidth:               32,
		SizeRatio:                   1.0,
		MaxSizeAmplificationPercent: 200,
	}
}

// New creates a new tiered compaction strategy.
func New(config Config) *Strategy {
	// TODO: Implement in Step 9
	return &Strategy{config: config}
}

// Name returns the strategy name.
func (s *Strategy) Name() compaction.StrategyType {
	return compaction.StrategyTiered
}

// PickCompaction selects files for the next compaction.
func (s *Strategy) PickCompaction(levels *compaction.LevelManager) *compaction.Task {
	// TODO: Implement in Step 9
	return nil
}

// ShouldTriggerCompaction checks if compaction should run.
func (s *Strategy) ShouldTriggerCompaction(levels *compaction.LevelManager) bool {
	// TODO: Implement in Step 9
	return false
}

// ShouldStallWrites checks if writes should be stalled.
func (s *Strategy) ShouldStallWrites(levels *compaction.LevelManager) bool {
	// TODO: Implement in Step 9
	return false
}
