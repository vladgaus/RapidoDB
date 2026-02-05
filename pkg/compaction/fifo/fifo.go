// Package fifo implements first-in-first-out compaction strategy.
//
// FIFO compaction simply deletes the oldest files when the total
// size exceeds a threshold. No actual merging is performed.
//
// Characteristics:
// - Very low write amplification (no merging)
// - Predictable space usage (capped by max size)
// - Data is deleted by age, not by key
//
// Good for:
// - Time-series data with TTL
// - Logs and metrics with retention policy
// - Data that naturally expires
//
// NOT good for:
// - Data that needs updates/deletes
// - Data that should persist indefinitely
//
// TODO: Implement in Step 10
package fifo

import (
	"github.com/rapidodb/rapidodb/pkg/compaction"
)

// Strategy implements FIFO compaction.
type Strategy struct {
	config Config
}

// Config configures FIFO compaction.
type Config struct {
	// MaxTableFilesSize is the max total size of all SSTable files
	MaxTableFilesSize int64

	// TTLSeconds is the time-to-live for data (0 = no TTL)
	TTLSeconds int64
}

// DefaultConfig returns default FIFO compaction configuration.
func DefaultConfig() Config {
	return Config{
		MaxTableFilesSize: 1024 * 1024 * 1024, // 1GB
		TTLSeconds:        0,                  // No TTL
	}
}

// New creates a new FIFO compaction strategy.
func New(config Config) *Strategy {
	// TODO: Implement in Step 10
	return &Strategy{config: config}
}

// Name returns the strategy name.
func (s *Strategy) Name() compaction.StrategyType {
	return compaction.StrategyFIFO
}

// PickCompaction selects files for the next compaction.
func (s *Strategy) PickCompaction(levels *compaction.LevelManager) *compaction.Task {
	// TODO: Implement in Step 10
	return nil
}

// ShouldTriggerCompaction checks if compaction should run.
func (s *Strategy) ShouldTriggerCompaction(levels *compaction.LevelManager) bool {
	// TODO: Implement in Step 10
	return false
}

// ShouldStallWrites checks if writes should be stalled.
func (s *Strategy) ShouldStallWrites(levels *compaction.LevelManager) bool {
	// TODO: Implement in Step 10
	return false
}
