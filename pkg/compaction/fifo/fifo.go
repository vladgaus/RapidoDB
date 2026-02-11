// Package fifo implements first-in-first-out compaction strategy.
//
// FIFO compaction is the simplest strategy - it simply deletes the oldest
// files when the total size exceeds a threshold. No merging is performed,
// which means zero write amplification for compaction.
//
// How it works:
//
//	┌─────────────────────────────────────────────────────────┐
//	│              Files ordered by age                       │
//	├─────────────────────────────────────────────────────────┤
//	│  [oldest] ─────────────────────────────────> [newest]   │
//	│  file1    file2    file3    file4    file5    file6     │
//	│    ↑                                                    │
//	│    └── Delete when total size > MaxTableFilesSize       │
//	│        or when file age > TTL                           │
//	└─────────────────────────────────────────────────────────┘
//
// Deletion triggers:
//  1. Total size exceeds MaxTableFilesSize
//  2. File age exceeds TTLSeconds (if TTL is enabled)
//
// Characteristics:
//   - ZERO write amplification (no merging!)
//   - Fixed, predictable space usage
//   - Data expires by age, NOT by key
//   - Delete operations don't actually remove data (tombstones age out)
//   - Extremely fast writes
//
// Perfect for:
//   - Time-series data (metrics, logs, events)
//   - Data with natural expiration (sessions, caches)
//   - High-throughput append-only workloads
//   - IoT sensor data with retention policies
//
// NOT suitable for:
//   - Data that needs updates/deletes to take immediate effect
//   - Data that should persist indefinitely
//   - Workloads where you need to delete specific keys
//   - When space efficiency for updated keys matters
//
// IMPORTANT: Delete() writes tombstones but old data remains until aged out!
package fifo

import (
	"sort"
	"time"

	"github.com/vladgaus/RapidoDB/pkg/compaction"
)

// Strategy implements FIFO compaction.
type Strategy struct {
	config Config
}

// Config configures FIFO compaction.
type Config struct {
	// MaxTableFilesSize is the maximum total size of all SSTable files.
	// When exceeded, oldest files are deleted.
	// Default: 1GB
	MaxTableFilesSize int64

	// TTLSeconds is the time-to-live for data in seconds.
	// Files older than TTL are deleted regardless of size.
	// 0 = no TTL (size-based deletion only)
	// Default: 0
	TTLSeconds int64

	// MaxFilesToDeletePerCycle limits deletions per compaction cycle.
	// Prevents deleting too much at once.
	// Default: 10
	MaxFilesToDeletePerCycle int

	// L0StopWritesTrigger stalls writes when this many total files exist.
	// Default: 100
	L0StopWritesTrigger int
}

// DefaultConfig returns default FIFO compaction configuration.
func DefaultConfig() Config {
	return Config{
		MaxTableFilesSize:        1024 * 1024 * 1024, // 1GB
		TTLSeconds:               0,                  // No TTL by default
		MaxFilesToDeletePerCycle: 10,
		L0StopWritesTrigger:      100,
	}
}

// New creates a new FIFO compaction strategy.
func New(config Config) *Strategy {
	if config.MaxTableFilesSize <= 0 {
		config.MaxTableFilesSize = 1024 * 1024 * 1024
	}
	if config.MaxFilesToDeletePerCycle <= 0 {
		config.MaxFilesToDeletePerCycle = 10
	}
	if config.L0StopWritesTrigger <= 0 {
		config.L0StopWritesTrigger = 100
	}

	return &Strategy{config: config}
}

// Name returns the strategy name.
func (s *Strategy) Name() compaction.StrategyType {
	return compaction.StrategyFIFO
}

// PickCompaction selects files for deletion.
// Note: In FIFO mode, "compaction" means deletion, not merging.
// The returned task has TargetLevel = -1 to indicate deletion.
func (s *Strategy) PickCompaction(levels *compaction.LevelManager) *compaction.Task {
	// First check for TTL-expired files
	if task := s.pickTTLExpiredFiles(levels); task != nil {
		return task
	}

	// Then check for size-based deletion
	if task := s.pickSizeBasedDeletion(levels); task != nil {
		return task
	}

	return nil
}

// pickTTLExpiredFiles finds files that have exceeded their TTL.
func (s *Strategy) pickTTLExpiredFiles(levels *compaction.LevelManager) *compaction.Task {
	if s.config.TTLSeconds <= 0 {
		return nil // TTL not enabled
	}

	allFiles := s.getAllFilesSortedByAge(levels)
	if len(allFiles) == 0 {
		return nil
	}

	// Calculate expiration threshold
	// We use file number as a proxy for creation time (lower = older)
	// In production, you'd store actual timestamps
	now := time.Now().Unix()
	cutoffSeq := uint64(now - s.config.TTLSeconds)

	expiredFiles := make([]*compaction.FileMetadata, 0, s.config.MaxFilesToDeletePerCycle)
	for _, f := range allFiles {
		// Use MaxSeq as proxy for file creation time
		if f.MaxSeq > 0 && f.MaxSeq < cutoffSeq {
			expiredFiles = append(expiredFiles, f)
			if len(expiredFiles) >= s.config.MaxFilesToDeletePerCycle {
				break
			}
		}
	}

	if len(expiredFiles) == 0 {
		return nil
	}

	return &compaction.Task{
		Level:       0,
		TargetLevel: -1, // -1 indicates deletion, not compaction
		Inputs:      expiredFiles,
		Overlapping: nil,
		Priority:    float64(len(expiredFiles)),
	}
}

// pickSizeBasedDeletion finds oldest files to delete when over size limit.
func (s *Strategy) pickSizeBasedDeletion(levels *compaction.LevelManager) *compaction.Task {
	totalSize := levels.TotalSize()
	if totalSize <= s.config.MaxTableFilesSize {
		return nil // Under the limit
	}

	allFiles := s.getAllFilesSortedByAge(levels)
	if len(allFiles) == 0 {
		return nil
	}

	// Calculate how much we need to delete
	excessSize := totalSize - s.config.MaxTableFilesSize

	filesToDelete := make([]*compaction.FileMetadata, 0, s.config.MaxFilesToDeletePerCycle)
	var deletedSize int64

	for _, f := range allFiles {
		if deletedSize >= excessSize {
			break
		}
		if len(filesToDelete) >= s.config.MaxFilesToDeletePerCycle {
			break
		}
		filesToDelete = append(filesToDelete, f)
		deletedSize += f.Size
	}

	if len(filesToDelete) == 0 {
		return nil
	}

	return &compaction.Task{
		Level:       0,
		TargetLevel: -1, // -1 indicates deletion
		Inputs:      filesToDelete,
		Overlapping: nil,
		Priority:    float64(totalSize) / float64(s.config.MaxTableFilesSize),
	}
}

// getAllFilesSortedByAge returns all files sorted oldest first.
func (s *Strategy) getAllFilesSortedByAge(levels *compaction.LevelManager) []*compaction.FileMetadata {
	// Count total files first
	totalFiles := 0
	for level := 0; level < compaction.MaxLevels; level++ {
		totalFiles += levels.NumFiles(level)
	}

	allFiles := make([]*compaction.FileMetadata, 0, totalFiles)
	for level := 0; level < compaction.MaxLevels; level++ {
		files := levels.LevelFiles(level)
		allFiles = append(allFiles, files...)
	}

	// Sort by file number (lower = older)
	sort.Slice(allFiles, func(i, j int) bool {
		return allFiles[i].FileNum < allFiles[j].FileNum
	})

	return allFiles
}

// ShouldTriggerCompaction checks if deletion is needed.
func (s *Strategy) ShouldTriggerCompaction(levels *compaction.LevelManager) bool {
	// Check size limit
	if levels.TotalSize() > s.config.MaxTableFilesSize {
		return true
	}

	// Check TTL if enabled
	if s.config.TTLSeconds > 0 {
		allFiles := s.getAllFilesSortedByAge(levels)
		now := time.Now().Unix()
		cutoffSeq := uint64(now - s.config.TTLSeconds)

		for _, f := range allFiles {
			if f.MaxSeq > 0 && f.MaxSeq < cutoffSeq {
				return true
			}
		}
	}

	return false
}

// ShouldStallWrites checks if writes should be stalled.
func (s *Strategy) ShouldStallWrites(levels *compaction.LevelManager) bool {
	totalFiles := 0
	for level := 0; level < compaction.MaxLevels; level++ {
		totalFiles += levels.NumFiles(level)
	}
	return totalFiles >= s.config.L0StopWritesTrigger
}

// Config returns the strategy configuration.
func (s *Strategy) Config() Config {
	return s.config
}

// Stats contains FIFO-specific statistics.
type Stats struct {
	TotalFiles         int
	TotalSize          int64
	MaxSize            int64
	UsagePercent       float64
	OldestFileNum      uint64
	NewestFileNum      uint64
	FilesOverSizeLimit int
	FilesExpiredByTTL  int
	TTLEnabled         bool
	TTLSeconds         int64
}

// GetStats returns current FIFO statistics.
func (s *Strategy) GetStats(levels *compaction.LevelManager) Stats {
	allFiles := s.getAllFilesSortedByAge(levels)

	stats := Stats{
		TotalFiles: len(allFiles),
		MaxSize:    s.config.MaxTableFilesSize,
		TTLEnabled: s.config.TTLSeconds > 0,
		TTLSeconds: s.config.TTLSeconds,
	}

	if len(allFiles) == 0 {
		return stats
	}

	// Calculate totals and find oldest/newest
	stats.OldestFileNum = allFiles[0].FileNum
	stats.NewestFileNum = allFiles[0].FileNum

	for _, f := range allFiles {
		stats.TotalSize += f.Size
		if f.FileNum < stats.OldestFileNum {
			stats.OldestFileNum = f.FileNum
		}
		if f.FileNum > stats.NewestFileNum {
			stats.NewestFileNum = f.FileNum
		}
	}

	if stats.MaxSize > 0 {
		stats.UsagePercent = float64(stats.TotalSize) / float64(stats.MaxSize) * 100
	}

	// Count files that would be deleted for size
	if stats.TotalSize > stats.MaxSize {
		excessSize := stats.TotalSize - stats.MaxSize
		var counted int64
		for _, f := range allFiles {
			if counted >= excessSize {
				break
			}
			stats.FilesOverSizeLimit++
			counted += f.Size
		}
	}

	// Count TTL-expired files
	if s.config.TTLSeconds > 0 {
		now := time.Now().Unix()
		cutoffSeq := uint64(now - s.config.TTLSeconds)
		for _, f := range allFiles {
			if f.MaxSeq > 0 && f.MaxSeq < cutoffSeq {
				stats.FilesExpiredByTTL++
			}
		}
	}

	return stats
}

// IsDeletionTask returns true if the task is a deletion (not merge).
// The compactor uses this to handle FIFO tasks specially.
func IsDeletionTask(task *compaction.Task) bool {
	return task != nil && task.TargetLevel == -1
}
