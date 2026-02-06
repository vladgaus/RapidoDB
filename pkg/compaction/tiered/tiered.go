// Package tiered implements size-tiered compaction strategy.
//
// Size-tiered compaction groups SSTables of similar size together
// and merges them when enough files accumulate in a size bucket.
//
// How it works:
//
//	┌─────────────────────────────────────────────────────────┐
//	│                   Size Buckets                          │
//	├─────────────────────────────────────────────────────────┤
//	│  Bucket 0 (smallest): [file1, file2, file3, file4]     │
//	│         ↓ merge when >= MinMergeWidth files             │
//	│  Bucket 1 (medium):   [merged1, file5]                 │
//	│         ↓ merge when >= MinMergeWidth files             │
//	│  Bucket 2 (larger):   [merged2]                        │
//	│         ↓                                               │
//	│  Bucket N (largest):  [...]                            │
//	└─────────────────────────────────────────────────────────┘
//
// Files are placed in buckets based on their size:
//   - Bucket 0: size < BaseBucketSize
//   - Bucket 1: size < BaseBucketSize * SizeRatio
//   - Bucket 2: size < BaseBucketSize * SizeRatio^2
//   - etc.
//
// Characteristics:
//   - Lower write amplification than leveled (each byte written ~SizeRatio times)
//   - Higher space amplification (multiple versions may coexist)
//   - Files within a bucket may have overlapping key ranges
//   - Good for write-heavy workloads
//
// Trade-offs vs Leveled:
//   - Better: Write throughput, write latency
//   - Worse: Read latency (more files to check), space usage
package tiered

import (
	"sort"

	"github.com/rapidodb/rapidodb/pkg/compaction"
)

// Strategy implements size-tiered compaction.
type Strategy struct {
	config Config
}

// Config configures tiered compaction.
type Config struct {
	// MinMergeWidth is minimum number of files to merge (default: 4)
	MinMergeWidth int

	// MaxMergeWidth is maximum number of files to merge at once (default: 32)
	MaxMergeWidth int

	// SizeRatio determines bucket boundaries (default: 4.0)
	// Files within SizeRatio of each other are in the same bucket
	SizeRatio float64

	// BaseBucketSize is the max size for the smallest bucket (default: 4MB)
	BaseBucketSize int64

	// MaxBuckets is the maximum number of size buckets (default: 10)
	MaxBuckets int

	// MaxSizeAmplificationPercent limits total space usage (default: 200)
	// If (total_size / live_data_size) > this, force compaction
	MaxSizeAmplificationPercent float64

	// L0StopWritesTrigger stalls writes when L0 has this many files
	L0StopWritesTrigger int
}

// DefaultConfig returns default tiered compaction configuration.
func DefaultConfig() Config {
	return Config{
		MinMergeWidth:               4,
		MaxMergeWidth:               32,
		SizeRatio:                   4.0,
		BaseBucketSize:              4 * 1024 * 1024, // 4MB
		MaxBuckets:                  10,
		MaxSizeAmplificationPercent: 200,
		L0StopWritesTrigger:         20,
	}
}

// New creates a new tiered compaction strategy.
func New(config Config) *Strategy {
	if config.MinMergeWidth <= 0 {
		config.MinMergeWidth = 4
	}
	if config.MaxMergeWidth <= 0 {
		config.MaxMergeWidth = 32
	}
	if config.SizeRatio <= 1.0 {
		config.SizeRatio = 4.0
	}
	if config.BaseBucketSize <= 0 {
		config.BaseBucketSize = 4 * 1024 * 1024
	}
	if config.MaxBuckets <= 0 {
		config.MaxBuckets = 10
	}
	if config.MaxSizeAmplificationPercent <= 0 {
		config.MaxSizeAmplificationPercent = 200
	}
	if config.L0StopWritesTrigger <= 0 {
		config.L0StopWritesTrigger = 20
	}

	return &Strategy{config: config}
}

// Name returns the strategy name.
func (s *Strategy) Name() compaction.StrategyType {
	return compaction.StrategyTiered
}

// sizeBucket represents a group of similarly-sized files.
type sizeBucket struct {
	bucketID int
	minSize  int64
	maxSize  int64
	files    []*compaction.FileMetadata
}

// PickCompaction selects files for the next compaction.
// It finds buckets with enough files and picks one to compact.
func (s *Strategy) PickCompaction(levels *compaction.LevelManager) *compaction.Task {
	// Collect all files from all levels into buckets by size
	buckets := s.buildSizeBuckets(levels)

	// Find the best bucket to compact
	var bestBucket *sizeBucket
	var bestScore float64

	for _, bucket := range buckets {
		if len(bucket.files) < s.config.MinMergeWidth {
			continue
		}

		// Score: more files = higher priority
		// Also consider bucket level (smaller buckets = higher priority)
		score := float64(len(bucket.files)) / float64(s.config.MinMergeWidth)
		score *= float64(s.config.MaxBuckets-bucket.bucketID) / float64(s.config.MaxBuckets)

		if score > bestScore {
			bestScore = score
			bestBucket = bucket
		}
	}

	if bestBucket == nil {
		return nil
	}

	// Select files to compact (up to MaxMergeWidth)
	filesToCompact := bestBucket.files
	if len(filesToCompact) > s.config.MaxMergeWidth {
		// Sort by size and take smallest files
		sort.Slice(filesToCompact, func(i, j int) bool {
			return filesToCompact[i].Size < filesToCompact[j].Size
		})
		filesToCompact = filesToCompact[:s.config.MaxMergeWidth]
	}

	// In tiered compaction, we merge files and output to the same "level"
	// We use level 0 for all files since there's no strict level structure
	return &compaction.Task{
		Level:       0,
		TargetLevel: 0, // Output stays at level 0
		Inputs:      filesToCompact,
		Overlapping: nil, // No separate overlapping concept in tiered
		Priority:    bestScore,
	}
}

// buildSizeBuckets groups all files by their size into buckets.
func (s *Strategy) buildSizeBuckets(levels *compaction.LevelManager) []*sizeBucket {
	buckets := make([]*sizeBucket, s.config.MaxBuckets)

	// Initialize buckets with size ranges
	for i := 0; i < s.config.MaxBuckets; i++ {
		minSize := int64(0)
		if i > 0 {
			minSize = s.bucketMaxSize(i - 1)
		}
		buckets[i] = &sizeBucket{
			bucketID: i,
			minSize:  minSize,
			maxSize:  s.bucketMaxSize(i),
			files:    make([]*compaction.FileMetadata, 0),
		}
	}

	// Collect files from all levels
	for level := 0; level < compaction.MaxLevels; level++ {
		files := levels.LevelFiles(level)
		for _, f := range files {
			bucketID := s.getBucketID(f.Size)
			if bucketID < s.config.MaxBuckets {
				buckets[bucketID].files = append(buckets[bucketID].files, f)
			}
		}
	}

	return buckets
}

// bucketMaxSize returns the maximum file size for a bucket.
func (s *Strategy) bucketMaxSize(bucketID int) int64 {
	size := float64(s.config.BaseBucketSize)
	for i := 0; i < bucketID; i++ {
		size *= s.config.SizeRatio
	}
	return int64(size)
}

// getBucketID returns which bucket a file of given size belongs to.
func (s *Strategy) getBucketID(fileSize int64) int {
	if fileSize <= 0 {
		return 0
	}

	bucket := 0
	maxSize := s.config.BaseBucketSize
	for bucket < s.config.MaxBuckets-1 && fileSize > maxSize {
		bucket++
		maxSize = int64(float64(maxSize) * s.config.SizeRatio)
	}
	return bucket
}

// ShouldTriggerCompaction checks if compaction should run.
func (s *Strategy) ShouldTriggerCompaction(levels *compaction.LevelManager) bool {
	buckets := s.buildSizeBuckets(levels)

	// Check if any bucket has enough files
	for _, bucket := range buckets {
		if len(bucket.files) >= s.config.MinMergeWidth {
			return true
		}
	}

	// Check space amplification
	totalSize := levels.TotalSize()
	if totalSize > 0 {
		// Rough estimate: assume 50% of data is "live"
		// In production, you'd track this more accurately
		estimatedLiveSize := totalSize / 2
		if estimatedLiveSize > 0 {
			amplification := float64(totalSize) / float64(estimatedLiveSize) * 100
			if amplification > s.config.MaxSizeAmplificationPercent {
				return true
			}
		}
	}

	return false
}

// ShouldStallWrites checks if writes should be stalled.
func (s *Strategy) ShouldStallWrites(levels *compaction.LevelManager) bool {
	// Count total files across all levels
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

// BucketStats returns statistics about file distribution across buckets.
type BucketStats struct {
	BucketID  int
	MinSize   int64
	MaxSize   int64
	FileCount int
	TotalSize int64
}

// GetBucketStats returns statistics for analysis/debugging.
func (s *Strategy) GetBucketStats(levels *compaction.LevelManager) []BucketStats {
	buckets := s.buildSizeBuckets(levels)
	stats := make([]BucketStats, len(buckets))

	for i, b := range buckets {
		var totalSize int64
		for _, f := range b.files {
			totalSize += f.Size
		}
		stats[i] = BucketStats{
			BucketID:  b.bucketID,
			MinSize:   b.minSize,
			MaxSize:   b.maxSize,
			FileCount: len(b.files),
			TotalSize: totalSize,
		}
	}

	return stats
}
