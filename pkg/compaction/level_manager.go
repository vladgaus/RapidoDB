package compaction

import (
	"bytes"
	"sort"
	"sync"

	"github.com/rapidodb/rapidodb/pkg/sstable"
)

// MaxLevels is the maximum number of levels in the LSM tree.
const MaxLevels = 7

// LevelManager manages SSTable files across all levels.
//
// Level 0 (L0): Files from MemTable flushes, may overlap in key ranges.
// Level 1+ (L1-L6): Non-overlapping files, sorted by key range.
//
// Compaction moves data from L0 → L1 → L2 → ... → L6
// Each level is ~10x larger than the previous (configurable).
type LevelManager struct {
	mu sync.RWMutex

	// Files at each level
	// levels[0] = L0 files (newest first, may overlap)
	// levels[1+] = L1+ files (sorted by min key, non-overlapping)
	levels [MaxLevels][]*FileMetadata

	// Open readers for each file (keyed by file number)
	readers map[uint64]*sstable.Reader

	// Configuration
	baseLevelSize int64   // Target size for L1 (default: 64MB)
	levelRatio    float64 // Size multiplier per level (default: 10)

	// SSTable directory path
	sstDir string

	// SSTable options for creating new files
	blockSize       int
	bloomBitsPerKey int
}

// NewLevelManager creates a new level manager.
func NewLevelManager(sstDir string, blockSize, bloomBitsPerKey int) *LevelManager {
	return &LevelManager{
		readers:         make(map[uint64]*sstable.Reader),
		baseLevelSize:   64 * 1024 * 1024, // 64MB
		levelRatio:      10,
		sstDir:          sstDir,
		blockSize:       blockSize,
		bloomBitsPerKey: bloomBitsPerKey,
	}
}

// SetLevelTargets updates the level size targets.
func (lm *LevelManager) SetLevelTargets(baseLevelSize int64, levelRatio float64) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lm.baseLevelSize = baseLevelSize
	lm.levelRatio = levelRatio
}

// Overlaps returns true if this file's key range overlaps with [start, end].
func (f *FileMetadata) Overlaps(start, end []byte) bool {
	// If start is after MaxKey, no overlap
	if start != nil && bytes.Compare(start, f.MaxKey) > 0 {
		return false
	}
	// If end is before MinKey, no overlap
	if end != nil && bytes.Compare(end, f.MinKey) < 0 {
		return false
	}
	return true
}

// ContainsKey returns true if the key might be in this file.
func (f *FileMetadata) ContainsKey(key []byte) bool {
	return bytes.Compare(key, f.MinKey) >= 0 && bytes.Compare(key, f.MaxKey) <= 0
}

// AddFile adds a file to a level.
func (lm *LevelManager) AddFile(level int, meta *FileMetadata, reader *sstable.Reader) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	meta.Level = level
	lm.levels[level] = append(lm.levels[level], meta)
	lm.readers[meta.FileNum] = reader

	// Keep L1+ sorted by MinKey for efficient lookups
	if level > 0 {
		sort.Slice(lm.levels[level], func(i, j int) bool {
			return bytes.Compare(lm.levels[level][i].MinKey, lm.levels[level][j].MinKey) < 0
		})
	}
}

// AddL0File adds a file to L0 (prepend, newest first).
func (lm *LevelManager) AddL0File(meta *FileMetadata, reader *sstable.Reader) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	meta.Level = 0
	// Prepend to L0 (newest first)
	lm.levels[0] = append([]*FileMetadata{meta}, lm.levels[0]...)
	lm.readers[meta.FileNum] = reader
}

// RemoveFile removes a file from its level.
func (lm *LevelManager) RemoveFile(meta *FileMetadata) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	level := meta.Level
	for i, f := range lm.levels[level] {
		if f.FileNum == meta.FileNum {
			lm.levels[level] = append(lm.levels[level][:i], lm.levels[level][i+1:]...)
			break
		}
	}

	// Close and remove reader
	if reader, ok := lm.readers[meta.FileNum]; ok {
		_ = reader.Close()
		delete(lm.readers, meta.FileNum)
	}
}

// GetReader returns the reader for a file.
func (lm *LevelManager) GetReader(fileNum uint64) *sstable.Reader {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	return lm.readers[fileNum]
}

// LevelFiles returns files at a level (copy for safety).
func (lm *LevelManager) LevelFiles(level int) []*FileMetadata {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	result := make([]*FileMetadata, len(lm.levels[level]))
	copy(result, lm.levels[level])
	return result
}

// NumFiles returns the number of files at a level.
func (lm *LevelManager) NumFiles(level int) int {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	return len(lm.levels[level])
}

// LevelSize returns the total size of files at a level.
func (lm *LevelManager) LevelSize(level int) int64 {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	var size int64
	for _, f := range lm.levels[level] {
		size += f.Size
	}
	return size
}

// TotalSize returns the total size of all files.
func (lm *LevelManager) TotalSize() int64 {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	var size int64
	for level := 0; level < MaxLevels; level++ {
		for _, f := range lm.levels[level] {
			size += f.Size
		}
	}
	return size
}

// TargetLevelSize returns the target size for a level.
func (lm *LevelManager) TargetLevelSize(level int) int64 {
	if level == 0 {
		return 0 // L0 is triggered by file count, not size
	}
	size := lm.baseLevelSize
	for i := 1; i < level; i++ {
		size = int64(float64(size) * lm.levelRatio)
	}
	return size
}

// OverlappingFiles returns files in a level that overlap with [minKey, maxKey].
func (lm *LevelManager) OverlappingFiles(level int, minKey, maxKey []byte) []*FileMetadata {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	var result []*FileMetadata

	if level == 0 {
		// L0 files may all overlap, check each one
		for _, f := range lm.levels[0] {
			if f.Overlaps(minKey, maxKey) {
				result = append(result, f)
			}
		}
	} else {
		// L1+ files are sorted and non-overlapping
		// Binary search for start position
		files := lm.levels[level]
		start := sort.Search(len(files), func(i int) bool {
			return bytes.Compare(files[i].MaxKey, minKey) >= 0
		})

		for i := start; i < len(files); i++ {
			if maxKey != nil && bytes.Compare(files[i].MinKey, maxKey) > 0 {
				break
			}
			result = append(result, files[i])
		}
	}

	return result
}

// FilesForKey returns files that might contain the key.
func (lm *LevelManager) FilesForKey(key []byte) [][]*FileMetadata {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	result := make([][]*FileMetadata, MaxLevels)

	for level := 0; level < MaxLevels; level++ {
		if level == 0 {
			// Check all L0 files (they may overlap)
			for _, f := range lm.levels[0] {
				if f.ContainsKey(key) {
					result[0] = append(result[0], f)
				}
			}
		} else {
			// Binary search in L1+
			files := lm.levels[level]
			idx := sort.Search(len(files), func(i int) bool {
				return bytes.Compare(files[i].MaxKey, key) >= 0
			})
			if idx < len(files) && files[idx].ContainsKey(key) {
				result[level] = append(result[level], files[idx])
			}
		}
	}

	return result
}

// Get searches for a key across all levels.
// Returns the value and whether the key was found (including tombstones).
func (lm *LevelManager) Get(key []byte) ([]byte, bool, error) {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	// Search L0 first (newest to oldest, may have multiple files)
	for _, f := range lm.levels[0] {
		if !f.ContainsKey(key) {
			continue
		}
		reader := lm.readers[f.FileNum]
		if reader == nil {
			continue
		}
		entry, err := reader.Get(key)
		if err != nil {
			return nil, false, err
		}
		if entry != nil {
			if entry.IsDeleted() {
				return nil, true, nil // Found tombstone
			}
			return entry.Value, true, nil
		}
	}

	// Search L1+ (at most one file per level can contain the key)
	for level := 1; level < MaxLevels; level++ {
		files := lm.levels[level]
		if len(files) == 0 {
			continue
		}

		// Binary search for the file that might contain the key
		idx := sort.Search(len(files), func(i int) bool {
			return bytes.Compare(files[i].MaxKey, key) >= 0
		})

		if idx >= len(files) || !files[idx].ContainsKey(key) {
			continue
		}

		reader := lm.readers[files[idx].FileNum]
		if reader == nil {
			continue
		}

		entry, err := reader.Get(key)
		if err != nil {
			return nil, false, err
		}
		if entry != nil {
			if entry.IsDeleted() {
				return nil, true, nil // Found tombstone
			}
			return entry.Value, true, nil
		}
	}

	return nil, false, nil
}

// Close closes all readers.
func (lm *LevelManager) Close() error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	var lastErr error
	for _, reader := range lm.readers {
		if err := reader.Close(); err != nil {
			lastErr = err
		}
	}
	lm.readers = make(map[uint64]*sstable.Reader)

	for i := range lm.levels {
		lm.levels[i] = nil
	}

	return lastErr
}

// GetStats returns statistics for all levels.
func (lm *LevelManager) GetStats(l0Trigger int) []LevelStats {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	stats := make([]LevelStats, MaxLevels)
	for level := 0; level < MaxLevels; level++ {
		var size int64
		for _, f := range lm.levels[level] {
			size += f.Size
		}

		stats[level] = LevelStats{
			Level:      level,
			NumFiles:   len(lm.levels[level]),
			Size:       size,
			TargetSize: lm.TargetLevelSize(level),
		}

		if level == 0 {
			stats[level].Score = float64(len(lm.levels[level])) / float64(l0Trigger)
		} else if stats[level].TargetSize > 0 {
			stats[level].Score = float64(size) / float64(stats[level].TargetSize)
		}
	}

	return stats
}

// SSTDir returns the SSTable directory.
func (lm *LevelManager) SSTDir() string {
	return lm.sstDir
}

// BlockSize returns the block size for new SSTables.
func (lm *LevelManager) BlockSize() int {
	return lm.blockSize
}

// BloomBitsPerKey returns bloom filter bits per key.
func (lm *LevelManager) BloomBitsPerKey() int {
	return lm.bloomBitsPerKey
}
