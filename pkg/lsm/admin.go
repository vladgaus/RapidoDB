package lsm

import (
	"context"
	"fmt"
)

// ============================================================================
// Admin Operations for Admin API
// ============================================================================

// ForceFlush forces an immediate flush of the active MemTable.
// This is useful for ensuring data durability before backup.
func (e *Engine) ForceFlush() error {
	e.mu.Lock()

	if e.closed.Load() {
		e.mu.Unlock()
		return fmt.Errorf("engine is closed")
	}

	// Only flush if there's data
	if e.memTable == nil || e.memTable.EntryCount() == 0 {
		e.mu.Unlock()
		return nil
	}

	// Rotate to create immutable
	if err := e.rotateMemTable(); err != nil {
		e.mu.Unlock()
		return fmt.Errorf("failed to rotate memtable: %w", err)
	}
	e.mu.Unlock()

	// Wait for flush to complete by syncing
	return e.Sync()
}

// TriggerCompaction triggers a manual compaction.
func (e *Engine) TriggerCompaction() {
	if e.compactor != nil {
		e.compactor.TriggerCompaction()
	}
}

// GetSSTableInfo returns information about all SSTables.
func (e *Engine) GetSSTableInfo() []SSTableFileInfo {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.levels == nil {
		return nil
	}

	var result []SSTableFileInfo
	for level := 0; level <= 6; level++ {
		files := e.levels.LevelFiles(level)
		for _, f := range files {
			if f.Creating {
				continue
			}
			result = append(result, SSTableFileInfo{
				FileNum: f.FileNum,
				Level:   f.Level,
				Size:    f.Size,
				MinKey:  string(f.MinKey),
				MaxKey:  string(f.MaxKey),
				NumKeys: f.NumKeys,
				MinSeq:  f.MinSeq,
				MaxSeq:  f.MaxSeq,
			})
		}
	}
	return result
}

// SSTableFileInfo contains information about an SSTable file.
type SSTableFileInfo struct {
	FileNum uint64
	Level   int
	Size    int64
	MinKey  string
	MaxKey  string
	NumKeys int64
	MinSeq  uint64
	MaxSeq  uint64
}

// GetLevelInfo returns information about each level.
func (e *Engine) GetLevelInfo() []LevelInfo {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.levels == nil {
		return nil
	}

	var result []LevelInfo
	for level := 0; level <= 6; level++ {
		numFiles := e.levels.NumFiles(level)
		size := e.levels.LevelSize(level)
		targetSize := e.levels.TargetLevelSize(level)

		var score float64
		if targetSize > 0 {
			score = float64(size) / float64(targetSize)
		}

		result = append(result, LevelInfo{
			Level:      level,
			NumFiles:   numFiles,
			Size:       size,
			TargetSize: targetSize,
			Score:      score,
		})
	}
	return result
}

// LevelInfo contains information about a level in the LSM tree.
type LevelInfo struct {
	Level      int
	NumFiles   int
	Size       int64
	TargetSize int64
	Score      float64
}

// DeleteRange deletes all keys in the range [startKey, endKey).
// This is a potentially expensive operation as it requires scanning.
func (e *Engine) DeleteRange(ctx context.Context, startKey, endKey []byte) (int64, error) {
	if e.closed.Load() {
		return 0, fmt.Errorf("engine is closed")
	}

	// Create an iterator for the range
	iter := e.Scan(startKey, endKey)
	defer func() { _ = iter.Close() }()

	var deleted int64
	for iter.Valid() {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return deleted, ctx.Err()
		default:
		}

		key := iter.Key()
		if err := e.Delete(key); err != nil {
			return deleted, fmt.Errorf("failed to delete key: %w", err)
		}
		deleted++
		iter.Next()
	}

	if err := iter.Error(); err != nil {
		return deleted, fmt.Errorf("iterator error: %w", err)
	}

	return deleted, nil
}

// GetProperties returns database properties.
func (e *Engine) GetProperties() Properties {
	e.mu.RLock()
	defer e.mu.RUnlock()

	props := Properties{
		DataDir:        e.opts.Dir,
		SequenceNumber: e.seqNum,
	}

	if e.memTable != nil {
		props.MemTableSize = e.memTable.Size()
	}
	props.ImmutableCount = len(e.immutableMemTables)

	if e.levels != nil {
		props.SSTableCount = 0
		props.TotalSize = 0
		for level := 0; level <= 6; level++ {
			props.SSTableCount += e.levels.NumFiles(level)
			props.TotalSize += e.levels.LevelSize(level)
		}
	}

	if e.compactor != nil {
		cstats := e.compactor.Stats()
		props.CompactionsTotal = cstats.CompactionsRun
		props.BytesCompacted = cstats.BytesWritten
		props.WriteStalled = e.compactor.ShouldStallWrites()
	}

	return props
}

// Properties contains database properties.
type Properties struct {
	DataDir          string
	MemTableSize     int64
	ImmutableCount   int
	SSTableCount     int
	TotalSize        int64
	CompactionsTotal int64
	BytesCompacted   int64
	WriteStalled     bool
	SequenceNumber   uint64
}

// IsWriteStalled returns whether writes are currently stalled.
func (e *Engine) IsWriteStalled() bool {
	if e.compactor == nil {
		return false
	}
	return e.compactor.ShouldStallWrites()
}

// EngineStats contains operation statistics.
type EngineStats struct {
	TotalReads   int64
	TotalWrites  int64
	TotalDeletes int64
	BytesRead    int64
	BytesWritten int64
	CacheHits    int64
	CacheMisses  int64
}

// GetStats returns engine operation statistics.
func (e *Engine) GetStats() EngineStats {
	return EngineStats{
		TotalReads:   e.statsReads.Load(),
		TotalWrites:  e.statsWrites.Load(),
		TotalDeletes: e.statsDeletes.Load(),
		BytesRead:    e.statsBytesRead.Load(),
		BytesWritten: e.statsBytesWritten.Load(),
		CacheHits:    e.statsCacheHits.Load(),
		CacheMisses:  e.statsCacheMisses.Load(),
	}
}

// GetDataDir returns the data directory path.
func (e *Engine) GetDataDir() string {
	return e.opts.Dir
}

// GetSequenceNumber returns the current sequence number.
func (e *Engine) GetSequenceNumber() uint64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.seqNum
}
