package lsm

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/vladgaus/RapidoDB/pkg/compaction"
	"github.com/vladgaus/RapidoDB/pkg/compaction/fifo"
	"github.com/vladgaus/RapidoDB/pkg/compaction/leveled"
	"github.com/vladgaus/RapidoDB/pkg/compaction/tiered"
	"github.com/vladgaus/RapidoDB/pkg/errors"
	"github.com/vladgaus/RapidoDB/pkg/manifest"
	"github.com/vladgaus/RapidoDB/pkg/memtable"
	"github.com/vladgaus/RapidoDB/pkg/mvcc"
	"github.com/vladgaus/RapidoDB/pkg/sstable"
	"github.com/vladgaus/RapidoDB/pkg/types"
	"github.com/vladgaus/RapidoDB/pkg/wal"
)

// Open opens or creates an LSM engine at the specified directory.
func Open(opts Options) (*Engine, error) {
	if err := opts.validate(); err != nil {
		return nil, err
	}

	// Create data directory if needed
	if err := os.MkdirAll(opts.Dir, 0755); err != nil {
		return nil, errors.NewIOError("mkdir", opts.Dir, err)
	}

	// Create subdirectories
	walDir := filepath.Join(opts.Dir, "wal")
	sstDir := filepath.Join(opts.Dir, "sst")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		return nil, errors.NewIOError("mkdir", walDir, err)
	}
	if err := os.MkdirAll(sstDir, 0755); err != nil {
		return nil, errors.NewIOError("mkdir", sstDir, err)
	}

	// Create engine
	e := &Engine{
		opts:        opts,
		walDir:      walDir,
		sstDir:      sstDir,
		flushChan:   make(chan *flushTask, opts.MaxBackgroundFlushes),
		closeChan:   make(chan struct{}),
		flushResult: make(chan error, 1),
	}
	e.nextFileNum.Store(1)

	// Initialize MVCC snapshot manager
	e.snapshots = mvcc.NewSnapshotManager()

	// Initialize level manager
	e.levels = compaction.NewLevelManager(sstDir, opts.BlockSize, opts.BloomBitsPerKey)
	e.levels.SetLevelTargets(opts.MaxBytesForLevelBase, opts.MaxBytesForLevelMultiplier)

	// Initialize version set (manifest) and recover
	e.versions = manifest.NewVersionSet(opts.Dir)
	if err := e.versions.Recover(); err != nil {
		return nil, errors.NewRecoveryError("manifest", err)
	}

	// Load SSTables from recovered manifest state
	if err := e.loadFromManifest(); err != nil {
		_ = e.versions.Close()
		return nil, err
	}

	// Restore counters from manifest
	e.seqNum = e.versions.LastSequence()
	e.nextFileNum.Store(e.versions.NextFileNumber())

	// Create compaction strategy based on configuration
	strategy := createStrategy(opts)

	// Initialize compactor with the strategy
	compactConfig := compaction.Config{
		Strategy:                 opts.CompactionStrategy,
		MaxBackgroundCompactions: opts.MaxBackgroundCompactions,
		NumLevels:                opts.NumLevels,
		L0CompactionTrigger:      opts.L0CompactionTrigger,
		L0StopWritesTrigger:      opts.L0StopWritesTrigger,
		BaseLevelSize:            opts.MaxBytesForLevelBase,
		LevelSizeMultiplier:      opts.MaxBytesForLevelMultiplier,
		TargetFileSizeBase:       opts.TargetFileSizeBase,
	}
	e.compactor = compaction.NewCompactor(e.levels, strategy, compactConfig, e.allocateFileNum)

	// Wire up compaction callback to log to manifest
	e.compactor.SetCompactionCallback(func(edit *compaction.CompactionEdit) error {
		vedít := manifest.NewVersionEdit()

		// Add deleted files
		for _, df := range edit.DeletedFiles {
			vedít.DeleteFile(df.Level, df.FileNum)
		}

		// Add new files
		for _, nf := range edit.NewFiles {
			vedít.AddFile(nf.Level, &manifest.FileMeta{
				FileNum: nf.Meta.FileNum,
				Size:    nf.Meta.Size,
				MinKey:  nf.Meta.MinKey,
				MaxKey:  nf.Meta.MaxKey,
				MinSeq:  nf.Meta.MinSeq,
				MaxSeq:  nf.Meta.MaxSeq,
				NumKeys: nf.Meta.NumKeys,
			})
		}

		vedít.SetNextFileNumber(e.nextFileNum.Load())

		return e.versions.LogAndApply(vedít)
	})

	// Wire up snapshot manager callback to update compactor's oldest snapshot
	e.snapshots.SetOldestChangeCallback(func(oldestSeq uint64) {
		e.compactor.SetOldestSnapshot(oldestSeq)
	})

	// Initialize WAL manager
	walOpts := wal.Options{
		Dir:         walDir,
		SyncOnWrite: opts.WALSyncOnWrite,
		MaxFileSize: opts.WALMaxFileSize,
	}
	walManager, err := wal.NewManager(walOpts)
	if err != nil {
		_ = e.versions.Close()
		_ = e.levels.Close()
		return nil, err
	}
	e.walManager = walManager

	// Recover from WAL (only logs >= manifest's LogNumber)
	if err := e.recoverWAL(); err != nil {
		_ = walManager.Close()
		_ = e.versions.Close()
		_ = e.levels.Close()
		return nil, err
	}

	// Open new WAL for writes
	if err := e.walManager.Open(0); err != nil {
		_ = walManager.Close()
		_ = e.versions.Close()
		_ = e.levels.Close()
		return nil, err
	}

	// Create initial MemTable if not recovered
	if e.memTable == nil {
		e.memTable = memtable.NewMemTable(e.walManager.CurrentFileNum(), opts.MemTableSize)
	}

	// Sync sequence number with snapshot manager
	e.snapshots.SetCurrentSeq(e.seqNum)

	// Start background workers
	e.closeWg.Add(1)
	go e.flushWorker()

	e.compactor.Start()

	return e, nil
}

// loadFromManifest loads SSTables based on manifest state.
//
//nolint:unparam // error return kept for future error propagation on SSTable load failures
func (e *Engine) loadFromManifest() error {
	version := e.versions.Current()

	for level := 0; level < manifest.MaxLevels; level++ {
		files := version.GetFiles(level)
		for _, f := range files {
			path := e.sstPath(f.FileNum)

			reader, err := sstable.OpenReader(path)
			if err != nil {
				// File doesn't exist or is corrupted, skip it
				// In production, this would be logged
				continue
			}

			// Convert manifest.FileMeta to compaction.FileMetadata
			meta := &compaction.FileMetadata{
				FileNum: f.FileNum,
				Level:   level,
				Size:    f.Size,
				MinKey:  f.MinKey,
				MaxKey:  f.MaxKey,
				MinSeq:  f.MinSeq,
				MaxSeq:  f.MaxSeq,
				NumKeys: f.NumKeys,
			}

			if level == 0 {
				e.levels.AddL0File(meta, reader)
			} else {
				e.levels.AddFile(level, meta, reader)
			}

			// Track highest file number
			e.versions.MarkFileNumberUsed(f.FileNum)
		}
	}

	return nil
}

// recoverWAL replays the WAL to restore state.
func (e *Engine) recoverWAL() error {
	minLogNum := e.versions.LogNumber()

	err := e.walManager.Recover(func(entry *types.Entry) error {
		// Create MemTable if needed
		if e.memTable == nil {
			e.memTable = memtable.NewMemTable(1, e.opts.MemTableSize)
		}

		// Apply entry to MemTable
		if entry.Type == types.EntryTypeDelete {
			if err := e.memTable.Delete(entry.Key, entry.SeqNum); err != nil {
				return err
			}
		} else {
			if err := e.memTable.Put(entry.Key, entry.Value, entry.SeqNum); err != nil {
				return err
			}
		}

		// Track max sequence number
		if entry.SeqNum > e.seqNum {
			e.seqNum = entry.SeqNum
		}

		return nil
	})

	if err != nil {
		return errors.NewRecoveryError("wal", err)
	}

	// Update log number in versions (we'll need this for next manifest write)
	_ = minLogNum // This would be used to skip old WAL files

	return nil
}

// createStrategy creates the appropriate compaction strategy.
func createStrategy(opts Options) compaction.Strategy {
	switch opts.CompactionStrategy {
	case CompactionTiered:
		return tiered.New(tiered.Config{
			MinMergeWidth:               4,
			MaxMergeWidth:               32,
			SizeRatio:                   4.0,
			BaseBucketSize:              opts.TargetFileSizeBase,
			MaxBuckets:                  10,
			MaxSizeAmplificationPercent: 200,
			L0StopWritesTrigger:         opts.L0StopWritesTrigger,
		})
	case CompactionFIFO:
		return fifo.New(fifo.Config{
			MaxTableFilesSize:        opts.MaxBytesForLevelBase * 10,
			TTLSeconds:               0,
			MaxFilesToDeletePerCycle: 10,
			L0StopWritesTrigger:      opts.L0StopWritesTrigger,
		})
	default:
		return leveled.New(leveled.Config{
			NumLevels:           opts.NumLevels,
			L0CompactionTrigger: opts.L0CompactionTrigger,
			L0StopWritesTrigger: opts.L0StopWritesTrigger,
			BaseLevelSize:       opts.MaxBytesForLevelBase,
			LevelSizeMultiplier: opts.MaxBytesForLevelMultiplier,
			TargetFileSizeBase:  opts.TargetFileSizeBase,
		})
	}
}

// Close closes the engine gracefully.
func (e *Engine) Close() error {
	// Use a 30 second default timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return e.GracefulClose(ctx)
}

// GracefulClose shuts down the engine gracefully, ensuring all data is persisted.
// The context can be used to set a timeout for the graceful shutdown.
//
// The shutdown process:
//  1. Stop accepting new writes (mark as closed)
//  2. Stop compactor and background workers
//  3. Flush any remaining immutable MemTables
//  4. Sync WAL and close all files
func (e *Engine) GracefulClose(ctx context.Context) error {
	// Mark as closed atomically
	if !e.closed.CompareAndSwap(false, true) {
		return nil // Already closed
	}

	var firstErr error

	// Stop compactor first (waits for current compaction to finish)
	if e.compactor != nil {
		e.compactor.Stop()
	}

	// Signal background workers to stop
	close(e.closeChan)

	// Wait for background workers (this will drain the flush channel)
	e.closeWg.Wait()

	// Now handle any remaining data that wasn't flushed
	e.mu.Lock()

	// If there's data in the active MemTable, rotate it
	if e.memTable != nil && !e.memTable.IsEmpty() {
		// Mark as immutable and add to list
		e.memTable.MarkImmutable()
		e.immutableMemTables = append([]*memtable.MemTable{e.memTable}, e.immutableMemTables...)
	}

	// Flush any remaining immutable MemTables synchronously
	for len(e.immutableMemTables) > 0 {
		// Check context — if timed out, stop flushing; data is safe in WAL.
		if ctx.Err() != nil {
			break
		}

		imm := e.immutableMemTables[len(e.immutableMemTables)-1]
		e.mu.Unlock()

		// Flush synchronously
		e.doFlush(&flushTask{
			mem:       imm,
			walFileID: imm.ID(),
		})

		e.mu.Lock()
	}

	e.mu.Unlock()

	// Sync WAL before closing
	if e.walManager != nil {
		if err := e.walManager.Sync(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	// Acquire write lock for final cleanup
	e.mu.Lock()
	defer e.mu.Unlock()

	// Close WAL
	if e.walManager != nil {
		if err := e.walManager.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	// Close manifest/versions
	if e.versions != nil {
		if err := e.versions.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	// Close level manager (closes all SSTable readers)
	if e.levels != nil {
		if err := e.levels.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// flushWorker runs in the background and flushes immutable MemTables.
func (e *Engine) flushWorker() {
	defer e.closeWg.Done()

	for {
		select {
		case <-e.closeChan:
			// Drain any remaining flush tasks
			for {
				select {
				case task := <-e.flushChan:
					e.doFlush(task)
				default:
					return
				}
			}
		case task := <-e.flushChan:
			e.doFlush(task)
		}
	}
}

// doFlush flushes one immutable MemTable to an SSTable.
func (e *Engine) doFlush(task *flushTask) {
	if task == nil || task.mem == nil || task.mem.IsEmpty() {
		e.removeImmutableMemTable(task.mem)
		return
	}

	// Allocate a file number for the new SSTable
	fileNum := e.allocateFileNum()
	sstPath := e.sstPath(fileNum)

	// Create SSTable writer
	writerOpts := sstable.WriterOptions{
		BlockSize:       e.opts.BlockSize,
		RestartInterval: 16, // Default
		BitsPerKey:      e.opts.BloomBitsPerKey,
	}
	writer, err := sstable.NewWriter(sstPath, writerOpts)
	if err != nil {
		// Log error and put memtable back
		return
	}

	// Track key range
	var minKey, maxKey []byte
	var minSeq, maxSeq uint64
	var numKeys int64

	// Iterate through MemTable and write to SSTable
	iter := task.mem.NewIterator()
	iter.SeekToFirst()

	for iter.Valid() {
		entry := iter.Entry()
		if entry == nil {
			iter.Next()
			continue
		}

		if err := writer.Add(entry); err != nil {
			if abortErr := writer.Abort(); abortErr != nil {
				return
			}
			return
		}

		// Track metadata
		if minKey == nil {
			minKey = append([]byte{}, entry.Key...)
		}
		maxKey = append(maxKey[:0], entry.Key...)
		if minSeq == 0 || entry.SeqNum < minSeq {
			minSeq = entry.SeqNum
		}
		if entry.SeqNum > maxSeq {
			maxSeq = entry.SeqNum
		}
		numKeys++

		iter.Next()
	}

	if err := iter.Close(); err != nil {
		if abortErr := writer.Abort(); abortErr != nil {
			return
		}
		return
	}

	// Finish writing SSTable
	sstMeta, err := writer.Finish()
	if err != nil {
		return
	}

	// Open the new SSTable for reading
	reader, err := sstable.OpenReader(sstPath)
	if err != nil {
		_ = os.Remove(sstPath)
		return
	}

	// Create file metadata
	meta := &compaction.FileMetadata{
		FileNum: fileNum,
		Level:   0,
		Size:    int64(sstMeta.FileSize),
		MinKey:  minKey,
		MaxKey:  maxKey,
		MinSeq:  minSeq,
		MaxSeq:  maxSeq,
		NumKeys: numKeys,
	}

	// Log new file to manifest (must succeed before adding to level manager)
	edit := manifest.NewVersionEdit()
	edit.AddFile(0, &manifest.FileMeta{
		FileNum: fileNum,
		Size:    int64(sstMeta.FileSize),
		MinKey:  minKey,
		MaxKey:  maxKey,
		MinSeq:  minSeq,
		MaxSeq:  maxSeq,
		NumKeys: numKeys,
	})
	edit.SetLastSequence(maxSeq)
	edit.SetLogNumber(task.walFileID)
	edit.SetNextFileNumber(e.nextFileNum.Load())

	if err := e.versions.LogAndApply(edit); err != nil {
		// Failed to log to manifest, clean up and abort
		_ = reader.Close()
		_ = os.Remove(sstPath)
		return
	}

	// Add to L0 and remove from immutable list
	e.mu.Lock()

	// Add to level manager
	e.levels.AddL0File(meta, reader)

	// Remove the flushed MemTable from immutable list
	e.removeImmutableMemTableLocked(task.mem)

	// Clean up old WAL files
	if task.walFileID > 0 {
		if err := e.walManager.CleanBefore(task.walFileID); err != nil {
			e.mu.Unlock()
			return
		}
	}

	// Check if compaction is needed
	needsCompaction := e.compactor != nil && e.compactor.ShouldTriggerCompaction()

	e.mu.Unlock()

	// Trigger compaction if needed
	if needsCompaction {
		e.compactor.TriggerCompaction()
	}
}

// removeImmutableMemTable removes a MemTable from the immutable list.
func (e *Engine) removeImmutableMemTable(mem *memtable.MemTable) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.removeImmutableMemTableLocked(mem)
}

// removeImmutableMemTableLocked removes a MemTable (caller must hold lock).
func (e *Engine) removeImmutableMemTableLocked(mem *memtable.MemTable) {
	for i, m := range e.immutableMemTables {
		if m == mem {
			e.immutableMemTables = append(e.immutableMemTables[:i], e.immutableMemTables[i+1:]...)
			return
		}
	}
}

// maybeScheduleFlush checks if a flush should be scheduled.
// Caller must hold e.mu.
func (e *Engine) maybeScheduleFlush() {
	if len(e.immutableMemTables) > 0 {
		// Get the oldest immutable MemTable
		oldest := e.immutableMemTables[len(e.immutableMemTables)-1]
		task := &flushTask{
			mem:       oldest,
			walFileID: oldest.ID(),
		}

		select {
		case e.flushChan <- task:
		default:
			// Flush channel full, will be scheduled later
		}
	}
}

// Sync ensures all pending writes are persisted to disk.
func (e *Engine) Sync() error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.closed.Load() {
		return ErrClosed
	}

	if e.walManager == nil {
		return nil
	}

	return e.walManager.Sync()
}
