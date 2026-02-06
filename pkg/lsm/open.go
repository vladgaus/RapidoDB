package lsm

import (
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/rapidodb/rapidodb/pkg/compaction"
	"github.com/rapidodb/rapidodb/pkg/compaction/fifo"
	"github.com/rapidodb/rapidodb/pkg/compaction/leveled"
	"github.com/rapidodb/rapidodb/pkg/compaction/tiered"
	"github.com/rapidodb/rapidodb/pkg/errors"
	"github.com/rapidodb/rapidodb/pkg/memtable"
	"github.com/rapidodb/rapidodb/pkg/sstable"
	"github.com/rapidodb/rapidodb/pkg/types"
	"github.com/rapidodb/rapidodb/pkg/wal"
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

	// Initialize level manager
	e.levels = compaction.NewLevelManager(sstDir, opts.BlockSize, opts.BloomBitsPerKey)
	e.levels.SetLevelTargets(opts.MaxBytesForLevelBase, opts.MaxBytesForLevelMultiplier)

	// Load existing SSTables
	if err := e.loadSSTables(); err != nil {
		return nil, err
	}

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

	// Initialize WAL manager
	walOpts := wal.Options{
		Dir:         walDir,
		SyncOnWrite: opts.WALSyncOnWrite,
		MaxFileSize: opts.WALMaxFileSize,
	}
	walManager, err := wal.NewManager(walOpts)
	if err != nil {
		e.levels.Close()
		return nil, err
	}
	e.walManager = walManager

	// Recover from WAL if exists
	if err := e.recover(); err != nil {
		walManager.Close()
		e.levels.Close()
		return nil, err
	}

	// Open new WAL for writes
	if err := e.walManager.Open(0); err != nil {
		walManager.Close()
		e.levels.Close()
		return nil, err
	}

	// Create initial MemTable if not recovered
	if e.memTable == nil {
		e.memTable = memtable.NewMemTable(e.walManager.CurrentFileNum(), opts.MemTableSize)
	}

	// Start background workers
	e.closeWg.Add(1)
	go e.flushWorker()

	e.compactor.Start()

	return e, nil
}

// createStrategy creates the appropriate compaction strategy.
func createStrategy(opts Options) compaction.Strategy {
	switch opts.CompactionStrategy {
	case CompactionTiered:
		return tiered.New(tiered.Config{
			MinMergeWidth:               4,
			MaxMergeWidth:               32,
			SizeRatio:                   4.0,
			BaseBucketSize:              opts.TargetFileSizeBase, // Use target file size as base
			MaxBuckets:                  10,
			MaxSizeAmplificationPercent: 200,
			L0StopWritesTrigger:         opts.L0StopWritesTrigger,
		})
	case CompactionFIFO:
		return fifo.New(fifo.Config{
			MaxTableFilesSize:        opts.MaxBytesForLevelBase * 10, // Use 10x base level size
			TTLSeconds:               0,                              // No TTL by default
			MaxFilesToDeletePerCycle: 10,
			L0StopWritesTrigger:      opts.L0StopWritesTrigger,
		})
	default:
		// Leveled compaction (default)
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

// loadSSTables discovers and opens existing SSTable files.
func (e *Engine) loadSSTables() error {
	entries, err := os.ReadDir(e.sstDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return errors.NewIOError("readdir", e.sstDir, err)
	}

	fileNums := make([]uint64, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if !strings.HasSuffix(name, ".sst") {
			continue
		}

		numStr := strings.TrimSuffix(name, ".sst")
		num, err := strconv.ParseUint(numStr, 10, 64)
		if err != nil {
			continue
		}

		fileNums = append(fileNums, num)

		// Track highest file number
		if num >= e.nextFileNum.Load() {
			e.nextFileNum.Store(num + 1)
		}
	}

	// Sort file numbers (newest first for L0)
	sort.Slice(fileNums, func(i, j int) bool {
		return fileNums[i] > fileNums[j]
	})

	// Open each SSTable and add to L0
	// TODO: In a real implementation, we would read level info from MANIFEST
	// For now, we assume all existing files are in L0
	for _, fileNum := range fileNums {
		path := e.sstPath(fileNum)
		reader, err := sstable.OpenReader(path)
		if err != nil {
			// Log error but continue (skip corrupted files)
			continue
		}

		// Get file info
		info, err := os.Stat(path)
		if err != nil {
			reader.Close()
			continue
		}

		meta := &compaction.FileMetadata{
			FileNum: fileNum,
			Level:   0, // Assume L0 for now
			Size:    info.Size(),
			MinKey:  reader.MinKey(),
			MaxKey:  reader.MaxKey(),
		}

		e.levels.AddL0File(meta, reader)
	}

	return nil
}

// recover replays the WAL to restore state.
func (e *Engine) recover() error {
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

	return nil
}

// Close closes the engine gracefully.
func (e *Engine) Close() error {
	// Mark as closed atomically
	if !e.closed.CompareAndSwap(false, true) {
		return nil // Already closed
	}

	// Stop compactor first
	if e.compactor != nil {
		e.compactor.Stop()
	}

	// Signal background workers to stop
	close(e.closeChan)

	// Wait for background workers
	e.closeWg.Wait()

	// Acquire write lock for final cleanup
	e.mu.Lock()
	defer e.mu.Unlock()

	// Flush remaining data if any
	// Note: In production, we might want to flush all MemTables
	// For now, we just close the WAL which ensures durability

	// Close WAL
	var walErr error
	if e.walManager != nil {
		walErr = e.walManager.Close()
	}

	// Close level manager (closes all SSTable readers)
	if e.levels != nil {
		e.levels.Close()
	}

	return walErr
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
		os.Remove(sstPath)
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
