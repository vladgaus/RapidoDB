package compaction

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/rapidodb/rapidodb/pkg/sstable"
)

// Compactor executes compaction tasks using a pluggable strategy.
type Compactor struct {
	mu sync.Mutex

	levels   *LevelManager
	strategy Strategy
	config   Config

	// File number allocator
	allocFileNum func() uint64

	// Statistics
	stats Stats

	// Oldest snapshot sequence number (for garbage collection)
	oldestSnapshot atomic.Uint64

	// Background runner
	runner *Runner
}

// NewCompactor creates a new compactor with the given strategy.
func NewCompactor(levels *LevelManager, strategy Strategy, config Config, allocFileNum func() uint64) *Compactor {
	c := &Compactor{
		levels:       levels,
		strategy:     strategy,
		config:       config,
		allocFileNum: allocFileNum,
	}
	c.runner = NewRunner(c, config.MaxBackgroundCompactions)
	return c
}

// SetStrategy changes the compaction strategy.
func (c *Compactor) SetStrategy(strategy Strategy) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.strategy = strategy
}

// Strategy returns the current strategy.
func (c *Compactor) Strategy() Strategy {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.strategy
}

// SetOldestSnapshot updates the oldest snapshot for garbage collection.
func (c *Compactor) SetOldestSnapshot(seqNum uint64) {
	c.oldestSnapshot.Store(seqNum)
}

// Levels returns the level manager.
func (c *Compactor) Levels() *LevelManager {
	return c.levels
}

// MaybeCompact checks if compaction is needed and runs it.
// Returns true if a compaction was performed.
func (c *Compactor) MaybeCompact() (bool, error) {
	c.mu.Lock()
	task := c.strategy.PickCompaction(c.levels)
	if task == nil {
		c.mu.Unlock()
		return false, nil
	}
	c.mu.Unlock()

	err := c.RunCompaction(task)
	return err == nil, err
}

// ShouldTriggerCompaction checks if compaction should run.
func (c *Compactor) ShouldTriggerCompaction() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.strategy.ShouldTriggerCompaction(c.levels)
}

// ShouldStallWrites checks if writes should be stalled.
func (c *Compactor) ShouldStallWrites() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.strategy.ShouldStallWrites(c.levels)
}

// RunCompaction executes a compaction task.
func (c *Compactor) RunCompaction(task *Task) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Collect all input files
	var allInputs []*FileMetadata
	allInputs = append(allInputs, task.Inputs...)
	allInputs = append(allInputs, task.Overlapping...)

	if len(allInputs) == 0 {
		return nil
	}

	// Create iterators for all input files
	iters := make([]*sstable.Iterator, 0, len(allInputs))
	levels := make([]int, 0, len(allInputs))
	fileNums := make([]uint64, 0, len(allInputs))

	for _, meta := range allInputs {
		reader := c.levels.GetReader(meta.FileNum)
		if reader == nil {
			continue
		}
		iters = append(iters, reader.NewIterator())
		levels = append(levels, meta.Level)
		fileNums = append(fileNums, meta.FileNum)
	}

	if len(iters) == 0 {
		return nil
	}

	// Create merge iterator
	oldestSnapshot := c.oldestSnapshot.Load()
	mergeIter := NewCompactionIterator(iters, levels, fileNums, oldestSnapshot)
	defer mergeIter.Close()

	// Output files
	var outputFiles []*FileMetadata
	var outputReaders []*sstable.Reader

	// Cleanup on error
	defer func() {
		if len(outputFiles) > 0 && len(outputReaders) == 0 {
			// Error occurred, cleanup partial output
			for _, meta := range outputFiles {
				os.Remove(filepath.Join(c.levels.SSTDir(), fmt.Sprintf("%06d.sst", meta.FileNum)))
			}
		}
	}()

	// Target file size
	targetSize := c.config.TargetFileSizeBase
	isBottomLevel := task.TargetLevel == MaxLevels-1

	// Current output file state
	var writer *sstable.Writer
	var currentMeta *FileMetadata
	var currentSize int64
	var prevKey []byte

	// Iterate through all entries
	mergeIter.SeekToFirst()
	for mergeIter.Valid() {
		key := mergeIter.Key()
		isNewest := !bytes.Equal(key, prevKey)

		// Check if we should drop this entry
		if mergeIter.ShouldDrop(isNewest, isBottomLevel) {
			prevKey = append(prevKey[:0], key...)
			mergeIter.Next()
			continue
		}

		// Start new output file if needed
		if writer == nil || currentSize >= targetSize {
			// Finish current file
			if writer != nil {
				meta, err := c.finishOutputFile(writer, currentMeta)
				if err != nil {
					return err
				}
				outputFiles = append(outputFiles, meta)
			}

			// Start new file
			fileNum := c.allocFileNum()
			path := filepath.Join(c.levels.SSTDir(), fmt.Sprintf("%06d.sst", fileNum))
			var err error
			writer, err = sstable.NewWriter(path, sstable.WriterOptions{
				BlockSize:       c.levels.BlockSize(),
				RestartInterval: 16,
				BitsPerKey:      c.levels.BloomBitsPerKey(),
			})
			if err != nil {
				return err
			}
			currentMeta = &FileMetadata{
				FileNum: fileNum,
				Level:   task.TargetLevel,
			}
			currentSize = 0
		}

		// Write entry
		entry := mergeIter.Entry()
		if err := writer.Add(entry); err != nil {
			writer.Abort()
			return err
		}

		// Update metadata
		if currentMeta.MinKey == nil {
			currentMeta.MinKey = append([]byte{}, key...)
		}
		currentMeta.MaxKey = append(currentMeta.MaxKey[:0], key...)
		if currentMeta.MinSeq == 0 || mergeIter.SeqNum() < currentMeta.MinSeq {
			currentMeta.MinSeq = mergeIter.SeqNum()
		}
		if mergeIter.SeqNum() > currentMeta.MaxSeq {
			currentMeta.MaxSeq = mergeIter.SeqNum()
		}
		currentMeta.NumKeys++
		currentSize += int64(len(key) + len(entry.Value) + 20) // Approximate

		prevKey = append(prevKey[:0], key...)
		mergeIter.Next()
	}

	if mergeIter.Error() != nil {
		if writer != nil {
			writer.Abort()
		}
		return mergeIter.Error()
	}

	// Finish last output file
	if writer != nil {
		meta, err := c.finishOutputFile(writer, currentMeta)
		if err != nil {
			return err
		}
		outputFiles = append(outputFiles, meta)
	}

	// Open readers for output files
	for _, meta := range outputFiles {
		path := filepath.Join(c.levels.SSTDir(), fmt.Sprintf("%06d.sst", meta.FileNum))
		reader, err := sstable.OpenReader(path)
		if err != nil {
			// Cleanup already created readers
			for _, r := range outputReaders {
				r.Close()
			}
			return err
		}
		outputReaders = append(outputReaders, reader)
	}

	// Atomically swap files
	// 1. Remove old files from level manager
	for _, meta := range allInputs {
		c.levels.RemoveFile(meta)
	}

	// 2. Add new files to level manager
	for i, meta := range outputFiles {
		c.levels.AddFile(task.TargetLevel, meta, outputReaders[i])
	}

	// 3. Delete old SSTable files
	for _, meta := range allInputs {
		path := filepath.Join(c.levels.SSTDir(), fmt.Sprintf("%06d.sst", meta.FileNum))
		os.Remove(path) // Ignore errors
	}

	// Update statistics
	c.stats.CompactionsRun++
	for _, meta := range allInputs {
		c.stats.BytesRead += meta.Size
		c.stats.FilesRead++
	}
	for _, meta := range outputFiles {
		c.stats.BytesWritten += meta.Size
		c.stats.FilesWritten++
	}

	return nil
}

// finishOutputFile finishes writing an SSTable and returns its metadata.
func (c *Compactor) finishOutputFile(writer *sstable.Writer, meta *FileMetadata) (*FileMetadata, error) {
	sstMeta, err := writer.Finish()
	if err != nil {
		return nil, err
	}

	meta.Size = int64(sstMeta.FileSize)
	return meta, nil
}

// Stats returns compaction statistics.
func (c *Compactor) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats
}

// Start starts background compaction.
func (c *Compactor) Start() {
	c.runner.Start()
}

// Stop stops background compaction.
func (c *Compactor) Stop() {
	c.runner.Stop()
}

// TriggerCompaction signals that compaction may be needed.
func (c *Compactor) TriggerCompaction() {
	c.runner.Trigger()
}

// Runner manages background compaction goroutines.
type Runner struct {
	compactor   *Compactor
	compactChan chan struct{}
	closeChan   chan struct{}
	closeWg     sync.WaitGroup
	running     atomic.Bool
	maxWorkers  int
}

// NewRunner creates a background compaction runner.
func NewRunner(compactor *Compactor, maxWorkers int) *Runner {
	if maxWorkers < 1 {
		maxWorkers = 1
	}
	return &Runner{
		compactor:   compactor,
		compactChan: make(chan struct{}, maxWorkers),
		closeChan:   make(chan struct{}),
		maxWorkers:  maxWorkers,
	}
}

// Start starts the background compaction workers.
func (r *Runner) Start() {
	if r.running.Swap(true) {
		return // Already running
	}

	r.closeChan = make(chan struct{})
	for i := 0; i < r.maxWorkers; i++ {
		r.closeWg.Add(1)
		go r.runLoop()
	}
}

// Stop stops the background compaction workers.
func (r *Runner) Stop() {
	if !r.running.Swap(false) {
		return // Not running
	}

	close(r.closeChan)
	r.closeWg.Wait()
}

// Trigger signals that compaction might be needed.
func (r *Runner) Trigger() {
	select {
	case r.compactChan <- struct{}{}:
	default:
		// Already triggered
	}
}

// runLoop is the background compaction worker.
func (r *Runner) runLoop() {
	defer r.closeWg.Done()

	for {
		select {
		case <-r.closeChan:
			return
		case <-r.compactChan:
			// Run compaction until nothing more to do
			for {
				select {
				case <-r.closeChan:
					return
				default:
				}

				did, err := r.compactor.MaybeCompact()
				if err != nil {
					// Log error but continue
					break
				}
				if !did {
					break
				}
			}
		}
	}
}
