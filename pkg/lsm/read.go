package lsm

import (
	"github.com/rapidodb/rapidodb/pkg/compaction"
	"github.com/rapidodb/rapidodb/pkg/errors"
	"github.com/rapidodb/rapidodb/pkg/iterator"
	"github.com/rapidodb/rapidodb/pkg/mvcc"
	"github.com/rapidodb/rapidodb/pkg/types"
)

// Get retrieves a value by key.
// Returns nil, nil if key not found.
// Returns the value and nil error if found.
// Returns nil and error if an error occurred.
func (e *Engine) Get(key []byte) ([]byte, error) {
	// Validate key
	if err := errors.ValidateKey(key); err != nil {
		return nil, err
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.closed.Load() {
		return nil, ErrClosed
	}

	// Use max sequence number to see all writes
	return e.getInternal(key, e.seqNum)
}

// getInternal performs a get with a specific sequence number.
// Caller must hold at least e.mu.RLock().
func (e *Engine) getInternal(key []byte, readSeqNum uint64) ([]byte, error) {
	// 1. Check active MemTable
	entry, err := e.memTable.Get(key, readSeqNum)
	if err == nil {
		if entry.IsDeleted() {
			return nil, nil // Key was deleted
		}
		return entry.Value, nil
	}
	if !errors.IsNotFound(err) {
		return nil, err
	}

	// 2. Check immutable MemTables (newest to oldest)
	for _, imm := range e.immutableMemTables {
		entry, err := imm.Get(key, readSeqNum)
		if err == nil {
			if entry.IsDeleted() {
				return nil, nil // Key was deleted
			}
			return entry.Value, nil
		}
		if !errors.IsNotFound(err) {
			return nil, err
		}
	}

	// 3. Check SSTables via LevelManager
	if e.levels != nil {
		value, found, err := e.levels.Get(key)
		if err != nil {
			return nil, err
		}
		if found {
			return value, nil // value is nil for tombstones
		}
	}

	// Not found
	return nil, nil
}

// Has checks if a key exists.
func (e *Engine) Has(key []byte) (bool, error) {
	value, err := e.Get(key)
	if err != nil {
		return false, err
	}
	return value != nil, nil
}

// Snapshot creates a point-in-time snapshot for consistent reads.
// The returned snapshot MUST be released via Release() when no longer needed
// to allow garbage collection of old versions.
//
// Example usage:
//
//	snap := engine.Snapshot()
//	defer snap.Release()
//	value, _ := snap.Get(key)
func (e *Engine) Snapshot() *Snapshot {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// Create a managed snapshot that's tracked for GC
	managed := e.snapshots.CreateSnapshot()

	return &Snapshot{
		engine:  e,
		managed: managed,
	}
}

// GetSnapshot is an alias for Snapshot() - creates a point-in-time view.
func (e *Engine) GetSnapshot() *Snapshot {
	return e.Snapshot()
}

// Snapshot represents a point-in-time view of the database.
// It provides a consistent read view: all reads through a snapshot
// see the database state at the time the snapshot was created.
//
// Snapshots prevent garbage collection of versions they might need,
// so always release them when done.
type Snapshot struct {
	engine  *Engine
	managed *mvcc.ManagedSnapshot
}

// Get retrieves a value as of the snapshot time.
func (s *Snapshot) Get(key []byte) ([]byte, error) {
	// Check if released
	if s.managed.IsReleased() {
		return nil, ErrSnapshotReleased
	}

	// Validate key
	if err := errors.ValidateKey(key); err != nil {
		return nil, err
	}

	s.engine.mu.RLock()
	defer s.engine.mu.RUnlock()

	if s.engine.closed.Load() {
		return nil, ErrClosed
	}

	return s.engine.getInternal(key, s.managed.SeqNum())
}

// Has checks if a key exists at the snapshot time.
func (s *Snapshot) Has(key []byte) (bool, error) {
	value, err := s.Get(key)
	if err != nil {
		return false, err
	}
	return value != nil, nil
}

// SeqNum returns the sequence number of the snapshot.
func (s *Snapshot) SeqNum() uint64 {
	return s.managed.SeqNum()
}

// Release releases the snapshot, allowing GC of old versions.
// After release, the snapshot should not be used for reads.
// Safe to call multiple times.
func (s *Snapshot) Release() {
	s.managed.Release()
}

// IsReleased returns true if the snapshot has been released.
func (s *Snapshot) IsReleased() bool {
	return s.managed.IsReleased()
}

// Iterator returns an iterator over all key-value pairs.
func (e *Engine) Iterator() *Iterator {
	return e.NewIterator(IteratorOptions{})
}

// IteratorOptions configures iteration behavior.
type IteratorOptions struct {
	// LowerBound is the inclusive lower bound for keys.
	LowerBound []byte

	// UpperBound is the exclusive upper bound for keys.
	UpperBound []byte

	// Prefix limits iteration to keys with this prefix.
	Prefix []byte

	// SnapshotSeq is the sequence number for MVCC visibility.
	// If 0, uses the current sequence number.
	SnapshotSeq uint64

	// IncludeTombstones includes deleted entries if true.
	IncludeTombstones bool
}

// NewIterator creates a new iterator with options.
func (e *Engine) NewIterator(opts IteratorOptions) *Iterator {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.closed.Load() {
		return &Iterator{err: ErrClosed}
	}

	// Use current sequence number if not specified
	snapshotSeq := opts.SnapshotSeq
	if snapshotSeq == 0 {
		snapshotSeq = e.seqNum
	}

	// Collect all sources
	sources := e.collectIteratorSources()

	// Build merge iterator options
	mergeOpts := iterator.Options{
		SnapshotSeq:       snapshotSeq,
		IncludeTombstones: opts.IncludeTombstones,
	}

	// Create merge iterator
	mergeIter := iterator.NewMergeIterator(sources, mergeOpts)

	// Apply bounds or prefix
	var resultIter iterator.Iterator = mergeIter
	if len(opts.Prefix) > 0 {
		resultIter = iterator.NewPrefixIterator(mergeIter, opts.Prefix)
	} else if len(opts.LowerBound) > 0 || len(opts.UpperBound) > 0 {
		resultIter = iterator.NewBoundedIterator(mergeIter, opts.LowerBound, opts.UpperBound)
	}

	return &Iterator{
		inner:  resultIter,
		seqNum: snapshotSeq,
	}
}

// Scan returns an iterator over keys in range [start, end).
// If end is nil, iterates to the end.
func (e *Engine) Scan(start, end []byte) *Iterator {
	return e.NewIterator(IteratorOptions{
		LowerBound: start,
		UpperBound: end,
	})
}

// ScanPrefix returns an iterator over keys with the given prefix.
func (e *Engine) ScanPrefix(prefix []byte) *Iterator {
	return e.NewIterator(IteratorOptions{
		Prefix: prefix,
	})
}

// collectIteratorSources collects all iterator sources.
// Caller must hold e.mu.RLock().
func (e *Engine) collectIteratorSources() []iterator.Source {
	var sources []iterator.Source

	// Active MemTable (level 0, highest priority)
	if e.memTable != nil {
		sources = append(sources, iterator.Source{
			Iter:  iterator.Wrap(e.memTable.NewIterator()),
			Level: 0,
			ID:    e.memTable.ID(),
		})
	}

	// Immutable MemTables (newest first)
	for i, imm := range e.immutableMemTables {
		sources = append(sources, iterator.Source{
			Iter:  iterator.Wrap(imm.NewIterator()),
			Level: 0,
			ID:    imm.ID() + uint64(i+1)*1000, // Ensure unique IDs
		})
	}

	// SSTable iterators from all levels
	if e.levels != nil {
		for level := 0; level < 7; level++ {
			files := e.levels.LevelFiles(level)
			for _, meta := range files {
				reader := e.levels.GetReader(meta.FileNum)
				if reader != nil {
					sources = append(sources, iterator.Source{
						Iter:  iterator.Wrap(reader.NewIterator()),
						Level: level + 1, // Level 1+ for SSTables
						ID:    meta.FileNum,
					})
				}
			}
		}
	}

	return sources
}

// Iterator iterates over key-value pairs in sorted order.
type Iterator struct {
	inner  iterator.Iterator
	seqNum uint64
	err    error
}

// Valid returns true if the iterator is positioned at a valid entry.
func (it *Iterator) Valid() bool {
	if it.err != nil {
		return false
	}
	if it.inner == nil {
		return false
	}
	return it.inner.Valid()
}

// Key returns the current key.
func (it *Iterator) Key() []byte {
	if it.inner == nil {
		return nil
	}
	return it.inner.Key()
}

// Value returns the current value.
func (it *Iterator) Value() []byte {
	if it.inner == nil {
		return nil
	}
	return it.inner.Value()
}

// Entry returns the full entry with metadata.
func (it *Iterator) Entry() *types.Entry {
	if it.inner == nil {
		return nil
	}
	return it.inner.Entry()
}

// SeekToFirst positions at the first entry.
func (it *Iterator) SeekToFirst() {
	if it.inner == nil {
		return
	}
	it.inner.SeekToFirst()
}

// SeekToLast positions at the last entry.
func (it *Iterator) SeekToLast() {
	if it.inner == nil {
		return
	}
	it.inner.SeekToLast()
}

// Seek positions at first entry >= target.
func (it *Iterator) Seek(target []byte) {
	if it.inner == nil {
		return
	}
	it.inner.Seek(target)
}

// SeekForPrev positions at last entry <= target.
func (it *Iterator) SeekForPrev(target []byte) {
	if it.inner == nil {
		return
	}
	it.inner.SeekForPrev(target)
}

// Next advances to the next entry.
func (it *Iterator) Next() {
	if it.inner == nil {
		return
	}
	it.inner.Next()
}

// Prev moves to the previous entry.
func (it *Iterator) Prev() {
	if it.inner == nil {
		return
	}
	it.inner.Prev()
}

// Error returns any error encountered.
func (it *Iterator) Error() error {
	if it.err != nil {
		return it.err
	}
	if it.inner == nil {
		return nil
	}
	return it.inner.Error()
}

// Close releases iterator resources.
func (it *Iterator) Close() error {
	if it.inner == nil {
		return nil
	}
	return it.inner.Close()
}

// Stats returns engine statistics.
type Stats struct {
	MemTableSize       int64
	ImmutableCount     int
	L0TableCount       int
	SeqNum             uint64
	TotalKeyValuePairs int64
	LevelStats         []compaction.LevelStats
	CompactionStats    compaction.Stats

	// MVCC stats
	ActiveSnapshots int    // Number of active (unreleased) snapshots
	OldestSnapshot  uint64 // Sequence number of oldest active snapshot
}

// Stats returns current engine statistics.
func (e *Engine) Stats() Stats {
	e.mu.RLock()
	defer e.mu.RUnlock()

	stats := Stats{
		ImmutableCount: len(e.immutableMemTables),
		SeqNum:         e.seqNum,
	}

	if e.memTable != nil {
		stats.MemTableSize = e.memTable.Size()
		stats.TotalKeyValuePairs = e.memTable.EntryCount()
	}

	for _, imm := range e.immutableMemTables {
		stats.TotalKeyValuePairs += imm.EntryCount()
	}

	if e.levels != nil {
		stats.L0TableCount = e.levels.NumFiles(0)
		stats.LevelStats = e.levels.GetStats(e.opts.L0CompactionTrigger)
	}

	if e.compactor != nil {
		stats.CompactionStats = e.compactor.Stats()
	}

	// MVCC snapshot stats
	if e.snapshots != nil {
		stats.ActiveSnapshots = e.snapshots.NumSnapshots()
		stats.OldestSnapshot = e.snapshots.GetOldestSeq()
	}

	return stats
}
