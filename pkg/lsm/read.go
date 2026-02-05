package lsm

import (
	"bytes"

	"github.com/rapidodb/rapidodb/pkg/compaction"
	"github.com/rapidodb/rapidodb/pkg/errors"
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
func (e *Engine) Snapshot() *Snapshot {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return &Snapshot{
		engine: e,
		seqNum: e.seqNum,
	}
}

// Snapshot represents a point-in-time view of the database.
type Snapshot struct {
	engine *Engine
	seqNum uint64
}

// Get retrieves a value as of the snapshot time.
func (s *Snapshot) Get(key []byte) ([]byte, error) {
	// Validate key
	if err := errors.ValidateKey(key); err != nil {
		return nil, err
	}

	s.engine.mu.RLock()
	defer s.engine.mu.RUnlock()

	if s.engine.closed.Load() {
		return nil, ErrClosed
	}

	return s.engine.getInternal(key, s.seqNum)
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
	return s.seqNum
}

// Iterator returns an iterator over all key-value pairs.
func (e *Engine) Iterator() *Iterator {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// Collect all iterators
	var memIters []types.Iterator

	// Active MemTable iterator
	memIters = append(memIters, e.memTable.NewIterator())

	// Immutable MemTable iterators
	for _, imm := range e.immutableMemTables {
		memIters = append(memIters, imm.NewIterator())
	}

	return &Iterator{
		engine:   e,
		seqNum:   e.seqNum,
		memIters: memIters,
	}
}

// Iterator iterates over key-value pairs in sorted order.
type Iterator struct {
	engine   *Engine
	seqNum   uint64
	memIters []types.Iterator

	// Merge heap for merging multiple iterators
	heap *mergeHeap

	// Current position
	key   []byte
	value []byte
	err   error
	valid bool
}

// heapItem represents an item in the merge heap.
type heapItem struct {
	key      []byte
	value    []byte
	seqNum   uint64
	isDelete bool
	source   int // Index of the source iterator
}

// mergeHeap is a min-heap for merging sorted iterators.
type mergeHeap struct {
	items []*heapItem
}

func newMergeHeap() *mergeHeap {
	return &mergeHeap{
		items: make([]*heapItem, 0),
	}
}

func (h *mergeHeap) Len() int {
	return len(h.items)
}

func (h *mergeHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h.items[i].key, h.items[j].key)
	if cmp != 0 {
		return cmp < 0
	}
	// Same key: higher seqNum (newer) comes first
	return h.items[i].seqNum > h.items[j].seqNum
}

func (h *mergeHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *mergeHeap) Push(x *heapItem) {
	h.items = append(h.items, x)
	h.up(len(h.items) - 1)
}

func (h *mergeHeap) Pop() *heapItem {
	if len(h.items) == 0 {
		return nil
	}
	item := h.items[0]
	n := len(h.items) - 1
	h.items[0] = h.items[n]
	h.items = h.items[:n]
	if n > 0 {
		h.down(0)
	}
	return item
}

func (h *mergeHeap) Peek() *heapItem {
	if len(h.items) == 0 {
		return nil
	}
	return h.items[0]
}

func (h *mergeHeap) up(i int) {
	for {
		parent := (i - 1) / 2
		if parent == i || !h.Less(i, parent) {
			break
		}
		h.Swap(parent, i)
		i = parent
	}
}

func (h *mergeHeap) down(i int) {
	n := len(h.items)
	for {
		left := 2*i + 1
		if left >= n {
			break
		}
		j := left
		if right := left + 1; right < n && h.Less(right, left) {
			j = right
		}
		if !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		i = j
	}
}

// Valid returns true if the iterator is positioned at a valid entry.
func (it *Iterator) Valid() bool {
	return it.valid && it.err == nil
}

// Key returns the current key.
func (it *Iterator) Key() []byte {
	return it.key
}

// Value returns the current value.
func (it *Iterator) Value() []byte {
	return it.value
}

// SeekToFirst positions at the first entry.
func (it *Iterator) SeekToFirst() {
	it.heap = newMergeHeap()

	// Position all MemTable iterators at first
	for i, iter := range it.memIters {
		iter.SeekToFirst()
		if iter.Valid() {
			it.pushFromMemIter(iter, i)
		}
	}

	// TODO: Add SSTable iterators (Step 8)

	it.advance()
}

// SeekToLast positions at the last entry.
func (it *Iterator) SeekToLast() {
	// For simplicity, scan through all entries
	// A more efficient implementation would use backward iteration
	it.SeekToFirst()

	var lastKey, lastValue []byte
	for it.Valid() {
		lastKey = append([]byte{}, it.key...)
		lastValue = append([]byte{}, it.value...)
		it.Next()
	}

	it.key = lastKey
	it.value = lastValue
	it.valid = lastKey != nil
	it.err = nil
}

// Seek positions at first entry >= target.
func (it *Iterator) Seek(target []byte) {
	it.heap = newMergeHeap()

	// Seek all MemTable iterators
	for i, iter := range it.memIters {
		iter.Seek(target)
		if iter.Valid() {
			it.pushFromMemIter(iter, i)
		}
	}

	// TODO: Add SSTable iterators (Step 8)

	it.advance()
}

// Next advances to the next entry.
func (it *Iterator) Next() {
	if !it.valid || it.heap == nil {
		return
	}

	// Skip all entries with the same key (they're older versions)
	currentKey := it.key
	for it.heap.Len() > 0 {
		top := it.heap.Peek()
		if !bytes.Equal(top.key, currentKey) {
			break
		}
		it.heap.Pop()
		it.advanceSource(top.source)
	}

	it.advance()
}

// advance moves to the next visible entry.
func (it *Iterator) advance() {
	for it.heap.Len() > 0 {
		item := it.heap.Pop()

		// Check if entry is visible (seqNum <= snapshot seqNum)
		if item.seqNum > it.seqNum {
			it.advanceSource(item.source)
			continue
		}

		// Skip deleted entries
		if item.isDelete {
			// Need to skip all older versions of this key too
			it.skipOldVersions(item.key)
			it.advanceSource(item.source)
			continue
		}

		// Found a valid entry
		it.key = item.key
		it.value = item.value
		it.valid = true

		// Advance source for next time
		it.advanceSource(item.source)

		// Skip any remaining versions of this key in the heap
		it.skipOldVersions(item.key)

		return
	}

	// No more entries
	it.key = nil
	it.value = nil
	it.valid = false
}

// skipOldVersions removes all older versions of key from the heap.
func (it *Iterator) skipOldVersions(key []byte) {
	for it.heap.Len() > 0 {
		top := it.heap.Peek()
		if !bytes.Equal(top.key, key) {
			break
		}
		item := it.heap.Pop()
		it.advanceSource(item.source)
	}
}

// advanceSource advances the source iterator and pushes new item if valid.
func (it *Iterator) advanceSource(source int) {
	if source < len(it.memIters) {
		iter := it.memIters[source]
		iter.Next()
		if iter.Valid() {
			it.pushFromMemIter(iter, source)
		}
	}
	// TODO: Handle SSTable sources (Step 8)
}

// pushFromMemIter pushes the current entry from a MemTable iterator.
func (it *Iterator) pushFromMemIter(iter types.Iterator, source int) {
	key := iter.Key()
	if key == nil {
		return
	}

	// We need to get the entry to check delete status and seqnum
	// For now, we approximate - the MemTable iterator returns all versions
	item := &heapItem{
		key:      append([]byte{}, key...),
		value:    append([]byte{}, iter.Value()...),
		seqNum:   it.seqNum, // Use snapshot seqnum as approximation
		isDelete: iter.Value() == nil,
		source:   source,
	}
	it.heap.Push(item)
}

// Error returns any error encountered.
func (it *Iterator) Error() error {
	return it.err
}

// Close releases iterator resources.
func (it *Iterator) Close() error {
	for _, iter := range it.memIters {
		iter.Close()
	}
	it.memIters = nil
	it.heap = nil
	return nil
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

	return stats
}
