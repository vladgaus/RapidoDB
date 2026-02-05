package compaction

import (
	"bytes"

	"github.com/rapidodb/rapidodb/pkg/sstable"
	"github.com/rapidodb/rapidodb/pkg/types"
)

// MergeIterator merges multiple sorted iterators into a single sorted stream.
// It uses a min-heap to efficiently find the smallest key across all iterators.
//
// For keys that appear in multiple iterators (from different levels),
// only the newest version (highest sequence number) is returned.
type MergeIterator struct {
	iters []iteratorWrapper
	heap  []int // indices into iters, organized as min-heap

	// Current key/value after deduplication
	currentKey   []byte
	currentValue []byte
	currentSeq   uint64
	isDelete     bool

	// For snapshot reads
	maxSeqNum uint64

	err   error
	valid bool
}

// iteratorWrapper wraps an iterator with its current position.
type iteratorWrapper struct {
	iter     *sstable.Iterator
	level    int    // Level this iterator comes from (lower = newer)
	fileNum  uint64 // For tie-breaking within same level
	key      []byte
	value    []byte
	seqNum   uint64
	isDelete bool
	valid    bool
}

// NewMergeIterator creates a merge iterator from multiple SSTable iterators.
// Iterators should be passed in order: L0 files (newest first), then L1+.
func NewMergeIterator(iters []*sstable.Iterator, levels []int, fileNums []uint64, maxSeqNum uint64) *MergeIterator {
	mi := &MergeIterator{
		iters:     make([]iteratorWrapper, len(iters)),
		heap:      make([]int, 0, len(iters)),
		maxSeqNum: maxSeqNum,
	}

	for i, iter := range iters {
		mi.iters[i] = iteratorWrapper{
			iter:    iter,
			level:   levels[i],
			fileNum: fileNums[i],
		}
	}

	return mi
}

// SeekToFirst positions all iterators at their first entry.
func (mi *MergeIterator) SeekToFirst() {
	mi.heap = mi.heap[:0]

	for i := range mi.iters {
		mi.iters[i].iter.SeekToFirst()
		mi.updateWrapper(i)
		if mi.iters[i].valid {
			mi.heapPush(i)
		}
	}

	mi.advance()
}

// Seek positions all iterators at first entry >= target.
func (mi *MergeIterator) Seek(target []byte) {
	mi.heap = mi.heap[:0]

	for i := range mi.iters {
		mi.iters[i].iter.Seek(target)
		mi.updateWrapper(i)
		if mi.iters[i].valid {
			mi.heapPush(i)
		}
	}

	mi.advance()
}

// Next advances to the next unique key.
func (mi *MergeIterator) Next() {
	if !mi.valid {
		return
	}

	// Skip all entries with the same key
	prevKey := mi.currentKey
	for len(mi.heap) > 0 {
		top := mi.heap[0]
		if !bytes.Equal(mi.iters[top].key, prevKey) {
			break
		}
		mi.advanceIterator(top)
	}

	mi.advance()
}

// advance finds the next visible key after deduplication.
func (mi *MergeIterator) advance() {
	for len(mi.heap) > 0 {
		// Get smallest key
		top := mi.heap[0]
		wrapper := &mi.iters[top]

		// Skip if sequence number is too high
		if wrapper.seqNum > mi.maxSeqNum {
			mi.advanceIterator(top)
			continue
		}

		// This is the winning entry for this key
		mi.currentKey = wrapper.key
		mi.currentValue = wrapper.value
		mi.currentSeq = wrapper.seqNum
		mi.isDelete = wrapper.isDelete
		mi.valid = true

		// Skip older versions of the same key
		mi.skipOlderVersions(wrapper.key)

		// If it's a delete, skip to next key
		if mi.isDelete {
			continue
		}

		return
	}

	mi.valid = false
	mi.currentKey = nil
	mi.currentValue = nil
}

// skipOlderVersions removes all entries with the same key from the heap.
func (mi *MergeIterator) skipOlderVersions(key []byte) {
	for len(mi.heap) > 0 {
		top := mi.heap[0]
		if !bytes.Equal(mi.iters[top].key, key) {
			break
		}
		mi.advanceIterator(top)
	}
}

// advanceIterator moves one iterator forward and fixes the heap.
func (mi *MergeIterator) advanceIterator(idx int) {
	mi.heapPop()
	mi.iters[idx].iter.Next()
	mi.updateWrapper(idx)
	if mi.iters[idx].valid {
		mi.heapPush(idx)
	}
}

// updateWrapper updates the cached state from an iterator.
func (mi *MergeIterator) updateWrapper(idx int) {
	wrapper := &mi.iters[idx]
	if !wrapper.iter.Valid() {
		wrapper.valid = false
		wrapper.key = nil
		wrapper.value = nil
		return
	}

	entry := wrapper.iter.Entry()
	if entry == nil {
		wrapper.valid = false
		return
	}

	wrapper.valid = true
	wrapper.key = entry.Key
	wrapper.value = entry.Value
	wrapper.seqNum = entry.SeqNum
	wrapper.isDelete = entry.IsDeleted()
}

// Valid returns true if positioned at a valid entry.
func (mi *MergeIterator) Valid() bool {
	return mi.valid && mi.err == nil
}

// Key returns the current key.
func (mi *MergeIterator) Key() []byte {
	return mi.currentKey
}

// Value returns the current value.
func (mi *MergeIterator) Value() []byte {
	return mi.currentValue
}

// Entry returns the current entry.
func (mi *MergeIterator) Entry() *types.Entry {
	if !mi.valid {
		return nil
	}
	entryType := types.EntryTypePut
	if mi.isDelete {
		entryType = types.EntryTypeDelete
	}
	return &types.Entry{
		Key:    mi.currentKey,
		Value:  mi.currentValue,
		Type:   entryType,
		SeqNum: mi.currentSeq,
	}
}

// Error returns any error encountered.
func (mi *MergeIterator) Error() error {
	return mi.err
}

// Close releases all iterator resources.
func (mi *MergeIterator) Close() error {
	for i := range mi.iters {
		if mi.iters[i].iter != nil {
			mi.iters[i].iter.Close()
		}
	}
	return nil
}

// Heap operations

func (mi *MergeIterator) heapPush(idx int) {
	mi.heap = append(mi.heap, idx)
	mi.heapUp(len(mi.heap) - 1)
}

func (mi *MergeIterator) heapPop() int {
	if len(mi.heap) == 0 {
		return -1
	}
	top := mi.heap[0]
	n := len(mi.heap) - 1
	mi.heap[0] = mi.heap[n]
	mi.heap = mi.heap[:n]
	if n > 0 {
		mi.heapDown(0)
	}
	return top
}

func (mi *MergeIterator) heapUp(i int) {
	for {
		parent := (i - 1) / 2
		if parent == i || !mi.heapLess(i, parent) {
			break
		}
		mi.heap[parent], mi.heap[i] = mi.heap[i], mi.heap[parent]
		i = parent
	}
}

func (mi *MergeIterator) heapDown(i int) {
	n := len(mi.heap)
	for {
		left := 2*i + 1
		if left >= n {
			break
		}
		j := left
		if right := left + 1; right < n && mi.heapLess(right, left) {
			j = right
		}
		if !mi.heapLess(j, i) {
			break
		}
		mi.heap[i], mi.heap[j] = mi.heap[j], mi.heap[i]
		i = j
	}
}

// heapLess compares two iterators by their current key.
// Ordering: smallest key first, then highest seqNum, then lower level (newer).
func (mi *MergeIterator) heapLess(i, j int) bool {
	a := &mi.iters[mi.heap[i]]
	b := &mi.iters[mi.heap[j]]

	// Compare keys
	cmp := bytes.Compare(a.key, b.key)
	if cmp != 0 {
		return cmp < 0
	}

	// Same key: higher seqNum comes first (newer)
	if a.seqNum != b.seqNum {
		return a.seqNum > b.seqNum
	}

	// Same seqNum: lower level comes first (L0 is newest)
	if a.level != b.level {
		return a.level < b.level
	}

	// Same level: higher file number is newer (for L0)
	return a.fileNum > b.fileNum
}

// CompactionIterator is a specialized merge iterator for compaction.
// Unlike MergeIterator, it includes tombstones and all sequence numbers.
type CompactionIterator struct {
	iters []iteratorWrapper
	heap  []int

	currentKey   []byte
	currentValue []byte
	currentSeq   uint64
	isDelete     bool

	// Oldest sequence number still in use (for garbage collection)
	oldestSnapshot uint64

	err   error
	valid bool
}

// NewCompactionIterator creates an iterator for compaction.
func NewCompactionIterator(iters []*sstable.Iterator, levels []int, fileNums []uint64, oldestSnapshot uint64) *CompactionIterator {
	ci := &CompactionIterator{
		iters:          make([]iteratorWrapper, len(iters)),
		heap:           make([]int, 0, len(iters)),
		oldestSnapshot: oldestSnapshot,
	}

	for i, iter := range iters {
		ci.iters[i] = iteratorWrapper{
			iter:    iter,
			level:   levels[i],
			fileNum: fileNums[i],
		}
	}

	return ci
}

// SeekToFirst positions all iterators at their first entry.
func (ci *CompactionIterator) SeekToFirst() {
	ci.heap = ci.heap[:0]

	for i := range ci.iters {
		ci.iters[i].iter.SeekToFirst()
		ci.updateWrapper(i)
		if ci.iters[i].valid {
			ci.heapPush(i)
		}
	}

	ci.findNext()
}

// Next advances to the next entry (including duplicates for compaction output).
func (ci *CompactionIterator) Next() {
	if !ci.valid || len(ci.heap) == 0 {
		ci.valid = false
		return
	}

	// Advance the top iterator
	top := ci.heap[0]
	ci.heapPop()
	ci.iters[top].iter.Next()
	ci.updateWrapper(top)
	if ci.iters[top].valid {
		ci.heapPush(top)
	}

	ci.findNext()
}

// findNext sets current to the next entry from the heap.
func (ci *CompactionIterator) findNext() {
	if len(ci.heap) == 0 {
		ci.valid = false
		ci.currentKey = nil
		ci.currentValue = nil
		return
	}

	top := ci.heap[0]
	wrapper := &ci.iters[top]

	ci.currentKey = wrapper.key
	ci.currentValue = wrapper.value
	ci.currentSeq = wrapper.seqNum
	ci.isDelete = wrapper.isDelete
	ci.valid = true
}

func (ci *CompactionIterator) updateWrapper(idx int) {
	wrapper := &ci.iters[idx]
	if !wrapper.iter.Valid() {
		wrapper.valid = false
		wrapper.key = nil
		wrapper.value = nil
		return
	}

	entry := wrapper.iter.Entry()
	if entry == nil {
		wrapper.valid = false
		return
	}

	wrapper.valid = true
	wrapper.key = entry.Key
	wrapper.value = entry.Value
	wrapper.seqNum = entry.SeqNum
	wrapper.isDelete = entry.IsDeleted()
}

// Valid returns true if positioned at a valid entry.
func (ci *CompactionIterator) Valid() bool {
	return ci.valid && ci.err == nil
}

// Key returns the current key.
func (ci *CompactionIterator) Key() []byte {
	return ci.currentKey
}

// Value returns the current value.
func (ci *CompactionIterator) Value() []byte {
	return ci.currentValue
}

// SeqNum returns the current sequence number.
func (ci *CompactionIterator) SeqNum() uint64 {
	return ci.currentSeq
}

// IsDelete returns true if current entry is a tombstone.
func (ci *CompactionIterator) IsDelete() bool {
	return ci.isDelete
}

// Entry returns the current entry.
func (ci *CompactionIterator) Entry() *types.Entry {
	if !ci.valid {
		return nil
	}
	entryType := types.EntryTypePut
	if ci.isDelete {
		entryType = types.EntryTypeDelete
	}
	return &types.Entry{
		Key:    ci.currentKey,
		Value:  ci.currentValue,
		Type:   entryType,
		SeqNum: ci.currentSeq,
	}
}

// ShouldDrop returns true if the current entry can be dropped during compaction.
// An entry can be dropped if:
// 1. It's an older version of a key (not the newest)
// 2. It's a tombstone and no snapshot might need it
func (ci *CompactionIterator) ShouldDrop(isNewest bool, compactingToBottomLevel bool) bool {
	// Always keep the newest version
	if isNewest {
		// But we can drop tombstones at the bottom level if no snapshots need them
		if ci.isDelete && compactingToBottomLevel && ci.currentSeq < ci.oldestSnapshot {
			return true
		}
		return false
	}

	// Drop older versions if no snapshot needs them
	return ci.currentSeq < ci.oldestSnapshot
}

// Error returns any error encountered.
func (ci *CompactionIterator) Error() error {
	return ci.err
}

// Close releases all iterator resources.
func (ci *CompactionIterator) Close() error {
	for i := range ci.iters {
		if ci.iters[i].iter != nil {
			ci.iters[i].iter.Close()
		}
	}
	return nil
}

// Heap operations for CompactionIterator

func (ci *CompactionIterator) heapPush(idx int) {
	ci.heap = append(ci.heap, idx)
	ci.heapUp(len(ci.heap) - 1)
}

func (ci *CompactionIterator) heapPop() int {
	if len(ci.heap) == 0 {
		return -1
	}
	top := ci.heap[0]
	n := len(ci.heap) - 1
	ci.heap[0] = ci.heap[n]
	ci.heap = ci.heap[:n]
	if n > 0 {
		ci.heapDown(0)
	}
	return top
}

func (ci *CompactionIterator) heapUp(i int) {
	for {
		parent := (i - 1) / 2
		if parent == i || !ci.heapLess(i, parent) {
			break
		}
		ci.heap[parent], ci.heap[i] = ci.heap[i], ci.heap[parent]
		i = parent
	}
}

func (ci *CompactionIterator) heapDown(i int) {
	n := len(ci.heap)
	for {
		left := 2*i + 1
		if left >= n {
			break
		}
		j := left
		if right := left + 1; right < n && ci.heapLess(right, left) {
			j = right
		}
		if !ci.heapLess(j, i) {
			break
		}
		ci.heap[i], ci.heap[j] = ci.heap[j], ci.heap[i]
		i = j
	}
}

func (ci *CompactionIterator) heapLess(i, j int) bool {
	a := &ci.iters[ci.heap[i]]
	b := &ci.iters[ci.heap[j]]

	cmp := bytes.Compare(a.key, b.key)
	if cmp != 0 {
		return cmp < 0
	}

	// Same key: higher seqNum first
	if a.seqNum != b.seqNum {
		return a.seqNum > b.seqNum
	}

	// Same seqNum: lower level first
	if a.level != b.level {
		return a.level < b.level
	}

	return a.fileNum > b.fileNum
}
