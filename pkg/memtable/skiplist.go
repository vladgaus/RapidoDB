// Package memtable provides in-memory table implementations for RapidoDB.
// The MemTable is the first destination for all writes and maintains
// data in sorted order for efficient reads and flushes to SSTable.
package memtable

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"

	"github.com/vladgaus/RapidoDB/pkg/types"
)

const (
	// MaxHeight is the maximum number of levels in the skip list.
	// With p=0.25 and maxHeight=12, we can efficiently handle ~16 million entries.
	// Height 12: 4^12 = 16,777,216 entries
	MaxHeight = 12

	// branchingFactor determines the probability of increasing height.
	// p = 1/branchingFactor. With 4, each level has 1/4 the nodes of the level below.
	branchingFactor = 4
)

// SkipList is a probabilistic data structure that provides O(log n) search,
// insert, and delete operations. It maintains elements in sorted order.
//
// This implementation is optimized for LSM-tree MemTables:
// - Supports concurrent reads with single writer
// - Uses internal keys for MVCC ordering
// - Tracks memory usage for flush decisions
//
// The skip list structure:
//
//	Level 3:  head -----------------------> [D] ---------> nil
//	Level 2:  head --------> [B] --------> [D] ---------> nil
//	Level 1:  head -> [A] -> [B] -> [C] -> [D] -> [E] --> nil
//	Level 0:  head -> [A] -> [B] -> [C] -> [D] -> [E] --> nil
type SkipList struct {
	head   *skipListNode
	height atomic.Int32 // Current max height in use

	// Concurrency control: multiple readers, single writer
	mu sync.RWMutex

	// Statistics
	size       atomic.Int64 // Approximate memory usage in bytes
	entryCount atomic.Int64 // Number of entries

	// Random source for level generation (per-skiplist to avoid contention)
	randMu sync.Mutex
	rand   *rand.Rand
}

// skipListNode represents a node in the skip list.
// Each node contains an entry and forward pointers for each level.
type skipListNode struct {
	entry   *types.Entry
	forward []*skipListNode // forward[i] points to the next node at level i
}

// newSkipListNode creates a new node with the given height.
func newSkipListNode(entry *types.Entry, height int) *skipListNode {
	return &skipListNode{
		entry:   entry,
		forward: make([]*skipListNode, height),
	}
}

// NewSkipList creates a new empty skip list.
func NewSkipList() *SkipList {
	sl := &SkipList{
		head: newSkipListNode(nil, MaxHeight),
		rand: rand.New(rand.NewSource(rand.Int63())),
	}
	sl.height.Store(1)
	return sl
}

// randomHeight generates a random height for a new node.
// Uses geometric distribution with p = 1/branchingFactor.
func (sl *SkipList) randomHeight() int {
	sl.randMu.Lock()
	defer sl.randMu.Unlock()

	height := 1
	for height < MaxHeight && sl.rand.Intn(branchingFactor) == 0 {
		height++
	}
	return height
}

// compareKeys compares two entries using internal key ordering.
// Order: user key (ascending) -> sequence number (descending) -> type (descending)
func compareKeys(a, b *types.Entry) int {
	// Compare user keys
	cmp := bytes.Compare(a.Key, b.Key)
	if cmp != 0 {
		return cmp
	}

	// Same user key: higher sequence number comes first
	if a.SeqNum > b.SeqNum {
		return -1
	}
	if a.SeqNum < b.SeqNum {
		return 1
	}

	// Same sequence: Delete (1) comes before Put (0)
	if a.Type > b.Type {
		return -1
	}
	if a.Type < b.Type {
		return 1
	}

	return 0
}

// Put inserts or updates an entry in the skip list.
// For MVCC, we always insert (never update in place) since each
// entry has a unique (key, seqNum) combination.
func (sl *SkipList) Put(entry *types.Entry) {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	// Track nodes that need updating at each level
	update := make([]*skipListNode, MaxHeight)
	current := sl.head

	// Find position from top level down
	for i := int(sl.height.Load()) - 1; i >= 0; i-- {
		for current.forward[i] != nil && compareKeys(current.forward[i].entry, entry) < 0 {
			current = current.forward[i]
		}
		update[i] = current
	}

	// Generate random height for new node
	newHeight := sl.randomHeight()

	// If new height exceeds current, update the update array
	currentHeight := int(sl.height.Load())
	if newHeight > currentHeight {
		for i := currentHeight; i < newHeight; i++ {
			update[i] = sl.head
		}
		sl.height.Store(int32(newHeight))
	}

	// Create and insert new node
	newNode := newSkipListNode(entry, newHeight)
	for i := 0; i < newHeight; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}

	// Update statistics
	sl.size.Add(entry.Size() + int64(newHeight*8)) // entry size + pointer overhead
	sl.entryCount.Add(1)
}

// Get finds an entry by key with sequence number <= maxSeqNum.
// Returns nil if not found.
func (sl *SkipList) Get(key []byte, maxSeqNum uint64) *types.Entry {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	current := sl.head

	// Search from top level down to find the position
	for i := int(sl.height.Load()) - 1; i >= 0; i-- {
		for current.forward[i] != nil {
			node := current.forward[i]
			cmp := bytes.Compare(node.entry.Key, key)

			if cmp < 0 {
				// Key is smaller, continue at this level
				current = node
			} else if cmp == 0 && node.entry.SeqNum > maxSeqNum {
				// Same key but seqNum too high, continue
				current = node
			} else {
				// Key is >= target or seqNum is valid, go down
				break
			}
		}
	}

	// Check the next node at level 0
	next := current.forward[0]
	if next != nil && bytes.Equal(next.entry.Key, key) && next.entry.SeqNum <= maxSeqNum {
		return next.entry
	}

	return nil
}

// Contains checks if a key exists with sequence number <= maxSeqNum.
func (sl *SkipList) Contains(key []byte, maxSeqNum uint64) bool {
	return sl.Get(key, maxSeqNum) != nil
}

// Size returns the approximate memory usage in bytes.
func (sl *SkipList) Size() int64 {
	return sl.size.Load()
}

// Len returns the number of entries in the skip list.
func (sl *SkipList) Len() int64 {
	return sl.entryCount.Load()
}

// IsEmpty returns true if the skip list has no entries.
func (sl *SkipList) IsEmpty() bool {
	return sl.entryCount.Load() == 0
}

// NewIterator returns an iterator over all entries in sorted order.
func (sl *SkipList) NewIterator() *SkipListIterator {
	return &SkipListIterator{
		sl:      sl,
		current: nil,
	}
}

// SkipListIterator provides sequential access to skip list entries.
type SkipListIterator struct {
	sl      *SkipList
	current *skipListNode
}

// Valid returns true if the iterator is positioned at a valid entry.
func (it *SkipListIterator) Valid() bool {
	return it.current != nil
}

// Key returns the current entry's key.
func (it *SkipListIterator) Key() []byte {
	if it.current == nil {
		return nil
	}
	return it.current.entry.Key
}

// Value returns the current entry's value.
func (it *SkipListIterator) Value() []byte {
	if it.current == nil {
		return nil
	}
	return it.current.entry.Value
}

// Entry returns the current entry.
func (it *SkipListIterator) Entry() *types.Entry {
	if it.current == nil {
		return nil
	}
	return it.current.entry
}

// Next advances the iterator to the next entry.
func (it *SkipListIterator) Next() {
	if it.current != nil {
		it.sl.mu.RLock()
		it.current = it.current.forward[0]
		it.sl.mu.RUnlock()
	}
}

// SeekToFirst positions the iterator at the first entry.
func (it *SkipListIterator) SeekToFirst() {
	it.sl.mu.RLock()
	it.current = it.sl.head.forward[0]
	it.sl.mu.RUnlock()
}

// SeekToLast positions the iterator at the last entry.
func (it *SkipListIterator) SeekToLast() {
	it.sl.mu.RLock()
	defer it.sl.mu.RUnlock()

	current := it.sl.head
	for i := int(it.sl.height.Load()) - 1; i >= 0; i-- {
		for current.forward[i] != nil {
			current = current.forward[i]
		}
	}

	if current == it.sl.head {
		it.current = nil
	} else {
		it.current = current
	}
}

// Seek positions the iterator at the first entry with key >= target.
func (it *SkipListIterator) Seek(target []byte) {
	it.sl.mu.RLock()
	defer it.sl.mu.RUnlock()

	current := it.sl.head

	for i := int(it.sl.height.Load()) - 1; i >= 0; i-- {
		for current.forward[i] != nil && bytes.Compare(current.forward[i].entry.Key, target) < 0 {
			current = current.forward[i]
		}
	}

	it.current = current.forward[0]
}

// SeekForPrev positions the iterator at the last entry with key <= target.
func (it *SkipListIterator) SeekForPrev(target []byte) {
	it.sl.mu.RLock()
	defer it.sl.mu.RUnlock()

	current := it.sl.head

	for i := int(it.sl.height.Load()) - 1; i >= 0; i-- {
		for current.forward[i] != nil && bytes.Compare(current.forward[i].entry.Key, target) <= 0 {
			current = current.forward[i]
		}
	}

	if current == it.sl.head {
		it.current = nil
	} else {
		it.current = current
	}
}

// Prev moves the iterator to the previous entry.
// Note: This is O(n) as skip lists don't have backward pointers.
// For frequent reverse iteration, consider using a different data structure.
func (it *SkipListIterator) Prev() {
	if it.current == nil {
		it.SeekToLast()
		return
	}

	it.sl.mu.RLock()
	defer it.sl.mu.RUnlock()

	// Find the node before current
	target := it.current.entry
	current := it.sl.head

	var prev *skipListNode
	for current.forward[0] != nil && compareKeys(current.forward[0].entry, target) < 0 {
		prev = current.forward[0]
		current = current.forward[0]
	}

	it.current = prev
}

// Close releases resources held by the iterator.
func (it *SkipListIterator) Close() error {
	it.current = nil
	it.sl = nil
	return nil
}

// Error returns any error encountered during iteration.
func (it *SkipListIterator) Error() error {
	return nil
}
