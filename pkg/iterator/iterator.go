// Package iterator provides composable iterators for the LSM-tree.
//
// This package includes:
// - MergeIterator: Merges multiple sorted iterators with MVCC awareness
// - FilteredIterator: Applies range bounds, prefix filtering, and snapshot visibility
// - BoundedIterator: Wraps any iterator with start/end bounds
//
// Architecture:
//
//	┌─────────────────────────────────────────────────────────────┐
//	│                    Iterator Stack                            │
//	├─────────────────────────────────────────────────────────────┤
//	│                                                              │
//	│  User sees:                                                  │
//	│  ┌─────────────────────────────────────────────────────┐    │
//	│  │           FilteredIterator (range/prefix)           │    │
//	│  └─────────────────────────────────────────────────────┘    │
//	│                          │                                   │
//	│                          ▼                                   │
//	│  ┌─────────────────────────────────────────────────────┐    │
//	│  │      MergeIterator (deduplicates by seqNum)         │    │
//	│  └─────────────────────────────────────────────────────┘    │
//	│           │              │              │                    │
//	│           ▼              ▼              ▼                    │
//	│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐           │
//	│  │  MemTable   │ │  MemTable   │ │  SSTable    │           │
//	│  │  Iterator   │ │  Iterator   │ │  Iterator   │           │
//	│  └─────────────┘ └─────────────┘ └─────────────┘           │
//	│                                                              │
//	└─────────────────────────────────────────────────────────────┘
//
// MVCC Visibility:
// - Each entry has a sequence number
// - A snapshot sees entries with seqNum <= snapshot's seqNum
// - MergeIterator picks the newest visible version of each key
// - Tombstones are filtered out (unless explicitly requested)
package iterator

import (
	"github.com/vladgaus/RapidoDB/pkg/types"
)

// Iterator extends the base types.Iterator with Entry() method.
// This is the interface all iterators in this package implement.
type Iterator interface {
	// Valid returns true if positioned at a valid entry.
	Valid() bool

	// Key returns the current key.
	Key() []byte

	// Value returns the current value.
	Value() []byte

	// Entry returns the full entry with metadata.
	Entry() *types.Entry

	// Next advances to the next entry.
	Next()

	// Prev moves to the previous entry.
	Prev()

	// Seek positions at first entry >= target.
	Seek(target []byte)

	// SeekToFirst positions at the first entry.
	SeekToFirst()

	// SeekToLast positions at the last entry.
	SeekToLast()

	// SeekForPrev positions at last entry <= target.
	SeekForPrev(target []byte)

	// Error returns any error encountered.
	Error() error

	// Close releases resources.
	Close() error
}

// Source wraps an iterator with metadata about its origin.
// Used by MergeIterator to track which source each entry came from.
type Source struct {
	Iter  Iterator
	Level int    // 0 = memtable, 1+ = SSTable level
	ID    uint64 // File number or memtable ID
}

// Options configures iterator behavior.
type Options struct {
	// Snapshot sequence number for MVCC visibility.
	// Only entries with SeqNum <= SnapshotSeq are visible.
	// If 0, all entries are visible.
	SnapshotSeq uint64

	// LowerBound is the inclusive lower bound for keys.
	// If nil, iteration starts from the beginning.
	LowerBound []byte

	// UpperBound is the exclusive upper bound for keys.
	// If nil, iteration continues to the end.
	UpperBound []byte

	// Prefix limits iteration to keys with this prefix.
	// If set, LowerBound/UpperBound are ignored.
	Prefix []byte

	// IncludeTombstones includes deleted entries if true.
	// Default is false (tombstones are skipped).
	IncludeTombstones bool

	// Reverse iterates in reverse order if true.
	Reverse bool
}

// Direction indicates the current iteration direction.
type Direction int

const (
	Forward Direction = iota
	Backward
)

// emptyIterator is an iterator with no entries.
type emptyIterator struct{}

// Empty returns an iterator with no entries.
func Empty() Iterator {
	return &emptyIterator{}
}

func (e *emptyIterator) Valid() bool         { return false }
func (e *emptyIterator) Key() []byte         { return nil }
func (e *emptyIterator) Value() []byte       { return nil }
func (e *emptyIterator) Entry() *types.Entry { return nil }
func (e *emptyIterator) Next()               {}
func (e *emptyIterator) Prev()               {}
func (e *emptyIterator) Seek([]byte)         {}
func (e *emptyIterator) SeekToFirst()        {}
func (e *emptyIterator) SeekToLast()         {}
func (e *emptyIterator) SeekForPrev([]byte)  {}
func (e *emptyIterator) Error() error        { return nil }
func (e *emptyIterator) Close() error        { return nil }

// sliceIterator wraps a slice of entries for testing.
type sliceIterator struct {
	entries []*types.Entry
	pos     int
	err     error
}

// FromSlice creates an iterator from a slice of entries.
// Entries should be pre-sorted by key.
func FromSlice(entries []*types.Entry) Iterator {
	return &sliceIterator{
		entries: entries,
		pos:     -1,
	}
}

func (s *sliceIterator) Valid() bool {
	return s.pos >= 0 && s.pos < len(s.entries) && s.err == nil
}

func (s *sliceIterator) Key() []byte {
	if !s.Valid() {
		return nil
	}
	return s.entries[s.pos].Key
}

func (s *sliceIterator) Value() []byte {
	if !s.Valid() {
		return nil
	}
	return s.entries[s.pos].Value
}

func (s *sliceIterator) Entry() *types.Entry {
	if !s.Valid() {
		return nil
	}
	return s.entries[s.pos]
}

func (s *sliceIterator) Next() {
	if s.pos < len(s.entries) {
		s.pos++
	}
}

func (s *sliceIterator) Prev() {
	if s.pos >= 0 {
		s.pos--
	}
}

func (s *sliceIterator) Seek(target []byte) {
	// Binary search for first key >= target
	s.pos = s.search(target)
}

func (s *sliceIterator) SeekForPrev(target []byte) {
	// Binary search for last key <= target
	s.pos = s.searchForPrev(target)
}

func (s *sliceIterator) SeekToFirst() {
	if len(s.entries) > 0 {
		s.pos = 0
	} else {
		s.pos = -1
	}
}

func (s *sliceIterator) SeekToLast() {
	if len(s.entries) > 0 {
		s.pos = len(s.entries) - 1
	} else {
		s.pos = -1
	}
}

func (s *sliceIterator) Error() error {
	return s.err
}

func (s *sliceIterator) Close() error {
	return nil
}

// search returns index of first entry with key >= target.
func (s *sliceIterator) search(target []byte) int {
	left, right := 0, len(s.entries)
	for left < right {
		mid := (left + right) / 2
		if compareKeys(s.entries[mid].Key, target) < 0 {
			left = mid + 1
		} else {
			right = mid
		}
	}
	if left >= len(s.entries) {
		return len(s.entries) // Invalid position
	}
	return left
}

// searchForPrev returns index of last entry with key <= target.
func (s *sliceIterator) searchForPrev(target []byte) int {
	left, right := 0, len(s.entries)
	for left < right {
		mid := (left + right) / 2
		if compareKeys(s.entries[mid].Key, target) <= 0 {
			left = mid + 1
		} else {
			right = mid
		}
	}
	return left - 1 // May be -1 if all keys > target
}

// compareKeys compares two keys lexicographically.
func compareKeys(a, b []byte) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	for i := 0; i < minLen; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}
