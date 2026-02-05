package memtable

import (
	"sync/atomic"

	"github.com/rapidodb/rapidodb/pkg/errors"
	"github.com/rapidodb/rapidodb/pkg/types"
)

// MemTable is an in-memory sorted data structure for recent writes.
// It wraps a SkipList and provides the MemTable interface.
//
// Lifecycle:
// 1. Active: Accepts both reads and writes
// 2. Immutable: Read-only, pending flush to SSTable
// 3. Flushed: Data persisted, MemTable can be discarded
//
// Thread Safety:
// - Multiple concurrent readers are supported
// - Single writer at a time (enforced by external lock in LSM engine)
// - Safe to read while writing
type MemTable struct {
	sl *SkipList

	// Unique ID for this MemTable (used for WAL association)
	id uint64

	// immutable indicates whether this MemTable accepts writes
	immutable atomic.Bool

	// Maximum size before triggering flush
	maxSize int64
}

// NewMemTable creates a new MemTable with the specified ID and max size.
func NewMemTable(id uint64, maxSize int64) *MemTable {
	return &MemTable{
		sl:      NewSkipList(),
		id:      id,
		maxSize: maxSize,
	}
}

// ID returns the unique identifier for this MemTable.
func (m *MemTable) ID() uint64 {
	return m.id
}

// Put inserts or updates a key-value pair.
// Returns ErrWriteStall if the MemTable is immutable.
func (m *MemTable) Put(key, value []byte, seqNum uint64) error {
	if m.immutable.Load() {
		return errors.ErrWriteStall
	}

	if err := errors.ValidateKey(key); err != nil {
		return err
	}

	if err := errors.ValidateValue(value); err != nil {
		return err
	}

	entry := &types.Entry{
		Key:    key,
		Value:  value,
		Type:   types.EntryTypePut,
		SeqNum: seqNum,
	}

	m.sl.Put(entry)
	return nil
}

// Delete inserts a tombstone for the key.
// Returns ErrWriteStall if the MemTable is immutable.
func (m *MemTable) Delete(key []byte, seqNum uint64) error {
	if m.immutable.Load() {
		return errors.ErrWriteStall
	}

	if err := errors.ValidateKey(key); err != nil {
		return err
	}

	entry := &types.Entry{
		Key:    key,
		Value:  nil,
		Type:   types.EntryTypeDelete,
		SeqNum: seqNum,
	}

	m.sl.Put(entry)
	return nil
}

// Get retrieves an entry by key with sequence number <= maxSeqNum.
// Returns ErrKeyNotFound if the key doesn't exist.
func (m *MemTable) Get(key []byte, maxSeqNum uint64) (*types.Entry, error) {
	if err := errors.ValidateKey(key); err != nil {
		return nil, err
	}

	entry := m.sl.Get(key, maxSeqNum)
	if entry == nil {
		return nil, errors.ErrKeyNotFound
	}

	return entry, nil
}

// NewIterator returns an iterator over all entries in sorted order.
func (m *MemTable) NewIterator() *memTableIterator {
	return &memTableIterator{
		iter: m.sl.NewIterator(),
	}
}

// Size returns the approximate size in bytes.
func (m *MemTable) Size() int64 {
	return m.sl.Size()
}

// EntryCount returns the number of entries.
func (m *MemTable) EntryCount() int64 {
	return m.sl.Len()
}

// IsEmpty returns true if the MemTable has no entries.
func (m *MemTable) IsEmpty() bool {
	return m.sl.IsEmpty()
}

// ShouldFlush returns true if the MemTable has reached its size limit.
func (m *MemTable) ShouldFlush() bool {
	return m.sl.Size() >= m.maxSize
}

// MarkImmutable marks this MemTable as immutable (read-only).
// This is called when the MemTable is full and ready for flush.
func (m *MemTable) MarkImmutable() {
	m.immutable.Store(true)
}

// IsImmutable returns true if this MemTable is immutable.
func (m *MemTable) IsImmutable() bool {
	return m.immutable.Load()
}

// memTableIterator wraps SkipListIterator to implement types.Iterator.
type memTableIterator struct {
	iter *SkipListIterator
}

// Valid returns true if the iterator is positioned at a valid entry.
func (it *memTableIterator) Valid() bool {
	return it.iter.Valid()
}

// Key returns the current key.
func (it *memTableIterator) Key() []byte {
	return it.iter.Key()
}

// Value returns the current value.
func (it *memTableIterator) Value() []byte {
	return it.iter.Value()
}

// Entry returns the current entry with full metadata.
func (it *memTableIterator) Entry() *types.Entry {
	return it.iter.Entry()
}

// Next advances the iterator to the next entry.
func (it *memTableIterator) Next() {
	it.iter.Next()
}

// Seek positions the iterator at the first key >= target.
func (it *memTableIterator) Seek(target []byte) {
	it.iter.Seek(target)
}

// SeekToFirst positions the iterator at the first key.
func (it *memTableIterator) SeekToFirst() {
	it.iter.SeekToFirst()
}

// SeekToLast positions the iterator at the last key.
func (it *memTableIterator) SeekToLast() {
	it.iter.SeekToLast()
}

// Prev moves the iterator to the previous entry.
func (it *memTableIterator) Prev() {
	it.iter.Prev()
}

// Error returns any error encountered during iteration.
func (it *memTableIterator) Error() error {
	return it.iter.Error()
}

// Close releases resources held by the iterator.
func (it *memTableIterator) Close() error {
	return it.iter.Close()
}
