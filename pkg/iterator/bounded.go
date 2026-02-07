package iterator

import (
	"bytes"

	"github.com/rapidodb/rapidodb/pkg/types"
)

// BoundedIterator wraps an iterator with range bounds.
// It filters entries outside [LowerBound, UpperBound).
type BoundedIterator struct {
	inner      Iterator
	lowerBound []byte // Inclusive
	upperBound []byte // Exclusive
	valid      bool
}

// NewBoundedIterator creates a bounded iterator.
func NewBoundedIterator(inner Iterator, lower, upper []byte) *BoundedIterator {
	return &BoundedIterator{
		inner:      inner,
		lowerBound: lower,
		upperBound: upper,
	}
}

// Valid returns true if positioned at a valid entry within bounds.
func (b *BoundedIterator) Valid() bool {
	return b.valid && b.inner.Valid()
}

// Key returns the current key.
func (b *BoundedIterator) Key() []byte {
	if !b.Valid() {
		return nil
	}
	return b.inner.Key()
}

// Value returns the current value.
func (b *BoundedIterator) Value() []byte {
	if !b.Valid() {
		return nil
	}
	return b.inner.Value()
}

// Entry returns the full entry.
func (b *BoundedIterator) Entry() *types.Entry {
	if !b.Valid() {
		return nil
	}
	return b.inner.Entry()
}

// SeekToFirst positions at the first entry >= lower bound.
func (b *BoundedIterator) SeekToFirst() {
	if b.lowerBound != nil {
		b.inner.Seek(b.lowerBound)
	} else {
		b.inner.SeekToFirst()
	}
	b.checkBounds()
}

// SeekToLast positions at the last entry < upper bound.
func (b *BoundedIterator) SeekToLast() {
	if b.upperBound != nil {
		b.inner.SeekForPrev(b.upperBound)
		// SeekForPrev finds <= target, but upper bound is exclusive
		// So we might land exactly on upper bound
		for b.inner.Valid() && bytes.Compare(b.inner.Key(), b.upperBound) >= 0 {
			b.inner.Prev()
		}
	} else {
		b.inner.SeekToLast()
	}
	b.checkBoundsReverse()
}

// Seek positions at first entry >= target within bounds.
func (b *BoundedIterator) Seek(target []byte) {
	// Clamp target to lower bound
	if b.lowerBound != nil && bytes.Compare(target, b.lowerBound) < 0 {
		target = b.lowerBound
	}
	b.inner.Seek(target)
	b.checkBounds()
}

// SeekForPrev positions at last entry <= target within bounds.
func (b *BoundedIterator) SeekForPrev(target []byte) {
	// Clamp target to just below upper bound
	if b.upperBound != nil && bytes.Compare(target, b.upperBound) >= 0 {
		// Find last entry < upper bound
		b.inner.SeekForPrev(b.upperBound)
		for b.inner.Valid() && bytes.Compare(b.inner.Key(), b.upperBound) >= 0 {
			b.inner.Prev()
		}
	} else {
		b.inner.SeekForPrev(target)
	}
	b.checkBoundsReverse()
}

// Next advances to the next entry within bounds.
func (b *BoundedIterator) Next() {
	b.inner.Next()
	b.checkBounds()
}

// Prev moves to the previous entry within bounds.
func (b *BoundedIterator) Prev() {
	b.inner.Prev()
	b.checkBoundsReverse()
}

// Error returns any error encountered.
func (b *BoundedIterator) Error() error {
	return b.inner.Error()
}

// Close releases resources.
func (b *BoundedIterator) Close() error {
	return b.inner.Close()
}

// checkBounds validates the current position (forward direction).
func (b *BoundedIterator) checkBounds() {
	if !b.inner.Valid() {
		b.valid = false
		return
	}

	key := b.inner.Key()

	// Check lower bound
	if b.lowerBound != nil && bytes.Compare(key, b.lowerBound) < 0 {
		b.valid = false
		return
	}

	// Check upper bound
	if b.upperBound != nil && bytes.Compare(key, b.upperBound) >= 0 {
		b.valid = false
		return
	}

	b.valid = true
}

// checkBoundsReverse validates the current position (backward direction).
func (b *BoundedIterator) checkBoundsReverse() {
	if !b.inner.Valid() {
		b.valid = false
		return
	}

	key := b.inner.Key()

	// Check lower bound
	if b.lowerBound != nil && bytes.Compare(key, b.lowerBound) < 0 {
		b.valid = false
		return
	}

	// Check upper bound
	if b.upperBound != nil && bytes.Compare(key, b.upperBound) >= 0 {
		b.valid = false
		return
	}

	b.valid = true
}

// PrefixIterator wraps an iterator to only return keys with a given prefix.
type PrefixIterator struct {
	inner  Iterator
	prefix []byte
	valid  bool
}

// NewPrefixIterator creates a prefix iterator.
func NewPrefixIterator(inner Iterator, prefix []byte) *PrefixIterator {
	return &PrefixIterator{
		inner:  inner,
		prefix: prefix,
	}
}

// Valid returns true if positioned at a valid entry with the prefix.
func (p *PrefixIterator) Valid() bool {
	return p.valid && p.inner.Valid()
}

// Key returns the current key.
func (p *PrefixIterator) Key() []byte {
	if !p.Valid() {
		return nil
	}
	return p.inner.Key()
}

// Value returns the current value.
func (p *PrefixIterator) Value() []byte {
	if !p.Valid() {
		return nil
	}
	return p.inner.Value()
}

// Entry returns the full entry.
func (p *PrefixIterator) Entry() *types.Entry {
	if !p.Valid() {
		return nil
	}
	return p.inner.Entry()
}

// SeekToFirst positions at the first entry with the prefix.
func (p *PrefixIterator) SeekToFirst() {
	p.inner.Seek(p.prefix)
	p.checkPrefix()
}

// SeekToLast positions at the last entry with the prefix.
func (p *PrefixIterator) SeekToLast() {
	// Seek to first key >= prefix + 0xFF...
	// This is tricky - we want the last key that starts with prefix
	endKey := prefixEnd(p.prefix)
	if endKey != nil {
		p.inner.SeekForPrev(endKey)
		// May land on endKey itself or past it
		for p.inner.Valid() && bytes.Compare(p.inner.Key(), endKey) >= 0 {
			p.inner.Prev()
		}
	} else {
		// Prefix is all 0xFF, seek to end
		p.inner.SeekToLast()
	}
	p.checkPrefix()
}

// Seek positions at first entry >= target with prefix.
func (p *PrefixIterator) Seek(target []byte) {
	// Clamp to prefix start
	if bytes.Compare(target, p.prefix) < 0 {
		target = p.prefix
	}
	p.inner.Seek(target)
	p.checkPrefix()
}

// SeekForPrev positions at last entry <= target with prefix.
func (p *PrefixIterator) SeekForPrev(target []byte) {
	// Clamp to end of prefix range
	endKey := prefixEnd(p.prefix)
	if endKey != nil && bytes.Compare(target, endKey) >= 0 {
		p.SeekToLast()
		return
	}
	p.inner.SeekForPrev(target)
	p.checkPrefix()
}

// Next advances to the next entry with prefix.
func (p *PrefixIterator) Next() {
	p.inner.Next()
	p.checkPrefix()
}

// Prev moves to the previous entry with prefix.
func (p *PrefixIterator) Prev() {
	p.inner.Prev()
	p.checkPrefix()
}

// Error returns any error encountered.
func (p *PrefixIterator) Error() error {
	return p.inner.Error()
}

// Close releases resources.
func (p *PrefixIterator) Close() error {
	return p.inner.Close()
}

// checkPrefix validates that current key has the prefix.
func (p *PrefixIterator) checkPrefix() {
	if !p.inner.Valid() {
		p.valid = false
		return
	}

	key := p.inner.Key()
	p.valid = bytes.HasPrefix(key, p.prefix)
}

// prefixEnd returns the first key that doesn't have the given prefix.
// Returns nil if prefix is empty or all 0xFF bytes.
func prefixEnd(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}

	// Find last byte that isn't 0xFF and increment it
	end := make([]byte, len(prefix))
	copy(end, prefix)

	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xFF {
			end[i]++
			return end[:i+1]
		}
	}

	// All bytes are 0xFF, no end key
	return nil
}

// SnapshotIterator wraps an iterator with snapshot visibility filtering.
type SnapshotIterator struct {
	inner       Iterator
	snapshotSeq uint64
	skipDeletes bool
	valid       bool
}

// NewSnapshotIterator creates a snapshot-filtered iterator.
func NewSnapshotIterator(inner Iterator, snapshotSeq uint64, skipDeletes bool) *SnapshotIterator {
	return &SnapshotIterator{
		inner:       inner,
		snapshotSeq: snapshotSeq,
		skipDeletes: skipDeletes,
	}
}

// Valid returns true if positioned at a valid visible entry.
func (s *SnapshotIterator) Valid() bool {
	return s.valid && s.inner.Valid()
}

// Key returns the current key.
func (s *SnapshotIterator) Key() []byte {
	if !s.Valid() {
		return nil
	}
	return s.inner.Key()
}

// Value returns the current value.
func (s *SnapshotIterator) Value() []byte {
	if !s.Valid() {
		return nil
	}
	return s.inner.Value()
}

// Entry returns the full entry.
func (s *SnapshotIterator) Entry() *types.Entry {
	if !s.Valid() {
		return nil
	}
	return s.inner.Entry()
}

// SeekToFirst positions at the first visible entry.
func (s *SnapshotIterator) SeekToFirst() {
	s.inner.SeekToFirst()
	s.skipInvisible(Forward)
}

// SeekToLast positions at the last visible entry.
func (s *SnapshotIterator) SeekToLast() {
	s.inner.SeekToLast()
	s.skipInvisible(Backward)
}

// Seek positions at first visible entry >= target.
func (s *SnapshotIterator) Seek(target []byte) {
	s.inner.Seek(target)
	s.skipInvisible(Forward)
}

// SeekForPrev positions at last visible entry <= target.
func (s *SnapshotIterator) SeekForPrev(target []byte) {
	s.inner.SeekForPrev(target)
	s.skipInvisible(Backward)
}

// Next advances to the next visible entry.
func (s *SnapshotIterator) Next() {
	s.inner.Next()
	s.skipInvisible(Forward)
}

// Prev moves to the previous visible entry.
func (s *SnapshotIterator) Prev() {
	s.inner.Prev()
	s.skipInvisible(Backward)
}

// Error returns any error encountered.
func (s *SnapshotIterator) Error() error {
	return s.inner.Error()
}

// Close releases resources.
func (s *SnapshotIterator) Close() error {
	return s.inner.Close()
}

// skipInvisible moves past entries not visible to this snapshot.
func (s *SnapshotIterator) skipInvisible(dir Direction) {
	for s.inner.Valid() {
		entry := s.inner.Entry()
		if entry == nil {
			s.valid = false
			return
		}

		// Check visibility
		if entry.SeqNum > s.snapshotSeq {
			if dir == Forward {
				s.inner.Next()
			} else {
				s.inner.Prev()
			}
			continue
		}

		// Check tombstone
		if s.skipDeletes && entry.Type == types.EntryTypeDelete {
			if dir == Forward {
				s.inner.Next()
			} else {
				s.inner.Prev()
			}
			continue
		}

		s.valid = true
		return
	}

	s.valid = false
}
