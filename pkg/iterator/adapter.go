package iterator

import (
	"github.com/vladgaus/RapidoDB/pkg/types"
)

// Adapter wraps any iterator that has Entry() method to implement our Iterator interface.
// This is useful for wrapping memtable and sstable iterators.
type Adapter struct {
	inner interface {
		Valid() bool
		Key() []byte
		Value() []byte
		Entry() *types.Entry
		Next()
		Prev()
		Seek(target []byte)
		SeekToFirst()
		SeekToLast()
		Error() error
		Close() error
	}
}

// Wrap creates an Adapter for an existing iterator.
func Wrap(inner interface {
	Valid() bool
	Key() []byte
	Value() []byte
	Entry() *types.Entry
	Next()
	Prev()
	Seek(target []byte)
	SeekToFirst()
	SeekToLast()
	Error() error
	Close() error
}) Iterator {
	return &Adapter{inner: inner}
}

func (a *Adapter) Valid() bool         { return a.inner.Valid() }
func (a *Adapter) Key() []byte         { return a.inner.Key() }
func (a *Adapter) Value() []byte       { return a.inner.Value() }
func (a *Adapter) Entry() *types.Entry { return a.inner.Entry() }
func (a *Adapter) Next()               { a.inner.Next() }
func (a *Adapter) Prev()               { a.inner.Prev() }
func (a *Adapter) Seek(target []byte)  { a.inner.Seek(target) }
func (a *Adapter) SeekToFirst()        { a.inner.SeekToFirst() }
func (a *Adapter) SeekToLast()         { a.inner.SeekToLast() }
func (a *Adapter) Error() error        { return a.inner.Error() }
func (a *Adapter) Close() error        { return a.inner.Close() }

// SeekForPrev positions at last entry <= target.
// Default implementation: seek to target, then back up if needed.
func (a *Adapter) SeekForPrev(target []byte) {
	a.inner.Seek(target)
	if a.inner.Valid() {
		// If we landed past target, go back
		if compareKeys(a.inner.Key(), target) > 0 {
			a.inner.Prev()
		}
	} else {
		// Seek went past end, go to last
		a.inner.SeekToLast()
	}
}

// TypesIteratorAdapter wraps a types.Iterator (without Entry method).
type TypesIteratorAdapter struct {
	inner types.Iterator
}

// WrapTypesIterator wraps a types.Iterator.
func WrapTypesIterator(inner types.Iterator) Iterator {
	return &TypesIteratorAdapter{inner: inner}
}

func (t *TypesIteratorAdapter) Valid() bool        { return t.inner.Valid() }
func (t *TypesIteratorAdapter) Key() []byte        { return t.inner.Key() }
func (t *TypesIteratorAdapter) Value() []byte      { return t.inner.Value() }
func (t *TypesIteratorAdapter) Next()              { t.inner.Next() }
func (t *TypesIteratorAdapter) Prev()              { t.inner.Prev() }
func (t *TypesIteratorAdapter) Seek(target []byte) { t.inner.Seek(target) }
func (t *TypesIteratorAdapter) SeekToFirst()       { t.inner.SeekToFirst() }
func (t *TypesIteratorAdapter) SeekToLast()        { t.inner.SeekToLast() }
func (t *TypesIteratorAdapter) Error() error       { return t.inner.Error() }
func (t *TypesIteratorAdapter) Close() error       { return t.inner.Close() }

// Entry returns nil since types.Iterator doesn't have Entry method.
// For MVCC to work, use Adapter with a concrete iterator that has Entry().
func (t *TypesIteratorAdapter) Entry() *types.Entry {
	// Create an entry from key/value
	if !t.inner.Valid() {
		return nil
	}
	return &types.Entry{
		Key:   t.inner.Key(),
		Value: t.inner.Value(),
		Type:  types.EntryTypePut,
	}
}

// SeekForPrev positions at last entry <= target.
func (t *TypesIteratorAdapter) SeekForPrev(target []byte) {
	t.inner.Seek(target)
	if t.inner.Valid() {
		if compareKeys(t.inner.Key(), target) > 0 {
			t.inner.Prev()
		}
	} else {
		t.inner.SeekToLast()
	}
}

// CollectAll collects all key-value pairs from an iterator.
// Useful for testing and debugging.
func CollectAll(iter Iterator) (keys, values [][]byte) {
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		keys = append(keys, append([]byte{}, iter.Key()...))
		values = append(values, append([]byte{}, iter.Value()...))
	}
	return
}

// CollectKeys collects all keys from an iterator.
func CollectKeys(iter Iterator) [][]byte {
	var keys [][]byte //nolint:prealloc // count unknown before iteration
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		keys = append(keys, append([]byte{}, iter.Key()...))
	}
	return keys
}

// Count counts the number of entries in an iterator.
func Count(iter Iterator) int {
	count := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		count++
	}
	return count
}
