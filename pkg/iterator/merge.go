package iterator

import (
	"bytes"

	"github.com/rapidodb/rapidodb/pkg/types"
)

// MergeIterator merges multiple sorted iterators into one.
//
// Key ordering: When multiple iterators have the same key, entries are
// ordered by (key ASC, seqNum DESC), so the newest version comes first.
//
// Deduplication: By default, only the newest visible version of each key
// is returned. Older versions and tombstones are skipped.
//
// Usage:
//
//	iter := NewMergeIterator(sources, opts)
//	defer iter.Close()
//	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
//	    fmt.Printf("%s = %s\n", iter.Key(), iter.Value())
//	}
type MergeIterator struct {
	sources []Source
	opts    Options
	heap    *mergeHeap
	dir     Direction

	// Current position
	current *types.Entry
	prevKey []byte // Used for deduplication
	err     error
}

// NewMergeIterator creates a merge iterator from multiple sources.
func NewMergeIterator(sources []Source, opts Options) *MergeIterator {
	return &MergeIterator{
		sources: sources,
		opts:    opts,
		heap:    newMergeHeap(len(sources)),
		dir:     Forward,
	}
}

// Valid returns true if positioned at a valid entry.
func (m *MergeIterator) Valid() bool {
	return m.current != nil && m.err == nil
}

// Key returns the current key.
func (m *MergeIterator) Key() []byte {
	if m.current == nil {
		return nil
	}
	return m.current.Key
}

// Value returns the current value.
func (m *MergeIterator) Value() []byte {
	if m.current == nil {
		return nil
	}
	return m.current.Value
}

// Entry returns the full entry.
func (m *MergeIterator) Entry() *types.Entry {
	return m.current
}

// SeekToFirst positions at the first entry.
func (m *MergeIterator) SeekToFirst() {
	m.dir = Forward
	m.heap.clear()
	m.prevKey = nil // Reset dedup tracking

	for i := range m.sources {
		m.sources[i].Iter.SeekToFirst()
		if m.sources[i].Iter.Valid() {
			m.heap.push(m.makeItem(i))
		}
	}

	m.findNextForward()
}

// SeekToLast positions at the last entry.
func (m *MergeIterator) SeekToLast() {
	m.dir = Backward
	m.heap.clear()
	m.prevKey = nil // Reset dedup tracking

	for i := range m.sources {
		m.sources[i].Iter.SeekToLast()
		if m.sources[i].Iter.Valid() {
			m.heap.push(m.makeItem(i))
		}
	}

	m.findNextBackward()
}

// Seek positions at first entry >= target.
func (m *MergeIterator) Seek(target []byte) {
	m.dir = Forward
	m.heap.clear()
	m.prevKey = nil // Reset dedup tracking

	for i := range m.sources {
		m.sources[i].Iter.Seek(target)
		if m.sources[i].Iter.Valid() {
			m.heap.push(m.makeItem(i))
		}
	}

	m.findNextForward()
}

// SeekForPrev positions at last entry <= target.
func (m *MergeIterator) SeekForPrev(target []byte) {
	m.dir = Backward
	m.heap.clear()
	m.prevKey = nil // Reset dedup tracking

	for i := range m.sources {
		m.sources[i].Iter.SeekForPrev(target)
		if m.sources[i].Iter.Valid() {
			m.heap.push(m.makeItem(i))
		}
	}

	m.findNextBackward()
}

// Next advances to the next entry.
func (m *MergeIterator) Next() {
	if m.dir == Backward {
		// Switch direction: need to reposition
		m.switchToForward()
		return
	}
	m.findNextForward()
}

// Prev moves to the previous entry.
func (m *MergeIterator) Prev() {
	if m.dir == Forward {
		// Switch direction: need to reposition
		m.switchToBackward()
		return
	}
	m.findNextBackward()
}

// Error returns any error encountered.
func (m *MergeIterator) Error() error {
	return m.err
}

// Close releases resources.
func (m *MergeIterator) Close() error {
	var firstErr error
	for i := range m.sources {
		if err := m.sources[i].Iter.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// findNextForward finds the next valid entry going forward.
func (m *MergeIterator) findNextForward() {
	m.current = nil

	for m.heap.len() > 0 {
		item := m.heap.pop()
		entry := m.sources[item.sourceIdx].Iter.Entry()

		if entry == nil {
			continue
		}

		// Check visibility (MVCC)
		if m.opts.SnapshotSeq > 0 && entry.SeqNum > m.opts.SnapshotSeq {
			// Entry is newer than snapshot, skip and advance source
			m.advanceSourceForward(item.sourceIdx)
			continue
		}

		// Check for duplicate key (from same or different sources)
		if m.prevKey != nil && bytes.Equal(entry.Key, m.prevKey) {
			// Same key, skip (we already returned the newest version)
			m.advanceSourceForward(item.sourceIdx)
			continue
		}

		// Check tombstone
		if entry.Type == types.EntryTypeDelete && !m.opts.IncludeTombstones {
			// Skip tombstone, but remember key for dedup
			m.prevKey = append(m.prevKey[:0], entry.Key...)
			// Advance this source first
			m.advanceSourceForward(item.sourceIdx)
			// Skip any other versions of this key from other sources
			m.skipAllVersionsForward(entry.Key)
			continue
		}

		// Valid entry found
		m.current = entry
		m.prevKey = append(m.prevKey[:0], entry.Key...)

		// Skip older versions of this key in heap
		m.skipOlderVersionsForward(entry.Key)

		// Advance this source for next iteration
		m.advanceSourceForward(item.sourceIdx)
		return
	}
}

// findNextBackward finds the next valid entry going backward.
func (m *MergeIterator) findNextBackward() {
	m.current = nil

	for m.heap.len() > 0 {
		item := m.heap.pop()
		entry := m.sources[item.sourceIdx].Iter.Entry()

		if entry == nil {
			continue
		}

		// Check visibility (MVCC)
		if m.opts.SnapshotSeq > 0 && entry.SeqNum > m.opts.SnapshotSeq {
			m.advanceSourceBackward(item.sourceIdx)
			continue
		}

		// Check for duplicate key
		if m.prevKey != nil && bytes.Equal(entry.Key, m.prevKey) {
			m.advanceSourceBackward(item.sourceIdx)
			continue
		}

		// Check tombstone
		if entry.Type == types.EntryTypeDelete && !m.opts.IncludeTombstones {
			m.prevKey = append(m.prevKey[:0], entry.Key...)
			m.advanceSourceBackward(item.sourceIdx)
			m.skipAllVersionsBackward(entry.Key)
			continue
		}

		// Valid entry found
		m.current = entry
		m.prevKey = append(m.prevKey[:0], entry.Key...)
		m.skipOlderVersionsBackward(entry.Key)
		m.advanceSourceBackward(item.sourceIdx)
		return
	}
}

// skipOlderVersionsForward removes older versions of key from heap.
func (m *MergeIterator) skipOlderVersionsForward(key []byte) {
	for m.heap.len() > 0 {
		top := m.heap.peek()
		entry := m.sources[top.sourceIdx].Iter.Entry()
		if entry == nil || !bytes.Equal(entry.Key, key) {
			break
		}
		m.heap.pop()
		m.advanceSourceForward(top.sourceIdx)
	}
}

// skipOlderVersionsBackward removes older versions of key from heap.
func (m *MergeIterator) skipOlderVersionsBackward(key []byte) {
	for m.heap.len() > 0 {
		top := m.heap.peek()
		entry := m.sources[top.sourceIdx].Iter.Entry()
		if entry == nil || !bytes.Equal(entry.Key, key) {
			break
		}
		m.heap.pop()
		m.advanceSourceBackward(top.sourceIdx)
	}
}

// skipAllVersionsForward skips all versions of a key (used for tombstones).
func (m *MergeIterator) skipAllVersionsForward(key []byte) {
	for m.heap.len() > 0 {
		top := m.heap.peek()
		entry := m.sources[top.sourceIdx].Iter.Entry()
		if entry == nil || !bytes.Equal(entry.Key, key) {
			break
		}
		m.heap.pop()
		m.advanceSourceForward(top.sourceIdx)
	}
}

// skipAllVersionsBackward skips all versions of a key (used for tombstones).
func (m *MergeIterator) skipAllVersionsBackward(key []byte) {
	for m.heap.len() > 0 {
		top := m.heap.peek()
		entry := m.sources[top.sourceIdx].Iter.Entry()
		if entry == nil || !bytes.Equal(entry.Key, key) {
			break
		}
		m.heap.pop()
		m.advanceSourceBackward(top.sourceIdx)
	}
}

// advanceSourceForward moves a source forward and re-adds to heap.
func (m *MergeIterator) advanceSourceForward(idx int) {
	m.sources[idx].Iter.Next()
	if m.sources[idx].Iter.Valid() {
		m.heap.push(m.makeItem(idx))
	}
}

// advanceSourceBackward moves a source backward and re-adds to heap.
func (m *MergeIterator) advanceSourceBackward(idx int) {
	m.sources[idx].Iter.Prev()
	if m.sources[idx].Iter.Valid() {
		m.heap.push(m.makeItem(idx))
	}
}

// switchToForward repositions heap for forward iteration.
func (m *MergeIterator) switchToForward() {
	if m.current == nil {
		m.SeekToFirst()
		return
	}

	key := m.current.Key
	m.dir = Forward
	m.heap.clear()

	// Position all sources at first entry > current key
	for i := range m.sources {
		m.sources[i].Iter.Seek(key)
		// Skip past entries with the same key
		for m.sources[i].Iter.Valid() {
			if bytes.Compare(m.sources[i].Iter.Key(), key) > 0 {
				break
			}
			m.sources[i].Iter.Next()
		}
		if m.sources[i].Iter.Valid() {
			m.heap.push(m.makeItem(i))
		}
	}

	m.findNextForward()
}

// switchToBackward repositions heap for backward iteration.
func (m *MergeIterator) switchToBackward() {
	if m.current == nil {
		m.SeekToLast()
		return
	}

	key := m.current.Key
	m.dir = Backward
	m.heap.clear()

	// Position all sources at last entry < current key
	for i := range m.sources {
		m.sources[i].Iter.SeekForPrev(key)
		// Skip past entries with the same key
		for m.sources[i].Iter.Valid() {
			if bytes.Compare(m.sources[i].Iter.Key(), key) < 0 {
				break
			}
			m.sources[i].Iter.Prev()
		}
		if m.sources[i].Iter.Valid() {
			m.heap.push(m.makeItem(i))
		}
	}

	m.findNextBackward()
}

// makeItem creates a heap item for a source.
func (m *MergeIterator) makeItem(sourceIdx int) *heapItem {
	entry := m.sources[sourceIdx].Iter.Entry()
	return &heapItem{
		key:       entry.Key,
		seqNum:    entry.SeqNum,
		sourceIdx: sourceIdx,
	}
}

// heapItem represents an entry in the merge heap.
type heapItem struct {
	key       []byte
	seqNum    uint64
	sourceIdx int
}

// mergeHeap is a min-heap for merge iteration.
// Ordering: (key ASC, seqNum DESC) - smallest key first, newest version first.
type mergeHeap struct {
	items   []*heapItem
	reverse bool // If true, use max-heap for reverse iteration
}

func newMergeHeap(capacity int) *mergeHeap {
	return &mergeHeap{
		items: make([]*heapItem, 0, capacity),
	}
}

func (h *mergeHeap) len() int {
	return len(h.items)
}

func (h *mergeHeap) clear() {
	h.items = h.items[:0]
}

func (h *mergeHeap) less(i, j int) bool {
	cmp := compareKeys(h.items[i].key, h.items[j].key)

	if h.reverse {
		// Max-heap: larger key first
		if cmp != 0 {
			return cmp > 0
		}
		// Same key: higher seqNum (newer) first
		return h.items[i].seqNum > h.items[j].seqNum
	}

	// Min-heap: smaller key first
	if cmp != 0 {
		return cmp < 0
	}
	// Same key: higher seqNum (newer) first
	return h.items[i].seqNum > h.items[j].seqNum
}

func (h *mergeHeap) swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *mergeHeap) push(item *heapItem) {
	h.items = append(h.items, item)
	h.up(len(h.items) - 1)
}

func (h *mergeHeap) pop() *heapItem {
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

func (h *mergeHeap) peek() *heapItem {
	if len(h.items) == 0 {
		return nil
	}
	return h.items[0]
}

func (h *mergeHeap) up(i int) {
	for {
		parent := (i - 1) / 2
		if parent == i || !h.less(i, parent) {
			break
		}
		h.swap(parent, i)
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
		if right := left + 1; right < n && h.less(right, left) {
			j = right
		}
		if !h.less(j, i) {
			break
		}
		h.swap(i, j)
		i = j
	}
}
