package sstable

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"sync"

	"github.com/rapidodb/rapidodb/pkg/bloom"
	"github.com/rapidodb/rapidodb/pkg/types"
)

// Reader reads entries from an SSTable file.
//
// Reader supports:
// - Point lookups with bloom filter optimization
// - Range scans via iterator
// - Concurrent reads (thread-safe)
type Reader struct {
	mu sync.RWMutex

	file   *os.File
	path   string
	size   int64
	footer *Footer

	// Cached index entries
	indexEntries []*IndexEntry

	// Bloom filter
	bloomFilter *bloom.Filter

	closed bool
}

// OpenReader opens an SSTable file for reading.
func OpenReader(path string) (*Reader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	// Get file size
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	size := info.Size()

	if size < FooterSize {
		file.Close()
		return nil, ErrInvalidFooter
	}

	// Read footer
	footerBuf := make([]byte, FooterSize)
	_, err = file.ReadAt(footerBuf, size-FooterSize)
	if err != nil {
		file.Close()
		return nil, err
	}

	footer, err := DecodeFooter(footerBuf)
	if err != nil {
		file.Close()
		return nil, err
	}

	r := &Reader{
		file:   file,
		path:   path,
		size:   size,
		footer: footer,
	}

	// Load index
	if err := r.loadIndex(); err != nil {
		file.Close()
		return nil, err
	}

	// Load filter (optional - don't fail if missing)
	r.loadFilter()

	return r, nil
}

// loadIndex reads and parses the index block.
func (r *Reader) loadIndex() error {
	if r.footer.IndexHandle.Size == 0 {
		return nil
	}

	// Read index block
	buf := make([]byte, r.footer.IndexHandle.Size)
	_, err := r.file.ReadAt(buf, int64(r.footer.IndexHandle.Offset))
	if err != nil {
		return err
	}

	// Verify CRC (last 4 bytes)
	if len(buf) < 4 {
		return ErrCorruption
	}
	content := buf[:len(buf)-4]
	storedCRC := binary.LittleEndian.Uint32(buf[len(buf)-4:])
	if computeCRC(content) != storedCRC {
		return ErrCorruption
	}

	// Parse index entries
	offset := 0
	for offset < len(content) {
		entry, n, err := DecodeIndexEntry(content[offset:])
		if err != nil {
			return err
		}
		r.indexEntries = append(r.indexEntries, entry)
		offset += n
	}

	return nil
}

// loadFilter reads the bloom filter block.
func (r *Reader) loadFilter() {
	if r.footer.FilterHandle.Size == 0 {
		return
	}

	buf := make([]byte, r.footer.FilterHandle.Size)
	_, err := r.file.ReadAt(buf, int64(r.footer.FilterHandle.Offset))
	if err != nil {
		return
	}

	// Verify CRC (last 4 bytes)
	if len(buf) < 5 { // At least 1 byte of filter data + CRC(4)
		return
	}
	content := buf[:len(buf)-4]
	storedCRC := binary.LittleEndian.Uint32(buf[len(buf)-4:])
	if computeCRC(content) != storedCRC {
		return
	}

	// Decode bloom filter
	filter, err := bloom.Decode(content)
	if err != nil {
		return
	}
	r.bloomFilter = filter
}

// mayContain checks bloom filter for key.
func (r *Reader) mayContain(key []byte) bool {
	if r.bloomFilter == nil {
		return true // No filter, assume key may exist
	}
	return r.bloomFilter.MayContain(key)
}

// Get looks up a key and returns the entry if found.
// Returns nil if not found (or if deleted).
func (r *Reader) Get(key []byte) (*types.Entry, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.closed {
		return nil, ErrClosed
	}

	// Check bloom filter first
	if !r.mayContain(key) {
		return nil, nil
	}

	// Binary search index to find candidate block
	blockIdx := r.findBlock(key)
	if blockIdx < 0 {
		return nil, nil
	}

	// Read and search the block
	entry, err := r.searchBlock(blockIdx, key)
	if err != nil {
		return nil, err
	}

	return entry, nil
}

// findBlock returns the index of the block that may contain key.
// Returns -1 if key is outside the range of all blocks.
func (r *Reader) findBlock(key []byte) int {
	// Binary search for first block where lastKey >= key
	left, right := 0, len(r.indexEntries)-1

	for left <= right {
		mid := (left + right) / 2
		cmp := bytes.Compare(r.indexEntries[mid].Key, key)
		if cmp < 0 {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	if left >= len(r.indexEntries) {
		return -1
	}
	return left
}

// searchBlock searches for key within a specific block.
func (r *Reader) searchBlock(blockIdx int, key []byte) (*types.Entry, error) {
	if blockIdx < 0 || blockIdx >= len(r.indexEntries) {
		return nil, nil
	}

	// Read block data
	handle := r.indexEntries[blockIdx].BlockHandle
	buf := make([]byte, handle.Size)
	_, err := r.file.ReadAt(buf, int64(handle.Offset))
	if err != nil {
		return nil, err
	}

	// Create block reader
	br, err := NewBlockReader(buf)
	if err != nil {
		return nil, err
	}

	// Seek to key
	br.Seek(key)
	if !br.Valid() {
		return nil, nil
	}

	// Check if we found exact match
	entry, err := br.Entry()
	if err != nil {
		return nil, err
	}
	if entry != nil && bytes.Equal(entry.Key, key) {
		return entry, nil
	}

	return nil, nil
}

// NewIterator returns an iterator over all entries.
func (r *Reader) NewIterator() *Iterator {
	return &Iterator{
		reader:   r,
		blockIdx: -1,
	}
}

// Close closes the reader.
func (r *Reader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil
	}
	r.closed = true
	return r.file.Close()
}

// Size returns the file size in bytes.
func (r *Reader) Size() int64 {
	return r.size
}

// EntryCount returns estimated entry count (from footer/index).
func (r *Reader) EntryCount() int {
	count := 0
	for range r.indexEntries {
		count++ // Rough estimate: assume some entries per block
	}
	return count * 100 // Very rough estimate
}

// MinKey returns the minimum key in the SSTable.
func (r *Reader) MinKey() []byte {
	if len(r.indexEntries) == 0 {
		return nil
	}
	// Read first block and get first key
	r.mu.RLock()
	defer r.mu.RUnlock()

	handle := r.indexEntries[0].BlockHandle
	buf := make([]byte, handle.Size)
	_, err := r.file.ReadAt(buf, int64(handle.Offset))
	if err != nil {
		return nil
	}

	br, err := NewBlockReader(buf)
	if err != nil {
		return nil
	}

	br.SeekToFirst()
	return br.Key()
}

// MaxKey returns the maximum key in the SSTable.
func (r *Reader) MaxKey() []byte {
	if len(r.indexEntries) == 0 {
		return nil
	}
	return r.indexEntries[len(r.indexEntries)-1].Key
}

// Iterator iterates over SSTable entries in sorted order.
type Iterator struct {
	reader      *Reader
	blockIdx    int
	blockReader *BlockReader
	err         error
}

// Valid returns true if positioned at a valid entry.
func (it *Iterator) Valid() bool {
	return it.err == nil && it.blockReader != nil && it.blockReader.Valid()
}

// Key returns the current key.
func (it *Iterator) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return it.blockReader.Key()
}

// Value returns the current value.
func (it *Iterator) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return it.blockReader.Value()
}

// Entry returns the current entry.
func (it *Iterator) Entry() *types.Entry {
	if !it.Valid() {
		return nil
	}
	entry, err := it.blockReader.Entry()
	if err != nil {
		it.err = err
		return nil
	}
	return entry
}

// SeekToFirst positions at the first entry.
func (it *Iterator) SeekToFirst() {
	it.reader.mu.RLock()
	defer it.reader.mu.RUnlock()

	if it.reader.closed {
		it.err = ErrClosed
		return
	}

	if len(it.reader.indexEntries) == 0 {
		it.blockReader = nil
		return
	}

	it.blockIdx = 0
	it.loadBlock()
	if it.blockReader != nil {
		it.blockReader.SeekToFirst()
	}
}

// SeekToLast positions at the last entry.
func (it *Iterator) SeekToLast() {
	it.reader.mu.RLock()
	defer it.reader.mu.RUnlock()

	if it.reader.closed {
		it.err = ErrClosed
		return
	}

	if len(it.reader.indexEntries) == 0 {
		it.blockReader = nil
		return
	}

	it.blockIdx = len(it.reader.indexEntries) - 1
	it.loadBlock()
	if it.blockReader != nil {
		it.blockReader.SeekToLast()
	}
}

// Seek positions at first entry >= target.
func (it *Iterator) Seek(target []byte) {
	it.reader.mu.RLock()
	defer it.reader.mu.RUnlock()

	if it.reader.closed {
		it.err = ErrClosed
		return
	}

	// Find the block
	it.blockIdx = it.reader.findBlock(target)
	if it.blockIdx < 0 {
		it.blockReader = nil
		return
	}

	it.loadBlock()
	if it.blockReader != nil {
		it.blockReader.Seek(target)
		// If not found in this block, try next
		if !it.blockReader.Valid() {
			it.nextBlock()
		}
	}
}

// Next advances to the next entry.
func (it *Iterator) Next() {
	if !it.Valid() {
		return
	}

	it.reader.mu.RLock()
	defer it.reader.mu.RUnlock()

	it.blockReader.Next()
	if !it.blockReader.Valid() {
		it.nextBlock()
	}
}

// Prev moves to the previous entry.
func (it *Iterator) Prev() {
	if it.blockReader == nil {
		return
	}

	it.reader.mu.RLock()
	defer it.reader.mu.RUnlock()

	it.blockReader.Prev()
	if it.blockReader.current < 0 {
		it.prevBlock()
	}
}

// Error returns any error encountered.
func (it *Iterator) Error() error {
	return it.err
}

// Close releases iterator resources.
func (it *Iterator) Close() error {
	it.blockReader = nil
	return nil
}

// loadBlock loads the current block.
func (it *Iterator) loadBlock() {
	if it.blockIdx < 0 || it.blockIdx >= len(it.reader.indexEntries) {
		it.blockReader = nil
		return
	}

	handle := it.reader.indexEntries[it.blockIdx].BlockHandle
	buf := make([]byte, handle.Size)
	_, err := it.reader.file.ReadAt(buf, int64(handle.Offset))
	if err != nil && err != io.EOF {
		it.err = err
		it.blockReader = nil
		return
	}

	br, err := NewBlockReader(buf)
	if err != nil {
		it.err = err
		it.blockReader = nil
		return
	}

	it.blockReader = br
}

// nextBlock advances to the next block.
func (it *Iterator) nextBlock() {
	it.blockIdx++
	if it.blockIdx >= len(it.reader.indexEntries) {
		it.blockReader = nil
		return
	}
	it.loadBlock()
	if it.blockReader != nil {
		it.blockReader.SeekToFirst()
	}
}

// prevBlock moves to the previous block.
func (it *Iterator) prevBlock() {
	it.blockIdx--
	if it.blockIdx < 0 {
		it.blockReader = nil
		return
	}
	it.loadBlock()
	if it.blockReader != nil {
		it.blockReader.SeekToLast()
	}
}
