package sstable

import (
	"bytes"
	"encoding/binary"

	"github.com/vladgaus/RapidoDB/internal/encoding"
	"github.com/vladgaus/RapidoDB/pkg/types"
)

// BlockBuilder builds a data block with restart points for binary search.
//
// Block format:
//
//	+------------------+
//	| Entry 0          |  (full key)
//	+------------------+
//	| Entry 1          |  (full key - no prefix compression for simplicity)
//	+------------------+
//	| ...              |
//	+------------------+
//	| Entry N          |
//	+------------------+
//	| Restart[0] (4B)  |  offset to entry 0
//	+------------------+
//	| Restart[1] (4B)  |  offset to entry at restart interval
//	+------------------+
//	| ...              |
//	+------------------+
//	| NumRestarts (4B) |
//	+------------------+
//	| CRC32 (4B)       |
//	+------------------+
type BlockBuilder struct {
	buf             []byte
	restarts        []uint32
	restartInterval int
	counter         int // entries since last restart
	entryCount      int
	lastKey         []byte
	finished        bool
}

// NewBlockBuilder creates a new block builder.
func NewBlockBuilder(restartInterval int) *BlockBuilder {
	if restartInterval < 1 {
		restartInterval = DefaultRestartInterval
	}
	return &BlockBuilder{
		buf:             make([]byte, 0, DefaultBlockSize),
		restarts:        []uint32{0}, // First entry is always a restart point
		restartInterval: restartInterval,
	}
}

// Add adds an entry to the block.
// Entries MUST be added in sorted order (by InternalKey comparison).
// Returns true if entry was added, false if block is full.
func (b *BlockBuilder) Add(entry *types.Entry) bool {
	if b.finished {
		return false
	}

	// Encode the entry
	encoded := encoding.EncodeEntry(entry)

	// Check if we have room (leave space for restarts + trailer)
	neededSpace := len(encoded) + (len(b.restarts)+1)*4 + 4 + BlockTrailerSize

	// If block already has entries and adding this would exceed limit, reject
	if b.entryCount > 0 && len(b.buf)+neededSpace > DefaultBlockSize {
		return false
	}

	// Track restart points
	if b.counter >= b.restartInterval {
		b.restarts = append(b.restarts, uint32(len(b.buf)))
		b.counter = 0
	}

	// Append entry
	b.buf = append(b.buf, encoded...)
	b.lastKey = append(b.lastKey[:0], entry.Key...)
	b.entryCount++
	b.counter++

	return true
}

// Finish finalizes the block and returns the complete data.
func (b *BlockBuilder) Finish() []byte {
	if b.finished {
		return b.buf
	}
	b.finished = true

	// Append restart array
	for _, r := range b.restarts {
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, r)
		b.buf = append(b.buf, tmp...)
	}

	// Append number of restarts
	tmp := make([]byte, 4)
	binary.LittleEndian.PutUint32(tmp, uint32(len(b.restarts)))
	b.buf = append(b.buf, tmp...)

	// Compute and append CRC
	crc := computeCRC(b.buf)
	binary.LittleEndian.PutUint32(tmp, crc)
	b.buf = append(b.buf, tmp...)

	return b.buf
}

// Reset resets the builder for reuse.
func (b *BlockBuilder) Reset() {
	b.buf = b.buf[:0]
	b.restarts = b.restarts[:1]
	b.restarts[0] = 0
	b.counter = 0
	b.entryCount = 0
	b.lastKey = b.lastKey[:0]
	b.finished = false
}

// EstimatedSize returns current size plus overhead estimate.
func (b *BlockBuilder) EstimatedSize() int {
	return len(b.buf) + len(b.restarts)*4 + 4 + BlockTrailerSize
}

// EntryCount returns number of entries added.
func (b *BlockBuilder) EntryCount() int {
	return b.entryCount
}

// LastKey returns the last key added.
func (b *BlockBuilder) LastKey() []byte {
	return b.lastKey
}

// IsEmpty returns true if no entries added.
func (b *BlockBuilder) IsEmpty() bool {
	return b.entryCount == 0
}

// BlockReader reads entries from a data block.
type BlockReader struct {
	data        []byte   // Block content (without CRC)
	restarts    []uint32 // Restart point offsets
	numRestarts int
	current     int // Current position in data
}

// NewBlockReader creates a reader for block data.
// Verifies CRC and parses restart points.
func NewBlockReader(data []byte) (*BlockReader, error) {
	if len(data) < 8 { // Minimum: NumRestarts(4) + CRC(4)
		return nil, ErrInvalidBlock
	}

	// Verify CRC (last 4 bytes)
	contentLen := len(data) - BlockTrailerSize
	content := data[:contentLen]
	storedCRC := binary.LittleEndian.Uint32(data[contentLen:])
	if computeCRC(content) != storedCRC {
		return nil, ErrCorruption
	}

	// Parse number of restarts (4 bytes before CRC)
	numRestarts := int(binary.LittleEndian.Uint32(content[len(content)-4:]))
	if numRestarts < 1 {
		return nil, ErrInvalidBlock
	}

	// Parse restart array
	restartOffset := len(content) - 4 - numRestarts*4
	if restartOffset < 0 {
		return nil, ErrInvalidBlock
	}

	restarts := make([]uint32, numRestarts)
	for i := 0; i < numRestarts; i++ {
		restarts[i] = binary.LittleEndian.Uint32(content[restartOffset+i*4:])
	}

	return &BlockReader{
		data:        content[:restartOffset], // Entry data only
		restarts:    restarts,
		numRestarts: numRestarts,
		current:     0,
	}, nil
}

// SeekToFirst positions at the first entry.
func (r *BlockReader) SeekToFirst() {
	r.current = 0
}

// SeekToLast positions at the last entry.
func (r *BlockReader) SeekToLast() {
	r.current = 0
	var lastPos int
	for r.current < len(r.data) {
		lastPos = r.current
		_, n, err := encoding.DecodeEntry(r.data[r.current:])
		if err != nil {
			break
		}
		r.current += n
	}
	r.current = lastPos
}

// Seek positions at first entry with key >= target.
// Uses binary search on restart points, then linear scan.
func (r *BlockReader) Seek(target []byte) {
	// Binary search restart points
	left, right := 0, r.numRestarts-1
	for left < right {
		mid := (left + right + 1) / 2
		pos := int(r.restarts[mid])
		entry, _, err := encoding.DecodeEntry(r.data[pos:])
		if err != nil {
			right = mid - 1
			continue
		}
		if bytes.Compare(entry.Key, target) < 0 {
			left = mid
		} else {
			right = mid - 1
		}
	}

	// Start linear scan from restart point
	r.current = int(r.restarts[left])

	// Scan until we find key >= target
	for r.current < len(r.data) {
		entry, n, err := encoding.DecodeEntry(r.data[r.current:])
		if err != nil {
			break
		}
		if bytes.Compare(entry.Key, target) >= 0 {
			return
		}
		r.current += n
	}
}

// Valid returns true if positioned at a valid entry.
func (r *BlockReader) Valid() bool {
	return r.current < len(r.data)
}

// Entry returns the current entry.
func (r *BlockReader) Entry() (*types.Entry, error) {
	if r.current >= len(r.data) {
		return nil, nil
	}
	entry, _, err := encoding.DecodeEntry(r.data[r.current:])
	return entry, err
}

// Key returns current key.
func (r *BlockReader) Key() []byte {
	entry, err := r.Entry()
	if err != nil || entry == nil {
		return nil
	}
	return entry.Key
}

// Value returns current value.
func (r *BlockReader) Value() []byte {
	entry, err := r.Entry()
	if err != nil || entry == nil {
		return nil
	}
	return entry.Value
}

// Next advances to the next entry.
func (r *BlockReader) Next() {
	if r.current >= len(r.data) {
		return
	}
	_, n, err := encoding.DecodeEntry(r.data[r.current:])
	if err != nil {
		r.current = len(r.data)
		return
	}
	r.current += n
}

// Prev moves to the previous entry (slow - linear scan from start).
func (r *BlockReader) Prev() {
	if r.current <= 0 {
		r.current = -1 // Invalid
		return
	}

	target := r.current
	r.current = 0
	var prevPos int

	for r.current < target {
		prevPos = r.current
		_, n, err := encoding.DecodeEntry(r.data[r.current:])
		if err != nil {
			break
		}
		r.current += n
	}
	r.current = prevPos
}
