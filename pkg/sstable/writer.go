package sstable

import (
	"bufio"
	"encoding/binary"
	"os"

	"github.com/rapidodb/rapidodb/pkg/types"
)

// Writer builds an SSTable file from sorted entries.
//
// Usage:
//
//	w, _ := NewWriter(path, opts)
//	for _, entry := range entries {
//	    w.Add(entry)
//	}
//	w.Finish()
//
// Entries MUST be added in sorted order (by InternalKey comparison).
type Writer struct {
	file   *os.File
	writer *bufio.Writer
	path   string

	// Block building
	dataBlock    *BlockBuilder
	indexEntries []*IndexEntry
	offset       uint64

	// Filter (bloom filter keys)
	filterKeys [][]byte

	// Options
	blockSize       int
	restartInterval int

	// Stats
	entryCount uint64
	minKey     []byte
	maxKey     []byte

	// State
	finished bool
	err      error
}

// WriterOptions configures the SSTable writer.
type WriterOptions struct {
	BlockSize       int // Target data block size (default: 4KB)
	RestartInterval int // Entries between restart points (default: 16)
}

// DefaultWriterOptions returns sensible defaults.
func DefaultWriterOptions() WriterOptions {
	return WriterOptions{
		BlockSize:       DefaultBlockSize,
		RestartInterval: DefaultRestartInterval,
	}
}

// NewWriter creates a new SSTable writer.
func NewWriter(path string, opts WriterOptions) (*Writer, error) {
	if opts.BlockSize <= 0 {
		opts.BlockSize = DefaultBlockSize
	}
	if opts.RestartInterval <= 0 {
		opts.RestartInterval = DefaultRestartInterval
	}

	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	return &Writer{
		file:            file,
		writer:          bufio.NewWriterSize(file, 64*1024), // 64KB buffer
		path:            path,
		dataBlock:       NewBlockBuilder(opts.RestartInterval),
		indexEntries:    make([]*IndexEntry, 0, 128),
		filterKeys:      make([][]byte, 0, 1024),
		blockSize:       opts.BlockSize,
		restartInterval: opts.RestartInterval,
	}, nil
}

// Add adds an entry to the SSTable.
// Entries MUST be added in sorted order.
func (w *Writer) Add(entry *types.Entry) error {
	if w.finished {
		return ErrClosed
	}
	if w.err != nil {
		return w.err
	}

	// Track min/max keys
	if w.entryCount == 0 {
		w.minKey = append([]byte{}, entry.Key...)
	}
	w.maxKey = append(w.maxKey[:0], entry.Key...)

	// Collect key for bloom filter (must copy since entry may be reused)
	keyCopy := make([]byte, len(entry.Key))
	copy(keyCopy, entry.Key)
	w.filterKeys = append(w.filterKeys, keyCopy)

	// Try to add to current block
	if !w.dataBlock.Add(entry) {
		// Block is full, flush it
		if err := w.flushDataBlock(); err != nil {
			w.err = err
			return err
		}

		// Start new block and add entry
		w.dataBlock.Reset()
		// This should always succeed for an empty block
		w.dataBlock.Add(entry)
	}

	w.entryCount++
	return nil
}

// flushDataBlock writes the current data block to file.
func (w *Writer) flushDataBlock() error {
	if w.dataBlock.IsEmpty() {
		return nil
	}

	// Get the last key before finishing the block
	lastKey := make([]byte, len(w.dataBlock.LastKey()))
	copy(lastKey, w.dataBlock.LastKey())

	// Finish and write block
	blockData := w.dataBlock.Finish()
	blockOffset := w.offset

	n, err := w.writer.Write(blockData)
	if err != nil {
		return err
	}
	w.offset += uint64(n)

	// Add index entry
	w.indexEntries = append(w.indexEntries, &IndexEntry{
		Key: lastKey,
		BlockHandle: BlockHandle{
			Offset: blockOffset,
			Size:   uint64(len(blockData)),
		},
	})

	return nil
}

// writeFilterBlock writes the bloom filter block.
// Returns the block handle.
func (w *Writer) writeFilterBlock() (BlockHandle, error) {
	handle := BlockHandle{Offset: w.offset, Size: 0}

	if len(w.filterKeys) == 0 {
		return handle, nil
	}

	// Build bloom filter
	// Using 10 bits per key which gives ~1% false positive rate
	bitsPerKey := 10
	numBits := len(w.filterKeys) * bitsPerKey
	if numBits < 64 {
		numBits = 64
	}
	// Round up to byte boundary - critical for matching read/write numBits
	numBytes := (numBits + 7) / 8
	numBits = numBytes * 8             // Use actual bits available
	filter := make([]byte, numBytes+1) // +1 for number of hash functions

	// Number of hash functions (k ≈ 0.69 * m/n)
	k := uint8((numBits / len(w.filterKeys)) * 69 / 100)
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	filter[len(filter)-1] = k

	// Add keys to filter using double hashing
	for _, key := range w.filterKeys {
		h := bloomHash(key)
		delta := (h >> 17) | (h << 15) // Rotate right 17 bits
		for j := uint8(0); j < k; j++ {
			bitpos := h % uint32(numBits)
			filter[bitpos/8] |= 1 << (bitpos % 8)
			h += delta
		}
	}

	// Write filter with CRC
	crc := computeCRC(filter)
	crcBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(crcBuf, crc)

	n1, err := w.writer.Write(filter)
	if err != nil {
		return handle, err
	}
	n2, err := w.writer.Write(crcBuf)
	if err != nil {
		return handle, err
	}

	handle.Size = uint64(n1 + n2)
	w.offset += handle.Size

	return handle, nil
}

// writeIndexBlock writes the index block.
// Returns the block handle.
func (w *Writer) writeIndexBlock() (BlockHandle, error) {
	handle := BlockHandle{Offset: w.offset, Size: 0}

	if len(w.indexEntries) == 0 {
		return handle, nil
	}

	// Encode all index entries
	var buf []byte
	for _, entry := range w.indexEntries {
		buf = append(buf, entry.Encode()...)
	}

	// Add CRC
	crc := computeCRC(buf)
	crcBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(crcBuf, crc)
	buf = append(buf, crcBuf...)

	n, err := w.writer.Write(buf)
	if err != nil {
		return handle, err
	}

	handle.Size = uint64(n)
	w.offset += handle.Size

	return handle, nil
}

// Finish completes the SSTable and closes the file.
// Returns metadata about the created SSTable.
func (w *Writer) Finish() (*Metadata, error) {
	if w.finished {
		return nil, ErrClosed
	}
	w.finished = true

	if w.err != nil {
		w.file.Close()
		os.Remove(w.path)
		return nil, w.err
	}

	// Flush any remaining data block
	if err := w.flushDataBlock(); err != nil {
		w.file.Close()
		os.Remove(w.path)
		return nil, err
	}

	// Write filter block
	filterHandle, err := w.writeFilterBlock()
	if err != nil {
		w.file.Close()
		os.Remove(w.path)
		return nil, err
	}

	// Write index block
	indexHandle, err := w.writeIndexBlock()
	if err != nil {
		w.file.Close()
		os.Remove(w.path)
		return nil, err
	}

	// Write footer
	footer := &Footer{
		FilterHandle: filterHandle,
		IndexHandle:  indexHandle,
		Version:      FormatVersion,
	}
	if _, err := w.writer.Write(footer.Encode()); err != nil {
		w.file.Close()
		os.Remove(w.path)
		return nil, err
	}

	// Flush and sync
	if err := w.writer.Flush(); err != nil {
		w.file.Close()
		os.Remove(w.path)
		return nil, err
	}
	if err := w.file.Sync(); err != nil {
		w.file.Close()
		os.Remove(w.path)
		return nil, err
	}
	if err := w.file.Close(); err != nil {
		os.Remove(w.path)
		return nil, err
	}

	// Build metadata
	meta := &Metadata{
		FileSize:   w.offset + FooterSize,
		EntryCount: w.entryCount,
		MinKey:     w.minKey,
		MaxKey:     w.maxKey,
		BlockCount: uint64(len(w.indexEntries)),
	}

	return meta, nil
}

// Abort cancels SSTable creation and removes the partial file.
func (w *Writer) Abort() error {
	if w.finished {
		return nil
	}
	w.finished = true
	w.file.Close()
	return os.Remove(w.path)
}

// EstimatedSize returns the estimated final file size.
func (w *Writer) EstimatedSize() uint64 {
	return w.offset + uint64(w.dataBlock.EstimatedSize()) + FooterSize
}

// Metadata contains information about a completed SSTable.
type Metadata struct {
	FileSize   uint64
	EntryCount uint64
	MinKey     []byte
	MaxKey     []byte
	BlockCount uint64
}

// bloomHash computes a hash for bloom filter.
// Uses a variant of MurmurHash.
func bloomHash(key []byte) uint32 {
	const (
		seed = 0xbc9f1d34
		m    = 0xc6a4a793
	)
	h := uint32(seed) ^ uint32(len(key))*m
	for len(key) >= 4 {
		h += uint32(key[0]) | uint32(key[1])<<8 | uint32(key[2])<<16 | uint32(key[3])<<24
		h *= m
		h ^= h >> 16
		key = key[4:]
	}
	switch len(key) {
	case 3:
		h += uint32(key[2]) << 16
		fallthrough
	case 2:
		h += uint32(key[1]) << 8
		fallthrough
	case 1:
		h += uint32(key[0])
		h *= m
		h ^= h >> 24
	}
	return h
}
