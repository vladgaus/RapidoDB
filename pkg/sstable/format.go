// Package sstable provides SSTable (Sorted String Table) implementation for RapidoDB.
//
// SSTables are immutable, sorted files that store key-value pairs. They are
// created by flushing MemTables and serve as the persistent storage layer.
//
// # File Format
//
//	+------------------+
//	| Data Block 0     |
//	+------------------+
//	| Data Block 1     |
//	+------------------+
//	| ...              |
//	+------------------+
//	| Data Block N     |
//	+------------------+
//	| Filter Block     |  (Bloom filter)
//	+------------------+
//	| Index Block      |  (Sparse index)
//	+------------------+
//	| Footer (48 bytes)|
//	+------------------+
//
// # Data Block Format
//
//	+------------------+
//	| Entry 0          |
//	+------------------+
//	| ...              |
//	+------------------+
//	| Entry N          |
//	+------------------+
//	| Restarts Array   |  (4 bytes each)
//	+------------------+
//	| Num Restarts (4B)|
//	+------------------+
//	| CRC32 (4 bytes)  |
//	+------------------+
package sstable

import (
	"encoding/binary"
	"hash/crc32"
)

// File format constants
const (
	// Magic number identifies valid SSTable files
	MagicNumber uint64 = 0x526170696454424C // "RapidTBL"

	// FormatVersion is the current SSTable format version
	FormatVersion uint32 = 1

	// FooterSize is the fixed size of the footer (48 bytes)
	FooterSize = 48

	// DefaultBlockSize is the target size for data blocks (4KB)
	DefaultBlockSize = 4 * 1024

	// DefaultRestartInterval is entries between restart points for prefix compression
	DefaultRestartInterval = 16

	// BlockTrailerSize is just CRC32 (4 bytes)
	BlockTrailerSize = 4
)

// BlockHandle points to a block within the file
type BlockHandle struct {
	Offset uint64
	Size   uint64
}

// Encode encodes the BlockHandle to bytes
func (h BlockHandle) Encode() []byte {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[0:8], h.Offset)
	binary.LittleEndian.PutUint64(buf[8:16], h.Size)
	return buf
}

// DecodeBlockHandle decodes a BlockHandle from bytes
func DecodeBlockHandle(data []byte) BlockHandle {
	return BlockHandle{
		Offset: binary.LittleEndian.Uint64(data[0:8]),
		Size:   binary.LittleEndian.Uint64(data[8:16]),
	}
}

// Footer contains SSTable metadata
type Footer struct {
	FilterHandle BlockHandle // Bloom filter block
	IndexHandle  BlockHandle // Index block
	Version      uint32
}

// Encode encodes the footer to exactly FooterSize bytes
func (f *Footer) Encode() []byte {
	buf := make([]byte, FooterSize)

	// Filter handle (16 bytes)
	binary.LittleEndian.PutUint64(buf[0:8], f.FilterHandle.Offset)
	binary.LittleEndian.PutUint64(buf[8:16], f.FilterHandle.Size)

	// Index handle (16 bytes)
	binary.LittleEndian.PutUint64(buf[16:24], f.IndexHandle.Offset)
	binary.LittleEndian.PutUint64(buf[24:32], f.IndexHandle.Size)

	// Version (4 bytes)
	binary.LittleEndian.PutUint32(buf[32:36], f.Version)

	// Padding (4 bytes) - already zeroed

	// Magic number (8 bytes)
	binary.LittleEndian.PutUint64(buf[40:48], MagicNumber)

	return buf
}

// DecodeFooter decodes a footer from bytes
func DecodeFooter(data []byte) (*Footer, error) {
	if len(data) != FooterSize {
		return nil, ErrInvalidFooter
	}

	// Verify magic number
	magic := binary.LittleEndian.Uint64(data[40:48])
	if magic != MagicNumber {
		return nil, ErrInvalidMagic
	}

	return &Footer{
		FilterHandle: BlockHandle{
			Offset: binary.LittleEndian.Uint64(data[0:8]),
			Size:   binary.LittleEndian.Uint64(data[8:16]),
		},
		IndexHandle: BlockHandle{
			Offset: binary.LittleEndian.Uint64(data[16:24]),
			Size:   binary.LittleEndian.Uint64(data[24:32]),
		},
		Version: binary.LittleEndian.Uint32(data[32:36]),
	}, nil
}

// IndexEntry represents one entry in the index block
type IndexEntry struct {
	Key         []byte      // Last key in the data block
	BlockHandle BlockHandle // Points to the data block
}

// Encode encodes an index entry
func (e *IndexEntry) Encode() []byte {
	// Format: KeyLen(4) + Key + Offset(8) + Size(8)
	buf := make([]byte, 4+len(e.Key)+16)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(e.Key)))
	copy(buf[4:4+len(e.Key)], e.Key)
	offset := 4 + len(e.Key)
	binary.LittleEndian.PutUint64(buf[offset:offset+8], e.BlockHandle.Offset)
	binary.LittleEndian.PutUint64(buf[offset+8:offset+16], e.BlockHandle.Size)
	return buf
}

// DecodeIndexEntry decodes an index entry, returns entry and bytes consumed
func DecodeIndexEntry(data []byte) (*IndexEntry, int, error) {
	if len(data) < 20 { // Minimum: KeyLen(4) + Offset(8) + Size(8)
		return nil, 0, ErrCorruption
	}

	keyLen := int(binary.LittleEndian.Uint32(data[0:4]))
	if 4+keyLen+16 > len(data) {
		return nil, 0, ErrCorruption
	}

	key := make([]byte, keyLen)
	copy(key, data[4:4+keyLen])

	offset := 4 + keyLen
	return &IndexEntry{
		Key: key,
		BlockHandle: BlockHandle{
			Offset: binary.LittleEndian.Uint64(data[offset : offset+8]),
			Size:   binary.LittleEndian.Uint64(data[offset+8 : offset+16]),
		},
	}, offset + 16, nil
}

// CRC32 table using Castagnoli polynomial (hardware accelerated on modern CPUs)
var crcTable = crc32.MakeTable(crc32.Castagnoli)

// computeCRC computes CRC32 checksum
func computeCRC(data []byte) uint32 {
	return crc32.Checksum(data, crcTable)
}

// SSTable errors
var (
	ErrInvalidFooter = &sstError{"invalid footer"}
	ErrInvalidMagic  = &sstError{"invalid magic number"}
	ErrInvalidBlock  = &sstError{"invalid block"}
	ErrKeyNotFound   = &sstError{"key not found"}
	ErrCorruption    = &sstError{"data corruption"}
	ErrClosed        = &sstError{"sstable closed"}
)

type sstError struct {
	msg string
}

func (e *sstError) Error() string {
	return "sstable: " + e.msg
}
