// Package wal provides Write-Ahead Log functionality for RapidoDB.
//
// The WAL ensures durability by persisting all writes before they are
// applied to the MemTable. On crash recovery, the WAL is replayed to
// restore the MemTable to its pre-crash state.
//
// # Record Format
//
// Each record in the WAL has the following format:
//
//	+------------+-------------+------------+--- ... ---+
//	| CRC (4B)   | Length (4B) | Type (1B)  | Payload   |
//	+------------+-------------+------------+--- ... ---+
//
// - CRC: CRC32 checksum of Length + Type + Payload
// - Length: Length of the payload in bytes (uint32)
// - Type: Record type (Full, First, Middle, Last)
// - Payload: The actual entry data
//
// # Block Structure
//
// The WAL is divided into fixed-size blocks (default 32KB) to simplify
// recovery. Records may span multiple blocks using fragmentation:
//
//   - Full: Complete record fits in one block
//   - First: First fragment of a record
//   - Middle: Middle fragment(s) of a record
//   - Last: Final fragment of a record
//
// This design is inspired by LevelDB/RocksDB WAL format.
package wal

import (
	"encoding/binary"
	"hash/crc32"
)

// Block size for WAL files (32KB like LevelDB)
const BlockSize = 32 * 1024

// Header size: CRC(4) + Length(4) + Type(1) = 9 bytes
const HeaderSize = 9

// Maximum payload size that fits in a single block
const MaxPayloadSize = BlockSize - HeaderSize

// RecordType indicates how a record is stored across blocks.
type RecordType byte

const (
	// RecordTypeFull indicates a complete record in a single fragment.
	RecordTypeFull RecordType = 1

	// RecordTypeFirst indicates the first fragment of a record.
	RecordTypeFirst RecordType = 2

	// RecordTypeMiddle indicates a middle fragment of a record.
	RecordTypeMiddle RecordType = 3

	// RecordTypeLast indicates the last fragment of a record.
	RecordTypeLast RecordType = 4

	// RecordTypeZero is used for pre-allocated/zeroed regions.
	RecordTypeZero RecordType = 0
)

// String returns a human-readable record type name.
func (t RecordType) String() string {
	switch t {
	case RecordTypeFull:
		return "FULL"
	case RecordTypeFirst:
		return "FIRST"
	case RecordTypeMiddle:
		return "MIDDLE"
	case RecordTypeLast:
		return "LAST"
	case RecordTypeZero:
		return "ZERO"
	default:
		return "UNKNOWN"
	}
}

// CRC32 table using Castagnoli polynomial (same as used in SSTable)
var crcTable = crc32.MakeTable(crc32.Castagnoli)

// computeCRC computes CRC32 checksum for a record.
// The checksum covers: type byte + payload
func computeCRC(recordType RecordType, payload []byte) uint32 {
	crc := crc32.New(crcTable)
	crc.Write([]byte{byte(recordType)})
	crc.Write(payload)
	return crc.Sum32()
}

// encodeHeader encodes the record header into the provided buffer.
// Buffer must be at least HeaderSize bytes.
func encodeHeader(buf []byte, crc uint32, length uint32, recordType RecordType) {
	binary.LittleEndian.PutUint32(buf[0:4], crc)
	binary.LittleEndian.PutUint32(buf[4:8], length)
	buf[8] = byte(recordType)
}

// decodeHeader decodes a record header from the buffer.
// Returns crc, length, recordType.
func decodeHeader(buf []byte) (uint32, uint32, RecordType) {
	crc := binary.LittleEndian.Uint32(buf[0:4])
	length := binary.LittleEndian.Uint32(buf[4:8])
	recordType := RecordType(buf[8])
	return crc, length, recordType
}
