// Package encoding provides binary encoding utilities for RapidoDB.
// All multi-byte integers are stored in Big-Endian format for
// consistent cross-platform storage and lexicographic ordering.
package encoding

import (
	"encoding/binary"
	"hash/crc32"

	"github.com/vladgaus/RapidoDB/pkg/types"
)

// Binary encoding uses Big-Endian byte order.
// This ensures:
// 1. Cross-platform compatibility
// 2. Lexicographic ordering of encoded integers
var ByteOrder = binary.BigEndian

// Fixed-size buffer pools could be added here for performance.
// For now, we keep it simple for educational clarity.

// PutUint16 encodes a uint16 into a byte slice.
// Returns the number of bytes written (always 2).
func PutUint16(dst []byte, v uint16) int {
	ByteOrder.PutUint16(dst, v)
	return 2
}

// PutUint32 encodes a uint32 into a byte slice.
// Returns the number of bytes written (always 4).
func PutUint32(dst []byte, v uint32) int {
	ByteOrder.PutUint32(dst, v)
	return 4
}

// PutUint64 encodes a uint64 into a byte slice.
// Returns the number of bytes written (always 8).
func PutUint64(dst []byte, v uint64) int {
	ByteOrder.PutUint64(dst, v)
	return 8
}

// GetUint16 decodes a uint16 from a byte slice.
func GetUint16(src []byte) uint16 {
	return ByteOrder.Uint16(src)
}

// GetUint32 decodes a uint32 from a byte slice.
func GetUint32(src []byte) uint32 {
	return ByteOrder.Uint32(src)
}

// GetUint64 decodes a uint64 from a byte slice.
func GetUint64(src []byte) uint64 {
	return ByteOrder.Uint64(src)
}

// PutVarint encodes a variable-length integer.
// Returns the number of bytes written.
// This is useful for encoding lengths where most values are small.
func PutVarint(dst []byte, v uint64) int {
	return binary.PutUvarint(dst, v)
}

// GetVarint decodes a variable-length integer.
// Returns the value and number of bytes read.
func GetVarint(src []byte) (uint64, int) {
	return binary.Uvarint(src)
}

// VarintLen returns the number of bytes needed to encode v as a varint.
func VarintLen(v uint64) int {
	switch {
	case v < 1<<7:
		return 1
	case v < 1<<14:
		return 2
	case v < 1<<21:
		return 3
	case v < 1<<28:
		return 4
	case v < 1<<35:
		return 5
	case v < 1<<42:
		return 6
	case v < 1<<49:
		return 7
	case v < 1<<56:
		return 8
	case v < 1<<63:
		return 9
	default:
		return 10
	}
}

// EntryHeader is the fixed-size header for each entry.
// Format: Type(1) + KeyLen(4) + ValLen(4) + SeqNum(8) = 17 bytes
const EntryHeaderSize = 17

// EncodeEntry encodes an entry into a byte slice.
// Returns the encoded bytes.
//
// Format:
// +--------+--------+--------+--------+-----+-------+
// | Type   | KeyLen | ValLen | SeqNum | Key | Value |
// | 1 byte | 4 bytes| 4 bytes| 8 bytes| ... | ...   |
// +--------+--------+--------+--------+-----+-------+
func EncodeEntry(e *types.Entry) []byte {
	keyLen := len(e.Key)
	valLen := len(e.Value)
	size := EntryHeaderSize + keyLen + valLen

	buf := make([]byte, size)

	// Type (1 byte)
	buf[0] = byte(e.Type)

	// Key length (4 bytes)
	PutUint32(buf[1:5], uint32(keyLen))

	// Value length (4 bytes)
	PutUint32(buf[5:9], uint32(valLen))

	// Sequence number (8 bytes)
	PutUint64(buf[9:17], e.SeqNum)

	// Key
	copy(buf[EntryHeaderSize:], e.Key)

	// Value
	copy(buf[EntryHeaderSize+keyLen:], e.Value)

	return buf
}

// DecodeEntry decodes an entry from a byte slice.
// Returns the entry and the number of bytes consumed.
func DecodeEntry(data []byte) (*types.Entry, int, error) {
	if len(data) < EntryHeaderSize {
		return nil, 0, ErrInsufficientData
	}

	entryType := types.EntryType(data[0])
	keyLen := int(GetUint32(data[1:5]))
	valLen := int(GetUint32(data[5:9]))
	seqNum := GetUint64(data[9:17])

	totalLen := EntryHeaderSize + keyLen + valLen
	if len(data) < totalLen {
		return nil, 0, ErrInsufficientData
	}

	entry := &types.Entry{
		Type:   entryType,
		SeqNum: seqNum,
		Key:    make([]byte, keyLen),
		Value:  make([]byte, valLen),
	}

	copy(entry.Key, data[EntryHeaderSize:EntryHeaderSize+keyLen])
	copy(entry.Value, data[EntryHeaderSize+keyLen:totalLen])

	return entry, totalLen, nil
}

// EncodeEntryTo encodes an entry into the provided buffer.
// Returns the number of bytes written.
// The buffer must be large enough (use EntryEncodedSize to check).
func EncodeEntryTo(buf []byte, e *types.Entry) int {
	keyLen := len(e.Key)
	valLen := len(e.Value)

	buf[0] = byte(e.Type)
	PutUint32(buf[1:5], uint32(keyLen))
	PutUint32(buf[5:9], uint32(valLen))
	PutUint64(buf[9:17], e.SeqNum)
	copy(buf[EntryHeaderSize:], e.Key)
	copy(buf[EntryHeaderSize+keyLen:], e.Value)

	return EntryHeaderSize + keyLen + valLen
}

// EntryEncodedSize returns the encoded size of an entry.
func EntryEncodedSize(e *types.Entry) int {
	return EntryHeaderSize + len(e.Key) + len(e.Value)
}

// CRC32 checksum using Castagnoli polynomial (same as used by many storage systems).
var crc32Table = crc32.MakeTable(crc32.Castagnoli)

// Checksum computes a CRC32 checksum of the data.
func Checksum(data []byte) uint32 {
	return crc32.Checksum(data, crc32Table)
}

// VerifyChecksum verifies that the checksum matches the data.
func VerifyChecksum(data []byte, expected uint32) bool {
	return Checksum(data) == expected
}

// AppendChecksum appends a CRC32 checksum to the data.
func AppendChecksum(data []byte) []byte {
	checksum := Checksum(data)
	result := make([]byte, len(data)+4)
	copy(result, data)
	PutUint32(result[len(data):], checksum)
	return result
}

// EncodeInternalKey encodes an internal key for storage.
// Format: UserKey + SeqNum(8) + Type(1)
func EncodeInternalKey(ik types.InternalKey) []byte {
	buf := make([]byte, len(ik.UserKey)+9)
	copy(buf, ik.UserKey)
	PutUint64(buf[len(ik.UserKey):], ik.SeqNum)
	buf[len(buf)-1] = byte(ik.Type)
	return buf
}

// DecodeInternalKey decodes an internal key from storage.
func DecodeInternalKey(data []byte) (types.InternalKey, error) {
	if len(data) < 9 {
		return types.InternalKey{}, ErrInsufficientData
	}

	keyLen := len(data) - 9
	return types.InternalKey{
		UserKey: data[:keyLen],
		SeqNum:  GetUint64(data[keyLen : keyLen+8]),
		Type:    types.EntryType(data[keyLen+8]),
	}, nil
}

// ErrInsufficientData is returned when decoding fails due to insufficient data.
var ErrInsufficientData = &insufficientDataError{}

type insufficientDataError struct{}

func (e *insufficientDataError) Error() string {
	return "encoding: insufficient data"
}
