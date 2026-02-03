// Package types defines core data types used throughout RapidoDB.
// These types form the foundation of the storage engine's data model.
package types

import (
	"bytes"
	"fmt"
)

// EntryType represents the type of an entry in the storage engine.
type EntryType byte

const (
	// EntryTypePut represents a normal key-value entry.
	EntryTypePut EntryType = iota

	// EntryTypeDelete represents a tombstone (deleted key).
	EntryTypeDelete
)

// String returns a human-readable representation of the entry type.
func (t EntryType) String() string {
	switch t {
	case EntryTypePut:
		return "PUT"
	case EntryTypeDelete:
		return "DELETE"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", t)
	}
}

// Entry represents a single key-value entry in the storage engine.
// This is the fundamental unit of data in RapidoDB.
//
// Entry format in memory and on disk:
// +----------+----------+----------+----------+----------+
// | Type (1) | KeyLen(4)| ValLen(4)| Key(...) | Val(...) |
// +----------+----------+----------+----------+----------+
//
// For MVCC support, entries also carry a sequence number (timestamp).
type Entry struct {
	// Key is the user-provided key.
	Key []byte

	// Value is the user-provided value. Nil for delete tombstones.
	Value []byte

	// Type indicates whether this is a Put or Delete.
	Type EntryType

	// SeqNum is the sequence number for MVCC ordering.
	// Higher sequence numbers are newer.
	SeqNum uint64
}

// NewEntry creates a new Put entry with the given key and value.
func NewEntry(key, value []byte) *Entry {
	return &Entry{
		Key:   key,
		Value: value,
		Type:  EntryTypePut,
	}
}

// NewTombstone creates a new Delete entry (tombstone) for the given key.
func NewTombstone(key []byte) *Entry {
	return &Entry{
		Key:   key,
		Value: nil,
		Type:  EntryTypeDelete,
	}
}

// Size returns the total size of the entry in bytes.
// This is used for memory accounting and flush decisions.
func (e *Entry) Size() int64 {
	// Type(1) + KeyLen(4) + ValLen(4) + SeqNum(8) + Key + Value
	return 1 + 4 + 4 + 8 + int64(len(e.Key)) + int64(len(e.Value))
}

// IsDeleted returns true if this entry is a tombstone.
func (e *Entry) IsDeleted() bool {
	return e.Type == EntryTypeDelete
}

// Clone creates a deep copy of the entry.
func (e *Entry) Clone() *Entry {
	clone := &Entry{
		Type:   e.Type,
		SeqNum: e.SeqNum,
	}

	if e.Key != nil {
		clone.Key = make([]byte, len(e.Key))
		copy(clone.Key, e.Key)
	}

	if e.Value != nil {
		clone.Value = make([]byte, len(e.Value))
		copy(clone.Value, e.Value)
	}

	return clone
}

// InternalKey combines a user key with sequence number and type.
// This is used internally for ordering entries with the same user key.
//
// InternalKey format:
// +-------------------+----------+----------+
// | UserKey (...)     | SeqNum(8)| Type (1) |
// +-------------------+----------+----------+
//
// Keys are ordered by:
// 1. User key (ascending, lexicographic)
// 2. Sequence number (descending - newer first)
// 3. Type (descending - Delete before Put for same seqnum)
type InternalKey struct {
	UserKey []byte
	SeqNum  uint64
	Type    EntryType
}

// NewInternalKey creates a new internal key.
func NewInternalKey(userKey []byte, seqNum uint64, entryType EntryType) InternalKey {
	return InternalKey{
		UserKey: userKey,
		SeqNum:  seqNum,
		Type:    entryType,
	}
}

// Compare compares two internal keys.
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
func (a InternalKey) Compare(b InternalKey) int {
	// First compare user keys
	cmp := bytes.Compare(a.UserKey, b.UserKey)
	if cmp != 0 {
		return cmp
	}

	// Same user key: higher sequence number comes first (descending)
	if a.SeqNum > b.SeqNum {
		return -1
	}
	if a.SeqNum < b.SeqNum {
		return 1
	}

	// Same sequence: Delete comes before Put (descending type)
	if a.Type > b.Type {
		return -1
	}
	if a.Type < b.Type {
		return 1
	}

	return 0
}

// KeyRange represents a range of keys [Start, End).
// Used for SSTable metadata and compaction decisions.
type KeyRange struct {
	Start []byte // Inclusive
	End   []byte // Exclusive (nil means unbounded)
}

// NewKeyRange creates a new key range.
func NewKeyRange(start, end []byte) KeyRange {
	return KeyRange{Start: start, End: end}
}

// Contains returns true if the key is within this range.
func (r KeyRange) Contains(key []byte) bool {
	if bytes.Compare(key, r.Start) < 0 {
		return false
	}
	if r.End != nil && bytes.Compare(key, r.End) >= 0 {
		return false
	}
	return true
}

// Overlaps returns true if this range overlaps with another range.
func (r KeyRange) Overlaps(other KeyRange) bool {
	// r.Start < other.End AND other.Start < r.End
	if other.End != nil && bytes.Compare(r.Start, other.End) >= 0 {
		return false
	}
	if r.End != nil && bytes.Compare(other.Start, r.End) >= 0 {
		return false
	}
	return true
}

// FileMetadata holds metadata about an SSTable file.
// This information is stored in the MANIFEST and used for compaction decisions.
type FileMetadata struct {
	// FileNum is the unique identifier for this file.
	FileNum uint64

	// Level is the LSM level this file belongs to.
	// Level 0 is special: files may overlap.
	Level int

	// Size is the file size in bytes.
	Size int64

	// KeyRange is the range of keys in this file.
	KeyRange KeyRange

	// EntryCount is the number of entries in this file.
	EntryCount int64

	// SmallestSeqNum is the smallest sequence number in this file.
	SmallestSeqNum uint64

	// LargestSeqNum is the largest sequence number in this file.
	LargestSeqNum uint64

	// CreatedAt is when this file was created (Unix timestamp).
	CreatedAt int64
}

// TableID generates a consistent table ID from file number.
func (m *FileMetadata) TableID() string {
	return fmt.Sprintf("%06d.sst", m.FileNum)
}
