package types

import (
	"bytes"
	"testing"
)

func TestEntryType(t *testing.T) {
	if EntryTypePut.String() != "PUT" {
		t.Errorf("expected PUT, got %s", EntryTypePut.String())
	}

	if EntryTypeDelete.String() != "DELETE" {
		t.Errorf("expected DELETE, got %s", EntryTypeDelete.String())
	}

	// Unknown type
	unknown := EntryType(99)
	if unknown.String() == "" {
		t.Error("unknown type should have a string representation")
	}
}

func TestNewEntry(t *testing.T) {
	key := []byte("test-key")
	value := []byte("test-value")

	entry := NewEntry(key, value)

	if entry.Type != EntryTypePut {
		t.Errorf("expected EntryTypePut, got %v", entry.Type)
	}

	if !bytes.Equal(entry.Key, key) {
		t.Error("key mismatch")
	}

	if !bytes.Equal(entry.Value, value) {
		t.Error("value mismatch")
	}
}

func TestNewTombstone(t *testing.T) {
	key := []byte("deleted-key")

	entry := NewTombstone(key)

	if entry.Type != EntryTypeDelete {
		t.Errorf("expected EntryTypeDelete, got %v", entry.Type)
	}

	if !bytes.Equal(entry.Key, key) {
		t.Error("key mismatch")
	}

	if entry.Value != nil {
		t.Error("tombstone value should be nil")
	}

	if !entry.IsDeleted() {
		t.Error("IsDeleted() should return true for tombstone")
	}
}

func TestEntrySize(t *testing.T) {
	tests := []struct {
		key      []byte
		value    []byte
		expected int64
	}{
		{[]byte("k"), []byte("v"), 1 + 4 + 4 + 8 + 1 + 1},
		{[]byte("key"), []byte("value"), 1 + 4 + 4 + 8 + 3 + 5},
		{make([]byte, 100), make([]byte, 1000), 1 + 4 + 4 + 8 + 100 + 1000},
		{[]byte("k"), nil, 1 + 4 + 4 + 8 + 1 + 0},
	}

	for i, tt := range tests {
		entry := &Entry{Key: tt.key, Value: tt.value}
		if entry.Size() != tt.expected {
			t.Errorf("test %d: expected size %d, got %d", i, tt.expected, entry.Size())
		}
	}
}

func TestEntryClone(t *testing.T) {
	original := &Entry{
		Key:    []byte("key"),
		Value:  []byte("value"),
		Type:   EntryTypePut,
		SeqNum: 42,
	}

	clone := original.Clone()

	// Verify values match
	if !bytes.Equal(clone.Key, original.Key) {
		t.Error("cloned key doesn't match")
	}

	if !bytes.Equal(clone.Value, original.Value) {
		t.Error("cloned value doesn't match")
	}

	if clone.Type != original.Type {
		t.Error("cloned type doesn't match")
	}

	if clone.SeqNum != original.SeqNum {
		t.Error("cloned seqnum doesn't match")
	}

	// Verify it's a deep copy
	clone.Key[0] = 'x'
	if original.Key[0] == 'x' {
		t.Error("clone is not a deep copy")
	}
}

func TestInternalKeyCompare(t *testing.T) {
	tests := []struct {
		a        InternalKey
		b        InternalKey
		expected int
	}{
		// Different user keys
		{
			NewInternalKey([]byte("aaa"), 100, EntryTypePut),
			NewInternalKey([]byte("bbb"), 100, EntryTypePut),
			-1,
		},
		{
			NewInternalKey([]byte("bbb"), 100, EntryTypePut),
			NewInternalKey([]byte("aaa"), 100, EntryTypePut),
			1,
		},
		// Same user key, different sequence (higher comes first)
		{
			NewInternalKey([]byte("key"), 100, EntryTypePut),
			NewInternalKey([]byte("key"), 50, EntryTypePut),
			-1,
		},
		{
			NewInternalKey([]byte("key"), 50, EntryTypePut),
			NewInternalKey([]byte("key"), 100, EntryTypePut),
			1,
		},
		// Same user key and sequence, different type
		{
			NewInternalKey([]byte("key"), 100, EntryTypeDelete),
			NewInternalKey([]byte("key"), 100, EntryTypePut),
			-1,
		},
		// Identical
		{
			NewInternalKey([]byte("key"), 100, EntryTypePut),
			NewInternalKey([]byte("key"), 100, EntryTypePut),
			0,
		},
	}

	for i, tt := range tests {
		result := tt.a.Compare(tt.b)
		if result != tt.expected {
			t.Errorf("test %d: expected %d, got %d", i, tt.expected, result)
		}
	}
}

func TestKeyRangeContains(t *testing.T) {
	// Range [b, e)
	r := NewKeyRange([]byte("b"), []byte("e"))

	tests := []struct {
		key      []byte
		expected bool
	}{
		{[]byte("a"), false},
		{[]byte("b"), true},
		{[]byte("c"), true},
		{[]byte("d"), true},
		{[]byte("e"), false},
		{[]byte("f"), false},
	}

	for _, tt := range tests {
		result := r.Contains(tt.key)
		if result != tt.expected {
			t.Errorf("Contains(%s): expected %v, got %v", tt.key, tt.expected, result)
		}
	}

	// Unbounded range [b, ∞)
	unbounded := NewKeyRange([]byte("b"), nil)
	if !unbounded.Contains([]byte("z")) {
		t.Error("unbounded range should contain 'z'")
	}
}

func TestKeyRangeOverlaps(t *testing.T) {
	tests := []struct {
		r1       KeyRange
		r2       KeyRange
		expected bool
	}{
		// Overlapping
		{NewKeyRange([]byte("a"), []byte("d")), NewKeyRange([]byte("c"), []byte("f")), true},
		{NewKeyRange([]byte("c"), []byte("f")), NewKeyRange([]byte("a"), []byte("d")), true},
		// One contains the other
		{NewKeyRange([]byte("a"), []byte("z")), NewKeyRange([]byte("m"), []byte("n")), true},
		// Adjacent (not overlapping)
		{NewKeyRange([]byte("a"), []byte("c")), NewKeyRange([]byte("c"), []byte("e")), false},
		// Disjoint
		{NewKeyRange([]byte("a"), []byte("b")), NewKeyRange([]byte("x"), []byte("z")), false},
		// Unbounded
		{NewKeyRange([]byte("a"), nil), NewKeyRange([]byte("x"), []byte("z")), true},
	}

	for i, tt := range tests {
		result := tt.r1.Overlaps(tt.r2)
		if result != tt.expected {
			t.Errorf("test %d: expected %v, got %v", i, tt.expected, result)
		}
	}
}

func TestFileMetadataTableID(t *testing.T) {
	meta := &FileMetadata{
		FileNum: 123,
	}

	expected := "000123.sst"
	if meta.TableID() != expected {
		t.Errorf("expected %s, got %s", expected, meta.TableID())
	}
}

func BenchmarkInternalKeyCompare(b *testing.B) {
	k1 := NewInternalKey([]byte("user-key-12345"), 100, EntryTypePut)
	k2 := NewInternalKey([]byte("user-key-12345"), 50, EntryTypePut)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k1.Compare(k2)
	}
}

func BenchmarkKeyRangeContains(b *testing.B) {
	r := NewKeyRange([]byte("aaaa"), []byte("zzzz"))
	key := []byte("mmmm")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Contains(key)
	}
}
