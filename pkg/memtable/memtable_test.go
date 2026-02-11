package memtable

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/vladgaus/RapidoDB/pkg/errors"
)

func TestMemTableBasicOperations(t *testing.T) {
	mt := NewMemTable(1, 64*1024*1024) // 64MB

	// Test empty memtable
	if !mt.IsEmpty() {
		t.Error("new memtable should be empty")
	}

	// Put
	err := mt.Put([]byte("key1"), []byte("value1"), 1)
	if err != nil {
		t.Fatalf("put failed: %v", err)
	}

	// Verify not empty
	if mt.IsEmpty() {
		t.Error("memtable should not be empty after put")
	}

	// Get
	entry, err := mt.Get([]byte("key1"), 1)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}

	if !bytes.Equal(entry.Value, []byte("value1")) {
		t.Errorf("value mismatch: expected value1, got %s", entry.Value)
	}

	// Get non-existent key
	_, err = mt.Get([]byte("nonexistent"), 100)
	if !errors.IsNotFound(err) {
		t.Errorf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestMemTableDelete(t *testing.T) {
	mt := NewMemTable(1, 64*1024*1024)

	// Put then delete
	mt.Put([]byte("key"), []byte("value"), 1)
	mt.Delete([]byte("key"), 2)

	// At seqNum 1, key should exist
	entry, err := mt.Get([]byte("key"), 1)
	if err != nil {
		t.Fatalf("get at seqNum 1 failed: %v", err)
	}
	if entry.IsDeleted() {
		t.Error("entry should not be deleted at seqNum 1")
	}

	// At seqNum 2, key should be tombstone
	entry, err = mt.Get([]byte("key"), 2)
	if err != nil {
		t.Fatalf("get at seqNum 2 failed: %v", err)
	}
	if !entry.IsDeleted() {
		t.Error("entry should be deleted at seqNum 2")
	}
}

func TestMemTableMVCC(t *testing.T) {
	mt := NewMemTable(1, 64*1024*1024)

	// Insert multiple versions
	mt.Put([]byte("key"), []byte("v1"), 1)
	mt.Put([]byte("key"), []byte("v2"), 5)
	mt.Put([]byte("key"), []byte("v3"), 10)

	tests := []struct {
		seqNum   uint64
		expected string
		wantErr  bool
	}{
		{0, "", true},     // No version visible
		{1, "v1", false},  // v1 visible
		{4, "v1", false},  // Still v1
		{5, "v2", false},  // v2 visible
		{9, "v2", false},  // Still v2
		{10, "v3", false}, // v3 visible
		{100, "v3", false},
	}

	for _, tt := range tests {
		entry, err := mt.Get([]byte("key"), tt.seqNum)
		if tt.wantErr {
			if err == nil {
				t.Errorf("seqNum=%d: expected error, got entry %v", tt.seqNum, entry)
			}
		} else {
			if err != nil {
				t.Errorf("seqNum=%d: unexpected error: %v", tt.seqNum, err)
			} else if string(entry.Value) != tt.expected {
				t.Errorf("seqNum=%d: expected %s, got %s", tt.seqNum, tt.expected, entry.Value)
			}
		}
	}
}

func TestMemTableImmutable(t *testing.T) {
	mt := NewMemTable(1, 64*1024*1024)

	// Initially mutable
	if mt.IsImmutable() {
		t.Error("new memtable should be mutable")
	}

	// Put should succeed
	err := mt.Put([]byte("key1"), []byte("value1"), 1)
	if err != nil {
		t.Fatalf("put failed: %v", err)
	}

	// Mark immutable
	mt.MarkImmutable()

	if !mt.IsImmutable() {
		t.Error("memtable should be immutable after MarkImmutable")
	}

	// Put should fail
	err = mt.Put([]byte("key2"), []byte("value2"), 2)
	if err != errors.ErrWriteStall {
		t.Errorf("expected ErrWriteStall, got %v", err)
	}

	// Delete should fail
	err = mt.Delete([]byte("key1"), 3)
	if err != errors.ErrWriteStall {
		t.Errorf("expected ErrWriteStall, got %v", err)
	}

	// Get should still work
	entry, err := mt.Get([]byte("key1"), 1)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if !bytes.Equal(entry.Value, []byte("value1")) {
		t.Error("get returned wrong value")
	}
}

func TestMemTableShouldFlush(t *testing.T) {
	maxSize := int64(1000) // Small size for testing
	mt := NewMemTable(1, maxSize)

	if mt.ShouldFlush() {
		t.Error("empty memtable should not need flush")
	}

	// Add entries until we exceed max size
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		value := []byte("value-that-is-reasonably-long-to-fill-space")
		mt.Put(key, value, uint64(i+1))

		if mt.Size() >= maxSize {
			break
		}
	}

	if !mt.ShouldFlush() {
		t.Error("memtable should need flush after exceeding max size")
	}
}

func TestMemTableIterator(t *testing.T) {
	mt := NewMemTable(1, 64*1024*1024)

	// Insert in random order
	mt.Put([]byte("c"), []byte("3"), 3)
	mt.Put([]byte("a"), []byte("1"), 1)
	mt.Put([]byte("b"), []byte("2"), 2)

	// Iterate and verify order
	iter := mt.NewIterator()
	iter.SeekToFirst()

	expected := []struct {
		key   string
		value string
	}{
		{"a", "1"},
		{"b", "2"},
		{"c", "3"},
	}

	i := 0
	for iter.Valid() {
		if string(iter.Key()) != expected[i].key {
			t.Errorf("position %d: expected key %s, got %s", i, expected[i].key, iter.Key())
		}
		if string(iter.Value()) != expected[i].value {
			t.Errorf("position %d: expected value %s, got %s", i, expected[i].value, iter.Value())
		}
		iter.Next()
		i++
	}

	if i != len(expected) {
		t.Errorf("expected %d entries, got %d", len(expected), i)
	}

	// Test Seek
	iter.Seek([]byte("b"))
	if !iter.Valid() || string(iter.Key()) != "b" {
		t.Error("seek to 'b' failed")
	}

	// Test SeekToLast
	iter.SeekToLast()
	if !iter.Valid() || string(iter.Key()) != "c" {
		t.Error("seek to last failed")
	}

	iter.Close()
}

func TestMemTableValidation(t *testing.T) {
	mt := NewMemTable(1, 64*1024*1024)

	// Empty key should fail
	err := mt.Put([]byte{}, []byte("value"), 1)
	if err == nil {
		t.Error("expected error for empty key")
	}

	// Empty key delete should fail
	err = mt.Delete([]byte{}, 1)
	if err == nil {
		t.Error("expected error for empty key delete")
	}

	// Empty key get should fail
	_, err = mt.Get([]byte{}, 1)
	if err == nil {
		t.Error("expected error for empty key get")
	}

	// Key too large
	largeKey := make([]byte, errors.MaxKeySize+1)
	err = mt.Put(largeKey, []byte("value"), 1)
	if err == nil {
		t.Error("expected error for key too large")
	}

	// Value too large
	largeValue := make([]byte, errors.MaxValueSize+1)
	err = mt.Put([]byte("key"), largeValue, 1)
	if err == nil {
		t.Error("expected error for value too large")
	}
}

func TestMemTableID(t *testing.T) {
	mt := NewMemTable(42, 64*1024*1024)
	if mt.ID() != 42 {
		t.Errorf("expected ID 42, got %d", mt.ID())
	}
}

func TestMemTableEntryCount(t *testing.T) {
	mt := NewMemTable(1, 64*1024*1024)

	if mt.EntryCount() != 0 {
		t.Error("new memtable should have 0 entries")
	}

	for i := 0; i < 100; i++ {
		mt.Put([]byte(fmt.Sprintf("key%d", i)), []byte("value"), uint64(i+1))
	}

	if mt.EntryCount() != 100 {
		t.Errorf("expected 100 entries, got %d", mt.EntryCount())
	}
}

// Benchmarks

func BenchmarkMemTablePut(b *testing.B) {
	mt := NewMemTable(1, 1024*1024*1024) // 1GB

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key%010d", i))
		mt.Put(key, []byte("value"), uint64(i+1))
	}
}

func BenchmarkMemTableGet(b *testing.B) {
	mt := NewMemTable(1, 1024*1024*1024)

	// Pre-populate
	for i := 0; i < 100000; i++ {
		key := []byte(fmt.Sprintf("key%010d", i))
		mt.Put(key, []byte("value"), uint64(i+1))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key%010d", i%100000))
		mt.Get(key, 100001)
	}
}

func BenchmarkMemTableIteratorScan(b *testing.B) {
	mt := NewMemTable(1, 1024*1024*1024)

	// Pre-populate
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("key%010d", i))
		mt.Put(key, []byte("value"), uint64(i+1))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter := mt.NewIterator()
		iter.SeekToFirst()
		for iter.Valid() {
			_ = iter.Key()
			_ = iter.Value()
			iter.Next()
		}
		iter.Close()
	}
}
