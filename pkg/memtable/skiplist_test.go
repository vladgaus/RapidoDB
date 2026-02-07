package memtable

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/rapidodb/rapidodb/pkg/types"
)

func TestSkipListBasicOperations(t *testing.T) {
	sl := NewSkipList()

	// Test empty skiplist
	if !sl.IsEmpty() {
		t.Error("new skiplist should be empty")
	}

	if sl.Len() != 0 {
		t.Errorf("expected length 0, got %d", sl.Len())
	}

	// Insert entries
	entries := []*types.Entry{
		{Key: []byte("key1"), Value: []byte("value1"), Type: types.EntryTypePut, SeqNum: 1},
		{Key: []byte("key2"), Value: []byte("value2"), Type: types.EntryTypePut, SeqNum: 2},
		{Key: []byte("key3"), Value: []byte("value3"), Type: types.EntryTypePut, SeqNum: 3},
	}

	for _, e := range entries {
		sl.Put(e)
	}

	// Verify not empty
	if sl.IsEmpty() {
		t.Error("skiplist should not be empty after inserts")
	}

	if sl.Len() != 3 {
		t.Errorf("expected length 3, got %d", sl.Len())
	}

	// Test Get
	for _, e := range entries {
		got := sl.Get(e.Key, e.SeqNum)
		if got == nil {
			t.Errorf("expected to find key %s", e.Key)
			continue
		}
		if !bytes.Equal(got.Value, e.Value) {
			t.Errorf("value mismatch for key %s: expected %s, got %s", e.Key, e.Value, got.Value)
		}
	}

	// Test Get for non-existent key
	got := sl.Get([]byte("nonexistent"), 100)
	if got != nil {
		t.Error("expected nil for non-existent key")
	}
}

func TestSkipListMVCC(t *testing.T) {
	sl := NewSkipList()

	// Insert multiple versions of the same key
	sl.Put(&types.Entry{Key: []byte("key"), Value: []byte("v1"), Type: types.EntryTypePut, SeqNum: 1})
	sl.Put(&types.Entry{Key: []byte("key"), Value: []byte("v2"), Type: types.EntryTypePut, SeqNum: 5})
	sl.Put(&types.Entry{Key: []byte("key"), Value: []byte("v3"), Type: types.EntryTypePut, SeqNum: 10})

	tests := []struct {
		maxSeqNum uint64
		expected  string
	}{
		{0, ""},     // No version visible
		{1, "v1"},   // Only v1 visible
		{4, "v1"},   // Still v1
		{5, "v2"},   // v2 becomes visible
		{9, "v2"},   // Still v2
		{10, "v3"},  // v3 becomes visible
		{100, "v3"}, // Still v3
	}

	for _, tt := range tests {
		got := sl.Get([]byte("key"), tt.maxSeqNum)
		if tt.expected == "" {
			if got != nil {
				t.Errorf("maxSeqNum=%d: expected nil, got %v", tt.maxSeqNum, got)
			}
		} else {
			if got == nil {
				t.Errorf("maxSeqNum=%d: expected %s, got nil", tt.maxSeqNum, tt.expected)
			} else if string(got.Value) != tt.expected {
				t.Errorf("maxSeqNum=%d: expected %s, got %s", tt.maxSeqNum, tt.expected, got.Value)
			}
		}
	}
}

func TestSkipListTombstone(t *testing.T) {
	sl := NewSkipList()

	// Put then delete
	sl.Put(&types.Entry{Key: []byte("key"), Value: []byte("value"), Type: types.EntryTypePut, SeqNum: 1})
	sl.Put(&types.Entry{Key: []byte("key"), Value: nil, Type: types.EntryTypeDelete, SeqNum: 2})

	// At seqNum 1, key should exist
	got := sl.Get([]byte("key"), 1)
	if got == nil || got.IsDeleted() {
		t.Error("expected to find non-deleted entry at seqNum 1")
	}

	// At seqNum 2, key should be deleted (tombstone)
	got = sl.Get([]byte("key"), 2)
	if got == nil {
		t.Error("expected to find tombstone at seqNum 2")
	}
	if !got.IsDeleted() {
		t.Error("expected entry to be a tombstone")
	}
}

func TestSkipListIterator(t *testing.T) {
	sl := NewSkipList()

	// Insert entries in random order
	keys := []string{"d", "b", "e", "a", "c"}
	for i, k := range keys {
		sl.Put(&types.Entry{
			Key:    []byte(k),
			Value:  []byte(fmt.Sprintf("value-%s", k)),
			Type:   types.EntryTypePut,
			SeqNum: uint64(i + 1),
		})
	}

	// Iterate and verify sorted order
	iter := sl.NewIterator()
	iter.SeekToFirst()

	expected := []string{"a", "b", "c", "d", "e"}
	i := 0
	for iter.Valid() {
		if string(iter.Key()) != expected[i] {
			t.Errorf("position %d: expected %s, got %s", i, expected[i], iter.Key())
		}
		iter.Next()
		i++
	}

	if i != len(expected) {
		t.Errorf("expected %d entries, got %d", len(expected), i)
	}
}

func TestSkipListIteratorSeek(t *testing.T) {
	sl := NewSkipList()

	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%02d", i))
		sl.Put(&types.Entry{Key: key, Value: []byte("v"), Type: types.EntryTypePut, SeqNum: uint64(i + 1)})
	}

	iter := sl.NewIterator()

	// Seek to exact key
	iter.Seek([]byte("key05"))
	if !iter.Valid() || string(iter.Key()) != "key05" {
		t.Errorf("seek exact: expected key05, got %s", iter.Key())
	}

	// Seek to key between existing keys
	iter.Seek([]byte("key045"))
	if !iter.Valid() || string(iter.Key()) != "key05" {
		t.Errorf("seek between: expected key05, got %s", iter.Key())
	}

	// Seek past end
	iter.Seek([]byte("key99"))
	if iter.Valid() {
		t.Error("seek past end should be invalid")
	}

	// SeekToLast
	iter.SeekToLast()
	if !iter.Valid() || string(iter.Key()) != "key09" {
		t.Errorf("seek to last: expected key09, got %s", iter.Key())
	}
}

func TestSkipListIteratorSeekForPrev(t *testing.T) {
	sl := NewSkipList()

	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("key%d", i*2)) // 0, 2, 4, 6, 8
		sl.Put(&types.Entry{Key: key, Value: []byte("v"), Type: types.EntryTypePut, SeqNum: uint64(i + 1)})
	}

	iter := sl.NewIterator()

	// SeekForPrev to exact key
	iter.SeekForPrev([]byte("key4"))
	if !iter.Valid() || string(iter.Key()) != "key4" {
		t.Errorf("seekforprev exact: expected key4, got %s", iter.Key())
	}

	// SeekForPrev to key between
	iter.SeekForPrev([]byte("key5"))
	if !iter.Valid() || string(iter.Key()) != "key4" {
		t.Errorf("seekforprev between: expected key4, got %s", iter.Key())
	}

	// SeekForPrev before first key
	iter.SeekForPrev([]byte("a"))
	if iter.Valid() {
		t.Error("seekforprev before first should be invalid")
	}
}

func TestSkipListConcurrentReads(t *testing.T) {
	sl := NewSkipList()

	// Pre-populate
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%04d", i))
		sl.Put(&types.Entry{Key: key, Value: []byte("value"), Type: types.EntryTypePut, SeqNum: uint64(i + 1)})
	}

	// Concurrent reads
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				key := []byte(fmt.Sprintf("key%04d", rand.Intn(1000)))
				sl.Get(key, 1001)
			}
		}()
	}

	wg.Wait()
}

func TestSkipListConcurrentReadWrite(t *testing.T) {
	sl := NewSkipList()

	var wg sync.WaitGroup

	// Writer
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			key := []byte(fmt.Sprintf("key%04d", i))
			sl.Put(&types.Entry{Key: key, Value: []byte("value"), Type: types.EntryTypePut, SeqNum: uint64(i + 1)})
		}
	}()

	// Readers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				key := []byte(fmt.Sprintf("key%04d", rand.Intn(1000)))
				sl.Get(key, 1001)
			}
		}()
	}

	wg.Wait()
}

func TestSkipListSortedOrder(t *testing.T) {
	sl := NewSkipList()

	// Insert random keys
	r := rand.New(rand.NewSource(42))
	keys := make([]string, 100)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%08d", r.Intn(1000000))
		keys[i] = key
		sl.Put(&types.Entry{Key: []byte(key), Value: []byte("v"), Type: types.EntryTypePut, SeqNum: uint64(i + 1)})
	}

	// Sort expected keys (may have duplicates from random)
	sort.Strings(keys)

	// Verify iteration is in sorted order
	iter := sl.NewIterator()
	iter.SeekToFirst()

	var prev string
	for iter.Valid() {
		key := string(iter.Key())
		if prev != "" && key < prev {
			t.Errorf("keys not in sorted order: %s came after %s", key, prev)
		}
		prev = key
		iter.Next()
	}
}

func TestSkipListSize(t *testing.T) {
	sl := NewSkipList()

	initialSize := sl.Size()
	if initialSize != 0 {
		t.Errorf("initial size should be 0, got %d", initialSize)
	}

	// Add entries and verify size increases
	for i := 0; i < 100; i++ {
		sl.Put(&types.Entry{
			Key:    []byte(fmt.Sprintf("key%04d", i)),
			Value:  []byte("value-that-is-reasonably-long"),
			Type:   types.EntryTypePut,
			SeqNum: uint64(i + 1),
		})
	}

	if sl.Size() <= 0 {
		t.Error("size should be positive after inserts")
	}

	if sl.Len() != 100 {
		t.Errorf("expected 100 entries, got %d", sl.Len())
	}
}

// Benchmarks

func BenchmarkSkipListPut(b *testing.B) {
	sl := NewSkipList()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key%010d", i))
		sl.Put(&types.Entry{Key: key, Value: []byte("value"), Type: types.EntryTypePut, SeqNum: uint64(i + 1)})
	}
}

func BenchmarkSkipListGet(b *testing.B) {
	sl := NewSkipList()

	// Pre-populate
	for i := 0; i < 100000; i++ {
		key := []byte(fmt.Sprintf("key%010d", i))
		sl.Put(&types.Entry{Key: key, Value: []byte("value"), Type: types.EntryTypePut, SeqNum: uint64(i + 1)})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key%010d", i%100000))
		sl.Get(key, 100001)
	}
}

func BenchmarkSkipListPutParallel(b *testing.B) {
	sl := NewSkipList()
	var counter uint64

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := atomic.AddUint64(&counter, 1)
			key := []byte(fmt.Sprintf("key%010d", i))
			sl.Put(&types.Entry{Key: key, Value: []byte("value"), Type: types.EntryTypePut, SeqNum: i})
		}
	})
}

func BenchmarkSkipListGetParallel(b *testing.B) {
	sl := NewSkipList()

	// Pre-populate
	for i := 0; i < 100000; i++ {
		key := []byte(fmt.Sprintf("key%010d", i))
		sl.Put(&types.Entry{Key: key, Value: []byte("value"), Type: types.EntryTypePut, SeqNum: uint64(i + 1)})
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := []byte(fmt.Sprintf("key%010d", rand.Intn(100000)))
			sl.Get(key, 100001)
		}
	})
}

func BenchmarkSkipListIterator(b *testing.B) {
	sl := NewSkipList()

	// Pre-populate
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("key%010d", i))
		sl.Put(&types.Entry{Key: key, Value: []byte("value"), Type: types.EntryTypePut, SeqNum: uint64(i + 1)})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter := sl.NewIterator()
		iter.SeekToFirst()
		for iter.Valid() {
			_ = iter.Key()
			_ = iter.Value()
			iter.Next()
		}
	}
}
