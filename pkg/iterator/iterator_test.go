package iterator

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/rapidodb/rapidodb/pkg/types"
)

// Helper to create entries
func entry(key string, value string, seqNum uint64) *types.Entry {
	return &types.Entry{
		Key:    []byte(key),
		Value:  []byte(value),
		SeqNum: seqNum,
		Type:   types.EntryTypePut,
	}
}

func tombstone(key string, seqNum uint64) *types.Entry {
	return &types.Entry{
		Key:    []byte(key),
		Value:  nil,
		SeqNum: seqNum,
		Type:   types.EntryTypeDelete,
	}
}

func TestEmptyIterator(t *testing.T) {
	iter := Empty()

	iter.SeekToFirst()
	if iter.Valid() {
		t.Error("empty iterator should not be valid")
	}

	iter.SeekToLast()
	if iter.Valid() {
		t.Error("empty iterator should not be valid after SeekToLast")
	}

	iter.Seek([]byte("a"))
	if iter.Valid() {
		t.Error("empty iterator should not be valid after Seek")
	}

	if err := iter.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

func TestSliceIterator(t *testing.T) {
	entries := []*types.Entry{
		entry("a", "1", 1),
		entry("b", "2", 2),
		entry("c", "3", 3),
		entry("d", "4", 4),
	}

	iter := FromSlice(entries)

	// SeekToFirst
	iter.SeekToFirst()
	if !iter.Valid() {
		t.Fatal("should be valid after SeekToFirst")
	}
	if string(iter.Key()) != "a" {
		t.Errorf("key = %s, want a", iter.Key())
	}

	// Next
	iter.Next()
	if string(iter.Key()) != "b" {
		t.Errorf("key = %s, want b", iter.Key())
	}

	// Seek
	iter.Seek([]byte("c"))
	if string(iter.Key()) != "c" {
		t.Errorf("key = %s, want c", iter.Key())
	}

	// SeekToLast
	iter.SeekToLast()
	if string(iter.Key()) != "d" {
		t.Errorf("key = %s, want d", iter.Key())
	}

	// Prev
	iter.Prev()
	if string(iter.Key()) != "c" {
		t.Errorf("key = %s, want c", iter.Key())
	}

	// SeekForPrev
	iter.SeekForPrev([]byte("c"))
	if string(iter.Key()) != "c" {
		t.Errorf("key = %s, want c after SeekForPrev", iter.Key())
	}

	iter.Close()
}

func TestSliceIteratorSeekPastEnd(t *testing.T) {
	entries := []*types.Entry{
		entry("a", "1", 1),
		entry("b", "2", 2),
	}

	iter := FromSlice(entries)

	// Seek past end
	iter.Seek([]byte("z"))
	if iter.Valid() {
		t.Error("should not be valid when seeking past end")
	}

	// SeekForPrev before start
	iter.SeekForPrev([]byte("0"))
	if iter.Valid() {
		t.Error("should not be valid when SeekForPrev before start")
	}

	iter.Close()
}

func TestMergeIteratorBasic(t *testing.T) {
	// Two sources with non-overlapping keys
	source1 := FromSlice([]*types.Entry{
		entry("a", "1", 1),
		entry("c", "3", 1),
	})
	source2 := FromSlice([]*types.Entry{
		entry("b", "2", 1),
		entry("d", "4", 1),
	})

	merge := NewMergeIterator([]Source{
		{Iter: source1, Level: 0, ID: 1},
		{Iter: source2, Level: 0, ID: 2},
	}, Options{})

	// Collect all keys
	var keys []string
	for merge.SeekToFirst(); merge.Valid(); merge.Next() {
		keys = append(keys, string(merge.Key()))
	}

	expected := []string{"a", "b", "c", "d"}
	if !equalStrings(keys, expected) {
		t.Errorf("keys = %v, want %v", keys, expected)
	}

	merge.Close()
}

func TestMergeIteratorDuplicateKeys(t *testing.T) {
	// Same key in multiple sources with different seqNums
	source1 := FromSlice([]*types.Entry{
		entry("a", "newer", 10),
	})
	source2 := FromSlice([]*types.Entry{
		entry("a", "older", 5),
	})

	merge := NewMergeIterator([]Source{
		{Iter: source1, Level: 0, ID: 1},
		{Iter: source2, Level: 1, ID: 2},
	}, Options{})

	merge.SeekToFirst()
	if !merge.Valid() {
		t.Fatal("should be valid")
	}
	if string(merge.Value()) != "newer" {
		t.Errorf("value = %s, want newer (highest seqNum)", merge.Value())
	}

	// Should only return one entry for key "a"
	merge.Next()
	if merge.Valid() {
		t.Errorf("should not have more entries, got key=%s", merge.Key())
	}

	merge.Close()
}

func TestMergeIteratorSnapshot(t *testing.T) {
	// Entries with different seqNums
	source := FromSlice([]*types.Entry{
		entry("a", "v3", 30),
		entry("a", "v2", 20),
		entry("a", "v1", 10),
		entry("b", "b2", 25),
		entry("b", "b1", 15),
	})

	// Snapshot at seqNum 20
	merge := NewMergeIterator([]Source{
		{Iter: source, Level: 0, ID: 1},
	}, Options{SnapshotSeq: 20})

	var results []string
	for merge.SeekToFirst(); merge.Valid(); merge.Next() {
		results = append(results, fmt.Sprintf("%s=%s", merge.Key(), merge.Value()))
	}

	// Should see: a=v2 (seqNum 20), b=b1 (seqNum 15)
	expected := []string{"a=v2", "b=b1"}
	if !equalStrings(results, expected) {
		t.Errorf("results = %v, want %v", results, expected)
	}

	merge.Close()
}

func TestMergeIteratorTombstones(t *testing.T) {
	source := FromSlice([]*types.Entry{
		entry("a", "1", 1),
		tombstone("b", 2),
		entry("c", "3", 3),
	})

	// Default: skip tombstones
	merge := NewMergeIterator([]Source{
		{Iter: source, Level: 0, ID: 1},
	}, Options{})

	var keys []string
	for merge.SeekToFirst(); merge.Valid(); merge.Next() {
		keys = append(keys, string(merge.Key()))
	}

	expected := []string{"a", "c"}
	if !equalStrings(keys, expected) {
		t.Errorf("keys = %v, want %v (b should be skipped)", keys, expected)
	}

	merge.Close()
}

func TestMergeIteratorIncludeTombstones(t *testing.T) {
	source := FromSlice([]*types.Entry{
		entry("a", "1", 1),
		tombstone("b", 2),
		entry("c", "3", 3),
	})

	merge := NewMergeIterator([]Source{
		{Iter: source, Level: 0, ID: 1},
	}, Options{IncludeTombstones: true})

	var keys []string
	for merge.SeekToFirst(); merge.Valid(); merge.Next() {
		keys = append(keys, string(merge.Key()))
	}

	expected := []string{"a", "b", "c"}
	if !equalStrings(keys, expected) {
		t.Errorf("keys = %v, want %v", keys, expected)
	}

	merge.Close()
}

func TestMergeIteratorReverse(t *testing.T) {
	source := FromSlice([]*types.Entry{
		entry("a", "1", 1),
		entry("b", "2", 2),
		entry("c", "3", 3),
	})

	merge := NewMergeIterator([]Source{
		{Iter: source, Level: 0, ID: 1},
	}, Options{})

	// Forward then backward
	merge.SeekToFirst()
	if string(merge.Key()) != "a" {
		t.Errorf("key = %s, want a", merge.Key())
	}

	merge.Next()
	if string(merge.Key()) != "b" {
		t.Errorf("key = %s, want b", merge.Key())
	}

	// Now go backward
	merge.Prev()
	if string(merge.Key()) != "a" {
		t.Errorf("after Prev, key = %s, want a", merge.Key())
	}

	merge.Close()
}

func TestBoundedIterator(t *testing.T) {
	entries := []*types.Entry{
		entry("a", "1", 1),
		entry("b", "2", 2),
		entry("c", "3", 3),
		entry("d", "4", 4),
		entry("e", "5", 5),
	}

	inner := FromSlice(entries)
	bounded := NewBoundedIterator(inner, []byte("b"), []byte("d"))

	// SeekToFirst should start at "b"
	bounded.SeekToFirst()
	if !bounded.Valid() {
		t.Fatal("should be valid")
	}
	if string(bounded.Key()) != "b" {
		t.Errorf("key = %s, want b", bounded.Key())
	}

	// Next to "c"
	bounded.Next()
	if string(bounded.Key()) != "c" {
		t.Errorf("key = %s, want c", bounded.Key())
	}

	// Next should make invalid (d is upper bound, exclusive)
	bounded.Next()
	if bounded.Valid() {
		t.Errorf("should not be valid, key = %s", bounded.Key())
	}

	bounded.Close()
}

func TestBoundedIteratorSeek(t *testing.T) {
	entries := []*types.Entry{
		entry("a", "1", 1),
		entry("b", "2", 2),
		entry("c", "3", 3),
		entry("d", "4", 4),
	}

	inner := FromSlice(entries)
	bounded := NewBoundedIterator(inner, []byte("b"), []byte("d"))

	// Seek below lower bound
	bounded.Seek([]byte("a"))
	if string(bounded.Key()) != "b" {
		t.Errorf("key = %s, want b (clamped to lower)", bounded.Key())
	}

	// Seek to upper bound (should be invalid)
	bounded.Seek([]byte("d"))
	if bounded.Valid() {
		t.Errorf("should not be valid when seeking to upper bound")
	}

	bounded.Close()
}

func TestBoundedIteratorReverse(t *testing.T) {
	entries := []*types.Entry{
		entry("a", "1", 1),
		entry("b", "2", 2),
		entry("c", "3", 3),
		entry("d", "4", 4),
	}

	inner := FromSlice(entries)
	bounded := NewBoundedIterator(inner, []byte("b"), []byte("d"))

	// SeekToLast should be at "c" (d is exclusive)
	bounded.SeekToLast()
	if !bounded.Valid() {
		t.Fatal("should be valid")
	}
	if string(bounded.Key()) != "c" {
		t.Errorf("key = %s, want c", bounded.Key())
	}

	// Prev to "b"
	bounded.Prev()
	if string(bounded.Key()) != "b" {
		t.Errorf("key = %s, want b", bounded.Key())
	}

	// Prev should make invalid
	bounded.Prev()
	if bounded.Valid() {
		t.Errorf("should not be valid after going before lower bound")
	}

	bounded.Close()
}

func TestPrefixIterator(t *testing.T) {
	entries := []*types.Entry{
		entry("aa", "1", 1),
		entry("ab", "2", 2),
		entry("ac", "3", 3),
		entry("b", "4", 4),
		entry("ba", "5", 5),
	}

	inner := FromSlice(entries)
	prefix := NewPrefixIterator(inner, []byte("a"))

	var keys []string
	for prefix.SeekToFirst(); prefix.Valid(); prefix.Next() {
		keys = append(keys, string(prefix.Key()))
	}

	expected := []string{"aa", "ab", "ac"}
	if !equalStrings(keys, expected) {
		t.Errorf("keys = %v, want %v", keys, expected)
	}

	prefix.Close()
}

func TestPrefixIteratorSeek(t *testing.T) {
	entries := []*types.Entry{
		entry("aa", "1", 1),
		entry("ab", "2", 2),
		entry("ac", "3", 3),
		entry("b", "4", 4),
	}

	inner := FromSlice(entries)
	prefix := NewPrefixIterator(inner, []byte("a"))

	// Seek within prefix
	prefix.Seek([]byte("ab"))
	if string(prefix.Key()) != "ab" {
		t.Errorf("key = %s, want ab", prefix.Key())
	}

	// Seek before prefix should go to first
	prefix.Seek([]byte("0"))
	if string(prefix.Key()) != "aa" {
		t.Errorf("key = %s, want aa", prefix.Key())
	}

	// Seek past prefix should be invalid
	prefix.Seek([]byte("b"))
	if prefix.Valid() {
		t.Errorf("should not be valid, key = %s", prefix.Key())
	}

	prefix.Close()
}

func TestPrefixIteratorReverse(t *testing.T) {
	entries := []*types.Entry{
		entry("aa", "1", 1),
		entry("ab", "2", 2),
		entry("ac", "3", 3),
		entry("b", "4", 4),
	}

	inner := FromSlice(entries)
	prefix := NewPrefixIterator(inner, []byte("a"))

	// SeekToLast with prefix
	prefix.SeekToLast()
	if !prefix.Valid() {
		t.Fatal("should be valid")
	}
	if string(prefix.Key()) != "ac" {
		t.Errorf("key = %s, want ac", prefix.Key())
	}

	// Prev
	prefix.Prev()
	if string(prefix.Key()) != "ab" {
		t.Errorf("key = %s, want ab", prefix.Key())
	}

	prefix.Close()
}

func TestPrefixEnd(t *testing.T) {
	tests := []struct {
		prefix []byte
		want   []byte
	}{
		{[]byte("a"), []byte("b")},
		{[]byte("ab"), []byte("ac")},
		{[]byte{0x00}, []byte{0x01}},
		{[]byte{0xFF}, nil}, // All 0xFF -> no end
		{[]byte{0xFF, 0xFF}, nil},
		{[]byte{0xAA, 0xFF}, []byte{0xAB}},
		{nil, nil},
	}

	for _, tt := range tests {
		got := prefixEnd(tt.prefix)
		if !bytes.Equal(got, tt.want) {
			t.Errorf("prefixEnd(%v) = %v, want %v", tt.prefix, got, tt.want)
		}
	}
}

func TestSnapshotIterator(t *testing.T) {
	// Test SnapshotIterator filters by sequence number
	// Note: SnapshotIterator doesn't deduplicate - use MergeIterator for that
	entries := []*types.Entry{
		entry("a", "a2", 20),
		entry("a", "a3", 30), // newer, should be filtered
		entry("b", "b1", 15),
		entry("b", "b2", 25), // newer, should be filtered
		tombstone("c", 22),   // should be filtered (seqNum > 20)
		entry("c", "c1", 12),
		entry("d", "d1", 18),
	}

	inner := FromSlice(entries)
	snap := NewSnapshotIterator(inner, 20, true) // skipDeletes=true

	var results []string
	for snap.SeekToFirst(); snap.Valid(); snap.Next() {
		results = append(results, fmt.Sprintf("%s=%s@%d",
			snap.Key(), snap.Value(), snap.Entry().SeqNum))
	}

	// seqNum <= 20, skip deletes
	// SnapshotIterator just filters, doesn't deduplicate
	expected := []string{"a=a2@20", "b=b1@15", "c=c1@12", "d=d1@18"}
	if !equalStrings(results, expected) {
		t.Errorf("results = %v, want %v", results, expected)
	}

	snap.Close()
}

func TestCompareKeys(t *testing.T) {
	tests := []struct {
		a, b []byte
		want int
	}{
		{[]byte("a"), []byte("b"), -1},
		{[]byte("b"), []byte("a"), 1},
		{[]byte("a"), []byte("a"), 0},
		{[]byte("aa"), []byte("a"), 1},
		{[]byte("a"), []byte("aa"), -1},
		{nil, nil, 0},
		{nil, []byte("a"), -1},
		{[]byte("a"), nil, 1},
	}

	for _, tt := range tests {
		got := compareKeys(tt.a, tt.b)
		if got != tt.want {
			t.Errorf("compareKeys(%q, %q) = %d, want %d", tt.a, tt.b, got, tt.want)
		}
	}
}

func TestMergeIteratorEmpty(t *testing.T) {
	merge := NewMergeIterator(nil, Options{})

	merge.SeekToFirst()
	if merge.Valid() {
		t.Error("empty merge iterator should not be valid")
	}

	merge.SeekToLast()
	if merge.Valid() {
		t.Error("empty merge iterator should not be valid")
	}

	merge.Close()
}

func TestMergeIteratorSingleSource(t *testing.T) {
	source := FromSlice([]*types.Entry{
		entry("a", "1", 1),
		entry("b", "2", 2),
		entry("c", "3", 3),
	})

	merge := NewMergeIterator([]Source{
		{Iter: source, Level: 0, ID: 1},
	}, Options{})

	var keys []string
	for merge.SeekToFirst(); merge.Valid(); merge.Next() {
		keys = append(keys, string(merge.Key()))
	}

	expected := []string{"a", "b", "c"}
	if !equalStrings(keys, expected) {
		t.Errorf("keys = %v, want %v", keys, expected)
	}

	merge.Close()
}

func TestMergeIteratorThreeSources(t *testing.T) {
	source1 := FromSlice([]*types.Entry{
		entry("a", "1", 1),
		entry("d", "4", 1),
	})
	source2 := FromSlice([]*types.Entry{
		entry("b", "2", 1),
		entry("e", "5", 1),
	})
	source3 := FromSlice([]*types.Entry{
		entry("c", "3", 1),
		entry("f", "6", 1),
	})

	merge := NewMergeIterator([]Source{
		{Iter: source1, Level: 0, ID: 1},
		{Iter: source2, Level: 1, ID: 2},
		{Iter: source3, Level: 2, ID: 3},
	}, Options{})

	var keys []string
	for merge.SeekToFirst(); merge.Valid(); merge.Next() {
		keys = append(keys, string(merge.Key()))
	}

	expected := []string{"a", "b", "c", "d", "e", "f"}
	if !equalStrings(keys, expected) {
		t.Errorf("keys = %v, want %v", keys, expected)
	}

	merge.Close()
}

func TestMergeIteratorSeek(t *testing.T) {
	source1 := FromSlice([]*types.Entry{
		entry("a", "1", 1),
		entry("c", "3", 1),
		entry("e", "5", 1),
	})
	source2 := FromSlice([]*types.Entry{
		entry("b", "2", 1),
		entry("d", "4", 1),
		entry("f", "6", 1),
	})

	merge := NewMergeIterator([]Source{
		{Iter: source1, Level: 0, ID: 1},
		{Iter: source2, Level: 1, ID: 2},
	}, Options{})

	// Seek to existing key
	merge.Seek([]byte("c"))
	if string(merge.Key()) != "c" {
		t.Errorf("key = %s, want c", merge.Key())
	}

	// Seek between keys
	merge.Seek([]byte("cc"))
	if string(merge.Key()) != "d" {
		t.Errorf("key = %s, want d", merge.Key())
	}

	// Seek past end
	merge.Seek([]byte("z"))
	if merge.Valid() {
		t.Error("should not be valid")
	}

	merge.Close()
}

// Helper to compare string slices
func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func BenchmarkMergeIterator(b *testing.B) {
	// Create 4 sources with 1000 entries each
	sources := make([]Source, 4)
	for s := 0; s < 4; s++ {
		entries := make([]*types.Entry, 1000)
		for i := 0; i < 1000; i++ {
			key := fmt.Sprintf("key%04d_%d", i, s)
			entries[i] = entry(key, "value", uint64(i))
		}
		sources[s] = Source{Iter: FromSlice(entries), Level: s, ID: uint64(s)}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		merge := NewMergeIterator(sources, Options{})
		count := 0
		for merge.SeekToFirst(); merge.Valid(); merge.Next() {
			count++
		}
		merge.Close()

		// Re-create sources for next iteration
		for s := 0; s < 4; s++ {
			entries := make([]*types.Entry, 1000)
			for j := 0; j < 1000; j++ {
				key := fmt.Sprintf("key%04d_%d", j, s)
				entries[j] = entry(key, "value", uint64(j))
			}
			sources[s] = Source{Iter: FromSlice(entries), Level: s, ID: uint64(s)}
		}
	}
}

func BenchmarkBoundedIterator(b *testing.B) {
	entries := make([]*types.Entry, 10000)
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key%05d", i)
		entries[i] = entry(key, "value", uint64(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		inner := FromSlice(entries)
		bounded := NewBoundedIterator(inner, []byte("key02000"), []byte("key08000"))
		count := 0
		for bounded.SeekToFirst(); bounded.Valid(); bounded.Next() {
			count++
		}
		bounded.Close()
	}
}
