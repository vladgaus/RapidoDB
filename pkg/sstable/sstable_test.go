package sstable

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/rapidodb/rapidodb/pkg/types"
)

func tempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "rapidodb-sstable-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func makeEntry(key, value string, seqNum uint64) *types.Entry {
	return &types.Entry{
		Key:    []byte(key),
		Value:  []byte(value),
		Type:   types.EntryTypePut,
		SeqNum: seqNum,
	}
}

func makeTombstone(key string, seqNum uint64) *types.Entry {
	return &types.Entry{
		Key:    []byte(key),
		Value:  nil,
		Type:   types.EntryTypeDelete,
		SeqNum: seqNum,
	}
}

func TestBlockBuilderBasic(t *testing.T) {
	bb := NewBlockBuilder(16)

	entries := []*types.Entry{
		makeEntry("key1", "value1", 1),
		makeEntry("key2", "value2", 2),
		makeEntry("key3", "value3", 3),
	}

	for _, e := range entries {
		if !bb.Add(e) {
			t.Fatalf("failed to add entry %s", e.Key)
		}
	}

	if bb.EntryCount() != 3 {
		t.Errorf("expected 3 entries, got %d", bb.EntryCount())
	}

	if !bytes.Equal(bb.LastKey(), []byte("key3")) {
		t.Errorf("last key mismatch")
	}

	// Finish block
	data := bb.Finish()
	if len(data) == 0 {
		t.Error("block data should not be empty")
	}

	// Read it back
	br, err := NewBlockReader(data)
	if err != nil {
		t.Fatalf("failed to create block reader: %v", err)
	}

	br.SeekToFirst()
	count := 0
	for br.Valid() {
		entry, err := br.Entry()
		if err != nil {
			t.Fatalf("failed to read entry: %v", err)
		}
		if !bytes.Equal(entry.Key, entries[count].Key) {
			t.Errorf("entry %d key mismatch", count)
		}
		br.Next()
		count++
	}

	if count != 3 {
		t.Errorf("expected 3 entries, read %d", count)
	}
}

func TestBlockReaderSeek(t *testing.T) {
	bb := NewBlockBuilder(4)

	// Add 10 entries
	for i := 0; i < 10; i++ {
		entry := makeEntry(fmt.Sprintf("key%02d", i), fmt.Sprintf("value%02d", i), uint64(i+1))
		bb.Add(entry)
	}

	data := bb.Finish()
	br, err := NewBlockReader(data)
	if err != nil {
		t.Fatalf("failed to create block reader: %v", err)
	}

	// Seek to exact key
	br.Seek([]byte("key05"))
	if !br.Valid() {
		t.Fatal("should be valid after seek")
	}
	if !bytes.Equal(br.Key(), []byte("key05")) {
		t.Errorf("expected key05, got %s", br.Key())
	}

	// Seek to key between entries
	br.Seek([]byte("key045"))
	if !br.Valid() {
		t.Fatal("should be valid after seek")
	}
	if !bytes.Equal(br.Key(), []byte("key05")) {
		t.Errorf("expected key05, got %s", br.Key())
	}

	// Seek past end
	br.Seek([]byte("key99"))
	if br.Valid() {
		t.Error("should be invalid when seeking past end")
	}
}

func TestSSTableWriteRead(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "test.sst")

	// Write SSTable
	w, err := NewWriter(path, DefaultWriterOptions())
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	entries := make([]*types.Entry, 100)
	for i := 0; i < 100; i++ {
		entries[i] = makeEntry(fmt.Sprintf("key%05d", i), fmt.Sprintf("value%05d", i), uint64(i+1))
	}

	for _, e := range entries {
		if err := w.Add(e); err != nil {
			t.Fatalf("failed to add entry: %v", err)
		}
	}

	meta, err := w.Finish()
	if err != nil {
		t.Fatalf("failed to finish: %v", err)
	}

	if meta.EntryCount != 100 {
		t.Errorf("expected 100 entries, got %d", meta.EntryCount)
	}

	t.Logf("Block count: %d", meta.BlockCount)

	// Read SSTable
	r, err := OpenReader(path)
	if err != nil {
		t.Fatalf("failed to open reader: %v", err)
	}
	defer r.Close()

	// Point lookups
	notFound := 0
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%05d", i)

		// Check bloom filter
		contains := r.mayContain([]byte(key))

		entry, err := r.Get([]byte(key))
		if err != nil {
			t.Fatalf("get failed for %s: %v", key, err)
		}
		if entry == nil {
			notFound++
			if notFound <= 3 {
				t.Logf("Not found: %s, mayContain=%v", key, contains)
			}
		} else {
			expectedValue := fmt.Sprintf("value%05d", i)
			if !bytes.Equal(entry.Value, []byte(expectedValue)) {
				t.Errorf("value mismatch for %s", key)
			}
		}
	}

	if notFound > 0 {
		t.Errorf("%d keys not found", notFound)
	}

	// Non-existent key
	entry, err := r.Get([]byte("nonexistent"))
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if entry != nil {
		t.Error("should not find nonexistent key")
	}
}

func TestSSTableIterator(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "test.sst")

	// Write SSTable
	w, err := NewWriter(path, DefaultWriterOptions())
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	for i := 0; i < 50; i++ {
		entry := makeEntry(fmt.Sprintf("key%05d", i), fmt.Sprintf("value%05d", i), uint64(i+1))
		w.Add(entry)
	}
	w.Finish()

	// Read with iterator
	r, err := OpenReader(path)
	if err != nil {
		t.Fatalf("failed to open reader: %v", err)
	}
	defer r.Close()

	iter := r.NewIterator()

	// Forward scan
	iter.SeekToFirst()
	count := 0
	var prevKey []byte
	for iter.Valid() {
		key := iter.Key()
		if prevKey != nil && bytes.Compare(key, prevKey) <= 0 {
			t.Errorf("keys not in order: %s <= %s", key, prevKey)
		}
		prevKey = append(prevKey[:0], key...)
		iter.Next()
		count++
	}
	if count != 50 {
		t.Errorf("expected 50 entries, got %d", count)
	}

	// Seek
	iter.Seek([]byte("key00025"))
	if !iter.Valid() {
		t.Fatal("should be valid after seek")
	}
	if !bytes.Equal(iter.Key(), []byte("key00025")) {
		t.Errorf("expected key00025, got %s", iter.Key())
	}

	// SeekToLast
	iter.SeekToLast()
	if !iter.Valid() {
		t.Fatal("should be valid at last")
	}
	if !bytes.Equal(iter.Key(), []byte("key00049")) {
		t.Errorf("expected key00049, got %s", iter.Key())
	}

	iter.Close()
}

func TestSSTableTombstones(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "test.sst")

	w, err := NewWriter(path, DefaultWriterOptions())
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	// Mix of puts and deletes
	w.Add(makeEntry("key1", "value1", 1))
	w.Add(makeTombstone("key2", 2))
	w.Add(makeEntry("key3", "value3", 3))
	w.Finish()

	r, err := OpenReader(path)
	if err != nil {
		t.Fatalf("failed to open reader: %v", err)
	}
	defer r.Close()

	// Check tombstone
	entry, _ := r.Get([]byte("key2"))
	if entry == nil {
		t.Fatal("tombstone should be found")
	}
	if !entry.IsDeleted() {
		t.Error("entry should be a tombstone")
	}
}

func TestSSTableLargeValues(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "test.sst")

	w, err := NewWriter(path, DefaultWriterOptions())
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	// Large value that will span multiple blocks
	largeValue := bytes.Repeat([]byte("x"), 10000)

	for i := 0; i < 10; i++ {
		entry := &types.Entry{
			Key:    []byte(fmt.Sprintf("key%02d", i)),
			Value:  largeValue,
			Type:   types.EntryTypePut,
			SeqNum: uint64(i + 1),
		}
		w.Add(entry)
	}

	meta, err := w.Finish()
	if err != nil {
		t.Fatalf("failed to finish: %v", err)
	}

	t.Logf("Block count: %d, Entry count: %d", meta.BlockCount, meta.EntryCount)

	r, err := OpenReader(path)
	if err != nil {
		t.Fatalf("failed to open reader: %v", err)
	}
	defer r.Close()

	t.Logf("Index entries: %d", len(r.indexEntries))
	for i, ie := range r.indexEntries {
		t.Logf("  Index %d: key=%s, offset=%d, size=%d", i, ie.Key, ie.BlockHandle.Offset, ie.BlockHandle.Size)
	}

	// Test iterator first
	iter := r.NewIterator()
	iter.SeekToFirst()
	iterCount := 0
	for iter.Valid() {
		iterCount++
		iter.Next()
	}
	t.Logf("Iterator found %d entries", iterCount)

	// Verify all entries
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%02d", i)
		entry, err := r.Get([]byte(key))
		if err != nil {
			t.Fatalf("get failed: %v", err)
		}
		if entry == nil {
			// Check mayContain
			contains := r.mayContain([]byte(key))
			blockIdx := r.findBlock([]byte(key))
			t.Errorf("entry not found: %s, mayContain=%v, blockIdx=%d", key, contains, blockIdx)
			continue
		}
		if !bytes.Equal(entry.Value, largeValue) {
			t.Errorf("value mismatch for %s", key)
		}
	}
}

func TestSSTableBloomFilter(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "test.sst")

	w, err := NewWriter(path, DefaultWriterOptions())
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	// Add entries
	for i := 0; i < 1000; i++ {
		entry := makeEntry(fmt.Sprintf("key%05d", i), "value", uint64(i+1))
		w.Add(entry)
	}
	w.Finish()

	r, err := OpenReader(path)
	if err != nil {
		t.Fatalf("failed to open reader: %v", err)
	}
	defer r.Close()

	// Check bloom filter is loaded
	if r.bloomFilter == nil {
		t.Error("bloom filter should be loaded")
	}

	// Test mayContain - all added keys should pass
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		if !r.mayContain(key) {
			t.Errorf("bloom filter false negative for %s", key)
		}
	}

	// Test false positive rate (should be ~1% with 10 bits/key)
	falsePositives := 0
	tests := 10000
	for i := 0; i < tests; i++ {
		key := []byte(fmt.Sprintf("notexist%06d", i))
		if r.mayContain(key) {
			falsePositives++
		}
	}

	fpRate := float64(falsePositives) / float64(tests)
	t.Logf("Bloom filter false positive rate: %.2f%%", fpRate*100)

	// Allow some tolerance (target is ~1%)
	if fpRate > 0.05 {
		t.Errorf("false positive rate too high: %.2f%%", fpRate*100)
	}
}

func TestFooterEncodeDecode(t *testing.T) {
	footer := &Footer{
		FilterHandle: BlockHandle{Offset: 1000, Size: 200},
		IndexHandle:  BlockHandle{Offset: 1200, Size: 300},
		Version:      FormatVersion,
	}

	encoded := footer.Encode()
	if len(encoded) != FooterSize {
		t.Errorf("footer size mismatch: got %d, want %d", len(encoded), FooterSize)
	}

	decoded, err := DecodeFooter(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.FilterHandle.Offset != footer.FilterHandle.Offset {
		t.Error("filter offset mismatch")
	}
	if decoded.FilterHandle.Size != footer.FilterHandle.Size {
		t.Error("filter size mismatch")
	}
	if decoded.IndexHandle.Offset != footer.IndexHandle.Offset {
		t.Error("index offset mismatch")
	}
	if decoded.IndexHandle.Size != footer.IndexHandle.Size {
		t.Error("index size mismatch")
	}
	if decoded.Version != footer.Version {
		t.Error("version mismatch")
	}
}

func TestSSTableAbort(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "test.sst")

	w, err := NewWriter(path, DefaultWriterOptions())
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	w.Add(makeEntry("key1", "value1", 1))

	// Abort instead of finish
	if err := w.Abort(); err != nil {
		t.Fatalf("abort failed: %v", err)
	}

	// File should not exist
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("file should have been removed on abort")
	}
}

func TestSSTableEmpty(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "test.sst")

	w, err := NewWriter(path, DefaultWriterOptions())
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	// Finish without adding entries
	meta, err := w.Finish()
	if err != nil {
		t.Fatalf("failed to finish: %v", err)
	}

	if meta.EntryCount != 0 {
		t.Errorf("expected 0 entries, got %d", meta.EntryCount)
	}

	// Should still be readable
	r, err := OpenReader(path)
	if err != nil {
		t.Fatalf("failed to open reader: %v", err)
	}
	defer r.Close()

	iter := r.NewIterator()
	iter.SeekToFirst()
	if iter.Valid() {
		t.Error("iterator should not be valid for empty SSTable")
	}
}

func TestSSTableSingleBlockGet(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "test.sst")

	w, err := NewWriter(path, DefaultWriterOptions())
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	// Small entries that fit in single block
	entries := []*types.Entry{
		makeEntry("aaa", "v1", 1),
		makeEntry("bbb", "v2", 2),
		makeEntry("ccc", "v3", 3),
	}

	for _, e := range entries {
		if err := w.Add(e); err != nil {
			t.Fatalf("failed to add: %v", err)
		}
	}

	meta, err := w.Finish()
	if err != nil {
		t.Fatalf("failed to finish: %v", err)
	}

	t.Logf("Entry count: %d, Block count: %d", meta.EntryCount, meta.BlockCount)

	r, err := OpenReader(path)
	if err != nil {
		t.Fatalf("failed to open: %v", err)
	}
	defer r.Close()

	t.Logf("Index entries: %d", len(r.indexEntries))
	for i, ie := range r.indexEntries {
		t.Logf("  Index %d: key=%s, offset=%d, size=%d", i, ie.Key, ie.BlockHandle.Offset, ie.BlockHandle.Size)
	}

	// Test iterator first
	iter := r.NewIterator()
	iter.SeekToFirst()
	t.Logf("Iterator results:")
	for iter.Valid() {
		t.Logf("  key=%s value=%s", iter.Key(), iter.Value())
		iter.Next()
	}

	// Test Get
	t.Logf("Get results:")
	for _, e := range entries {
		entry, err := r.Get(e.Key)
		if err != nil {
			t.Errorf("Get %s error: %v", e.Key, err)
		} else if entry == nil {
			t.Errorf("Get %s: not found", e.Key)
		} else {
			t.Logf("  Get %s: found value=%s", e.Key, entry.Value)
		}
	}
}

// Benchmarks

func BenchmarkSSTableWrite(b *testing.B) {
	dir, _ := os.MkdirTemp("", "rapidodb-bench-*")
	defer os.RemoveAll(dir)

	entries := make([]*types.Entry, 1000)
	for i := 0; i < 1000; i++ {
		entries[i] = makeEntry(
			fmt.Sprintf("key%08d", i),
			fmt.Sprintf("value%08d", i),
			uint64(i+1),
		)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		path := filepath.Join(dir, fmt.Sprintf("test%d.sst", i))
		w, _ := NewWriter(path, DefaultWriterOptions())
		for _, e := range entries {
			w.Add(e)
		}
		w.Finish()
	}
}

func BenchmarkSSTableRead(b *testing.B) {
	dir, _ := os.MkdirTemp("", "rapidodb-bench-*")
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "test.sst")
	w, _ := NewWriter(path, DefaultWriterOptions())
	for i := 0; i < 10000; i++ {
		entry := makeEntry(
			fmt.Sprintf("key%08d", i),
			fmt.Sprintf("value%08d", i),
			uint64(i+1),
		)
		w.Add(entry)
	}
	w.Finish()

	r, _ := OpenReader(path)
	defer r.Close()

	keys := make([][]byte, 1000)
	for i := 0; i < 1000; i++ {
		keys[i] = []byte(fmt.Sprintf("key%08d", i*10))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Get(keys[i%1000])
	}
}

func BenchmarkSSTableIterate(b *testing.B) {
	dir, _ := os.MkdirTemp("", "rapidodb-bench-*")
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "test.sst")
	w, _ := NewWriter(path, DefaultWriterOptions())
	for i := 0; i < 10000; i++ {
		entry := makeEntry(
			fmt.Sprintf("key%08d", i),
			fmt.Sprintf("value%08d", i),
			uint64(i+1),
		)
		w.Add(entry)
	}
	w.Finish()

	r, _ := OpenReader(path)
	defer r.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter := r.NewIterator()
		iter.SeekToFirst()
		for iter.Valid() {
			_ = iter.Key()
			iter.Next()
		}
		iter.Close()
	}
}
