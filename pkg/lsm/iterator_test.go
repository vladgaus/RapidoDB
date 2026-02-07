package lsm

import (
	"bytes"
	"fmt"
	"testing"
	"time"
)

func TestIteratorBasic(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions(dir)

	e, err := Open(opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer e.Close()

	// Write some data
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		value := []byte(fmt.Sprintf("value%05d", i))
		if err := e.Put(key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Iterate and verify order
	iter := e.Iterator()
	defer iter.Close()

	count := 0
	var prevKey []byte
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		if prevKey != nil && bytes.Compare(iter.Key(), prevKey) <= 0 {
			t.Errorf("keys not in order: %s <= %s", iter.Key(), prevKey)
		}
		prevKey = append(prevKey[:0], iter.Key()...)
		count++
	}

	if iter.Error() != nil {
		t.Errorf("Iterator error: %v", iter.Error())
	}

	if count != 100 {
		t.Errorf("count = %d, want 100", count)
	}
}

func TestIteratorSeek(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions(dir)

	e, err := Open(opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer e.Close()

	// Write data with gaps
	keys := []string{"aaa", "bbb", "ccc", "ddd", "eee"}
	for _, k := range keys {
		e.Put([]byte(k), []byte("value"))
	}

	iter := e.Iterator()
	defer iter.Close()

	// Seek to existing key
	iter.Seek([]byte("ccc"))
	if !iter.Valid() || string(iter.Key()) != "ccc" {
		t.Errorf("Seek to ccc: got %s", iter.Key())
	}

	// Seek between keys
	iter.Seek([]byte("bbz"))
	if !iter.Valid() || string(iter.Key()) != "ccc" {
		t.Errorf("Seek to bbz: got %s, want ccc", iter.Key())
	}

	// Seek before first
	iter.Seek([]byte("000"))
	if !iter.Valid() || string(iter.Key()) != "aaa" {
		t.Errorf("Seek to 000: got %s, want aaa", iter.Key())
	}

	// Seek past end
	iter.Seek([]byte("zzz"))
	if iter.Valid() {
		t.Errorf("Seek to zzz should be invalid, got %s", iter.Key())
	}
}

func TestIteratorSeekToLast(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions(dir)

	e, err := Open(opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer e.Close()

	keys := []string{"aaa", "bbb", "ccc", "ddd", "eee"}
	for _, k := range keys {
		e.Put([]byte(k), []byte("value"))
	}

	iter := e.Iterator()
	defer iter.Close()

	iter.SeekToLast()
	if !iter.Valid() {
		t.Fatal("SeekToLast should be valid")
	}
	if string(iter.Key()) != "eee" {
		t.Errorf("SeekToLast: got %s, want eee", iter.Key())
	}
}

func TestIteratorPrev(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions(dir)

	e, err := Open(opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer e.Close()

	keys := []string{"aaa", "bbb", "ccc", "ddd", "eee"}
	for _, k := range keys {
		e.Put([]byte(k), []byte("value"))
	}

	iter := e.Iterator()
	defer iter.Close()

	// Go to end and iterate backward
	iter.SeekToLast()

	var collected []string
	for iter.Valid() {
		collected = append(collected, string(iter.Key()))
		iter.Prev()
	}

	expected := []string{"eee", "ddd", "ccc", "bbb", "aaa"}
	if len(collected) != len(expected) {
		t.Fatalf("collected %d keys, want %d", len(collected), len(expected))
	}
	for i, k := range collected {
		if k != expected[i] {
			t.Errorf("collected[%d] = %s, want %s", i, k, expected[i])
		}
	}
}

func TestScan(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions(dir)

	e, err := Open(opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer e.Close()

	// Write data
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		e.Put(key, []byte("value"))
	}

	// Scan range [key00020, key00030)
	iter := e.Scan([]byte("key00020"), []byte("key00030"))
	defer iter.Close()

	var keys []string
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		keys = append(keys, string(iter.Key()))
	}

	if len(keys) != 10 {
		t.Errorf("got %d keys, want 10", len(keys))
	}
	if len(keys) > 0 {
		if keys[0] != "key00020" {
			t.Errorf("first key = %s, want key00020", keys[0])
		}
		if keys[len(keys)-1] != "key00029" {
			t.Errorf("last key = %s, want key00029", keys[len(keys)-1])
		}
	}
}

func TestScanPrefix(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions(dir)

	e, err := Open(opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer e.Close()

	// Write data with different prefixes
	prefixes := []string{"user:", "post:", "comment:"}
	for _, prefix := range prefixes {
		for i := 0; i < 10; i++ {
			key := []byte(fmt.Sprintf("%s%03d", prefix, i))
			e.Put(key, []byte("value"))
		}
	}

	// Scan by prefix
	iter := e.ScanPrefix([]byte("post:"))
	defer iter.Close()

	var keys []string
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		keys = append(keys, string(iter.Key()))
	}

	if len(keys) != 10 {
		t.Errorf("got %d keys, want 10", len(keys))
	}
	for _, k := range keys {
		if !bytes.HasPrefix([]byte(k), []byte("post:")) {
			t.Errorf("key %s doesn't have prefix post:", k)
		}
	}
}

func TestIteratorWithDeletes(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions(dir)

	e, err := Open(opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer e.Close()

	// Write and delete some keys
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%02d", i))
		e.Put(key, []byte("value"))
	}
	// Delete even keys
	for i := 0; i < 10; i += 2 {
		key := []byte(fmt.Sprintf("key%02d", i))
		e.Delete(key)
	}

	// Iterate should skip deleted keys
	iter := e.Iterator()
	defer iter.Close()

	var keys []string
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		keys = append(keys, string(iter.Key()))
	}

	// Should only have odd keys: 01, 03, 05, 07, 09
	if len(keys) != 5 {
		t.Errorf("got %d keys, want 5: %v", len(keys), keys)
	}
}

func TestIteratorWithSSTable(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions(dir)
	opts.MemTableSize = 2048 // Small enough to trigger flush but not cause stalls

	e, err := Open(opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer e.Close()

	// Write enough to trigger flush
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		value := bytes.Repeat([]byte("v"), 50)
		// Retry on write stall
		for retries := 0; retries < 10; retries++ {
			err := e.Put(key, value)
			if err == nil {
				break
			}
			if err.Error() == "lsm: write stall" {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Wait for flush
	time.Sleep(300 * time.Millisecond)

	// Write more to memtable
	for i := 50; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		value := []byte("newer")
		// Retry on write stall
		for retries := 0; retries < 10; retries++ {
			err := e.Put(key, value)
			if err == nil {
				break
			}
			if err.Error() == "lsm: write stall" {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Iterator should see all 100 keys from both memtable and sstable
	iter := e.Iterator()
	defer iter.Close()

	count := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		count++
	}

	if count != 100 {
		t.Errorf("count = %d, want 100", count)
	}
}

func TestIteratorMergesMultipleSources(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions(dir)
	opts.MemTableSize = 1024 // Small to force multiple flushes

	e, err := Open(opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer e.Close()

	// Write data that will span multiple SSTables
	for i := 0; i < 30; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		value := bytes.Repeat([]byte("v"), 50)
		// Retry on write stall
		for retries := 0; retries < 10; retries++ {
			err := e.Put(key, value)
			if err == nil {
				break
			}
			if err.Error() == "lsm: write stall" {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Wait for flushes
	time.Sleep(300 * time.Millisecond)

	// Verify we can iterate all keys in order
	iter := e.Iterator()
	defer iter.Close()

	var keys []string
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		keys = append(keys, string(iter.Key()))
	}

	if len(keys) != 30 {
		t.Errorf("got %d keys, want 30", len(keys))
	}

	// Check order
	for i := 1; i < len(keys); i++ {
		if keys[i] <= keys[i-1] {
			t.Errorf("keys not sorted: %s <= %s", keys[i], keys[i-1])
		}
	}
}

func TestIteratorEmpty(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions(dir)

	e, err := Open(opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer e.Close()

	iter := e.Iterator()
	defer iter.Close()

	iter.SeekToFirst()
	if iter.Valid() {
		t.Error("empty iterator should not be valid")
	}

	iter.SeekToLast()
	if iter.Valid() {
		t.Error("empty iterator SeekToLast should not be valid")
	}
}

func BenchmarkIterator(b *testing.B) {
	dir := b.TempDir()
	opts := DefaultOptions(dir)

	e, _ := Open(opts)
	defer e.Close()

	// Write data
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("key%08d", i))
		value := []byte("value")
		e.Put(key, value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter := e.Iterator()
		count := 0
		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			count++
		}
		iter.Close()
	}
}

func BenchmarkIteratorSeek(b *testing.B) {
	dir := b.TempDir()
	opts := DefaultOptions(dir)

	e, _ := Open(opts)
	defer e.Close()

	// Write data
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("key%08d", i))
		value := []byte("value")
		e.Put(key, value)
	}

	targets := make([][]byte, 1000)
	for i := range targets {
		targets[i] = []byte(fmt.Sprintf("key%08d", i*10))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter := e.Iterator()
		for _, target := range targets {
			iter.Seek(target)
		}
		iter.Close()
	}
}
