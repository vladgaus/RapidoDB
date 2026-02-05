package lsm

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"testing"
)

func tempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "rapidodb-lsm-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func TestEngineOpenClose(t *testing.T) {
	dir := tempDir(t)

	e, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}

	if err := e.Close(); err != nil {
		t.Fatalf("failed to close engine: %v", err)
	}
}

func TestEngineDoubleClose(t *testing.T) {
	dir := tempDir(t)

	e, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}

	if err := e.Close(); err != nil {
		t.Fatalf("first close failed: %v", err)
	}

	// Second close should be no-op
	if err := e.Close(); err != nil {
		t.Fatalf("second close failed: %v", err)
	}
}

func TestEnginePutGet(t *testing.T) {
	dir := tempDir(t)

	e, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}
	defer e.Close()

	// Put
	key := []byte("hello")
	value := []byte("world")

	if err := e.Put(key, value); err != nil {
		t.Fatalf("put failed: %v", err)
	}

	// Get
	got, err := e.Get(key)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}

	if !bytes.Equal(got, value) {
		t.Errorf("value mismatch: got %q, want %q", got, value)
	}
}

func TestEngineGetNonExistent(t *testing.T) {
	dir := tempDir(t)

	e, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}
	defer e.Close()

	// Get non-existent key
	got, err := e.Get([]byte("nonexistent"))
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil for non-existent key, got %q", got)
	}
}

func TestEngineDelete(t *testing.T) {
	dir := tempDir(t)

	e, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}
	defer e.Close()

	key := []byte("key")
	value := []byte("value")

	// Put
	if err := e.Put(key, value); err != nil {
		t.Fatalf("put failed: %v", err)
	}

	// Verify exists
	got, err := e.Get(key)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if got == nil {
		t.Fatal("key should exist before delete")
	}

	// Delete
	if err := e.Delete(key); err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	// Verify deleted
	got, err = e.Get(key)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if got != nil {
		t.Errorf("key should be deleted, got %q", got)
	}
}

func TestEngineMultipleKeys(t *testing.T) {
	dir := tempDir(t)

	e, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}
	defer e.Close()

	// Insert multiple keys
	n := 100
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		value := []byte(fmt.Sprintf("value%05d", i))
		if err := e.Put(key, value); err != nil {
			t.Fatalf("put failed for %d: %v", i, err)
		}
	}

	// Verify all keys
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		expectedValue := []byte(fmt.Sprintf("value%05d", i))

		got, err := e.Get(key)
		if err != nil {
			t.Fatalf("get failed for %d: %v", i, err)
		}
		if !bytes.Equal(got, expectedValue) {
			t.Errorf("value mismatch for %d: got %q, want %q", i, got, expectedValue)
		}
	}
}

func TestEngineOverwrite(t *testing.T) {
	dir := tempDir(t)

	e, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}
	defer e.Close()

	key := []byte("key")

	// Write initial value
	if err := e.Put(key, []byte("v1")); err != nil {
		t.Fatalf("put failed: %v", err)
	}

	// Overwrite
	if err := e.Put(key, []byte("v2")); err != nil {
		t.Fatalf("put failed: %v", err)
	}

	// Should get latest value
	got, err := e.Get(key)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if !bytes.Equal(got, []byte("v2")) {
		t.Errorf("expected v2, got %q", got)
	}
}

func TestEngineRecovery(t *testing.T) {
	dir := tempDir(t)

	// Write data
	e1, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}

	key := []byte("persist")
	value := []byte("this value")

	if err := e1.Put(key, value); err != nil {
		t.Fatalf("put failed: %v", err)
	}

	if err := e1.Sync(); err != nil {
		t.Fatalf("sync failed: %v", err)
	}

	if err := e1.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Reopen and verify
	e2, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("failed to reopen engine: %v", err)
	}
	defer e2.Close()

	got, err := e2.Get(key)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if !bytes.Equal(got, value) {
		t.Errorf("recovered value mismatch: got %q, want %q", got, value)
	}
}

func TestEngineRecoveryWithDelete(t *testing.T) {
	dir := tempDir(t)

	// Write and delete data
	e1, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}

	key := []byte("deleted")

	if err := e1.Put(key, []byte("value")); err != nil {
		t.Fatalf("put failed: %v", err)
	}

	if err := e1.Delete(key); err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	if err := e1.Sync(); err != nil {
		t.Fatalf("sync failed: %v", err)
	}

	if err := e1.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Reopen and verify
	e2, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("failed to reopen engine: %v", err)
	}
	defer e2.Close()

	got, err := e2.Get(key)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if got != nil {
		t.Errorf("key should remain deleted after recovery, got %q", got)
	}
}

func TestEngineHas(t *testing.T) {
	dir := tempDir(t)

	e, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}
	defer e.Close()

	key := []byte("key")

	// Should not exist initially
	exists, err := e.Has(key)
	if err != nil {
		t.Fatalf("has failed: %v", err)
	}
	if exists {
		t.Error("key should not exist initially")
	}

	// Put
	if err := e.Put(key, []byte("value")); err != nil {
		t.Fatalf("put failed: %v", err)
	}

	// Should exist now
	exists, err = e.Has(key)
	if err != nil {
		t.Fatalf("has failed: %v", err)
	}
	if !exists {
		t.Error("key should exist after put")
	}

	// Delete
	if err := e.Delete(key); err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	// Should not exist after delete
	exists, err = e.Has(key)
	if err != nil {
		t.Fatalf("has failed: %v", err)
	}
	if exists {
		t.Error("key should not exist after delete")
	}
}

func TestEngineSnapshot(t *testing.T) {
	dir := tempDir(t)

	e, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}
	defer e.Close()

	key := []byte("key")

	// Write v1
	if err := e.Put(key, []byte("v1")); err != nil {
		t.Fatalf("put failed: %v", err)
	}

	// Take snapshot
	snap := e.Snapshot()

	// Write v2
	if err := e.Put(key, []byte("v2")); err != nil {
		t.Fatalf("put failed: %v", err)
	}

	// Current read should see v2
	got, err := e.Get(key)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if !bytes.Equal(got, []byte("v2")) {
		t.Errorf("current read should see v2, got %q", got)
	}

	// Snapshot read should see v1
	got, err = snap.Get(key)
	if err != nil {
		t.Fatalf("snapshot get failed: %v", err)
	}
	if !bytes.Equal(got, []byte("v1")) {
		t.Errorf("snapshot read should see v1, got %q", got)
	}
}

func TestEngineIterator(t *testing.T) {
	dir := tempDir(t)

	e, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}
	defer e.Close()

	// Insert keys
	keys := []string{"a", "b", "c", "d", "e"}
	for _, k := range keys {
		if err := e.Put([]byte(k), []byte("value-"+k)); err != nil {
			t.Fatalf("put failed: %v", err)
		}
	}

	// Iterate
	iter := e.Iterator()
	defer iter.Close()

	iter.SeekToFirst()
	count := 0
	for iter.Valid() {
		if count < len(keys) {
			if string(iter.Key()) != keys[count] {
				t.Errorf("key mismatch at %d: got %s, want %s", count, iter.Key(), keys[count])
			}
		}
		iter.Next()
		count++
	}

	if count != len(keys) {
		t.Errorf("expected %d keys, got %d", len(keys), count)
	}

	if iter.Error() != nil {
		t.Errorf("iterator error: %v", iter.Error())
	}
}

func TestEngineIteratorSeek(t *testing.T) {
	dir := tempDir(t)

	e, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}
	defer e.Close()

	// Insert keys
	for _, k := range []string{"a", "c", "e", "g"} {
		if err := e.Put([]byte(k), []byte("value")); err != nil {
			t.Fatalf("put failed: %v", err)
		}
	}

	iter := e.Iterator()
	defer iter.Close()

	// Seek to "b" should position at "c"
	iter.Seek([]byte("b"))
	if !iter.Valid() {
		t.Fatal("iterator should be valid after seek")
	}
	if string(iter.Key()) != "c" {
		t.Errorf("expected key 'c', got %q", iter.Key())
	}

	// Seek to "c" should position at "c"
	iter.Seek([]byte("c"))
	if !iter.Valid() {
		t.Fatal("iterator should be valid after seek")
	}
	if string(iter.Key()) != "c" {
		t.Errorf("expected key 'c', got %q", iter.Key())
	}
}

func TestEngineClosedOperations(t *testing.T) {
	dir := tempDir(t)

	e, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}

	if err := e.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Operations should fail on closed engine
	if err := e.Put([]byte("key"), []byte("value")); err != ErrClosed {
		t.Errorf("put on closed should return ErrClosed, got %v", err)
	}

	if _, err := e.Get([]byte("key")); err != ErrClosed {
		t.Errorf("get on closed should return ErrClosed, got %v", err)
	}

	if err := e.Delete([]byte("key")); err != ErrClosed {
		t.Errorf("delete on closed should return ErrClosed, got %v", err)
	}
}

func TestEngineInvalidOptions(t *testing.T) {
	_, err := Open(Options{}) // Empty dir
	if err != ErrInvalidOptions {
		t.Errorf("expected ErrInvalidOptions, got %v", err)
	}
}

func TestEngineEmptyKey(t *testing.T) {
	dir := tempDir(t)

	e, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}
	defer e.Close()

	err = e.Put([]byte{}, []byte("value"))
	if err == nil {
		t.Error("expected error for empty key")
	}
}

func TestEngineStats(t *testing.T) {
	dir := tempDir(t)

	e, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}
	defer e.Close()

	// Insert some data
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		if err := e.Put(key, value); err != nil {
			t.Fatalf("put failed: %v", err)
		}
	}

	stats := e.Stats()
	if stats.TotalKeyValuePairs != 10 {
		t.Errorf("expected 10 key-value pairs, got %d", stats.TotalKeyValuePairs)
	}
	if stats.SeqNum != 10 {
		t.Errorf("expected seqnum 10, got %d", stats.SeqNum)
	}
}

func TestEngineConcurrentReads(t *testing.T) {
	dir := tempDir(t)

	e, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}
	defer e.Close()

	// Insert data
	n := 100
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		value := []byte(fmt.Sprintf("value%05d", i))
		if err := e.Put(key, value); err != nil {
			t.Fatalf("put failed: %v", err)
		}
	}

	// Concurrent reads
	var wg sync.WaitGroup
	readers := 10
	errors := make(chan error, readers*n)

	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				key := []byte(fmt.Sprintf("key%05d", i))
				expectedValue := []byte(fmt.Sprintf("value%05d", i))

				got, err := e.Get(key)
				if err != nil {
					errors <- fmt.Errorf("get failed: %v", err)
					return
				}
				if !bytes.Equal(got, expectedValue) {
					errors <- fmt.Errorf("value mismatch: got %q, want %q", got, expectedValue)
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}
}

func TestEngineLargeValues(t *testing.T) {
	dir := tempDir(t)

	e, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}
	defer e.Close()

	// Write large value
	key := []byte("large")
	value := bytes.Repeat([]byte("x"), 1024*1024) // 1MB

	if err := e.Put(key, value); err != nil {
		t.Fatalf("put failed: %v", err)
	}

	got, err := e.Get(key)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}

	if !bytes.Equal(got, value) {
		t.Error("large value mismatch")
	}
}

// Benchmarks

func BenchmarkEnginePut(b *testing.B) {
	dir, _ := os.MkdirTemp("", "rapidodb-bench-*")
	defer os.RemoveAll(dir)

	e, _ := Open(DefaultOptions(dir))
	defer e.Close()

	key := []byte("key")
	value := bytes.Repeat([]byte("v"), 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Put(key, value)
	}
}

func BenchmarkEngineGet(b *testing.B) {
	dir, _ := os.MkdirTemp("", "rapidodb-bench-*")
	defer os.RemoveAll(dir)

	e, _ := Open(DefaultOptions(dir))
	defer e.Close()

	// Populate
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("key%08d", i))
		value := []byte(fmt.Sprintf("value%08d", i))
		e.Put(key, value)
	}

	keys := make([][]byte, 1000)
	for i := 0; i < 1000; i++ {
		keys[i] = []byte(fmt.Sprintf("key%08d", i*10))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Get(keys[i%1000])
	}
}

func BenchmarkEngineParallelGet(b *testing.B) {
	dir, _ := os.MkdirTemp("", "rapidodb-bench-*")
	defer os.RemoveAll(dir)

	e, _ := Open(DefaultOptions(dir))
	defer e.Close()

	// Populate
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("key%08d", i))
		value := []byte(fmt.Sprintf("value%08d", i))
		e.Put(key, value)
	}

	keys := make([][]byte, 1000)
	for i := 0; i < 1000; i++ {
		keys[i] = []byte(fmt.Sprintf("key%08d", i*10))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			e.Get(keys[i%1000])
			i++
		}
	})
}

func BenchmarkEnginePutGet(b *testing.B) {
	dir, _ := os.MkdirTemp("", "rapidodb-bench-*")
	defer os.RemoveAll(dir)

	e, _ := Open(DefaultOptions(dir))
	defer e.Close()

	value := bytes.Repeat([]byte("v"), 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key%08d", i))
		e.Put(key, value)
		e.Get(key)
	}
}
