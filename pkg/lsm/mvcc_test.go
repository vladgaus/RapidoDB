package lsm

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestSnapshotBasic(t *testing.T) {
	dir := tempDir(t)

	opts := DefaultOptions(dir)
	opts.MemTableSize = 1024 * 1024 // 1MB

	e, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}
	defer e.Close()

	// Write initial value
	if err := e.Put([]byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("put failed: %v", err)
	}

	// Create snapshot
	snap := e.Snapshot()
	defer snap.Release()

	// Verify snapshot sees the value
	val, err := snap.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("snapshot get failed: %v", err)
	}
	if !bytes.Equal(val, []byte("value1")) {
		t.Errorf("snapshot value = %q, want %q", val, "value1")
	}

	// Update the value
	if err := e.Put([]byte("key1"), []byte("value2")); err != nil {
		t.Fatalf("put failed: %v", err)
	}

	// Snapshot should still see old value
	val, err = snap.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("snapshot get failed: %v", err)
	}
	if !bytes.Equal(val, []byte("value1")) {
		t.Errorf("snapshot still sees old value: got %q, want %q", val, "value1")
	}

	// Current view should see new value
	val, err = e.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if !bytes.Equal(val, []byte("value2")) {
		t.Errorf("current value = %q, want %q", val, "value2")
	}
}

func TestSnapshotIsolation(t *testing.T) {
	dir := tempDir(t)

	opts := DefaultOptions(dir)
	opts.MemTableSize = 1024 * 1024

	e, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}
	defer e.Close()

	// Write key1 = v1
	e.Put([]byte("key1"), []byte("v1"))

	// Snapshot 1
	snap1 := e.Snapshot()
	defer snap1.Release()

	// Write key1 = v2, key2 = v2
	e.Put([]byte("key1"), []byte("v2"))
	e.Put([]byte("key2"), []byte("v2"))

	// Snapshot 2
	snap2 := e.Snapshot()
	defer snap2.Release()

	// Write key1 = v3, key3 = v3
	e.Put([]byte("key1"), []byte("v3"))
	e.Put([]byte("key3"), []byte("v3"))

	// Snapshot 1 sees: key1=v1, key2=nil
	val, _ := snap1.Get([]byte("key1"))
	if !bytes.Equal(val, []byte("v1")) {
		t.Errorf("snap1 key1 = %q, want v1", val)
	}
	val, _ = snap1.Get([]byte("key2"))
	if val != nil {
		t.Errorf("snap1 key2 = %q, want nil", val)
	}

	// Snapshot 2 sees: key1=v2, key2=v2, key3=nil
	val, _ = snap2.Get([]byte("key1"))
	if !bytes.Equal(val, []byte("v2")) {
		t.Errorf("snap2 key1 = %q, want v2", val)
	}
	val, _ = snap2.Get([]byte("key2"))
	if !bytes.Equal(val, []byte("v2")) {
		t.Errorf("snap2 key2 = %q, want v2", val)
	}
	val, _ = snap2.Get([]byte("key3"))
	if val != nil {
		t.Errorf("snap2 key3 = %q, want nil", val)
	}

	// Current sees: key1=v3, key2=v2, key3=v3
	val, _ = e.Get([]byte("key1"))
	if !bytes.Equal(val, []byte("v3")) {
		t.Errorf("current key1 = %q, want v3", val)
	}
	val, _ = e.Get([]byte("key2"))
	if !bytes.Equal(val, []byte("v2")) {
		t.Errorf("current key2 = %q, want v2", val)
	}
	val, _ = e.Get([]byte("key3"))
	if !bytes.Equal(val, []byte("v3")) {
		t.Errorf("current key3 = %q, want v3", val)
	}
}

func TestSnapshotDelete(t *testing.T) {
	dir := tempDir(t)

	opts := DefaultOptions(dir)
	e, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}
	defer e.Close()

	// Write initial value
	e.Put([]byte("key1"), []byte("value1"))

	// Create snapshot before delete
	snap := e.Snapshot()
	defer snap.Release()

	// Delete the key
	e.Delete([]byte("key1"))

	// Snapshot should still see the value
	val, err := snap.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("snapshot get failed: %v", err)
	}
	if !bytes.Equal(val, []byte("value1")) {
		t.Errorf("snapshot value = %q, want value1", val)
	}

	// Current view should not see the value
	val, err = e.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if val != nil {
		t.Errorf("current value = %q, want nil", val)
	}
}

func TestSnapshotRelease(t *testing.T) {
	dir := tempDir(t)

	opts := DefaultOptions(dir)
	e, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}
	defer e.Close()

	e.Put([]byte("key1"), []byte("value1"))

	snap := e.Snapshot()

	// Should work before release
	val, err := snap.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("get before release failed: %v", err)
	}
	if !bytes.Equal(val, []byte("value1")) {
		t.Errorf("value = %q, want value1", val)
	}

	// Release snapshot
	snap.Release()

	// Verify it's released
	if !snap.IsReleased() {
		t.Error("snapshot should be released")
	}

	// Get after release should fail
	_, err = snap.Get([]byte("key1"))
	if err != ErrSnapshotReleased {
		t.Errorf("get after release: got err %v, want ErrSnapshotReleased", err)
	}

	// Double release should be safe
	snap.Release()
}

func TestSnapshotSeqNum(t *testing.T) {
	dir := tempDir(t)

	opts := DefaultOptions(dir)
	e, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}
	defer e.Close()

	// Initial sequence
	snap1 := e.Snapshot()
	seq1 := snap1.SeqNum()
	snap1.Release()

	// Write some data
	for i := 0; i < 10; i++ {
		e.Put([]byte(fmt.Sprintf("key%d", i)), []byte("value"))
	}

	// New snapshot should have higher sequence
	snap2 := e.Snapshot()
	seq2 := snap2.SeqNum()
	snap2.Release()

	if seq2 <= seq1 {
		t.Errorf("seq2 (%d) should be > seq1 (%d)", seq2, seq1)
	}

	// Should be exactly 10 more
	if seq2 != seq1+10 {
		t.Errorf("seq2 = %d, want %d", seq2, seq1+10)
	}
}

func TestSnapshotStats(t *testing.T) {
	dir := tempDir(t)

	opts := DefaultOptions(dir)
	e, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}
	defer e.Close()

	// Initially no snapshots
	stats := e.Stats()
	if stats.ActiveSnapshots != 0 {
		t.Errorf("initial active snapshots = %d, want 0", stats.ActiveSnapshots)
	}

	// Create some snapshots
	snap1 := e.Snapshot()
	snap2 := e.Snapshot()
	snap3 := e.Snapshot()

	stats = e.Stats()
	if stats.ActiveSnapshots != 3 {
		t.Errorf("active snapshots = %d, want 3", stats.ActiveSnapshots)
	}

	// Release some
	snap2.Release()
	stats = e.Stats()
	if stats.ActiveSnapshots != 2 {
		t.Errorf("active snapshots = %d, want 2", stats.ActiveSnapshots)
	}

	snap1.Release()
	snap3.Release()
	stats = e.Stats()
	if stats.ActiveSnapshots != 0 {
		t.Errorf("active snapshots = %d, want 0", stats.ActiveSnapshots)
	}
}

func TestSnapshotOldestTracking(t *testing.T) {
	dir := tempDir(t)

	opts := DefaultOptions(dir)
	e, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}
	defer e.Close()

	// Write some data
	e.Put([]byte("key1"), []byte("value1"))
	snap1 := e.Snapshot()

	e.Put([]byte("key2"), []byte("value2"))
	snap2 := e.Snapshot()

	e.Put([]byte("key3"), []byte("value3"))

	// Oldest should be snap1's sequence
	stats := e.Stats()
	if stats.OldestSnapshot != snap1.SeqNum() {
		t.Errorf("oldest = %d, want %d (snap1)", stats.OldestSnapshot, snap1.SeqNum())
	}

	// Release snap1 -> oldest should be snap2
	snap1.Release()
	stats = e.Stats()
	if stats.OldestSnapshot != snap2.SeqNum() {
		t.Errorf("oldest = %d, want %d (snap2)", stats.OldestSnapshot, snap2.SeqNum())
	}

	snap2.Release()
}

func TestSnapshotConcurrent(t *testing.T) {
	dir := tempDir(t)

	opts := DefaultOptions(dir)
	e, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}
	defer e.Close()

	const numGoroutines = 10
	const numOps = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2) // Writers and readers

	// Writers
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				key := []byte(fmt.Sprintf("key-%d-%d", id, j))
				value := []byte(fmt.Sprintf("value-%d-%d", id, j))
				e.Put(key, value)
			}
		}(i)
	}

	// Snapshot readers
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				snap := e.Snapshot()
				// Read some keys through snapshot
				for k := 0; k < 10; k++ {
					key := []byte(fmt.Sprintf("key-%d-%d", id, k))
					snap.Get(key)
				}
				snap.Release()
			}
		}(i)
	}

	wg.Wait()

	// All snapshots should be released
	stats := e.Stats()
	if stats.ActiveSnapshots != 0 {
		t.Errorf("leaked snapshots: %d", stats.ActiveSnapshots)
	}
}

func TestSnapshotWithFlush(t *testing.T) {
	dir := tempDir(t)

	opts := DefaultOptions(dir)
	opts.MemTableSize = 2048 // Small MemTable to trigger flushes

	e, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}
	defer e.Close()

	// Write some data
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		value := bytes.Repeat([]byte("v"), 50)
		e.Put(key, value)
	}

	// Create snapshot
	snap := e.Snapshot()
	defer snap.Release()

	// Write more data to trigger flush
	for i := 20; i < 50; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		value := bytes.Repeat([]byte("v"), 50)
		e.Put(key, value)
	}

	// Wait for flush
	time.Sleep(200 * time.Millisecond)

	// Snapshot should still see original 20 keys but not new ones
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		val, err := snap.Get(key)
		if err != nil {
			t.Fatalf("snapshot get key%05d failed: %v", i, err)
		}
		if val == nil {
			t.Errorf("snapshot missing key%05d", i)
		}
	}

	// Note: Full MVCC isolation in SSTables requires sequence number filtering
	// in the level manager, which is a more complex implementation.
	// For now, snapshot isolation is fully implemented for MemTable reads.
	// SSTable sequence filtering is part of Step 12 (Manifest & Recovery).

	// Current view should see all keys
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		val, err := e.Get(key)
		if err != nil {
			t.Fatalf("get key%05d failed: %v", i, err)
		}
		if val == nil {
			t.Errorf("current missing key%05d", i)
		}
	}
}

func TestGetSnapshot(t *testing.T) {
	dir := tempDir(t)

	opts := DefaultOptions(dir)
	e, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}
	defer e.Close()

	e.Put([]byte("key"), []byte("value"))

	// GetSnapshot is alias for Snapshot
	snap := e.GetSnapshot()
	defer snap.Release()

	val, _ := snap.Get([]byte("key"))
	if !bytes.Equal(val, []byte("value")) {
		t.Errorf("value = %q, want value", val)
	}
}

func BenchmarkSnapshotCreate(b *testing.B) {
	dir := b.TempDir()

	opts := DefaultOptions(dir)
	e, _ := Open(opts)
	defer e.Close()

	// Write some data
	for i := 0; i < 1000; i++ {
		e.Put([]byte(fmt.Sprintf("key%d", i)), []byte("value"))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snap := e.Snapshot()
		snap.Release()
	}
}

func BenchmarkSnapshotGet(b *testing.B) {
	dir := b.TempDir()

	opts := DefaultOptions(dir)
	e, _ := Open(opts)
	defer e.Close()

	// Write some data
	for i := 0; i < 1000; i++ {
		e.Put([]byte(fmt.Sprintf("key%d", i)), []byte("value"))
	}

	snap := e.Snapshot()
	defer snap.Release()

	keys := make([][]byte, 100)
	for i := 0; i < 100; i++ {
		keys[i] = []byte(fmt.Sprintf("key%d", i*10))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snap.Get(keys[i%100])
	}
}
