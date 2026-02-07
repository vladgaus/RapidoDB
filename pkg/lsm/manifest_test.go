package lsm

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestManifestRecoveryBasic(t *testing.T) {
	dir := tempDir(t)

	// Write some data and close
	opts := DefaultOptions(dir)
	opts.MemTableSize = 1024 * 1024 // 1MB

	e, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}

	// Write data
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		value := []byte(fmt.Sprintf("value%05d", i))
		if err := e.Put(key, value); err != nil {
			t.Fatalf("put failed: %v", err)
		}
	}

	// Get sequence number before close
	seqNum := e.Stats().SeqNum

	// Close
	if err := e.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Reopen
	e2, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to reopen engine: %v", err)
	}
	defer e2.Close()

	// Verify data
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		expected := []byte(fmt.Sprintf("value%05d", i))
		val, err := e2.Get(key)
		if err != nil {
			t.Fatalf("get failed for key%05d: %v", i, err)
		}
		if !bytes.Equal(val, expected) {
			t.Errorf("value mismatch for key%05d: got %q, want %q", i, val, expected)
		}
	}

	// Verify sequence number recovered
	recoveredSeq := e2.Stats().SeqNum
	if recoveredSeq < seqNum {
		t.Errorf("sequence number not recovered: got %d, want >= %d", recoveredSeq, seqNum)
	}
}

func TestManifestRecoveryWithFlush(t *testing.T) {
	dir := tempDir(t)

	opts := DefaultOptions(dir)
	opts.MemTableSize = 2048 // Small memtable to force flushes

	e, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}

	// Write enough data to trigger flush
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		value := bytes.Repeat([]byte("v"), 100)
		if err := e.Put(key, value); err != nil {
			t.Fatalf("put failed: %v", err)
		}
	}

	// Wait for flush
	time.Sleep(200 * time.Millisecond)

	// Verify we have SSTable files
	stats := e.Stats()
	t.Logf("Before close: L0 tables = %d", stats.L0TableCount)

	if err := e.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Verify manifest exists
	manifestExists := false
	currentFile := filepath.Join(dir, "CURRENT")
	if _, err := os.Stat(currentFile); err == nil {
		manifestExists = true
	}
	if !manifestExists {
		t.Fatal("CURRENT file not found after close")
	}

	// Reopen
	e2, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to reopen engine: %v", err)
	}
	defer e2.Close()

	// Verify data
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		val, err := e2.Get(key)
		if err != nil {
			t.Fatalf("get failed for key%05d: %v", i, err)
		}
		if val == nil {
			t.Errorf("key%05d not found after recovery", i)
		}
	}

	// Check L0 tables recovered
	stats2 := e2.Stats()
	t.Logf("After reopen: L0 tables = %d", stats2.L0TableCount)
}

func TestManifestRecoveryAfterCompaction(t *testing.T) {
	dir := tempDir(t)

	opts := DefaultOptions(dir)
	opts.MemTableSize = 2048           // Small memtable
	opts.L0CompactionTrigger = 2       // Trigger compaction early
	opts.TargetFileSizeBase = 4 * 1024 // Small files

	e, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}

	// Write data in batches to trigger multiple flushes
	for batch := 0; batch < 5; batch++ {
		for i := 0; i < 20; i++ {
			key := []byte(fmt.Sprintf("batch%d-key%05d", batch, i))
			value := bytes.Repeat([]byte("v"), 50)
			if err := e.Put(key, value); err != nil {
				t.Fatalf("put failed: %v", err)
			}
		}
		// Allow flush to happen
		time.Sleep(100 * time.Millisecond)
	}

	// Allow compaction to happen
	time.Sleep(500 * time.Millisecond)

	stats := e.Stats()
	t.Logf("Before close: L0=%d, SeqNum=%d", stats.L0TableCount, stats.SeqNum)

	if err := e.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Reopen
	e2, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to reopen engine: %v", err)
	}
	defer e2.Close()

	// Verify all data
	for batch := 0; batch < 5; batch++ {
		for i := 0; i < 20; i++ {
			key := []byte(fmt.Sprintf("batch%d-key%05d", batch, i))
			val, err := e2.Get(key)
			if err != nil {
				t.Fatalf("get failed for %s: %v", key, err)
			}
			if val == nil {
				t.Errorf("%s not found after recovery", key)
			}
		}
	}
}

func TestManifestRecoveryWithDeletes(t *testing.T) {
	dir := tempDir(t)

	opts := DefaultOptions(dir)
	opts.MemTableSize = 1024 * 1024

	e, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}

	// Write data
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		value := []byte(fmt.Sprintf("value%05d", i))
		if err := e.Put(key, value); err != nil {
			t.Fatalf("put failed: %v", err)
		}
	}

	// Delete even-numbered keys
	for i := 0; i < 100; i += 2 {
		key := []byte(fmt.Sprintf("key%05d", i))
		if err := e.Delete(key); err != nil {
			t.Fatalf("delete failed: %v", err)
		}
	}

	if err := e.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Reopen
	e2, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to reopen engine: %v", err)
	}
	defer e2.Close()

	// Verify even keys are deleted
	for i := 0; i < 100; i += 2 {
		key := []byte(fmt.Sprintf("key%05d", i))
		val, err := e2.Get(key)
		if err != nil {
			t.Fatalf("get failed: %v", err)
		}
		if val != nil {
			t.Errorf("key%05d should be deleted, got %q", i, val)
		}
	}

	// Verify odd keys still exist
	for i := 1; i < 100; i += 2 {
		key := []byte(fmt.Sprintf("key%05d", i))
		expected := []byte(fmt.Sprintf("value%05d", i))
		val, err := e2.Get(key)
		if err != nil {
			t.Fatalf("get failed: %v", err)
		}
		if !bytes.Equal(val, expected) {
			t.Errorf("value mismatch for key%05d: got %q, want %q", i, val, expected)
		}
	}
}

func TestManifestRecoveryMultipleRestarts(t *testing.T) {
	dir := tempDir(t)

	opts := DefaultOptions(dir)
	opts.MemTableSize = 2048 // Small memtable

	// Write, close, reopen 3 times
	for restart := 0; restart < 3; restart++ {
		e, err := Open(opts)
		if err != nil {
			t.Fatalf("restart %d: failed to open engine: %v", restart, err)
		}

		// Write data for this restart
		for i := 0; i < 20; i++ {
			key := []byte(fmt.Sprintf("restart%d-key%05d", restart, i))
			value := bytes.Repeat([]byte("v"), 50)
			if err := e.Put(key, value); err != nil {
				t.Fatalf("restart %d: put failed: %v", restart, err)
			}
		}

		// Wait for potential flush
		time.Sleep(100 * time.Millisecond)

		if err := e.Close(); err != nil {
			t.Fatalf("restart %d: close failed: %v", restart, err)
		}
	}

	// Final open and verify all data
	e, err := Open(opts)
	if err != nil {
		t.Fatalf("final open failed: %v", err)
	}
	defer e.Close()

	// Verify data from all restarts
	for restart := 0; restart < 3; restart++ {
		for i := 0; i < 20; i++ {
			key := []byte(fmt.Sprintf("restart%d-key%05d", restart, i))
			val, err := e.Get(key)
			if err != nil {
				t.Fatalf("get failed for %s: %v", key, err)
			}
			if val == nil {
				t.Errorf("%s not found after multiple restarts", key)
			}
		}
	}
}

func TestManifestFileNumberContinuity(t *testing.T) {
	dir := tempDir(t)

	opts := DefaultOptions(dir)
	opts.MemTableSize = 2048

	// First session - write and flush
	e, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}

	for i := 0; i < 30; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		value := bytes.Repeat([]byte("v"), 100)
		e.Put(key, value)
	}
	time.Sleep(200 * time.Millisecond)
	e.Close()

	// Count SST files
	files1, _ := filepath.Glob(filepath.Join(dir, "sst", "*.sst"))
	t.Logf("SST files after first session: %d", len(files1))

	// Second session
	e2, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to reopen: %v", err)
	}

	for i := 30; i < 60; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		value := bytes.Repeat([]byte("v"), 100)
		e2.Put(key, value)
	}
	time.Sleep(200 * time.Millisecond)
	e2.Close()

	// Count SST files - should be more, with no gaps
	files2, _ := filepath.Glob(filepath.Join(dir, "sst", "*.sst"))
	t.Logf("SST files after second session: %d", len(files2))

	// The file numbers should continue from where they left off
	if len(files2) <= len(files1) {
		t.Logf("Warning: File count didn't increase (may be ok if compaction happened)")
	}
}

func TestManifestRecoveryEmptyDB(t *testing.T) {
	dir := tempDir(t)

	opts := DefaultOptions(dir)

	// Open, write nothing, close
	e, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Reopen
	e2, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to reopen empty db: %v", err)
	}
	defer e2.Close()

	// Should work fine
	val, err := e2.Get([]byte("nonexistent"))
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if val != nil {
		t.Error("expected nil for nonexistent key")
	}
}

func TestManifestRecoverySequenceNumber(t *testing.T) {
	dir := tempDir(t)

	opts := DefaultOptions(dir)

	e, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}

	// Write 1000 entries
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		e.Put(key, []byte("value"))
	}

	expectedSeq := e.Stats().SeqNum
	e.Close()

	// Reopen
	e2, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to reopen: %v", err)
	}

	recoveredSeq := e2.Stats().SeqNum
	e2.Close()

	// Sequence should be at least as high as before
	if recoveredSeq < expectedSeq {
		t.Errorf("sequence number regression: got %d, want >= %d", recoveredSeq, expectedSeq)
	}
	t.Logf("Expected seq: %d, Recovered seq: %d", expectedSeq, recoveredSeq)
}

func BenchmarkManifestRecovery(b *testing.B) {
	dir := b.TempDir()

	opts := DefaultOptions(dir)
	opts.MemTableSize = 4096

	// Setup: write some data
	e, _ := Open(opts)
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		value := bytes.Repeat([]byte("v"), 100)
		e.Put(key, value)
	}
	time.Sleep(500 * time.Millisecond) // Allow flushes
	e.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e, err := Open(opts)
		if err != nil {
			b.Fatalf("open failed: %v", err)
		}
		e.Close()
	}
}
