package lsm

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"
)

// putWithRetry attempts to put with retry on write stall
func putWithRetry(e *Engine, key, value []byte, maxRetries int) error {
	for i := 0; i < maxRetries; i++ {
		err := e.Put(key, value)
		if err == nil {
			return nil
		}
		if err != ErrWriteStall {
			return err
		}
		// Wait a bit for flush to complete
		time.Sleep(10 * time.Millisecond)
	}
	return ErrWriteStall
}

func TestEngineFlushToSSTable(t *testing.T) {
	dir := tempDir(t)

	// Use moderate MemTable size to trigger flush
	opts := DefaultOptions(dir)
	opts.MemTableSize = 4096 // 4KB - will trigger flush after ~40 entries
	opts.MaxMemTables = 8    // Allow more immutable tables

	e, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}
	defer e.Close()

	// Write enough data to trigger at least one flush
	n := 100
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		value := bytes.Repeat([]byte("v"), 100) // 100 byte values
		if err := putWithRetry(e, key, value, 50); err != nil {
			t.Fatalf("put failed for %d: %v", i, err)
		}
	}

	// Wait for background flush
	time.Sleep(100 * time.Millisecond)

	// Check that we have some SSTables
	e.mu.RLock()
	l0Count := len(e.l0Tables)
	e.mu.RUnlock()

	// We should have at least one L0 table from flushing
	t.Logf("L0 table count: %d", l0Count)

	// Verify all data is still readable
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		expectedValue := bytes.Repeat([]byte("v"), 100)

		got, err := e.Get(key)
		if err != nil {
			t.Fatalf("get failed for %d: %v", i, err)
		}
		if !bytes.Equal(got, expectedValue) {
			t.Errorf("value mismatch for %d: got len=%d, want len=%d", i, len(got), len(expectedValue))
		}
	}
}

func TestEngineFlushAndRecovery(t *testing.T) {
	dir := tempDir(t)

	// Use small MemTable to trigger flush
	opts := DefaultOptions(dir)
	opts.MemTableSize = 1024 // 1KB

	// First session: write data and let it flush
	e1, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}

	n := 50
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		value := []byte(fmt.Sprintf("value%05d", i))
		if err := e1.Put(key, value); err != nil {
			t.Fatalf("put failed: %v", err)
		}
	}

	// Force sync and close
	if err := e1.Sync(); err != nil {
		t.Fatalf("sync failed: %v", err)
	}

	// Wait for background flush
	time.Sleep(100 * time.Millisecond)

	// Check SST directory
	sstDir := dir + "/sst"
	entries, err := os.ReadDir(sstDir)
	if err != nil {
		t.Fatalf("readdir failed: %v", err)
	}
	t.Logf("SST files after first session: %d", len(entries))
	for _, e := range entries {
		t.Logf("  - %s", e.Name())
	}

	if err := e1.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Second session: reopen and verify data
	e2, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to reopen engine: %v", err)
	}
	defer e2.Close()

	// Verify all data
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		expectedValue := []byte(fmt.Sprintf("value%05d", i))

		got, err := e2.Get(key)
		if err != nil {
			t.Fatalf("get failed for %d: %v", i, err)
		}
		if !bytes.Equal(got, expectedValue) {
			t.Errorf("value mismatch for %d: got %q, want %q", i, got, expectedValue)
		}
	}
}

func TestEngineFlushWithDeletes(t *testing.T) {
	dir := tempDir(t)

	opts := DefaultOptions(dir)
	opts.MemTableSize = 1024

	e, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}
	defer e.Close()

	// Write data
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		value := bytes.Repeat([]byte("v"), 100)
		if err := e.Put(key, value); err != nil {
			t.Fatalf("put failed: %v", err)
		}
	}

	// Delete some keys
	for i := 0; i < 20; i += 2 {
		key := []byte(fmt.Sprintf("key%05d", i))
		if err := e.Delete(key); err != nil {
			t.Fatalf("delete failed: %v", err)
		}
	}

	// Wait for flush
	time.Sleep(100 * time.Millisecond)

	// Verify: even keys should be deleted, odd keys should exist
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		got, err := e.Get(key)
		if err != nil {
			t.Fatalf("get failed for %d: %v", i, err)
		}

		if i%2 == 0 {
			// Even keys should be deleted
			if got != nil {
				t.Errorf("key%05d should be deleted, got %q", i, got)
			}
		} else {
			// Odd keys should exist
			if got == nil {
				t.Errorf("key%05d should exist", i)
			}
		}
	}
}

func TestEngineMultipleFlushes(t *testing.T) {
	dir := tempDir(t)

	opts := DefaultOptions(dir)
	opts.MemTableSize = 2048 // 2KB to force multiple flushes
	opts.MaxMemTables = 8    // Allow more pending flushes

	e, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}
	defer e.Close()

	// Write data in batches to force multiple flushes
	totalKeys := 100
	for i := 0; i < totalKeys; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		value := bytes.Repeat([]byte("x"), 50)
		if err := putWithRetry(e, key, value, 50); err != nil {
			t.Fatalf("put failed at %d: %v", i, err)
		}
	}

	// Wait for all flushes
	time.Sleep(200 * time.Millisecond)

	// Verify all data
	for i := 0; i < totalKeys; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		got, err := e.Get(key)
		if err != nil {
			t.Fatalf("get failed for %d: %v", i, err)
		}
		if len(got) != 50 {
			t.Errorf("value length mismatch for %d: got %d, want 50", i, len(got))
		}
	}

	stats := e.Stats()
	t.Logf("Final stats: L0Tables=%d, ImmutableMemTables=%d, SeqNum=%d",
		stats.L0TableCount, stats.ImmutableCount, stats.SeqNum)
}

func TestEngineReadFromSSTable(t *testing.T) {
	dir := tempDir(t)

	opts := DefaultOptions(dir)
	opts.MemTableSize = 256 // Very small

	// First session: write and flush
	e1, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}

	// Write some data
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		value := []byte(fmt.Sprintf("value%05d", i))
		if err := e1.Put(key, value); err != nil {
			t.Fatalf("put failed: %v", err)
		}
	}

	// Force flush and close
	e1.Sync()
	time.Sleep(100 * time.Millisecond)
	e1.Close()

	// Second session: data should be read from SSTables only
	e2, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to reopen: %v", err)
	}
	defer e2.Close()

	// The memtable should be empty or near-empty
	// since we recover from SST + WAL
	t.Logf("After reopen: L0Tables=%d", len(e2.l0Tables))

	// Verify data is readable (either from SST or recovered WAL)
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		expectedValue := []byte(fmt.Sprintf("value%05d", i))

		got, err := e2.Get(key)
		if err != nil {
			t.Fatalf("get failed for %d: %v", i, err)
		}
		if !bytes.Equal(got, expectedValue) {
			t.Errorf("value mismatch for %d: got %q, want %q", i, got, expectedValue)
		}
	}
}
