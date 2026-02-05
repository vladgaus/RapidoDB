package lsm

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestCompactionBasic(t *testing.T) {
	dir := tempDir(t)

	// Configure to trigger compaction quickly
	opts := DefaultOptions(dir)
	opts.MemTableSize = 2048       // Small memtable
	opts.L0CompactionTrigger = 2   // Compact after 2 L0 files
	opts.L0StopWritesTrigger = 8   // Allow more L0 files before stall
	opts.MaxMemTables = 8          // Allow more immutable tables
	opts.MaxBytesForLevelBase = 4096 // Small L1 target

	e, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}
	defer e.Close()

	// Write enough data to trigger L0 -> L1 compaction
	n := 100
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		value := bytes.Repeat([]byte("v"), 50)
		if err := putWithRetry(e, key, value, 100); err != nil {
			t.Fatalf("put failed at %d: %v", i, err)
		}
	}

	// Wait for flush and compaction
	time.Sleep(300 * time.Millisecond)

	// Get stats
	stats := e.Stats()
	t.Logf("Stats: L0=%d, Compactions=%d, BytesWritten=%d",
		stats.L0TableCount, stats.CompactionStats.CompactionsRun, stats.CompactionStats.BytesWritten)

	// Verify all data is still readable
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		got, err := e.Get(key)
		if err != nil {
			t.Fatalf("get failed for %d: %v", i, err)
		}
		if len(got) != 50 {
			t.Errorf("value length mismatch for %d: got %d, want 50", i, len(got))
		}
	}
}

func TestCompactionWithDeletes(t *testing.T) {
	dir := tempDir(t)

	opts := DefaultOptions(dir)
	opts.MemTableSize = 2048
	opts.L0CompactionTrigger = 2
	opts.L0StopWritesTrigger = 8
	opts.MaxMemTables = 8
	opts.MaxBytesForLevelBase = 4096

	e, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}
	defer e.Close()

	// Write data
	n := 50
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		value := bytes.Repeat([]byte("v"), 50)
		if err := putWithRetry(e, key, value, 100); err != nil {
			t.Fatalf("put failed at %d: %v", i, err)
		}
	}

	// Delete half the keys
	for i := 0; i < n; i += 2 {
		key := []byte(fmt.Sprintf("key%05d", i))
		if err := e.Delete(key); err != nil {
			t.Fatalf("delete failed for %d: %v", i, err)
		}
	}

	// Wait for compaction
	time.Sleep(300 * time.Millisecond)

	// Verify: even keys should be deleted, odd keys should exist
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		got, err := e.Get(key)
		if err != nil {
			t.Fatalf("get failed for %d: %v", i, err)
		}

		if i%2 == 0 {
			if got != nil {
				t.Errorf("key%05d should be deleted, got value", i)
			}
		} else {
			if got == nil {
				t.Errorf("key%05d should exist", i)
			}
		}
	}
}

func TestCompactionOverwrites(t *testing.T) {
	dir := tempDir(t)

	opts := DefaultOptions(dir)
	opts.MemTableSize = 2048
	opts.L0CompactionTrigger = 2
	opts.L0StopWritesTrigger = 8
	opts.MaxMemTables = 8
	opts.MaxBytesForLevelBase = 4096

	e, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}
	defer e.Close()

	// Write initial values
	n := 20
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		value := []byte(fmt.Sprintf("value-v1-%05d", i))
		if err := putWithRetry(e, key, value, 100); err != nil {
			t.Fatalf("put failed at %d: %v", i, err)
		}
	}

	// Wait for flush
	time.Sleep(100 * time.Millisecond)

	// Overwrite with new values
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		value := []byte(fmt.Sprintf("value-v2-%05d", i))
		if err := putWithRetry(e, key, value, 100); err != nil {
			t.Fatalf("put failed at %d: %v", i, err)
		}
	}

	// Wait for compaction
	time.Sleep(300 * time.Millisecond)

	// Verify: should see the v2 values
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		expected := []byte(fmt.Sprintf("value-v2-%05d", i))

		got, err := e.Get(key)
		if err != nil {
			t.Fatalf("get failed for %d: %v", i, err)
		}
		if !bytes.Equal(got, expected) {
			t.Errorf("value mismatch for %d: got %q, want %q", i, got, expected)
		}
	}
}

func TestCompactionRecovery(t *testing.T) {
	dir := tempDir(t)

	opts := DefaultOptions(dir)
	opts.MemTableSize = 2048
	opts.L0CompactionTrigger = 2
	opts.L0StopWritesTrigger = 8
	opts.MaxMemTables = 8

	// First session: write data and let it compact
	e1, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}

	n := 50
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		value := []byte(fmt.Sprintf("value%05d", i))
		if err := putWithRetry(e1, key, value, 100); err != nil {
			t.Fatalf("put failed at %d: %v", i, err)
		}
	}

	// Wait for compaction
	time.Sleep(300 * time.Millisecond)

	stats := e1.Stats()
	t.Logf("Before close: L0=%d, Compactions=%d", stats.L0TableCount, stats.CompactionStats.CompactionsRun)

	e1.Sync()
	e1.Close()

	// List SST files
	entries, _ := os.ReadDir(dir + "/sst")
	t.Logf("SST files: %d", len(entries))
	for _, entry := range entries {
		t.Logf("  - %s", entry.Name())
	}

	// Second session: reopen and verify
	e2, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to reopen engine: %v", err)
	}
	defer e2.Close()

	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		expected := []byte(fmt.Sprintf("value%05d", i))

		got, err := e2.Get(key)
		if err != nil {
			t.Fatalf("get failed for %d: %v", i, err)
		}
		if !bytes.Equal(got, expected) {
			t.Errorf("value mismatch for %d: got %q, want %q", i, got, expected)
		}
	}
}

func TestLevelStats(t *testing.T) {
	dir := tempDir(t)

	opts := DefaultOptions(dir)
	opts.MemTableSize = 2048
	opts.L0CompactionTrigger = 4
	opts.MaxMemTables = 8

	e, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}
	defer e.Close()

	// Write data
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		value := bytes.Repeat([]byte("x"), 50)
		if err := putWithRetry(e, key, value, 100); err != nil {
			t.Fatalf("put failed: %v", err)
		}
	}

	// Wait for flush
	time.Sleep(200 * time.Millisecond)

	stats := e.Stats()
	t.Logf("Engine stats:")
	t.Logf("  MemTable size: %d", stats.MemTableSize)
	t.Logf("  Immutable count: %d", stats.ImmutableCount)
	t.Logf("  L0 files: %d", stats.L0TableCount)
	t.Logf("  SeqNum: %d", stats.SeqNum)
	t.Logf("  Compactions: %d", stats.CompactionStats.CompactionsRun)
	t.Logf("  Bytes written: %d", stats.CompactionStats.BytesWritten)

	if len(stats.LevelStats) > 0 {
		t.Logf("Level stats:")
		for _, ls := range stats.LevelStats {
			if ls.NumFiles > 0 || ls.Size > 0 {
				t.Logf("  L%d: files=%d, size=%d, target=%d, score=%.2f",
					ls.Level, ls.NumFiles, ls.Size, ls.TargetSize, ls.Score)
			}
		}
	}
}

func TestCompactionStrategySelection(t *testing.T) {
	dir := tempDir(t)

	// Test that we can select different strategies
	strategies := []CompactionStrategy{
		CompactionLeveled,
		CompactionTiered, // Will fall back to leveled for now
		CompactionFIFO,   // Will fall back to leveled for now
	}

	for _, strategy := range strategies {
		t.Run(string(strategy), func(t *testing.T) {
			subdir := dir + "/" + string(strategy)
			opts := DefaultOptions(subdir)
			opts.CompactionStrategy = strategy
			opts.MemTableSize = 2048

			e, err := Open(opts)
			if err != nil {
				t.Fatalf("failed to open with %s strategy: %v", strategy, err)
			}
			defer e.Close()

			// Write some data
			for i := 0; i < 10; i++ {
				key := []byte(fmt.Sprintf("key%05d", i))
				value := []byte(fmt.Sprintf("value%05d", i))
				if err := e.Put(key, value); err != nil {
					t.Fatalf("put failed: %v", err)
				}
			}

			// Verify data
			for i := 0; i < 10; i++ {
				key := []byte(fmt.Sprintf("key%05d", i))
				expected := []byte(fmt.Sprintf("value%05d", i))
				got, err := e.Get(key)
				if err != nil {
					t.Fatalf("get failed: %v", err)
				}
				if !bytes.Equal(got, expected) {
					t.Errorf("mismatch for %d", i)
				}
			}
		})
	}
}

// Benchmark compaction impact on read performance
func BenchmarkGetWithCompaction(b *testing.B) {
	dir, _ := os.MkdirTemp("", "rapidodb-bench-*")
	defer os.RemoveAll(dir)

	opts := DefaultOptions(dir)
	opts.MemTableSize = 1024 * 1024 // 1MB
	opts.L0CompactionTrigger = 4

	e, _ := Open(opts)
	defer e.Close()

	// Populate with data
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("key%08d", i))
		value := []byte(fmt.Sprintf("value%08d", i))
		e.Put(key, value)
	}

	// Wait for background work
	time.Sleep(500 * time.Millisecond)

	keys := make([][]byte, 1000)
	for i := 0; i < 1000; i++ {
		keys[i] = []byte(fmt.Sprintf("key%08d", i*10))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Get(keys[i%1000])
	}
}
