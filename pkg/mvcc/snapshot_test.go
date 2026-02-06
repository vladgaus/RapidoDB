package mvcc

import (
	"sync"
	"testing"
)

func TestSnapshotManagerBasic(t *testing.T) {
	sm := NewSnapshotManager()

	// Set current sequence
	sm.SetCurrentSeq(10)
	if got := sm.GetCurrentSeq(); got != 10 {
		t.Errorf("GetCurrentSeq() = %d, want 10", got)
	}

	// Create snapshot
	snap := sm.CreateSnapshot()
	if snap.SeqNum() != 10 {
		t.Errorf("snapshot seqNum = %d, want 10", snap.SeqNum())
	}

	// Should have 1 active snapshot
	if sm.NumSnapshots() != 1 {
		t.Errorf("NumSnapshots() = %d, want 1", sm.NumSnapshots())
	}

	// Release snapshot
	snap.Release()

	// Should have 0 active snapshots
	if sm.NumSnapshots() != 0 {
		t.Errorf("NumSnapshots() = %d, want 0", sm.NumSnapshots())
	}
}

func TestSnapshotManagerOldestSeq(t *testing.T) {
	sm := NewSnapshotManager()

	// No snapshots: oldest should be current
	sm.SetCurrentSeq(100)
	if got := sm.GetOldestSeq(); got != 100 {
		t.Errorf("GetOldestSeq() = %d, want 100 (current)", got)
	}

	// Create snapshot at seq 100
	snap1 := sm.CreateSnapshot()

	// Advance current
	sm.SetCurrentSeq(200)

	// Oldest should still be 100
	if got := sm.GetOldestSeq(); got != 100 {
		t.Errorf("GetOldestSeq() = %d, want 100", got)
	}

	// Create another snapshot at 200
	snap2 := sm.CreateSnapshot()

	// Oldest should still be 100
	if got := sm.GetOldestSeq(); got != 100 {
		t.Errorf("GetOldestSeq() = %d, want 100", got)
	}

	// Release first snapshot
	snap1.Release()

	// Oldest should now be 200
	if got := sm.GetOldestSeq(); got != 200 {
		t.Errorf("GetOldestSeq() = %d, want 200", got)
	}

	// Release second snapshot
	snap2.Release()

	// Oldest should be current (200)
	if got := sm.GetOldestSeq(); got != 200 {
		t.Errorf("GetOldestSeq() = %d, want 200 (current)", got)
	}
}

func TestSnapshotManagerMultipleSnapshots(t *testing.T) {
	sm := NewSnapshotManager()

	// Create snapshots at different sequence numbers
	sm.SetCurrentSeq(10)
	snap1 := sm.CreateSnapshot()

	sm.SetCurrentSeq(20)
	snap2 := sm.CreateSnapshot()

	sm.SetCurrentSeq(30)
	snap3 := sm.CreateSnapshot()

	sm.SetCurrentSeq(40)
	snap4 := sm.CreateSnapshot()

	// Verify sequence numbers
	if snap1.SeqNum() != 10 || snap2.SeqNum() != 20 ||
		snap3.SeqNum() != 30 || snap4.SeqNum() != 40 {
		t.Error("snapshot sequence numbers incorrect")
	}

	// Verify count
	if sm.NumSnapshots() != 4 {
		t.Errorf("NumSnapshots() = %d, want 4", sm.NumSnapshots())
	}

	// Oldest should be 10
	if got := sm.GetOldestSeq(); got != 10 {
		t.Errorf("GetOldestSeq() = %d, want 10", got)
	}

	// Release in different order (not oldest first)
	snap2.Release()
	if got := sm.GetOldestSeq(); got != 10 {
		t.Errorf("after releasing snap2: GetOldestSeq() = %d, want 10", got)
	}

	snap1.Release()
	if got := sm.GetOldestSeq(); got != 30 {
		t.Errorf("after releasing snap1: GetOldestSeq() = %d, want 30", got)
	}

	snap4.Release()
	if got := sm.GetOldestSeq(); got != 30 {
		t.Errorf("after releasing snap4: GetOldestSeq() = %d, want 30", got)
	}

	snap3.Release()
	if got := sm.GetOldestSeq(); got != 40 {
		t.Errorf("after releasing all: GetOldestSeq() = %d, want 40 (current)", got)
	}
}

func TestSnapshotManagerCallback(t *testing.T) {
	sm := NewSnapshotManager()

	var callbackValues []uint64
	sm.SetOldestChangeCallback(func(oldestSeq uint64) {
		callbackValues = append(callbackValues, oldestSeq)
	})

	sm.SetCurrentSeq(10)
	snap1 := sm.CreateSnapshot()

	sm.SetCurrentSeq(20)
	snap2 := sm.CreateSnapshot()

	sm.SetCurrentSeq(30)

	// Release should trigger callback
	snap1.Release()
	if len(callbackValues) != 1 || callbackValues[0] != 20 {
		t.Errorf("callback not called correctly: got %v", callbackValues)
	}

	snap2.Release()
	if len(callbackValues) != 2 || callbackValues[1] != 30 {
		t.Errorf("callback not called correctly: got %v", callbackValues)
	}
}

func TestSnapshotManagerDoubleRelease(t *testing.T) {
	sm := NewSnapshotManager()
	sm.SetCurrentSeq(10)

	snap := sm.CreateSnapshot()

	// First release
	snap.Release()
	if sm.NumSnapshots() != 0 {
		t.Error("snapshot not released")
	}

	// Second release should be safe (no-op)
	snap.Release()
	if sm.NumSnapshots() != 0 {
		t.Error("double release caused issue")
	}
}

func TestSnapshotIsReleased(t *testing.T) {
	sm := NewSnapshotManager()
	sm.SetCurrentSeq(10)

	snap := sm.CreateSnapshot()

	if snap.IsReleased() {
		t.Error("snapshot should not be released yet")
	}

	snap.Release()

	if !snap.IsReleased() {
		t.Error("snapshot should be released")
	}
}

func TestSnapshotManagerCreateAt(t *testing.T) {
	sm := NewSnapshotManager()
	sm.SetCurrentSeq(100)

	// Create snapshot at a specific sequence
	snap := sm.CreateSnapshotAt(50)

	if snap.SeqNum() != 50 {
		t.Errorf("SeqNum() = %d, want 50", snap.SeqNum())
	}

	// Oldest should be the manually created one
	if got := sm.GetOldestSeq(); got != 50 {
		t.Errorf("GetOldestSeq() = %d, want 50", got)
	}

	snap.Release()
}

func TestSnapshotManagerGetAllSnapshots(t *testing.T) {
	sm := NewSnapshotManager()

	sm.SetCurrentSeq(10)
	snap1 := sm.CreateSnapshot()

	sm.SetCurrentSeq(20)
	sm.CreateSnapshot() // snap2

	sm.SetCurrentSeq(30)
	sm.CreateSnapshot() // snap3

	all := sm.GetAllSnapshots()
	if len(all) != 3 {
		t.Fatalf("expected 3 snapshots, got %d", len(all))
	}

	// Should be sorted (min-heap order)
	if all[0] != 10 {
		t.Errorf("first snapshot should be oldest (10), got %d", all[0])
	}

	snap1.Release()
	all = sm.GetAllSnapshots()
	if len(all) != 2 {
		t.Fatalf("expected 2 snapshots, got %d", len(all))
	}
}

func TestSnapshotManagerConcurrent(t *testing.T) {
	sm := NewSnapshotManager()

	const numGoroutines = 100
	const numOpsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()

			for j := 0; j < numOpsPerGoroutine; j++ {
				seq := uint64(id*numOpsPerGoroutine + j)
				sm.SetCurrentSeq(seq)

				snap := sm.CreateSnapshot()
				_ = sm.GetOldestSeq()
				_ = sm.NumSnapshots()
				snap.Release()
			}
		}(i)
	}

	wg.Wait()

	// All snapshots should be released
	if sm.NumSnapshots() != 0 {
		t.Errorf("expected 0 snapshots, got %d", sm.NumSnapshots())
	}
}

func TestSnapshotManagerUniqueIDs(t *testing.T) {
	sm := NewSnapshotManager()

	ids := make(map[uint64]bool)
	for i := 0; i < 1000; i++ {
		sm.SetCurrentSeq(uint64(i))
		snap := sm.CreateSnapshot()
		if ids[snap.ID()] {
			t.Errorf("duplicate snapshot ID: %d", snap.ID())
		}
		ids[snap.ID()] = true
		snap.Release()
	}
}

func BenchmarkSnapshotCreate(b *testing.B) {
	sm := NewSnapshotManager()
	sm.SetCurrentSeq(1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snap := sm.CreateSnapshot()
		snap.Release()
	}
}

func BenchmarkSnapshotGetOldest(b *testing.B) {
	sm := NewSnapshotManager()
	sm.SetCurrentSeq(1)

	// Create some snapshots
	for i := 0; i < 100; i++ {
		sm.SetCurrentSeq(uint64(i))
		sm.CreateSnapshot()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sm.GetOldestSeq()
	}
}
