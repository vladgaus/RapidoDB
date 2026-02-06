// Package mvcc provides Multi-Version Concurrency Control for RapidoDB.
//
// MVCC allows multiple versions of the same key to coexist, enabling:
// - Snapshot isolation: readers see a consistent view at a point in time
// - Non-blocking reads: readers don't block writers
// - Consistent iteration: iterators see a stable view of data
//
// How it works:
//
//	┌─────────────────────────────────────────────────────────────┐
//	│                    Version Timeline                          │
//	├─────────────────────────────────────────────────────────────┤
//	│  SeqNum:    1     2     3     4     5     6     7     8     │
//	│             │     │     │     │     │     │     │     │     │
//	│  key "A":   PUT───┼─────┼───PUT─────┼───DEL─────┼─────┼──── │
//	│  key "B":   ──────PUT───┼─────┼─────┼─────┼───PUT─────┼──── │
//	│  key "C":   ────────────PUT───┼─────┼─────┼─────┼─────PUT── │
//	│                         │     │                       │     │
//	│  Snapshot S1 ───────────┘     │                       │     │
//	│  (sees: A@1, B@2, C@3)        │                       │     │
//	│                               │                       │     │
//	│  Snapshot S2 ─────────────────┘                       │     │
//	│  (sees: A@4, B@2, C@3)                                │     │
//	│                                                       │     │
//	│  Current (seqNum=8) ──────────────────────────────────┘     │
//	│  (sees: A=deleted, B@7, C@8)                                │
//	└─────────────────────────────────────────────────────────────┘
//
// Garbage Collection:
// - Versions older than the oldest active snapshot can be garbage collected
// - But only if there's a newer version visible to all snapshots
// - Tombstones (deletes) at the bottom level can be removed entirely
package mvcc

import (
	"container/heap"
	"sync"
	"sync/atomic"
)

// SnapshotManager manages active snapshots and tracks the oldest sequence number.
//
// This is critical for garbage collection during compaction:
// - We cannot delete any version that might be visible to an active snapshot
// - The oldest snapshot sequence number is the lower bound for GC
//
// Thread-safe: all operations can be called concurrently.
type SnapshotManager struct {
	mu sync.Mutex

	// All active snapshots, organized as a min-heap by sequence number
	snapshots snapshotHeap

	// Map from snapshot ID to snapshot for O(1) lookup
	snapshotMap map[uint64]*ManagedSnapshot

	// Next snapshot ID
	nextID uint64

	// Current sequence number (monotonically increasing)
	currentSeq atomic.Uint64

	// Callback when oldest snapshot changes (for compaction)
	onOldestChange func(oldestSeq uint64)
}

// ManagedSnapshot is a snapshot managed by the SnapshotManager.
type ManagedSnapshot struct {
	id       uint64
	seqNum   uint64
	manager  *SnapshotManager
	released atomic.Bool
	index    int // Index in the heap (for efficient removal)
}

// snapshotHeap is a min-heap of snapshots ordered by sequence number.
type snapshotHeap []*ManagedSnapshot

func (h snapshotHeap) Len() int           { return len(h) }
func (h snapshotHeap) Less(i, j int) bool { return h[i].seqNum < h[j].seqNum }
func (h snapshotHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *snapshotHeap) Push(x any) {
	n := len(*h)
	snap := x.(*ManagedSnapshot) //nolint:errcheck
	snap.index = n
	*h = append(*h, snap)
}

func (h *snapshotHeap) Pop() any {
	old := *h
	n := len(old)
	snap := old[n-1]
	old[n-1] = nil  // avoid memory leak
	snap.index = -1 // for safety
	*h = old[0 : n-1]
	return snap
}

// NewSnapshotManager creates a new snapshot manager.
func NewSnapshotManager() *SnapshotManager {
	sm := &SnapshotManager{
		snapshots:   make(snapshotHeap, 0),
		snapshotMap: make(map[uint64]*ManagedSnapshot),
		nextID:      1,
	}
	heap.Init(&sm.snapshots)
	return sm
}

// SetCurrentSeq sets the current sequence number.
// This should be called after each write operation.
func (sm *SnapshotManager) SetCurrentSeq(seq uint64) {
	sm.currentSeq.Store(seq)
}

// GetCurrentSeq returns the current sequence number.
func (sm *SnapshotManager) GetCurrentSeq() uint64 {
	return sm.currentSeq.Load()
}

// SetOldestChangeCallback sets a callback that's called when the oldest
// snapshot changes. This is used to notify the compactor.
func (sm *SnapshotManager) SetOldestChangeCallback(cb func(oldestSeq uint64)) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.onOldestChange = cb
}

// CreateSnapshot creates a new snapshot at the current sequence number.
// The snapshot must be released when no longer needed.
func (sm *SnapshotManager) CreateSnapshot() *ManagedSnapshot {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	seq := sm.currentSeq.Load()
	id := sm.nextID
	sm.nextID++

	snap := &ManagedSnapshot{
		id:      id,
		seqNum:  seq,
		manager: sm,
		index:   -1,
	}

	heap.Push(&sm.snapshots, snap)
	sm.snapshotMap[id] = snap

	return snap
}

// CreateSnapshotAt creates a snapshot at a specific sequence number.
// Useful for testing or replaying from a known point.
func (sm *SnapshotManager) CreateSnapshotAt(seq uint64) *ManagedSnapshot {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	id := sm.nextID
	sm.nextID++

	snap := &ManagedSnapshot{
		id:      id,
		seqNum:  seq,
		manager: sm,
		index:   -1,
	}

	heap.Push(&sm.snapshots, snap)
	sm.snapshotMap[id] = snap

	return snap
}

// ReleaseSnapshot releases a snapshot, allowing its versions to be GC'd.
func (sm *SnapshotManager) ReleaseSnapshot(snap *ManagedSnapshot) {
	if snap == nil || snap.released.Swap(true) {
		return // Already released or nil
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Remove from map
	delete(sm.snapshotMap, snap.id)

	// Remove from heap
	if snap.index >= 0 && snap.index < len(sm.snapshots) {
		heap.Remove(&sm.snapshots, snap.index)
	}

	// Notify if oldest changed
	if sm.onOldestChange != nil {
		sm.onOldestChange(sm.getOldestSeqLocked())
	}
}

// GetOldestSeq returns the sequence number of the oldest active snapshot.
// If no snapshots exist, returns the current sequence number.
// This is the lower bound for garbage collection.
func (sm *SnapshotManager) GetOldestSeq() uint64 {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.getOldestSeqLocked()
}

func (sm *SnapshotManager) getOldestSeqLocked() uint64 {
	if len(sm.snapshots) == 0 {
		return sm.currentSeq.Load()
	}
	return sm.snapshots[0].seqNum
}

// NumSnapshots returns the number of active snapshots.
func (sm *SnapshotManager) NumSnapshots() int {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return len(sm.snapshots)
}

// GetAllSnapshots returns all active snapshot sequence numbers.
// Useful for debugging and testing.
func (sm *SnapshotManager) GetAllSnapshots() []uint64 {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	result := make([]uint64, len(sm.snapshots))
	for i, snap := range sm.snapshots {
		result[i] = snap.seqNum
	}
	return result
}

// ID returns the snapshot's unique identifier.
func (s *ManagedSnapshot) ID() uint64 {
	return s.id
}

// SeqNum returns the snapshot's sequence number.
func (s *ManagedSnapshot) SeqNum() uint64 {
	return s.seqNum
}

// Release releases the snapshot.
// After release, the snapshot should not be used.
func (s *ManagedSnapshot) Release() {
	if s.manager != nil {
		s.manager.ReleaseSnapshot(s)
	}
}

// IsReleased returns true if the snapshot has been released.
func (s *ManagedSnapshot) IsReleased() bool {
	return s.released.Load()
}
