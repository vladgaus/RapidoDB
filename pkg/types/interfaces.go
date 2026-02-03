// Package types defines core interfaces for RapidoDB components.
package types

import (
	"context"
	"io"
)

// Storage defines the main interface for the key-value storage engine.
// This is the primary API that clients interact with.
type Storage interface {
	// Put stores a key-value pair.
	// If the key already exists, its value is overwritten.
	Put(ctx context.Context, key, value []byte) error

	// Get retrieves the value for a key.
	// Returns ErrKeyNotFound if the key doesn't exist.
	Get(ctx context.Context, key []byte) ([]byte, error)

	// Delete removes a key from storage.
	// This creates a tombstone; actual deletion happens during compaction.
	Delete(ctx context.Context, key []byte) error

	// Scan returns an iterator over keys in the range [start, end).
	// If end is nil, iterates to the end of the keyspace.
	Scan(ctx context.Context, start, end []byte) (Iterator, error)

	// Close gracefully shuts down the storage engine.
	// It flushes any pending data and releases resources.
	Close() error
}

// SnapshotStorage extends Storage with MVCC snapshot support.
type SnapshotStorage interface {
	Storage

	// Snapshot creates a point-in-time snapshot for consistent reads.
	// The snapshot must be released with Release() when done.
	Snapshot() (Snapshot, error)
}

// Snapshot represents a point-in-time view of the database.
type Snapshot interface {
	// Get retrieves the value as of this snapshot's sequence number.
	Get(ctx context.Context, key []byte) ([]byte, error)

	// Scan returns an iterator over the snapshot's view.
	Scan(ctx context.Context, start, end []byte) (Iterator, error)

	// SeqNum returns the sequence number of this snapshot.
	SeqNum() uint64

	// Release frees resources associated with this snapshot.
	Release()
}

// Iterator provides sequential access to key-value pairs.
// Iterators are not thread-safe.
type Iterator interface {
	// Valid returns true if the iterator is positioned at a valid entry.
	Valid() bool

	// Key returns the current key. Only valid when Valid() returns true.
	// The returned slice is only valid until the next iterator operation.
	Key() []byte

	// Value returns the current value. Only valid when Valid() returns true.
	// The returned slice is only valid until the next iterator operation.
	Value() []byte

	// Next advances the iterator to the next entry.
	Next()

	// Seek positions the iterator at the first key >= target.
	Seek(target []byte)

	// SeekToFirst positions the iterator at the first key.
	SeekToFirst()

	// SeekToLast positions the iterator at the last key.
	SeekToLast()

	// Prev moves the iterator to the previous entry.
	Prev()

	// Error returns any error encountered during iteration.
	Error() error

	// Close releases resources held by the iterator.
	Close() error
}

// MemTable defines the interface for in-memory tables.
// MemTables are write-optimized and support concurrent access.
type MemTable interface {
	// Put inserts or updates a key-value pair.
	Put(key, value []byte, seqNum uint64) error

	// Delete inserts a tombstone for the key.
	Delete(key []byte, seqNum uint64) error

	// Get retrieves an entry by key.
	// Returns the entry with the highest sequence number <= maxSeqNum.
	Get(key []byte, maxSeqNum uint64) (*Entry, error)

	// NewIterator returns an iterator over all entries.
	NewIterator() Iterator

	// Size returns the approximate size in bytes.
	Size() int64

	// EntryCount returns the number of entries.
	EntryCount() int64

	// IsEmpty returns true if the MemTable has no entries.
	IsEmpty() bool
}

// SSTableWriter defines the interface for writing SSTables.
type SSTableWriter interface {
	// Add adds an entry to the SSTable.
	// Entries must be added in sorted order.
	Add(entry *Entry) error

	// Finish completes the SSTable and writes metadata.
	// Returns the file metadata for the created table.
	Finish() (*FileMetadata, error)

	// Abort cancels the write and cleans up resources.
	Abort() error

	// EstimatedSize returns the estimated file size so far.
	EstimatedSize() int64
}

// SSTableReader defines the interface for reading SSTables.
type SSTableReader interface {
	// Get retrieves an entry by key.
	// Returns nil if the key is not found.
	Get(key []byte) (*Entry, error)

	// NewIterator returns an iterator over all entries.
	NewIterator() Iterator

	// Metadata returns the file metadata.
	Metadata() *FileMetadata

	// ContainsKey uses the Bloom filter to check if a key might exist.
	// False means definitely not present, true means possibly present.
	ContainsKey(key []byte) bool

	// Close releases resources.
	Close() error
}

// WALWriter defines the interface for Write-Ahead Log writing.
type WALWriter interface {
	// Write appends an entry to the WAL.
	Write(entry *Entry) error

	// Sync flushes pending writes to stable storage.
	Sync() error

	// Close closes the WAL file.
	Close() error

	// Size returns the current WAL file size.
	Size() int64
}

// WALReader defines the interface for reading WAL during recovery.
type WALReader interface {
	// Read reads all entries from the WAL.
	// Calls the provided function for each entry.
	Read(fn func(*Entry) error) error

	// Close closes the WAL reader.
	Close() error
}

// Compactor defines the interface for compaction strategies.
type Compactor interface {
	// Plan returns the next compaction task, or nil if none needed.
	Plan(levels [][]FileMetadata) *CompactionTask

	// Apply executes the compaction task.
	Apply(ctx context.Context, task *CompactionTask) error
}

// CompactionTask represents a single compaction job.
type CompactionTask struct {
	// Level is the level being compacted from.
	Level int

	// TargetLevel is the level being compacted to.
	TargetLevel int

	// Inputs are the files to compact.
	Inputs []FileMetadata

	// IsFullCompaction indicates if this is a full compaction.
	IsFullCompaction bool
}

// WriteCloseSyncer combines io.WriteCloser with a Sync method.
type WriteCloseSyncer interface {
	io.WriteCloser
	Sync() error
}
