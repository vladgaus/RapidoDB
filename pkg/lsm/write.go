package lsm

import (
	"github.com/rapidodb/rapidodb/pkg/errors"
	"github.com/rapidodb/rapidodb/pkg/memtable"
	"github.com/rapidodb/rapidodb/pkg/types"
)

// Put stores a key-value pair.
func (e *Engine) Put(key, value []byte) error {
	// Validate key and value
	if err := errors.ValidateKey(key); err != nil {
		return err
	}
	if err := errors.ValidateValue(value); err != nil {
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed.Load() {
		return ErrClosed
	}

	// Check for write stall conditions
	if err := e.checkWriteStall(); err != nil {
		return err
	}

	// Get next sequence number
	e.seqNum++
	seqNum := e.seqNum

	// Create entry
	entry := &types.Entry{
		Key:    key,
		Value:  value,
		Type:   types.EntryTypePut,
		SeqNum: seqNum,
	}

	// Write to WAL first (durability)
	if err := e.walManager.Write(entry); err != nil {
		e.seqNum-- // Rollback sequence number
		return err
	}

	// Write to MemTable
	if err := e.memTable.Put(key, value, seqNum); err != nil {
		e.seqNum-- // Rollback sequence number
		return err
	}

	// Check if MemTable needs rotation
	if e.memTable.ShouldFlush() {
		if err := e.rotateMemTable(); err != nil {
			return err
		}
	}

	return nil
}

// Delete removes a key.
func (e *Engine) Delete(key []byte) error {
	// Validate key
	if err := errors.ValidateKey(key); err != nil {
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed.Load() {
		return ErrClosed
	}

	// Check for write stall conditions
	if err := e.checkWriteStall(); err != nil {
		return err
	}

	// Get next sequence number
	e.seqNum++
	seqNum := e.seqNum

	// Create tombstone entry
	entry := &types.Entry{
		Key:    key,
		Value:  nil,
		Type:   types.EntryTypeDelete,
		SeqNum: seqNum,
	}

	// Write to WAL first (durability)
	if err := e.walManager.Write(entry); err != nil {
		e.seqNum-- // Rollback sequence number
		return err
	}

	// Write tombstone to MemTable
	if err := e.memTable.Delete(key, seqNum); err != nil {
		e.seqNum-- // Rollback sequence number
		return err
	}

	// Check if MemTable needs rotation
	if e.memTable.ShouldFlush() {
		if err := e.rotateMemTable(); err != nil {
			return err
		}
	}

	return nil
}

// rotateMemTable makes the current MemTable immutable and creates a new one.
// Caller must hold e.mu.
func (e *Engine) rotateMemTable() error {
	// Mark current MemTable as immutable
	e.memTable.MarkImmutable()

	// Add to immutable list (newest first)
	e.immutableMemTables = append([]*memtable.MemTable{e.memTable}, e.immutableMemTables...)

	// Open new WAL
	if err := e.walManager.Open(0); err != nil {
		return err
	}

	// Create new MemTable
	e.memTable = memtable.NewMemTable(e.walManager.CurrentFileNum(), e.opts.MemTableSize)

	// Schedule flush
	e.maybeScheduleFlush()

	return nil
}

// checkWriteStall checks if writes should be stalled.
// Returns ErrWriteStall if too many immutable MemTables or L0 files exist.
// Caller must hold e.mu.
func (e *Engine) checkWriteStall() error {
	// Check if compactor says we should stall
	if e.compactor != nil && e.compactor.ShouldStallWrites() {
		// Trigger compaction
		e.compactor.TriggerCompaction()
		return ErrWriteStall
	}

	// Check immutable MemTable count
	if len(e.immutableMemTables) >= e.opts.MaxMemTables {
		// Try to trigger flush
		e.maybeScheduleFlush()
		// Return stall only if we're really backed up
		if len(e.immutableMemTables) > e.opts.MaxMemTables {
			return ErrWriteStall
		}
	}

	// Trigger compaction if needed
	if e.compactor != nil && e.compactor.ShouldTriggerCompaction() {
		e.compactor.TriggerCompaction()
	}

	return nil
}

// WriteBatch writes multiple key-value pairs atomically.
func (e *Engine) WriteBatch(entries []*types.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed.Load() {
		return ErrClosed
	}

	// Check for write stall conditions
	if err := e.checkWriteStall(); err != nil {
		return err
	}

	// Assign sequence numbers to all entries
	startSeqNum := e.seqNum + 1
	for i, entry := range entries {
		entry.SeqNum = startSeqNum + uint64(i)
	}

	// Write batch to WAL atomically
	if err := e.walManager.WriteBatch(entries); err != nil {
		return err
	}

	// Update sequence number
	e.seqNum = startSeqNum + uint64(len(entries)) - 1

	// Apply to MemTable
	for _, entry := range entries {
		if entry.Type == types.EntryTypeDelete {
			if err := e.memTable.Delete(entry.Key, entry.SeqNum); err != nil {
				return err
			}
		} else {
			if err := e.memTable.Put(entry.Key, entry.Value, entry.SeqNum); err != nil {
				return err
			}
		}
	}

	// Check if MemTable needs rotation
	if e.memTable.ShouldFlush() {
		if err := e.rotateMemTable(); err != nil {
			return err
		}
	}

	return nil
}
