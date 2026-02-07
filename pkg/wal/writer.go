package wal

import (
	"io"
	"os"
	"sync"

	"github.com/rapidodb/rapidodb/internal/encoding"
	"github.com/rapidodb/rapidodb/pkg/errors"
	"github.com/rapidodb/rapidodb/pkg/types"
)

// Writer writes entries to a WAL file.
//
// The Writer handles:
// - Record fragmentation across blocks
// - CRC checksums for corruption detection
// - Optional sync-on-write for durability
//
// Thread Safety: Writer is safe for concurrent use.
type Writer struct {
	mu sync.Mutex

	file     *os.File
	filePath string

	// Current position within the current block
	blockOffset int

	// Buffer for building records
	buf []byte

	// Options
	syncOnWrite bool

	// Statistics
	bytesWritten int64
}

// WriterOptions configures WAL writer behavior.
type WriterOptions struct {
	// SyncOnWrite forces fsync after each write.
	// Provides strongest durability but impacts performance.
	SyncOnWrite bool
}

// DefaultWriterOptions returns default writer options.
func DefaultWriterOptions() WriterOptions {
	return WriterOptions{
		SyncOnWrite: false,
	}
}

// NewWriter creates a new WAL writer.
// If the file exists, it will be truncated.
func NewWriter(path string, opts WriterOptions) (*Writer, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, errors.NewIOError("create", path, err)
	}

	return &Writer{
		file:        file,
		filePath:    path,
		blockOffset: 0,
		buf:         make([]byte, BlockSize),
		syncOnWrite: opts.SyncOnWrite,
	}, nil
}

// OpenWriter opens an existing WAL file for appending.
// The file position is set to the end.
func OpenWriter(path string, opts WriterOptions) (*Writer, error) {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, errors.NewIOError("open", path, err)
	}

	// Get current file size to determine block offset
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, errors.NewIOError("stat", path, err)
	}

	blockOffset := int(info.Size() % BlockSize)

	return &Writer{
		file:         file,
		filePath:     path,
		blockOffset:  blockOffset,
		buf:          make([]byte, BlockSize),
		syncOnWrite:  opts.SyncOnWrite,
		bytesWritten: info.Size(),
	}, nil
}

// Write writes an entry to the WAL.
func (w *Writer) Write(entry *types.Entry) error {
	// Encode the entry
	data := encoding.EncodeEntry(entry)
	return w.WriteRaw(data)
}

// WriteRaw writes raw bytes to the WAL.
// This is useful for batch operations or custom record types.
func (w *Writer) WriteRaw(data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.writeRecord(data)
}

// writeRecord writes a complete record, fragmenting if necessary.
func (w *Writer) writeRecord(data []byte) error {
	left := len(data)
	offset := 0
	isFirstFragment := true

	for left > 0 {
		// Calculate available space in current block
		availableInBlock := BlockSize - w.blockOffset

		// If we can't fit even a header, move to next block
		if availableInBlock < HeaderSize {
			// Fill remainder with zeros
			if availableInBlock > 0 {
				zeros := make([]byte, availableInBlock)
				if _, err := w.file.Write(zeros); err != nil {
					return errors.NewIOError("write", w.filePath, err)
				}
				w.bytesWritten += int64(availableInBlock)
			}
			w.blockOffset = 0
			availableInBlock = BlockSize
		}

		// Calculate how much payload we can write
		payloadSize := availableInBlock - HeaderSize
		if payloadSize > left {
			payloadSize = left
		}

		// Determine record type
		var recordType RecordType
		isLastFragment := (left == payloadSize)

		if isFirstFragment && isLastFragment {
			recordType = RecordTypeFull
		} else if isFirstFragment {
			recordType = RecordTypeFirst
		} else if isLastFragment {
			recordType = RecordTypeLast
		} else {
			recordType = RecordTypeMiddle
		}

		// Write the fragment
		err := w.writeFragment(recordType, data[offset:offset+payloadSize])
		if err != nil {
			return err
		}

		offset += payloadSize
		left -= payloadSize
		isFirstFragment = false
	}

	// Sync if configured
	if w.syncOnWrite {
		return w.file.Sync()
	}

	return nil
}

// writeFragment writes a single fragment to the WAL.
func (w *Writer) writeFragment(recordType RecordType, payload []byte) error {
	// Compute CRC
	crc := computeCRC(recordType, payload)

	// Build header
	encodeHeader(w.buf, crc, uint32(len(payload)), recordType)

	// Copy payload
	copy(w.buf[HeaderSize:], payload)

	// Write to file
	totalSize := HeaderSize + len(payload)
	_, err := w.file.Write(w.buf[:totalSize])
	if err != nil {
		return errors.NewIOError("write", w.filePath, err)
	}

	w.blockOffset += totalSize
	w.bytesWritten += int64(totalSize)

	return nil
}

// Sync flushes pending writes to stable storage.
func (w *Writer) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Sync(); err != nil {
		return errors.NewIOError("sync", w.filePath, err)
	}
	return nil
}

// Close closes the WAL file.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Sync before close
	if err := w.file.Sync(); err != nil {
		_ = w.file.Close()
		return errors.NewIOError("sync", w.filePath, err)
	}

	if err := w.file.Close(); err != nil {
		return errors.NewIOError("close", w.filePath, err)
	}

	return nil
}

// Size returns the current WAL file size.
func (w *Writer) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.bytesWritten
}

// Path returns the WAL file path.
func (w *Writer) Path() string {
	return w.filePath
}

// WriteBatch writes multiple entries atomically.
// All entries are written before any sync occurs.
func (w *Writer) WriteBatch(entries []*types.Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, entry := range entries {
		data := encoding.EncodeEntry(entry)
		if err := w.writeRecord(data); err != nil {
			return err
		}
	}

	if w.syncOnWrite {
		return w.file.Sync()
	}

	return nil
}

// Truncate truncates the WAL file.
// Used during compaction when the WAL is no longer needed.
func (w *Writer) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Truncate(0); err != nil {
		return errors.NewIOError("truncate", w.filePath, err)
	}

	_, err := w.file.Seek(0, io.SeekStart)
	if err != nil {
		return errors.NewIOError("seek", w.filePath, err)
	}

	w.blockOffset = 0
	w.bytesWritten = 0

	return nil
}
