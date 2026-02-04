package wal

import (
	"io"
	"os"

	"github.com/rapidodb/rapidodb/internal/encoding"
	"github.com/rapidodb/rapidodb/pkg/errors"
	"github.com/rapidodb/rapidodb/pkg/types"
)

// Reader reads entries from a WAL file.
//
// The Reader handles:
// - Reassembling fragmented records
// - Verifying CRC checksums
// - Detecting and reporting corruption
//
// Thread Safety: Reader is NOT safe for concurrent use.
type Reader struct {
	file     *os.File
	filePath string

	// Current block being read
	block       []byte
	blockOffset int // Position within current block
	blockEnd    int // Valid bytes in current block

	// Buffer for assembling fragmented records
	recordBuf []byte

	// Statistics
	bytesRead    int64
	recordsRead  int64
	corruptBytes int64

	// Options
	reportCorruption bool
	skipCorruption   bool
}

// ReaderOptions configures WAL reader behavior.
type ReaderOptions struct {
	// ReportCorruption logs corruption details when detected.
	ReportCorruption bool

	// SkipCorruption attempts to continue past corrupt records.
	// If false, reading stops at first corruption.
	SkipCorruption bool
}

// DefaultReaderOptions returns default reader options.
func DefaultReaderOptions() ReaderOptions {
	return ReaderOptions{
		ReportCorruption: true,
		SkipCorruption:   false,
	}
}

// NewReader creates a new WAL reader.
func NewReader(path string, opts ReaderOptions) (*Reader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.NewIOError("open", path, err)
	}

	return &Reader{
		file:             file,
		filePath:         path,
		block:            make([]byte, BlockSize),
		blockOffset:      0,
		blockEnd:         0,
		recordBuf:        make([]byte, 0, BlockSize),
		reportCorruption: opts.ReportCorruption,
		skipCorruption:   opts.SkipCorruption,
	}, nil
}

// Read reads all entries from the WAL, calling fn for each entry.
// If fn returns an error, reading stops and that error is returned.
func (r *Reader) Read(fn func(*types.Entry) error) error {
	for {
		record, err := r.readRecord()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		// Decode the entry
		entry, _, err := encoding.DecodeEntry(record)
		if err != nil {
			if r.skipCorruption {
				r.corruptBytes += int64(len(record))
				continue
			}
			return errors.NewCorruptionError(r.filePath, r.bytesRead, "failed to decode entry")
		}

		r.recordsRead++

		if err := fn(entry); err != nil {
			return err
		}
	}
}

// ReadRaw reads all raw records from the WAL, calling fn for each.
// This is useful for tools that need to inspect the WAL without decoding entries.
func (r *Reader) ReadRaw(fn func([]byte) error) error {
	for {
		record, err := r.readRecord()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		r.recordsRead++

		if err := fn(record); err != nil {
			return err
		}
	}
}

// readRecord reads and reassembles a complete record.
func (r *Reader) readRecord() ([]byte, error) {
	// Clear the record buffer
	r.recordBuf = r.recordBuf[:0]
	inFragment := false

	for {
		// Read next fragment
		recordType, payload, err := r.readFragment()
		if err != nil {
			return nil, err
		}

		switch recordType {
		case RecordTypeFull:
			if inFragment {
				// Corrupt: got FULL while assembling fragments
				if r.skipCorruption {
					r.recordBuf = r.recordBuf[:0]
					inFragment = false
					// Return this record as-is
					return payload, nil
				}
				return nil, errors.NewCorruptionError(r.filePath, r.bytesRead,
					"unexpected FULL record while assembling fragments")
			}
			return payload, nil

		case RecordTypeFirst:
			if inFragment {
				// Corrupt: got FIRST while assembling
				if r.skipCorruption {
					r.recordBuf = r.recordBuf[:0]
				} else {
					return nil, errors.NewCorruptionError(r.filePath, r.bytesRead,
						"unexpected FIRST record while assembling fragments")
				}
			}
			r.recordBuf = append(r.recordBuf, payload...)
			inFragment = true

		case RecordTypeMiddle:
			if !inFragment {
				// Corrupt: got MIDDLE without FIRST
				if r.skipCorruption {
					continue
				}
				return nil, errors.NewCorruptionError(r.filePath, r.bytesRead,
					"unexpected MIDDLE record without FIRST")
			}
			r.recordBuf = append(r.recordBuf, payload...)

		case RecordTypeLast:
			if !inFragment {
				// Corrupt: got LAST without FIRST
				if r.skipCorruption {
					continue
				}
				return nil, errors.NewCorruptionError(r.filePath, r.bytesRead,
					"unexpected LAST record without FIRST")
			}
			r.recordBuf = append(r.recordBuf, payload...)
			result := make([]byte, len(r.recordBuf))
			copy(result, r.recordBuf)
			return result, nil

		case RecordTypeZero:
			// Skip zero/padding records
			continue

		default:
			if r.skipCorruption {
				continue
			}
			return nil, errors.NewCorruptionError(r.filePath, r.bytesRead,
				"unknown record type")
		}
	}
}

// readFragment reads a single fragment from the WAL.
func (r *Reader) readFragment() (RecordType, []byte, error) {
	for {
		// Ensure we have enough data for a header
		if r.blockOffset+HeaderSize > r.blockEnd {
			// Need to read a new block
			if err := r.readBlock(); err != nil {
				return 0, nil, err
			}
		}

		// Check available space in block
		availableInBlock := r.blockEnd - r.blockOffset
		if availableInBlock < HeaderSize {
			// Skip to next block (padding)
			r.blockOffset = r.blockEnd
			continue
		}

		// Decode header
		header := r.block[r.blockOffset : r.blockOffset+HeaderSize]
		crc, length, recordType := decodeHeader(header)

		// Handle zero records (padding)
		if recordType == RecordTypeZero && length == 0 {
			r.blockOffset = r.blockEnd
			continue
		}

		// Validate length
		if int(length) > availableInBlock-HeaderSize {
			if r.skipCorruption {
				r.corruptBytes += int64(availableInBlock)
				r.blockOffset = r.blockEnd
				continue
			}
			return 0, nil, errors.NewCorruptionError(r.filePath, r.bytesRead+int64(r.blockOffset),
				"record length exceeds block boundary")
		}

		// Read payload
		payload := r.block[r.blockOffset+HeaderSize : r.blockOffset+HeaderSize+int(length)]

		// Verify CRC
		expectedCRC := computeCRC(recordType, payload)
		if crc != expectedCRC {
			if r.skipCorruption {
				r.corruptBytes += int64(HeaderSize + int(length))
				r.blockOffset += HeaderSize + int(length)
				continue
			}
			return 0, nil, errors.NewCorruptionError(r.filePath, r.bytesRead+int64(r.blockOffset),
				"CRC mismatch")
		}

		// Advance position
		r.blockOffset += HeaderSize + int(length)

		// Make a copy of the payload
		result := make([]byte, len(payload))
		copy(result, payload)

		return recordType, result, nil
	}
}

// readBlock reads the next block from the file.
func (r *Reader) readBlock() error {
	n, err := io.ReadFull(r.file, r.block)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		if n == 0 {
			return io.EOF
		}
		// Partial block at end of file
		r.blockEnd = n
		r.blockOffset = 0
		r.bytesRead += int64(n)
		return nil
	}
	if err != nil {
		return errors.NewIOError("read", r.filePath, err)
	}

	r.blockEnd = n
	r.blockOffset = 0
	r.bytesRead += int64(n)

	return nil
}

// Close closes the WAL reader.
func (r *Reader) Close() error {
	if err := r.file.Close(); err != nil {
		return errors.NewIOError("close", r.filePath, err)
	}
	return nil
}

// Stats returns reader statistics.
func (r *Reader) Stats() ReaderStats {
	return ReaderStats{
		BytesRead:    r.bytesRead,
		RecordsRead:  r.recordsRead,
		CorruptBytes: r.corruptBytes,
	}
}

// ReaderStats contains WAL reader statistics.
type ReaderStats struct {
	BytesRead    int64
	RecordsRead  int64
	CorruptBytes int64
}
