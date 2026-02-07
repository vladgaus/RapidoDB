package manifest

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// Record format for manifest entries (similar to WAL):
//
//	┌───────────┬───────────┬───────────┬──────────────┐
//	│ CRC (4B)  │ Len (4B)  │ Type (1B) │ Data (Len B) │
//	└───────────┴───────────┴───────────┴──────────────┘
//
// Types:
//   - Full (1): Complete record
//   - First (2): First fragment
//   - Middle (3): Middle fragment
//   - Last (4): Last fragment

const (
	maxRecordSize = 32 * 1024 // 32KB max record size

	recordTypeFull   byte = 1
	recordTypeFirst  byte = 2
	recordTypeMiddle byte = 3
	recordTypeLast   byte = 4
)

var crc32Table = crc32.MakeTable(crc32.Castagnoli)

// Writer writes VersionEdits to a manifest file.
type Writer struct {
	mu   sync.Mutex
	file *os.File
	buf  *bufio.Writer
}

// NewWriter creates a new manifest writer.
func NewWriter(path string) (*Writer, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open manifest: %w", err)
	}

	return &Writer{
		file: file,
		buf:  bufio.NewWriter(file),
	}, nil
}

// WriteEdit writes a VersionEdit to the manifest.
func (w *Writer) WriteEdit(edit *VersionEdit) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	data := edit.Encode()
	return w.writeRecord(data)
}

// writeRecord writes a record, potentially splitting into fragments.
func (w *Writer) writeRecord(data []byte) error {
	if len(data) <= maxRecordSize {
		// Single full record
		return w.writeFragment(recordTypeFull, data)
	}

	// Split into fragments
	offset := 0
	first := true
	for offset < len(data) {
		end := offset + maxRecordSize
		if end > len(data) {
			end = len(data)
		}

		var recType byte
		if first {
			recType = recordTypeFirst
			first = false
		} else if end == len(data) {
			recType = recordTypeLast
		} else {
			recType = recordTypeMiddle
		}

		if err := w.writeFragment(recType, data[offset:end]); err != nil {
			return err
		}
		offset = end
	}

	return nil
}

func (w *Writer) writeFragment(recType byte, data []byte) error {
	// Calculate CRC over type + data
	crc := crc32.Checksum(append([]byte{recType}, data...), crc32Table)

	// Write header
	if err := binary.Write(w.buf, binary.LittleEndian, crc); err != nil {
		return err
	}
	if err := binary.Write(w.buf, binary.LittleEndian, uint32(len(data))); err != nil {
		return err
	}
	if err := w.buf.WriteByte(recType); err != nil {
		return err
	}

	// Write data
	if _, err := w.buf.Write(data); err != nil {
		return err
	}

	return nil
}

// Sync flushes and syncs the manifest to disk.
func (w *Writer) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.buf.Flush(); err != nil {
		return err
	}
	return w.file.Sync()
}

// Close closes the writer.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.buf.Flush(); err != nil {
		return err
	}
	return w.file.Close()
}

// Reader reads VersionEdits from a manifest file.
type Reader struct {
	file *os.File
	buf  *bufio.Reader
}

// NewReader creates a new manifest reader.
func NewReader(path string) (*Reader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open manifest: %w", err)
	}

	return &Reader{
		file: file,
		buf:  bufio.NewReader(file),
	}, nil
}

// ReadEdit reads the next VersionEdit from the manifest.
// Returns io.EOF when no more edits.
func (r *Reader) ReadEdit() (*VersionEdit, error) {
	data, err := r.readRecord()
	if err != nil {
		return nil, err
	}

	edit := NewVersionEdit()
	if err := edit.Decode(data); err != nil {
		return nil, fmt.Errorf("decode edit: %w", err)
	}

	return edit, nil
}

// readRecord reads a complete record, assembling fragments if needed.
func (r *Reader) readRecord() ([]byte, error) {
	var result []byte
	inFragmentedRecord := false

	for {
		// Read header
		var crc uint32
		var length uint32
		var recType byte

		if err := binary.Read(r.buf, binary.LittleEndian, &crc); err != nil {
			if err == io.EOF && !inFragmentedRecord {
				return nil, io.EOF
			}
			return nil, fmt.Errorf("read crc: %w", err)
		}
		if err := binary.Read(r.buf, binary.LittleEndian, &length); err != nil {
			return nil, fmt.Errorf("read length: %w", err)
		}
		if err := binary.Read(r.buf, binary.LittleEndian, &recType); err != nil {
			return nil, fmt.Errorf("read type: %w", err)
		}

		// Read data
		data := make([]byte, length)
		if _, err := io.ReadFull(r.buf, data); err != nil {
			return nil, fmt.Errorf("read data: %w", err)
		}

		// Verify CRC
		expectedCRC := crc32.Checksum(append([]byte{recType}, data...), crc32Table)
		if crc != expectedCRC {
			return nil, fmt.Errorf("crc mismatch: got %x, want %x", crc, expectedCRC)
		}

		switch recType {
		case recordTypeFull:
			if inFragmentedRecord {
				return nil, fmt.Errorf("unexpected full record in fragment")
			}
			return data, nil

		case recordTypeFirst:
			if inFragmentedRecord {
				return nil, fmt.Errorf("unexpected first record in fragment")
			}
			result = append(result, data...)
			inFragmentedRecord = true

		case recordTypeMiddle:
			if !inFragmentedRecord {
				return nil, fmt.Errorf("unexpected middle record outside fragment")
			}
			result = append(result, data...)

		case recordTypeLast:
			if !inFragmentedRecord {
				return nil, fmt.Errorf("unexpected last record outside fragment")
			}
			result = append(result, data...)
			return result, nil

		default:
			return nil, fmt.Errorf("unknown record type: %d", recType)
		}
	}
}

// Close closes the reader.
func (r *Reader) Close() error {
	return r.file.Close()
}

// ManifestFileName returns the manifest filename for a number.
func ManifestFileName(num uint64) string {
	return fmt.Sprintf("MANIFEST-%06d", num)
}

// CurrentFileName returns the CURRENT filename.
func CurrentFileName() string {
	return "CURRENT"
}

// ParseManifestNumber extracts the manifest number from a filename.
func ParseManifestNumber(name string) (uint64, bool) {
	if !strings.HasPrefix(name, "MANIFEST-") {
		return 0, false
	}
	numStr := strings.TrimPrefix(name, "MANIFEST-")
	num, err := strconv.ParseUint(numStr, 10, 64)
	if err != nil {
		return 0, false
	}
	return num, true
}

// SetCurrent atomically updates the CURRENT file to point to a manifest.
func SetCurrent(dir string, manifestNum uint64) error {
	manifestName := ManifestFileName(manifestNum)
	currentPath := filepath.Join(dir, CurrentFileName())
	tempPath := currentPath + ".tmp"

	// Write to temp file
	if err := os.WriteFile(tempPath, []byte(manifestName+"\n"), 0644); err != nil {
		return fmt.Errorf("write temp current: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tempPath, currentPath); err != nil {
		_ = os.Remove(tempPath) // best-effort cleanup
		return fmt.Errorf("rename current: %w", err)
	}

	return nil
}

// ReadCurrent reads the current manifest name from the CURRENT file.
func ReadCurrent(dir string) (string, error) {
	currentPath := filepath.Join(dir, CurrentFileName())
	data, err := os.ReadFile(currentPath)
	if err != nil {
		return "", err
	}

	name := strings.TrimSpace(string(data))
	if name == "" {
		return "", fmt.Errorf("empty CURRENT file")
	}

	return name, nil
}
