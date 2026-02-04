package wal

import (
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/rapidodb/rapidodb/pkg/errors"
	"github.com/rapidodb/rapidodb/pkg/types"
)

// Manager coordinates WAL operations for the storage engine.
//
// The Manager handles:
// - Creating and rotating WAL files
// - Recovery by replaying WAL files
// - Cleaning up obsolete WAL files after flush
//
// Thread Safety: Manager is safe for concurrent use.
type Manager struct {
	mu sync.RWMutex

	dir     string
	opts    Options
	writer  *Writer
	fileNum uint64
}

// Options configures the WAL manager.
type Options struct {
	// Dir is the directory for WAL files.
	Dir string

	// SyncOnWrite forces fsync after each write.
	SyncOnWrite bool

	// MaxFileSize is the maximum size of a single WAL file.
	// When exceeded, a new WAL file is created.
	// Default: 64MB
	MaxFileSize int64
}

// DefaultOptions returns default WAL options.
func DefaultOptions(dir string) Options {
	return Options{
		Dir:         dir,
		SyncOnWrite: false,
		MaxFileSize: 64 * 1024 * 1024, // 64MB
	}
}

// NewManager creates a new WAL manager.
func NewManager(opts Options) (*Manager, error) {
	// Ensure directory exists
	if err := os.MkdirAll(opts.Dir, 0755); err != nil {
		return nil, errors.NewIOError("mkdir", opts.Dir, err)
	}

	m := &Manager{
		dir:  opts.Dir,
		opts: opts,
	}

	return m, nil
}

// Open opens or creates a WAL file for writing.
// If fileNum is 0, a new file number is generated.
func (m *Manager) Open(fileNum uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Close existing writer if any
	if m.writer != nil {
		if err := m.writer.Close(); err != nil {
			return err
		}
	}

	// Determine file number
	if fileNum == 0 {
		// Find highest existing file number
		existing, err := m.listWALFiles()
		if err != nil {
			return err
		}

		for _, num := range existing {
			if num >= m.fileNum {
				m.fileNum = num + 1
			}
		}
		if m.fileNum == 0 {
			m.fileNum = 1
		}
	} else {
		m.fileNum = fileNum
	}

	// Create writer
	path := m.walPath(m.fileNum)
	writer, err := NewWriter(path, WriterOptions{
		SyncOnWrite: m.opts.SyncOnWrite,
	})
	if err != nil {
		return err
	}

	m.writer = writer
	return nil
}

// Write writes an entry to the current WAL.
func (m *Manager) Write(entry *types.Entry) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.writer == nil {
		return errors.ErrDBClosed
	}

	err := m.writer.Write(entry)
	if err != nil {
		return err
	}

	// Check if rotation is needed
	if m.opts.MaxFileSize > 0 && m.writer.Size() >= m.opts.MaxFileSize {
		return m.rotate()
	}

	return nil
}

// WriteBatch writes multiple entries atomically.
func (m *Manager) WriteBatch(entries []*types.Entry) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.writer == nil {
		return errors.ErrDBClosed
	}

	err := m.writer.WriteBatch(entries)
	if err != nil {
		return err
	}

	// Check if rotation is needed
	if m.opts.MaxFileSize > 0 && m.writer.Size() >= m.opts.MaxFileSize {
		return m.rotate()
	}

	return nil
}

// Sync flushes pending writes to stable storage.
func (m *Manager) Sync() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.writer == nil {
		return nil
	}

	return m.writer.Sync()
}

// rotate creates a new WAL file.
// Caller must hold at least a read lock.
func (m *Manager) rotate() error {
	// Need to upgrade to write lock
	m.mu.RUnlock()
	m.mu.Lock()
	defer func() {
		m.mu.Unlock()
		m.mu.RLock()
	}()

	// Double-check size after acquiring write lock
	if m.writer.Size() < m.opts.MaxFileSize {
		return nil
	}

	// Close current writer
	if err := m.writer.Close(); err != nil {
		return err
	}

	// Increment file number
	m.fileNum++

	// Create new writer
	path := m.walPath(m.fileNum)
	writer, err := NewWriter(path, WriterOptions{
		SyncOnWrite: m.opts.SyncOnWrite,
	})
	if err != nil {
		return err
	}

	m.writer = writer
	return nil
}

// Recover replays all WAL files and calls fn for each entry.
// Files are processed in order by file number.
func (m *Manager) Recover(fn func(*types.Entry) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	files, err := m.listWALFiles()
	if err != nil {
		return err
	}

	if len(files) == 0 {
		return nil
	}

	// Sort files by number
	sort.Slice(files, func(i, j int) bool {
		return files[i] < files[j]
	})

	// Replay each file
	for _, fileNum := range files {
		path := m.walPath(fileNum)

		reader, err := NewReader(path, ReaderOptions{
			ReportCorruption: true,
			SkipCorruption:   true, // Continue past corruption during recovery
		})
		if err != nil {
			// Skip missing files
			if os.IsNotExist(err) {
				continue
			}
			return err
		}

		err = reader.Read(fn)
		closeErr := reader.Close()

		if err != nil {
			return errors.NewRecoveryError("wal", err)
		}
		if closeErr != nil {
			return closeErr
		}

		// Track highest file number
		if fileNum >= m.fileNum {
			m.fileNum = fileNum
		}
	}

	return nil
}

// CleanBefore removes WAL files with file number < maxFileNum.
// This is called after MemTable flush to remove obsolete WALs.
func (m *Manager) CleanBefore(maxFileNum uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	files, err := m.listWALFiles()
	if err != nil {
		return err
	}

	for _, fileNum := range files {
		if fileNum < maxFileNum {
			path := m.walPath(fileNum)
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return errors.NewIOError("remove", path, err)
			}
		}
	}

	return nil
}

// Close closes the WAL manager.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.writer != nil {
		err := m.writer.Close()
		m.writer = nil
		return err
	}

	return nil
}

// CurrentFileNum returns the current WAL file number.
func (m *Manager) CurrentFileNum() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.fileNum
}

// Size returns the current WAL file size.
func (m *Manager) Size() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.writer == nil {
		return 0
	}
	return m.writer.Size()
}

// listWALFiles returns all WAL file numbers in the directory.
func (m *Manager) listWALFiles() ([]uint64, error) {
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, errors.NewIOError("readdir", m.dir, err)
	}

	files := make([]uint64, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if !strings.HasSuffix(name, ".wal") {
			continue
		}

		numStr := strings.TrimSuffix(name, ".wal")
		num, err := strconv.ParseUint(numStr, 10, 64)
		if err != nil {
			continue
		}

		files = append(files, num)
	}

	return files, nil
}

// walPath returns the path for a WAL file with the given number.
func (m *Manager) walPath(fileNum uint64) string {
	return filepath.Join(m.dir, walFileName(fileNum))
}

// walFileName returns the filename for a WAL with the given number.
func walFileName(fileNum uint64) string {
	return strconv.FormatUint(fileNum, 10) + ".wal"
}
