package logging

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// RotatingFile is a file writer with automatic rotation.
type RotatingFile struct {
	mu sync.Mutex

	// Configuration
	path       string
	maxSize    int64 // Max size in bytes before rotation
	maxBackups int   // Max number of old log files to keep
	maxAge     int   // Max age in days for old log files
	compress   bool  // Compress rotated files

	// State
	file        *os.File
	size        int64
	lastRotate  time.Time
	rotateDaily bool
}

// RotatingFileOptions configures the rotating file writer.
type RotatingFileOptions struct {
	// Path is the log file path.
	Path string

	// MaxSize is the maximum size in megabytes before rotation.
	// Default: 100MB
	MaxSize int

	// MaxBackups is the maximum number of old log files to keep.
	// Default: 5
	MaxBackups int

	// MaxAge is the maximum age in days for old log files.
	// 0 means no age-based deletion.
	// Default: 0
	MaxAge int

	// Compress determines if rotated files are compressed.
	// Default: false
	Compress bool

	// RotateDaily enables daily rotation regardless of size.
	// Default: false
	RotateDaily bool
}

// DefaultRotatingFileOptions returns sensible defaults.
func DefaultRotatingFileOptions() RotatingFileOptions {
	return RotatingFileOptions{
		MaxSize:    100,
		MaxBackups: 5,
		MaxAge:     0,
		Compress:   false,
	}
}

// NewRotatingFile creates a new rotating file writer.
func NewRotatingFile(opts RotatingFileOptions) (*RotatingFile, error) {
	if opts.Path == "" {
		return nil, fmt.Errorf("log file path is required")
	}
	if opts.MaxSize <= 0 {
		opts.MaxSize = 100
	}
	if opts.MaxBackups <= 0 {
		opts.MaxBackups = 5
	}

	rf := &RotatingFile{
		path:        opts.Path,
		maxSize:     int64(opts.MaxSize) * 1024 * 1024, // MB to bytes
		maxBackups:  opts.MaxBackups,
		maxAge:      opts.MaxAge,
		compress:    opts.Compress,
		rotateDaily: opts.RotateDaily,
		lastRotate:  time.Now(),
	}

	if err := rf.openFile(); err != nil {
		return nil, err
	}

	return rf, nil
}

// Write writes data to the log file, rotating if necessary.
func (rf *RotatingFile) Write(p []byte) (n int, err error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Check if rotation is needed
	if rf.shouldRotate(len(p)) {
		if err := rf.rotate(); err != nil {
			return 0, err
		}
	}

	n, err = rf.file.Write(p)
	rf.size += int64(n)
	return n, err
}

// Close closes the log file.
func (rf *RotatingFile) Close() error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.file == nil {
		return nil
	}
	return rf.file.Close()
}

// Sync flushes the log file to disk.
func (rf *RotatingFile) Sync() error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.file == nil {
		return nil
	}
	return rf.file.Sync()
}

// Rotate forces a rotation.
func (rf *RotatingFile) Rotate() error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.rotate()
}

// shouldRotate checks if rotation is needed.
func (rf *RotatingFile) shouldRotate(writeSize int) bool {
	// Size-based rotation
	if rf.size+int64(writeSize) > rf.maxSize {
		return true
	}

	// Daily rotation
	if rf.rotateDaily {
		now := time.Now()
		if now.Day() != rf.lastRotate.Day() ||
			now.Month() != rf.lastRotate.Month() ||
			now.Year() != rf.lastRotate.Year() {
			return true
		}
	}

	return false
}

// rotate performs the actual rotation.
func (rf *RotatingFile) rotate() error {
	// Close current file
	if rf.file != nil {
		if err := rf.file.Close(); err != nil {
			return err
		}
	}

	// Generate rotated filename with timestamp
	timestamp := time.Now().Format("20060102-150405")
	dir := filepath.Dir(rf.path)
	base := filepath.Base(rf.path)
	ext := filepath.Ext(base)
	name := strings.TrimSuffix(base, ext)
	rotatedPath := filepath.Join(dir, fmt.Sprintf("%s.%s%s", name, timestamp, ext))

	// Rename current file
	if err := os.Rename(rf.path, rotatedPath); err != nil && !os.IsNotExist(err) {
		return err
	}

	// Compress if enabled
	if rf.compress {
		go rf.compressFile(rotatedPath)
	}

	// Open new file
	if err := rf.openFile(); err != nil {
		return err
	}

	rf.lastRotate = time.Now()

	// Cleanup old files
	go rf.cleanup()

	return nil
}

// openFile opens or creates the log file.
func (rf *RotatingFile) openFile() error {
	// Ensure directory exists
	dir := filepath.Dir(rf.path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	// Open file
	f, err := os.OpenFile(rf.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	// Get current size
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return err
	}

	rf.file = f
	rf.size = info.Size()
	return nil
}

// compressFile compresses a rotated log file.
func (rf *RotatingFile) compressFile(path string) {
	// Read the file
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}

	// Create gzip file
	gzPath := path + ".gz"
	gzFile, err := os.Create(gzPath)
	if err != nil {
		return
	}
	defer gzFile.Close()

	gzWriter := gzip.NewWriter(gzFile)
	defer gzWriter.Close()

	if _, err := gzWriter.Write(data); err != nil {
		_ = os.Remove(gzPath)
		return
	}

	// Close gzip writer explicitly before removing original
	gzWriter.Close()
	gzFile.Close()

	// Remove original
	_ = os.Remove(path)
}

// cleanup removes old log files.
func (rf *RotatingFile) cleanup() {
	dir := filepath.Dir(rf.path)
	base := filepath.Base(rf.path)
	ext := filepath.Ext(base)
	name := strings.TrimSuffix(base, ext)
	pattern := name + ".*" + ext

	// Find rotated files
	matches, err := filepath.Glob(filepath.Join(dir, pattern))
	if err != nil {
		return
	}

	// Also find compressed files
	gzMatches, _ := filepath.Glob(filepath.Join(dir, pattern+".gz"))
	matches = append(matches, gzMatches...)

	// Sort by modification time (newest first)
	type fileInfo struct {
		path    string
		modTime time.Time
	}
	files := make([]fileInfo, 0, len(matches))
	for _, match := range matches {
		// Skip the current log file
		if match == rf.path {
			continue
		}
		info, err := os.Stat(match)
		if err != nil {
			continue
		}
		files = append(files, fileInfo{path: match, modTime: info.ModTime()})
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].modTime.After(files[j].modTime)
	})

	// Delete files exceeding max backups
	for i := rf.maxBackups; i < len(files); i++ {
		_ = os.Remove(files[i].path)
	}

	// Delete files exceeding max age
	if rf.maxAge > 0 {
		cutoff := time.Now().Add(-time.Duration(rf.maxAge) * 24 * time.Hour)
		for _, f := range files {
			if f.modTime.Before(cutoff) {
				_ = os.Remove(f.path)
			}
		}
	}
}

// ============================================================================
// Multi-writer support
// ============================================================================

// MultiWriter writes to multiple writers.
type MultiWriter struct {
	writers []io.Writer
	mu      sync.Mutex
}

// NewMultiWriter creates a writer that duplicates writes to all writers.
func NewMultiWriter(writers ...io.Writer) *MultiWriter {
	return &MultiWriter{writers: writers}
}

// Write writes to all underlying writers.
func (mw *MultiWriter) Write(p []byte) (n int, err error) {
	mw.mu.Lock()
	defer mw.mu.Unlock()

	for _, w := range mw.writers {
		n, err = w.Write(p)
		if err != nil {
			return
		}
		if n != len(p) {
			err = io.ErrShortWrite
			return
		}
	}
	return len(p), nil
}

// AddWriter adds a writer.
func (mw *MultiWriter) AddWriter(w io.Writer) {
	mw.mu.Lock()
	defer mw.mu.Unlock()
	mw.writers = append(mw.writers, w)
}

// ============================================================================
// Syslog-style priority
// ============================================================================

// Facility represents syslog facility codes.
type Facility int

const (
	FacilityKern     Facility = 0
	FacilityUser     Facility = 1
	FacilityMail     Facility = 2
	FacilityDaemon   Facility = 3
	FacilityAuth     Facility = 4
	FacilitySyslog   Facility = 5
	FacilityLpr      Facility = 6
	FacilityNews     Facility = 7
	FacilityUucp     Facility = 8
	FacilityCron     Facility = 9
	FacilityAuthpriv Facility = 10
	FacilityFtp      Facility = 11
	FacilityLocal0   Facility = 16
	FacilityLocal1   Facility = 17
	FacilityLocal2   Facility = 18
	FacilityLocal3   Facility = 19
	FacilityLocal4   Facility = 20
	FacilityLocal5   Facility = 21
	FacilityLocal6   Facility = 22
	FacilityLocal7   Facility = 23
)

// Severity represents syslog severity codes.
type Severity int

const (
	SeverityEmergency Severity = 0
	SeverityAlert     Severity = 1
	SeverityCritical  Severity = 2
	SeverityError     Severity = 3
	SeverityWarning   Severity = 4
	SeverityNotice    Severity = 5
	SeverityInfo      Severity = 6
	SeverityDebug     Severity = 7
)

// LevelToSeverity converts a log level to syslog severity.
func LevelToSeverity(level Level) Severity {
	switch {
	case level >= LevelError:
		return SeverityError
	case level >= LevelWarn:
		return SeverityWarning
	case level >= LevelInfo:
		return SeverityInfo
	default:
		return SeverityDebug
	}
}
