// Package utils provides utility functions for RapidoDB.
package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
)

// FileType represents the type of a RapidoDB file.
type FileType int

const (
	FileTypeUnknown FileType = iota
	FileTypeSSTable          // .sst files
	FileTypeWAL              // .wal files
	FileTypeManifest         // MANIFEST files
	FileTypeCurrent          // CURRENT file
	FileTypeLock             // LOCK file
	FileTypeLog              // .log files (info/error logs)
	FileTypeTemp             // .tmp files
)

// File extensions and names.
const (
	SSTableExtension  = ".sst"
	WALExtension      = ".wal"
	TempExtension     = ".tmp"
	ManifestPrefix    = "MANIFEST-"
	CurrentFileName   = "CURRENT"
	LockFileName      = "LOCK"
)

// MakeSSTablePath creates a path for an SSTable file.
func MakeSSTablePath(dir string, fileNum uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%06d%s", fileNum, SSTableExtension))
}

// MakeWALPath creates a path for a WAL file.
func MakeWALPath(dir string, fileNum uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%06d%s", fileNum, WALExtension))
}

// MakeManifestPath creates a path for a MANIFEST file.
func MakeManifestPath(dir string, fileNum uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%s%06d", ManifestPrefix, fileNum))
}

// MakeTempPath creates a path for a temporary file.
func MakeTempPath(dir string, fileNum uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%06d%s", fileNum, TempExtension))
}

// ParseFileNum extracts the file number from a filename.
// Returns 0 if the filename doesn't contain a valid number.
func ParseFileNum(filename string) uint64 {
	base := filepath.Base(filename)

	// Remove extension
	ext := filepath.Ext(base)
	name := strings.TrimSuffix(base, ext)

	// Handle MANIFEST files
	if strings.HasPrefix(name, ManifestPrefix) {
		name = strings.TrimPrefix(name, ManifestPrefix)
	}

	num, err := strconv.ParseUint(name, 10, 64)
	if err != nil {
		return 0
	}
	return num
}

// GetFileType determines the type of a file based on its name.
func GetFileType(filename string) FileType {
	base := filepath.Base(filename)

	switch {
	case strings.HasSuffix(base, SSTableExtension):
		return FileTypeSSTable
	case strings.HasSuffix(base, WALExtension):
		return FileTypeWAL
	case strings.HasPrefix(base, ManifestPrefix):
		return FileTypeManifest
	case base == CurrentFileName:
		return FileTypeCurrent
	case base == LockFileName:
		return FileTypeLock
	case strings.HasSuffix(base, TempExtension):
		return FileTypeTemp
	default:
		return FileTypeUnknown
	}
}

// FileExists checks if a file exists.
func FileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// EnsureDir creates a directory if it doesn't exist.
func EnsureDir(dir string) error {
	return os.MkdirAll(dir, 0755)
}

// RemoveFile removes a file, ignoring "not exists" errors.
func RemoveFile(path string) error {
	err := os.Remove(path)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

// SyncDir syncs a directory to ensure durability of file operations.
// This is important after creating/renaming files to ensure the
// directory entry is persisted.
func SyncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// AtomicWrite atomically writes data to a file by writing to a
// temporary file first and then renaming it.
func AtomicWrite(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	tempFile, err := os.CreateTemp(dir, "rapidodb-temp-*")
	if err != nil {
		return err
	}
	tempPath := tempFile.Name()

	// Clean up temp file on error
	success := false
	defer func() {
		if !success {
			tempFile.Close()
			os.Remove(tempPath)
		}
	}()

	// Write data
	if _, err := tempFile.Write(data); err != nil {
		return err
	}

	// Sync to disk
	if err := tempFile.Sync(); err != nil {
		return err
	}

	// Close before rename
	if err := tempFile.Close(); err != nil {
		return err
	}

	// Set permissions
	if err := os.Chmod(tempPath, perm); err != nil {
		return err
	}

	// Atomic rename
	if err := os.Rename(tempPath, path); err != nil {
		return err
	}

	// Sync directory
	if err := SyncDir(dir); err != nil {
		return err
	}

	success = true
	return nil
}

// FileNumGenerator generates unique file numbers atomically.
type FileNumGenerator struct {
	next atomic.Uint64
}

// NewFileNumGenerator creates a new file number generator.
func NewFileNumGenerator(start uint64) *FileNumGenerator {
	g := &FileNumGenerator{}
	g.next.Store(start)
	return g
}

// Next returns the next file number.
func (g *FileNumGenerator) Next() uint64 {
	return g.next.Add(1)
}

// Current returns the current file number without incrementing.
func (g *FileNumGenerator) Current() uint64 {
	return g.next.Load()
}

// SetNext sets the next file number (used during recovery).
func (g *FileNumGenerator) SetNext(n uint64) {
	g.next.Store(n)
}

// BytesToHumanReadable converts bytes to a human-readable string.
func BytesToHumanReadable(bytes int64) string {
	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
		TB = 1024 * GB
	)

	switch {
	case bytes >= TB:
		return fmt.Sprintf("%.2f TB", float64(bytes)/TB)
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/GB)
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/MB)
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/KB)
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

// Min returns the minimum of two integers.
func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Max returns the maximum of two integers.
func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Min64 returns the minimum of two int64 values.
func Min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// Max64 returns the maximum of two int64 values.
func Max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// CloneBytes creates a copy of a byte slice.
func CloneBytes(src []byte) []byte {
	if src == nil {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}
