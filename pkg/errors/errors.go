// Package errors defines custom error types for RapidoDB.
// These errors provide detailed information about failure conditions
// and support error wrapping for context.
package errors

import (
	"errors"
	"fmt"
)

// Sentinel errors for common conditions.
var (
	// ErrKeyNotFound is returned when a key doesn't exist.
	ErrKeyNotFound = errors.New("rapidodb: key not found")

	// ErrDBClosed is returned when operating on a closed database.
	ErrDBClosed = errors.New("rapidodb: database is closed")

	// ErrCorruption is returned when data corruption is detected.
	ErrCorruption = errors.New("rapidodb: data corruption detected")

	// ErrWriteStall is returned when writes are blocked due to compaction lag.
	ErrWriteStall = errors.New("rapidodb: write stalled due to compaction pressure")

	// ErrEmptyKey is returned when a key is empty.
	ErrEmptyKey = errors.New("rapidodb: key cannot be empty")

	// ErrKeyTooLarge is returned when a key exceeds the maximum size.
	ErrKeyTooLarge = errors.New("rapidodb: key size exceeds maximum")

	// ErrValueTooLarge is returned when a value exceeds the maximum size.
	ErrValueTooLarge = errors.New("rapidodb: value size exceeds maximum")

	// ErrIteratorInvalid is returned when using an invalid iterator.
	ErrIteratorInvalid = errors.New("rapidodb: iterator is not valid")

	// ErrSnapshotReleased is returned when using a released snapshot.
	ErrSnapshotReleased = errors.New("rapidodb: snapshot has been released")

	// ErrReadOnly is returned when writing to a read-only database.
	ErrReadOnly = errors.New("rapidodb: database is read-only")

	// ErrManifestCorruption is returned when MANIFEST file is corrupted.
	ErrManifestCorruption = errors.New("rapidodb: manifest corruption detected")

	// ErrWALCorruption is returned when WAL file is corrupted.
	ErrWALCorruption = errors.New("rapidodb: WAL corruption detected")

	// ErrChecksumMismatch is returned when data checksum verification fails.
	ErrChecksumMismatch = errors.New("rapidodb: checksum mismatch")

	// ErrNoSpace is returned when there's insufficient disk space.
	ErrNoSpace = errors.New("rapidodb: no space left on device")

	// ErrInvalidArgument is returned for invalid function arguments.
	ErrInvalidArgument = errors.New("rapidodb: invalid argument")
)

// Maximum sizes for keys and values.
const (
	MaxKeySize   = 64 * 1024         // 64KB
	MaxValueSize = 256 * 1024 * 1024 // 256MB
)

// IOError wraps an I/O error with context about the operation.
type IOError struct {
	Op   string // Operation: "read", "write", "sync", "close"
	Path string // File path
	Err  error  // Underlying error
}

func (e *IOError) Error() string {
	return fmt.Sprintf("rapidodb: %s %s: %v", e.Op, e.Path, e.Err)
}

func (e *IOError) Unwrap() error {
	return e.Err
}

// NewIOError creates a new IOError.
func NewIOError(op, path string, err error) *IOError {
	return &IOError{Op: op, Path: path, Err: err}
}

// CorruptionError provides details about data corruption.
type CorruptionError struct {
	File    string // File where corruption was detected
	Offset  int64  // Byte offset in file
	Message string // Description of corruption
}

func (e *CorruptionError) Error() string {
	if e.Offset >= 0 {
		return fmt.Sprintf("rapidodb: corruption in %s at offset %d: %s", e.File, e.Offset, e.Message)
	}
	return fmt.Sprintf("rapidodb: corruption in %s: %s", e.File, e.Message)
}

// NewCorruptionError creates a new CorruptionError.
func NewCorruptionError(file string, offset int64, message string) *CorruptionError {
	return &CorruptionError{File: file, Offset: offset, Message: message}
}

// CompactionError wraps errors that occur during compaction.
type CompactionError struct {
	Level int
	Err   error
}

func (e *CompactionError) Error() string {
	return fmt.Sprintf("rapidodb: compaction failed at level %d: %v", e.Level, e.Err)
}

func (e *CompactionError) Unwrap() error {
	return e.Err
}

// NewCompactionError creates a new CompactionError.
func NewCompactionError(level int, err error) *CompactionError {
	return &CompactionError{Level: level, Err: err}
}

// RecoveryError wraps errors that occur during database recovery.
type RecoveryError struct {
	Phase string // "wal", "manifest", "sstable"
	Err   error
}

func (e *RecoveryError) Error() string {
	return fmt.Sprintf("rapidodb: recovery failed during %s: %v", e.Phase, e.Err)
}

func (e *RecoveryError) Unwrap() error {
	return e.Err
}

// NewRecoveryError creates a new RecoveryError.
func NewRecoveryError(phase string, err error) *RecoveryError {
	return &RecoveryError{Phase: phase, Err: err}
}

// ValidateKey checks if a key is valid for storage.
func ValidateKey(key []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	if len(key) > MaxKeySize {
		return fmt.Errorf("%w: size %d exceeds maximum %d", ErrKeyTooLarge, len(key), MaxKeySize)
	}
	return nil
}

// ValidateValue checks if a value is valid for storage.
func ValidateValue(value []byte) error {
	if len(value) > MaxValueSize {
		return fmt.Errorf("%w: size %d exceeds maximum %d", ErrValueTooLarge, len(value), MaxValueSize)
	}
	return nil
}

// IsNotFound returns true if the error indicates a key was not found.
func IsNotFound(err error) bool {
	return errors.Is(err, ErrKeyNotFound)
}

// IsCorruption returns true if the error indicates data corruption.
func IsCorruption(err error) bool {
	if errors.Is(err, ErrCorruption) {
		return true
	}
	var ce *CorruptionError
	return errors.As(err, &ce)
}

// Wrap adds context to an error if it's not nil.
func Wrap(err error, message string) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", message, err)
}

// Wrapf adds formatted context to an error if it's not nil.
func Wrapf(err error, format string, args ...any) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf(format+": %w", append(args, err)...)
}
