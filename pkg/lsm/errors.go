package lsm

import "errors"

// Engine errors
var (
	// ErrClosed is returned when operating on a closed engine.
	ErrClosed = errors.New("lsm: engine closed")

	// ErrInvalidOptions is returned when options are invalid.
	ErrInvalidOptions = errors.New("lsm: invalid options")

	// ErrKeyNotFound is returned when a key is not found.
	ErrKeyNotFound = errors.New("lsm: key not found")

	// ErrKeyTooLarge is returned when a key exceeds the maximum size.
	ErrKeyTooLarge = errors.New("lsm: key too large")

	// ErrValueTooLarge is returned when a value exceeds the maximum size.
	ErrValueTooLarge = errors.New("lsm: value too large")

	// ErrWriteStall is returned when writes are stalled due to too many MemTables or L0 files.
	ErrWriteStall = errors.New("lsm: write stall")

	// ErrEmptyKey is returned when a key is empty.
	ErrEmptyKey = errors.New("lsm: empty key")

	// ErrCorruption is returned when data corruption is detected.
	ErrCorruption = errors.New("lsm: data corruption")
)
