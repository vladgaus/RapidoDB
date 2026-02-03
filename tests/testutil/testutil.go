// Package testutil provides testing utilities for RapidoDB tests.
package testutil

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/rapidodb/rapidodb/pkg/types"
)

// TempDir creates a temporary directory for tests.
// Returns a cleanup function that should be deferred.
func TempDir(t *testing.T) (string, func()) {
	t.Helper()

	dir, err := os.MkdirTemp("", "rapidodb-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	cleanup := func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Logf("warning: failed to remove temp dir: %v", err)
		}
	}

	return dir, cleanup
}

// TempFile creates a temporary file for tests.
func TempFile(t *testing.T, dir, pattern string) *os.File {
	t.Helper()

	f, err := os.CreateTemp(dir, pattern)
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}

	return f
}

// RandomBytes generates random bytes of the given length.
func RandomBytes(n int) []byte {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("failed to generate random bytes: %v", err))
	}
	return b
}

// RandomKey generates a random key with the given prefix.
func RandomKey(prefix string, size int) []byte {
	key := make([]byte, len(prefix)+size)
	copy(key, prefix)
	if _, err := rand.Read(key[len(prefix):]); err != nil {
		panic(fmt.Sprintf("failed to generate random key: %v", err))
	}
	return key
}

// SequentialKey generates a sequential key.
func SequentialKey(prefix string, num int) []byte {
	return []byte(fmt.Sprintf("%s%010d", prefix, num))
}

// SequentialValue generates a sequential value of the given size.
func SequentialValue(num, size int) []byte {
	pattern := fmt.Sprintf("value-%010d-", num)
	value := make([]byte, size)
	for i := 0; i < size; i++ {
		value[i] = pattern[i%len(pattern)]
	}
	return value
}

// MakeEntry creates an entry for testing.
func MakeEntry(key, value []byte, seqNum uint64) *types.Entry {
	return &types.Entry{
		Key:    key,
		Value:  value,
		Type:   types.EntryTypePut,
		SeqNum: seqNum,
	}
}

// MakeTombstone creates a tombstone entry for testing.
func MakeTombstone(key []byte, seqNum uint64) *types.Entry {
	return &types.Entry{
		Key:    key,
		Value:  nil,
		Type:   types.EntryTypeDelete,
		SeqNum: seqNum,
	}
}

// GenerateEntries generates n sequential entries.
func GenerateEntries(n int, keySize, valueSize int) []*types.Entry {
	entries := make([]*types.Entry, n)
	for i := 0; i < n; i++ {
		entries[i] = MakeEntry(
			SequentialKey("key", i),
			SequentialValue(i, valueSize),
			uint64(i+1),
		)
	}
	return entries
}

// AssertFileExists asserts that a file exists.
func AssertFileExists(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Errorf("expected file to exist: %s", path)
	}
}

// AssertFileNotExists asserts that a file does not exist.
func AssertFileNotExists(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err == nil {
		t.Errorf("expected file to not exist: %s", path)
	}
}

// AssertBytesEqual asserts that two byte slices are equal.
func AssertBytesEqual(t *testing.T, expected, actual []byte) {
	t.Helper()
	if len(expected) != len(actual) {
		t.Errorf("byte slices have different lengths: expected %d, got %d", len(expected), len(actual))
		return
	}
	for i := range expected {
		if expected[i] != actual[i] {
			t.Errorf("byte slices differ at index %d: expected %d, got %d", i, expected[i], actual[i])
			return
		}
	}
}

// WriteFile writes data to a file in the given directory.
func WriteFile(t *testing.T, dir, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("failed to write file: %v", err)
	}
	return path
}

// ReadFile reads a file and returns its contents.
func ReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}
	return data
}

// ListDir lists files in a directory.
func ListDir(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("failed to list directory: %v", err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}
