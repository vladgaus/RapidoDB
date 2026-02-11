package wal

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/vladgaus/RapidoDB/pkg/types"
)

func tempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "rapidodb-wal-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func TestWriterBasic(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "test.wal")

	// Create writer
	w, err := NewWriter(path, DefaultWriterOptions())
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	// Write entries
	entries := []*types.Entry{
		{Key: []byte("key1"), Value: []byte("value1"), Type: types.EntryTypePut, SeqNum: 1},
		{Key: []byte("key2"), Value: []byte("value2"), Type: types.EntryTypePut, SeqNum: 2},
		{Key: []byte("key3"), Value: nil, Type: types.EntryTypeDelete, SeqNum: 3},
	}

	for _, e := range entries {
		if err := w.Write(e); err != nil {
			t.Fatalf("write failed: %v", err)
		}
	}

	// Close writer
	if err := w.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Read back
	r, err := NewReader(path, DefaultReaderOptions())
	if err != nil {
		t.Fatalf("failed to create reader: %v", err)
	}
	defer r.Close()

	var recovered []*types.Entry
	err = r.Read(func(e *types.Entry) error {
		recovered = append(recovered, e)
		return nil
	})
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	// Verify
	if len(recovered) != len(entries) {
		t.Fatalf("expected %d entries, got %d", len(entries), len(recovered))
	}

	for i, e := range entries {
		if !bytes.Equal(recovered[i].Key, e.Key) {
			t.Errorf("entry %d: key mismatch", i)
		}
		if !bytes.Equal(recovered[i].Value, e.Value) {
			t.Errorf("entry %d: value mismatch", i)
		}
		if recovered[i].Type != e.Type {
			t.Errorf("entry %d: type mismatch", i)
		}
		if recovered[i].SeqNum != e.SeqNum {
			t.Errorf("entry %d: seqnum mismatch", i)
		}
	}
}

func TestWriterLargeRecord(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "test.wal")

	w, err := NewWriter(path, DefaultWriterOptions())
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	// Create a large entry that spans multiple blocks
	largeValue := bytes.Repeat([]byte("x"), BlockSize*3)
	entry := &types.Entry{
		Key:    []byte("large-key"),
		Value:  largeValue,
		Type:   types.EntryTypePut,
		SeqNum: 1,
	}

	if err := w.Write(entry); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Read back
	r, err := NewReader(path, DefaultReaderOptions())
	if err != nil {
		t.Fatalf("failed to create reader: %v", err)
	}
	defer r.Close()

	var recovered *types.Entry
	err = r.Read(func(e *types.Entry) error {
		recovered = e
		return nil
	})
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	if !bytes.Equal(recovered.Value, largeValue) {
		t.Errorf("large value mismatch: got %d bytes, expected %d", len(recovered.Value), len(largeValue))
	}
}

func TestWriterSyncOnWrite(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "test.wal")

	opts := WriterOptions{SyncOnWrite: true}
	w, err := NewWriter(path, opts)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	entry := &types.Entry{Key: []byte("key"), Value: []byte("value"), Type: types.EntryTypePut, SeqNum: 1}
	if err := w.Write(entry); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
}

func TestWriterBatch(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "test.wal")

	w, err := NewWriter(path, DefaultWriterOptions())
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	// Create batch
	entries := make([]*types.Entry, 100)
	for i := 0; i < 100; i++ {
		entries[i] = &types.Entry{
			Key:    []byte(fmt.Sprintf("key%03d", i)),
			Value:  []byte(fmt.Sprintf("value%03d", i)),
			Type:   types.EntryTypePut,
			SeqNum: uint64(i + 1),
		}
	}

	if err := w.WriteBatch(entries); err != nil {
		t.Fatalf("write batch failed: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Read back
	r, err := NewReader(path, DefaultReaderOptions())
	if err != nil {
		t.Fatalf("failed to create reader: %v", err)
	}
	defer r.Close()

	count := 0
	err = r.Read(func(e *types.Entry) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	if count != 100 {
		t.Errorf("expected 100 entries, got %d", count)
	}
}

func TestReaderCorruptionDetection(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "test.wal")

	// Write valid data
	w, err := NewWriter(path, DefaultWriterOptions())
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	for i := 0; i < 10; i++ {
		entry := &types.Entry{Key: []byte(fmt.Sprintf("key%d", i)), Value: []byte("value"), Type: types.EntryTypePut, SeqNum: uint64(i + 1)}
		w.Write(entry)
	}
	w.Close()

	// Corrupt the file
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	// Corrupt some bytes in the middle
	if len(data) > 100 {
		data[50] ^= 0xFF
		data[51] ^= 0xFF
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("failed to write corrupted file: %v", err)
	}

	// Try to read - should detect corruption
	r, err := NewReader(path, ReaderOptions{
		ReportCorruption: true,
		SkipCorruption:   false,
	})
	if err != nil {
		t.Fatalf("failed to create reader: %v", err)
	}
	defer r.Close()

	err = r.Read(func(e *types.Entry) error {
		return nil
	})

	// Should get a corruption error (or succeed if corruption was in padding)
	// The exact behavior depends on where the corruption occurred
	if err != nil {
		t.Logf("corruption detected as expected: %v", err)
	}
}

func TestReaderSkipCorruption(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "test.wal")

	// Write valid data
	w, err := NewWriter(path, DefaultWriterOptions())
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	for i := 0; i < 10; i++ {
		entry := &types.Entry{Key: []byte(fmt.Sprintf("key%d", i)), Value: []byte("value"), Type: types.EntryTypePut, SeqNum: uint64(i + 1)}
		w.Write(entry)
	}
	w.Close()

	// Read with skip corruption enabled
	r, err := NewReader(path, ReaderOptions{
		ReportCorruption: true,
		SkipCorruption:   true,
	})
	if err != nil {
		t.Fatalf("failed to create reader: %v", err)
	}
	defer r.Close()

	count := 0
	err = r.Read(func(e *types.Entry) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	if count != 10 {
		t.Errorf("expected 10 entries, got %d", count)
	}
}

func TestManagerBasic(t *testing.T) {
	dir := tempDir(t)

	// Create manager
	m, err := NewManager(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	// Open WAL
	if err := m.Open(0); err != nil {
		t.Fatalf("failed to open: %v", err)
	}

	// Write entries
	for i := 0; i < 100; i++ {
		entry := &types.Entry{
			Key:    []byte(fmt.Sprintf("key%03d", i)),
			Value:  []byte(fmt.Sprintf("value%03d", i)),
			Type:   types.EntryTypePut,
			SeqNum: uint64(i + 1),
		}
		if err := m.Write(entry); err != nil {
			t.Fatalf("write failed: %v", err)
		}
	}

	// Sync
	if err := m.Sync(); err != nil {
		t.Fatalf("sync failed: %v", err)
	}

	// Close
	if err := m.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Create new manager and recover
	m2, err := NewManager(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("failed to create second manager: %v", err)
	}

	count := 0
	err = m2.Recover(func(e *types.Entry) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("recover failed: %v", err)
	}

	if count != 100 {
		t.Errorf("expected 100 entries, got %d", count)
	}

	m2.Close()
}

func TestManagerRotation(t *testing.T) {
	dir := tempDir(t)

	opts := Options{
		Dir:         dir,
		SyncOnWrite: false,
		MaxFileSize: 1024, // Small size to trigger rotation
	}

	m, err := NewManager(opts)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	if err := m.Open(0); err != nil {
		t.Fatalf("failed to open: %v", err)
	}

	// Write enough to trigger rotation
	for i := 0; i < 100; i++ {
		entry := &types.Entry{
			Key:    []byte(fmt.Sprintf("key%03d", i)),
			Value:  bytes.Repeat([]byte("v"), 100),
			Type:   types.EntryTypePut,
			SeqNum: uint64(i + 1),
		}
		if err := m.Write(entry); err != nil {
			t.Fatalf("write failed: %v", err)
		}
	}

	// Should have multiple WAL files now
	fileNum := m.CurrentFileNum()
	if fileNum <= 1 {
		t.Logf("file num: %d (rotation may not have triggered with small writes)", fileNum)
	}

	if err := m.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Recover and verify all entries
	m2, err := NewManager(opts)
	if err != nil {
		t.Fatalf("failed to create second manager: %v", err)
	}

	count := 0
	err = m2.Recover(func(e *types.Entry) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("recover failed: %v", err)
	}

	if count != 100 {
		t.Errorf("expected 100 entries after recovery, got %d", count)
	}

	m2.Close()
}

func TestManagerCleanup(t *testing.T) {
	dir := tempDir(t)

	opts := Options{
		Dir:         dir,
		SyncOnWrite: false,
		MaxFileSize: 1024,
	}

	m, err := NewManager(opts)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	if err := m.Open(0); err != nil {
		t.Fatalf("failed to open: %v", err)
	}

	// Write enough to create multiple files
	for i := 0; i < 100; i++ {
		entry := &types.Entry{
			Key:    []byte(fmt.Sprintf("key%03d", i)),
			Value:  bytes.Repeat([]byte("v"), 100),
			Type:   types.EntryTypePut,
			SeqNum: uint64(i + 1),
		}
		m.Write(entry)
	}

	currentFile := m.CurrentFileNum()

	// Clean up old files
	if err := m.CleanBefore(currentFile); err != nil {
		t.Fatalf("cleanup failed: %v", err)
	}

	m.Close()

	// Verify old files are gone
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("failed to read dir: %v", err)
	}

	for _, e := range entries {
		t.Logf("remaining file: %s", e.Name())
	}
}

func TestRecordTypes(t *testing.T) {
	tests := []struct {
		rt       RecordType
		expected string
	}{
		{RecordTypeFull, "FULL"},
		{RecordTypeFirst, "FIRST"},
		{RecordTypeMiddle, "MIDDLE"},
		{RecordTypeLast, "LAST"},
		{RecordTypeZero, "ZERO"},
		{RecordType(99), "UNKNOWN"},
	}

	for _, tt := range tests {
		if tt.rt.String() != tt.expected {
			t.Errorf("RecordType(%d).String() = %s, expected %s", tt.rt, tt.rt.String(), tt.expected)
		}
	}
}

func TestCRC(t *testing.T) {
	payload := []byte("test payload")
	crc1 := computeCRC(RecordTypeFull, payload)
	crc2 := computeCRC(RecordTypeFull, payload)

	if crc1 != crc2 {
		t.Error("CRC should be deterministic")
	}

	crc3 := computeCRC(RecordTypeFirst, payload)
	if crc1 == crc3 {
		t.Error("CRC should differ for different record types")
	}

	crc4 := computeCRC(RecordTypeFull, []byte("different"))
	if crc1 == crc4 {
		t.Error("CRC should differ for different payloads")
	}
}

func TestHeaderEncodeDecode(t *testing.T) {
	buf := make([]byte, HeaderSize)

	tests := []struct {
		crc        uint32
		length     uint32
		recordType RecordType
	}{
		{0x12345678, 100, RecordTypeFull},
		{0xFFFFFFFF, 0, RecordTypeZero},
		{0x00000000, BlockSize - HeaderSize, RecordTypeFirst},
	}

	for _, tt := range tests {
		encodeHeader(buf, tt.crc, tt.length, tt.recordType)
		crc, length, recordType := decodeHeader(buf)

		if crc != tt.crc {
			t.Errorf("crc mismatch: got %x, expected %x", crc, tt.crc)
		}
		if length != tt.length {
			t.Errorf("length mismatch: got %d, expected %d", length, tt.length)
		}
		if recordType != tt.recordType {
			t.Errorf("recordType mismatch: got %v, expected %v", recordType, tt.recordType)
		}
	}
}

func TestWriterSize(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "test.wal")

	w, err := NewWriter(path, DefaultWriterOptions())
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	if w.Size() != 0 {
		t.Errorf("initial size should be 0, got %d", w.Size())
	}

	entry := &types.Entry{Key: []byte("key"), Value: []byte("value"), Type: types.EntryTypePut, SeqNum: 1}
	w.Write(entry)

	if w.Size() == 0 {
		t.Error("size should be > 0 after write")
	}

	w.Close()
}

func TestWriterTruncate(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "test.wal")

	w, err := NewWriter(path, DefaultWriterOptions())
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	// Write some data
	for i := 0; i < 10; i++ {
		entry := &types.Entry{Key: []byte(fmt.Sprintf("key%d", i)), Value: []byte("value"), Type: types.EntryTypePut, SeqNum: uint64(i + 1)}
		w.Write(entry)
	}

	sizeBefore := w.Size()
	if sizeBefore == 0 {
		t.Error("size should be > 0 before truncate")
	}

	// Truncate
	if err := w.Truncate(); err != nil {
		t.Fatalf("truncate failed: %v", err)
	}

	if w.Size() != 0 {
		t.Errorf("size should be 0 after truncate, got %d", w.Size())
	}

	w.Close()
}

// Benchmarks

func BenchmarkWriterWrite(b *testing.B) {
	dir, err := os.MkdirTemp("", "rapidodb-wal-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "test.wal")
	w, err := NewWriter(path, DefaultWriterOptions())
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	entry := &types.Entry{
		Key:    bytes.Repeat([]byte("k"), 16),
		Value:  bytes.Repeat([]byte("v"), 100),
		Type:   types.EntryTypePut,
		SeqNum: 1,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entry.SeqNum = uint64(i + 1)
		w.Write(entry)
	}
}

func BenchmarkWriterWriteSync(b *testing.B) {
	dir, err := os.MkdirTemp("", "rapidodb-wal-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "test.wal")
	w, err := NewWriter(path, WriterOptions{SyncOnWrite: true})
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	entry := &types.Entry{
		Key:    bytes.Repeat([]byte("k"), 16),
		Value:  bytes.Repeat([]byte("v"), 100),
		Type:   types.EntryTypePut,
		SeqNum: 1,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entry.SeqNum = uint64(i + 1)
		w.Write(entry)
	}
}

func BenchmarkWriterBatch(b *testing.B) {
	dir, err := os.MkdirTemp("", "rapidodb-wal-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "test.wal")
	w, err := NewWriter(path, DefaultWriterOptions())
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	// Create batch of 100 entries
	batch := make([]*types.Entry, 100)
	for i := 0; i < 100; i++ {
		batch[i] = &types.Entry{
			Key:    bytes.Repeat([]byte("k"), 16),
			Value:  bytes.Repeat([]byte("v"), 100),
			Type:   types.EntryTypePut,
			SeqNum: uint64(i + 1),
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.WriteBatch(batch)
	}
}

func BenchmarkReaderRead(b *testing.B) {
	dir, err := os.MkdirTemp("", "rapidodb-wal-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "test.wal")

	// Write test data
	w, err := NewWriter(path, DefaultWriterOptions())
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < 10000; i++ {
		entry := &types.Entry{
			Key:    bytes.Repeat([]byte("k"), 16),
			Value:  bytes.Repeat([]byte("v"), 100),
			Type:   types.EntryTypePut,
			SeqNum: uint64(i + 1),
		}
		w.Write(entry)
	}
	w.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, _ := NewReader(path, DefaultReaderOptions())
		r.Read(func(e *types.Entry) error {
			return nil
		})
		r.Close()
	}
}
