package manifest

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestVersionEditEncodeDecode(t *testing.T) {
	edit := NewVersionEdit()
	edit.SetComparator("bytewise")
	edit.SetLogNumber(5)
	edit.SetNextFileNumber(10)
	edit.SetLastSequence(100)

	edit.AddFile(0, &FileMeta{
		FileNum: 1,
		Size:    1024,
		MinKey:  []byte("aaa"),
		MaxKey:  []byte("zzz"),
		MinSeq:  1,
		MaxSeq:  50,
		NumKeys: 100,
	})

	edit.AddFile(1, &FileMeta{
		FileNum: 2,
		Size:    2048,
		MinKey:  []byte("bbb"),
		MaxKey:  []byte("yyy"),
		MinSeq:  10,
		MaxSeq:  40,
		NumKeys: 50,
	})

	edit.DeleteFile(0, 99)
	edit.DeleteFile(1, 100)

	// Encode
	data := edit.Encode()

	// Decode
	decoded := NewVersionEdit()
	if err := decoded.Decode(data); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	// Verify
	if !decoded.HasComparator || decoded.Comparator != "bytewise" {
		t.Errorf("comparator mismatch: got %q", decoded.Comparator)
	}
	if !decoded.HasLogNumber || decoded.LogNumber != 5 {
		t.Errorf("log number mismatch: got %d", decoded.LogNumber)
	}
	if !decoded.HasNextFileNumber || decoded.NextFileNumber != 10 {
		t.Errorf("next file number mismatch: got %d", decoded.NextFileNumber)
	}
	if !decoded.HasLastSequence || decoded.LastSequence != 100 {
		t.Errorf("last sequence mismatch: got %d", decoded.LastSequence)
	}

	if len(decoded.NewFiles) != 2 {
		t.Fatalf("new files count mismatch: got %d", len(decoded.NewFiles))
	}

	// Check first new file
	nf := decoded.NewFiles[0]
	if nf.Level != 0 || nf.Meta.FileNum != 1 {
		t.Errorf("new file 0 mismatch: level=%d, filenum=%d", nf.Level, nf.Meta.FileNum)
	}
	if !bytes.Equal(nf.Meta.MinKey, []byte("aaa")) || !bytes.Equal(nf.Meta.MaxKey, []byte("zzz")) {
		t.Errorf("new file 0 keys mismatch")
	}

	if len(decoded.DeletedFiles) != 2 {
		t.Fatalf("deleted files count mismatch: got %d", len(decoded.DeletedFiles))
	}
	if decoded.DeletedFiles[0].Level != 0 || decoded.DeletedFiles[0].FileNum != 99 {
		t.Errorf("deleted file 0 mismatch")
	}
}

func TestVersionEditEmpty(t *testing.T) {
	edit := NewVersionEdit()
	data := edit.Encode()

	decoded := NewVersionEdit()
	if err := decoded.Decode(data); err != nil {
		t.Fatalf("decode empty edit failed: %v", err)
	}

	if decoded.HasComparator || decoded.HasLogNumber || decoded.HasNextFileNumber || decoded.HasLastSequence {
		t.Error("empty edit should have no fields set")
	}
	if len(decoded.NewFiles) != 0 || len(decoded.DeletedFiles) != 0 {
		t.Error("empty edit should have no files")
	}
}

func TestManifestWriteRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "MANIFEST-000001")

	// Write some edits
	writer, err := NewWriter(path)
	if err != nil {
		t.Fatalf("create writer: %v", err)
	}

	edit1 := NewVersionEdit()
	edit1.SetComparator("bytewise")
	edit1.SetNextFileNumber(5)

	edit2 := NewVersionEdit()
	edit2.AddFile(0, &FileMeta{
		FileNum: 1,
		Size:    1024,
		MinKey:  []byte("a"),
		MaxKey:  []byte("z"),
	})

	edit3 := NewVersionEdit()
	edit3.DeleteFile(0, 1)
	edit3.AddFile(1, &FileMeta{
		FileNum: 2,
		Size:    2048,
		MinKey:  []byte("a"),
		MaxKey:  []byte("z"),
	})

	if err := writer.WriteEdit(edit1); err != nil {
		t.Fatalf("write edit1: %v", err)
	}
	if err := writer.WriteEdit(edit2); err != nil {
		t.Fatalf("write edit2: %v", err)
	}
	if err := writer.WriteEdit(edit3); err != nil {
		t.Fatalf("write edit3: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	// Read back
	reader, err := NewReader(path)
	if err != nil {
		t.Fatalf("create reader: %v", err)
	}
	defer reader.Close()

	// Edit 1
	e1, err := reader.ReadEdit()
	if err != nil {
		t.Fatalf("read edit1: %v", err)
	}
	if !e1.HasComparator || e1.Comparator != "bytewise" {
		t.Errorf("edit1 comparator mismatch")
	}

	// Edit 2
	e2, err := reader.ReadEdit()
	if err != nil {
		t.Fatalf("read edit2: %v", err)
	}
	if len(e2.NewFiles) != 1 {
		t.Errorf("edit2 new files mismatch")
	}

	// Edit 3
	e3, err := reader.ReadEdit()
	if err != nil {
		t.Fatalf("read edit3: %v", err)
	}
	if len(e3.DeletedFiles) != 1 || len(e3.NewFiles) != 1 {
		t.Errorf("edit3 files mismatch")
	}

	// EOF
	_, err = reader.ReadEdit()
	if err != io.EOF {
		t.Errorf("expected EOF, got: %v", err)
	}
}

func TestCurrentFile(t *testing.T) {
	dir := t.TempDir()

	// Set current
	if err := SetCurrent(dir, 5); err != nil {
		t.Fatalf("set current: %v", err)
	}

	// Read current
	name, err := ReadCurrent(dir)
	if err != nil {
		t.Fatalf("read current: %v", err)
	}

	if name != "MANIFEST-000005" {
		t.Errorf("current name = %q, want MANIFEST-000005", name)
	}

	// Update current
	if err := SetCurrent(dir, 10); err != nil {
		t.Fatalf("update current: %v", err)
	}

	name, err = ReadCurrent(dir)
	if err != nil {
		t.Fatalf("read updated current: %v", err)
	}

	if name != "MANIFEST-000010" {
		t.Errorf("updated current name = %q, want MANIFEST-000010", name)
	}
}

func TestVersionSetBasic(t *testing.T) {
	dir := t.TempDir()

	vs := NewVersionSet(dir)

	// Add a file
	edit := NewVersionEdit()
	edit.AddFile(0, &FileMeta{
		FileNum: 1,
		Size:    1024,
		MinKey:  []byte("a"),
		MaxKey:  []byte("z"),
	})
	edit.SetLastSequence(10)

	if err := vs.LogAndApply(edit); err != nil {
		t.Fatalf("log and apply: %v", err)
	}

	// Check state
	if vs.LastSequence() != 10 {
		t.Errorf("last sequence = %d, want 10", vs.LastSequence())
	}

	v := vs.Current()
	if v.NumFiles(0) != 1 {
		t.Errorf("L0 files = %d, want 1", v.NumFiles(0))
	}

	if err := vs.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen and recover
	vs2 := NewVersionSet(dir)
	if err := vs2.Recover(); err != nil {
		t.Fatalf("recover: %v", err)
	}

	// Check recovered state
	if vs2.LastSequence() != 10 {
		t.Errorf("recovered last sequence = %d, want 10", vs2.LastSequence())
	}

	v2 := vs2.Current()
	if v2.NumFiles(0) != 1 {
		t.Errorf("recovered L0 files = %d, want 1", v2.NumFiles(0))
	}

	if err := vs2.Close(); err != nil {
		t.Fatalf("close vs2: %v", err)
	}
}

func TestVersionSetCompaction(t *testing.T) {
	dir := t.TempDir()

	vs := NewVersionSet(dir)

	// Add files to L0
	for i := 1; i <= 4; i++ {
		edit := NewVersionEdit()
		edit.AddFile(0, &FileMeta{
			FileNum: uint64(i),
			Size:    1024,
			MinKey:  []byte("a"),
			MaxKey:  []byte("z"),
		})
		if err := vs.LogAndApply(edit); err != nil {
			t.Fatalf("add file %d: %v", i, err)
		}
	}

	if vs.Current().NumFiles(0) != 4 {
		t.Errorf("L0 files = %d, want 4", vs.Current().NumFiles(0))
	}

	// Compact L0 -> L1 (delete L0 files, add L1 file)
	edit := NewVersionEdit()
	for i := 1; i <= 4; i++ {
		edit.DeleteFile(0, uint64(i))
	}
	edit.AddFile(1, &FileMeta{
		FileNum: 5,
		Size:    4096,
		MinKey:  []byte("a"),
		MaxKey:  []byte("z"),
	})

	if err := vs.LogAndApply(edit); err != nil {
		t.Fatalf("compaction edit: %v", err)
	}

	v := vs.Current()
	if v.NumFiles(0) != 0 {
		t.Errorf("L0 files after compaction = %d, want 0", v.NumFiles(0))
	}
	if v.NumFiles(1) != 1 {
		t.Errorf("L1 files after compaction = %d, want 1", v.NumFiles(1))
	}

	// Verify recovery
	if err := vs.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	vs2 := NewVersionSet(dir)
	if err := vs2.Recover(); err != nil {
		t.Fatalf("recover: %v", err)
	}

	v2 := vs2.Current()
	if v2.NumFiles(0) != 0 || v2.NumFiles(1) != 1 {
		t.Errorf("recovered state incorrect: L0=%d, L1=%d", v2.NumFiles(0), v2.NumFiles(1))
	}

	vs2.Close()
}

func TestVersionSetNextFileNumber(t *testing.T) {
	dir := t.TempDir()

	vs := NewVersionSet(dir)

	// Get some file numbers
	n1 := vs.NextFileNumber()
	n2 := vs.NextFileNumber()
	n3 := vs.NextFileNumber()

	if n2 != n1+1 || n3 != n2+1 {
		t.Errorf("file numbers not sequential: %d, %d, %d", n1, n2, n3)
	}

	// Mark a higher number used
	vs.MarkFileNumberUsed(100)

	n4 := vs.NextFileNumber()
	if n4 != 101 {
		t.Errorf("after mark used, next = %d, want 101", n4)
	}

	vs.Close()
}

func TestVersionSetNoManifest(t *testing.T) {
	dir := t.TempDir()

	// Recover with no existing manifest
	vs := NewVersionSet(dir)
	if err := vs.Recover(); err != nil {
		t.Fatalf("recover empty: %v", err)
	}

	// Should have empty state
	if vs.Current().NumFiles(0) != 0 {
		t.Error("expected empty version")
	}

	vs.Close()
}

func TestVersionBuilder(t *testing.T) {
	base := NewVersion()
	base.Files[0] = []*FileMeta{
		{FileNum: 1, MinKey: []byte("a"), MaxKey: []byte("m")},
		{FileNum: 2, MinKey: []byte("n"), MaxKey: []byte("z")},
	}
	base.Files[1] = []*FileMeta{
		{FileNum: 3, MinKey: []byte("a"), MaxKey: []byte("z")},
	}

	builder := NewVersionBuilder(base)

	// Add and delete
	edit := NewVersionEdit()
	edit.DeleteFile(0, 1)
	edit.AddFile(0, &FileMeta{FileNum: 4, MinKey: []byte("a"), MaxKey: []byte("m")})
	edit.AddFile(1, &FileMeta{FileNum: 5, MinKey: []byte("aa"), MaxKey: []byte("mm")})

	if err := builder.Apply(edit); err != nil {
		t.Fatalf("apply: %v", err)
	}

	v := builder.Build()

	// L0: file 2 (kept), file 4 (added); file 1 deleted
	if v.NumFiles(0) != 2 {
		t.Errorf("L0 files = %d, want 2", v.NumFiles(0))
	}

	// L1: file 3 (kept), file 5 (added), sorted by MinKey
	if v.NumFiles(1) != 2 {
		t.Errorf("L1 files = %d, want 2", v.NumFiles(1))
	}

	// L1 should be sorted by MinKey
	l1Files := v.GetFiles(1)
	if l1Files[0].FileNum != 3 || l1Files[1].FileNum != 5 {
		t.Errorf("L1 not sorted correctly: %d, %d", l1Files[0].FileNum, l1Files[1].FileNum)
	}
}

func TestManifestLargeEdit(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "MANIFEST-000001")

	// Create edit with many files
	edit := NewVersionEdit()
	for i := 0; i < 1000; i++ {
		edit.AddFile(i%7, &FileMeta{
			FileNum: uint64(i),
			Size:    int64(i * 1024),
			MinKey:  []byte("aaaaaaaaaaaaaaaaaaaa"),
			MaxKey:  []byte("zzzzzzzzzzzzzzzzzzzz"),
			MinSeq:  uint64(i),
			MaxSeq:  uint64(i + 100),
			NumKeys: int64(i * 10),
		})
	}

	// Write
	writer, err := NewWriter(path)
	if err != nil {
		t.Fatalf("create writer: %v", err)
	}

	if err := writer.WriteEdit(edit); err != nil {
		t.Fatalf("write edit: %v", err)
	}
	writer.Close()

	// Read back
	reader, err := NewReader(path)
	if err != nil {
		t.Fatalf("create reader: %v", err)
	}
	defer reader.Close()

	decoded, err := reader.ReadEdit()
	if err != nil {
		t.Fatalf("read edit: %v", err)
	}

	if len(decoded.NewFiles) != 1000 {
		t.Errorf("new files = %d, want 1000", len(decoded.NewFiles))
	}
}

func TestParseManifestNumber(t *testing.T) {
	tests := []struct {
		name   string
		num    uint64
		valid  bool
	}{
		{"MANIFEST-000001", 1, true},
		{"MANIFEST-000123", 123, true},
		{"MANIFEST-999999", 999999, true},
		{"CURRENT", 0, false},
		{"manifest-000001", 0, false},
		{"MANIFEST-", 0, false},
		{"MANIFEST-abc", 0, false},
	}

	for _, tt := range tests {
		num, valid := ParseManifestNumber(tt.name)
		if valid != tt.valid {
			t.Errorf("ParseManifestNumber(%q) valid = %v, want %v", tt.name, valid, tt.valid)
		}
		if valid && num != tt.num {
			t.Errorf("ParseManifestNumber(%q) = %d, want %d", tt.name, num, tt.num)
		}
	}
}

func TestVersionEditString(t *testing.T) {
	edit := NewVersionEdit()
	edit.SetComparator("bytewise")
	edit.SetLogNumber(5)
	edit.AddFile(0, &FileMeta{FileNum: 1, Size: 1024})
	edit.DeleteFile(0, 99)

	s := edit.String()
	if s == "" {
		t.Error("String() returned empty")
	}

	// Should contain key info
	if !bytes.Contains([]byte(s), []byte("Comparator")) {
		t.Error("String() missing Comparator")
	}
	if !bytes.Contains([]byte(s), []byte("LogNumber")) {
		t.Error("String() missing LogNumber")
	}
}

func BenchmarkVersionEditEncode(b *testing.B) {
	edit := NewVersionEdit()
	edit.SetComparator("bytewise")
	edit.SetNextFileNumber(100)
	for i := 0; i < 10; i++ {
		edit.AddFile(i%7, &FileMeta{
			FileNum: uint64(i),
			Size:    1024,
			MinKey:  []byte("key"),
			MaxKey:  []byte("key2"),
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = edit.Encode()
	}
}

func BenchmarkVersionEditDecode(b *testing.B) {
	edit := NewVersionEdit()
	edit.SetComparator("bytewise")
	edit.SetNextFileNumber(100)
	for i := 0; i < 10; i++ {
		edit.AddFile(i%7, &FileMeta{
			FileNum: uint64(i),
			Size:    1024,
			MinKey:  []byte("key"),
			MaxKey:  []byte("key2"),
		})
	}
	data := edit.Encode()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		decoded := NewVersionEdit()
		_ = decoded.Decode(data)
	}
}

func TestManifestCorruption(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "MANIFEST-000001")

	// Write a valid edit
	writer, err := NewWriter(path)
	if err != nil {
		t.Fatalf("create writer: %v", err)
	}

	edit := NewVersionEdit()
	edit.SetComparator("bytewise")
	if err := writer.WriteEdit(edit); err != nil {
		t.Fatalf("write edit: %v", err)
	}
	writer.Close()

	// Corrupt the file
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	data[0] ^= 0xff // Flip CRC bytes
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("write corrupted: %v", err)
	}

	// Try to read - should fail CRC check
	reader, err := NewReader(path)
	if err != nil {
		t.Fatalf("create reader: %v", err)
	}
	defer reader.Close()

	_, err = reader.ReadEdit()
	if err == nil {
		t.Error("expected CRC error, got nil")
	}
}
