package backup

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// ============================================================================
// Mock Engine
// ============================================================================

type mockEngine struct {
	dataDir  string
	seqNum   uint64
	flushed  bool
	sstables []SSTableInfo
}

func newMockEngine(t *testing.T) *mockEngine {
	dir := t.TempDir()

	// Create directories
	os.MkdirAll(filepath.Join(dir, "sst"), 0755)
	os.MkdirAll(filepath.Join(dir, "wal"), 0755)

	// Create some test files
	os.WriteFile(filepath.Join(dir, "sst", "000001.sst"), []byte("sstable1"), 0644)
	os.WriteFile(filepath.Join(dir, "sst", "000002.sst"), []byte("sstable2data"), 0644)
	os.WriteFile(filepath.Join(dir, "wal", "000001.wal"), []byte("waldata"), 0644)
	os.WriteFile(filepath.Join(dir, "MANIFEST"), []byte("manifest"), 0644)
	os.WriteFile(filepath.Join(dir, "CURRENT"), []byte("current"), 0644)

	return &mockEngine{
		dataDir: dir,
		seqNum:  12345,
		sstables: []SSTableInfo{
			{FileNum: 1, Level: 0, Size: 8},
			{FileNum: 2, Level: 0, Size: 12},
		},
	}
}

func (m *mockEngine) GetDataDir() string            { return m.dataDir }
func (m *mockEngine) GetSequenceNumber() uint64     { return m.seqNum }
func (m *mockEngine) ForceFlush() error             { m.flushed = true; return nil }
func (m *mockEngine) GetSSTableInfo() []SSTableInfo { return m.sstables }
func (m *mockEngine) IsClosed() bool                { return false }

// ============================================================================
// Local Backend Tests
// ============================================================================

func TestLocalBackend_Write(t *testing.T) {
	dir := t.TempDir()
	backend := NewLocalBackend(dir)

	ctx := context.Background()

	// Write a file
	err := backend.WriteBytes(ctx, "test/file.txt", []byte("hello world"))
	if err != nil {
		t.Fatalf("WriteBytes failed: %v", err)
	}

	// Check it exists
	exists, err := backend.Exists(ctx, "test/file.txt")
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if !exists {
		t.Error("File should exist")
	}

	// Read it back
	reader, err := backend.Read(ctx, "test/file.txt")
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	defer reader.Close()

	data := make([]byte, 100)
	n, _ := reader.Read(data)
	if string(data[:n]) != "hello world" {
		t.Errorf("Expected 'hello world', got '%s'", string(data[:n]))
	}
}

func TestLocalBackend_List(t *testing.T) {
	dir := t.TempDir()
	backend := NewLocalBackend(dir)

	ctx := context.Background()

	// Create some directories
	backend.MkdirAll(ctx, "backups/backup-1")
	backend.MkdirAll(ctx, "backups/backup-2")
	backend.MkdirAll(ctx, "backups/backup-3")

	// List
	entries, err := backend.List(ctx, "backups")
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if len(entries) != 3 {
		t.Errorf("Expected 3 entries, got %d", len(entries))
	}
}

func TestLocalBackend_Delete(t *testing.T) {
	dir := t.TempDir()
	backend := NewLocalBackend(dir)

	ctx := context.Background()

	// Create and delete
	backend.WriteBytes(ctx, "test/file.txt", []byte("data"))
	err := backend.Delete(ctx, "test")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	exists, _ := backend.Exists(ctx, "test/file.txt")
	if exists {
		t.Error("File should not exist after delete")
	}
}

func TestLocalBackend_Type(t *testing.T) {
	backend := NewLocalBackend("/tmp")
	if backend.Type() != "local" {
		t.Errorf("Expected type 'local', got '%s'", backend.Type())
	}
}

// ============================================================================
// Backup Manager Tests
// ============================================================================

func TestManager_CreateBackup_Full(t *testing.T) {
	engine := newMockEngine(t)
	backupDir := t.TempDir()
	backend := NewLocalBackend(backupDir)

	manager := NewManager(ManagerOptions{
		Engine:  engine,
		Backend: backend,
	})

	ctx := context.Background()

	// Create full backup
	info, err := manager.CreateBackup(ctx, BackupOptions{
		Type: BackupTypeFull,
	})
	if err != nil {
		t.Fatalf("CreateBackup failed: %v", err)
	}

	if info.Status != BackupStatusCompleted {
		t.Errorf("Expected status completed, got %s", info.Status)
	}

	if info.Type != BackupTypeFull {
		t.Errorf("Expected type full, got %s", info.Type)
	}

	if info.FileCount == 0 {
		t.Error("Expected files to be backed up")
	}

	if info.TotalSize == 0 {
		t.Error("Expected non-zero total size")
	}

	if info.SequenceNumber != engine.seqNum {
		t.Errorf("Expected sequence number %d, got %d", engine.seqNum, info.SequenceNumber)
	}

	if !engine.flushed {
		t.Error("Engine should have been flushed")
	}

	// Verify backup files exist
	exists, _ := backend.Exists(ctx, filepath.Join("backups", info.ID, "backup.json"))
	if !exists {
		t.Error("Backup metadata should exist")
	}
}

func TestManager_CreateBackup_NoEngine(t *testing.T) {
	backend := NewLocalBackend(t.TempDir())

	manager := NewManager(ManagerOptions{
		Backend: backend,
	})

	ctx := context.Background()

	_, err := manager.CreateBackup(ctx, BackupOptions{})
	if err == nil {
		t.Error("Expected error with no engine")
	}
}

func TestManager_CreateBackup_NoBackend(t *testing.T) {
	engine := newMockEngine(t)

	manager := NewManager(ManagerOptions{
		Engine: engine,
	})

	ctx := context.Background()

	_, err := manager.CreateBackup(ctx, BackupOptions{})
	if err == nil {
		t.Error("Expected error with no backend")
	}
}

func TestManager_CreateBackup_Concurrent(t *testing.T) {
	engine := newMockEngine(t)
	backend := NewLocalBackend(t.TempDir())

	manager := NewManager(ManagerOptions{
		Engine:  engine,
		Backend: backend,
	})

	ctx := context.Background()

	// Mark as in progress manually
	manager.inProgress.Store(true)
	defer manager.inProgress.Store(false)

	// Second backup should fail immediately
	_, err := manager.CreateBackup(ctx, BackupOptions{})
	if err == nil {
		t.Error("Concurrent backup should fail")
	}
}

func TestManager_ListBackups(t *testing.T) {
	engine := newMockEngine(t)
	backupDir := t.TempDir()
	backend := NewLocalBackend(backupDir)

	manager := NewManager(ManagerOptions{
		Engine:  engine,
		Backend: backend,
	})

	ctx := context.Background()

	// Create a couple backups with delay
	info1, _ := manager.CreateBackup(ctx, BackupOptions{Type: BackupTypeFull})
	time.Sleep(10 * time.Millisecond) // Ensure different timestamps
	info2, _ := manager.CreateBackup(ctx, BackupOptions{Type: BackupTypeFull})

	if info1.ID == info2.ID {
		t.Fatalf("Backups should have different IDs: %s vs %s", info1.ID, info2.ID)
	}

	// List
	backups, err := manager.ListBackups(ctx)
	if err != nil {
		t.Fatalf("ListBackups failed: %v", err)
	}

	if len(backups) != 2 {
		t.Errorf("Expected 2 backups, got %d", len(backups))
		return
	}

	// Should be sorted by time
	if backups[0].StartTime.After(backups[1].StartTime) {
		t.Error("Backups should be sorted by start time")
	}
}

func TestManager_GetBackup(t *testing.T) {
	engine := newMockEngine(t)
	backend := NewLocalBackend(t.TempDir())

	manager := NewManager(ManagerOptions{
		Engine:  engine,
		Backend: backend,
	})

	ctx := context.Background()

	// Create backup
	created, _ := manager.CreateBackup(ctx, BackupOptions{})

	// Get it back
	retrieved, err := manager.GetBackup(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetBackup failed: %v", err)
	}

	if retrieved.ID != created.ID {
		t.Errorf("Expected ID %s, got %s", created.ID, retrieved.ID)
	}
}

func TestManager_GetBackup_NotFound(t *testing.T) {
	backend := NewLocalBackend(t.TempDir())

	manager := NewManager(ManagerOptions{
		Backend: backend,
	})

	ctx := context.Background()

	_, err := manager.GetBackup(ctx, "nonexistent")
	if err == nil {
		t.Error("Expected error for nonexistent backup")
	}
}

func TestManager_Restore(t *testing.T) {
	engine := newMockEngine(t)
	backupDir := t.TempDir()
	restoreDir := t.TempDir()
	backend := NewLocalBackend(backupDir)

	manager := NewManager(ManagerOptions{
		Engine:  engine,
		Backend: backend,
	})

	ctx := context.Background()

	// Create backup
	info, err := manager.CreateBackup(ctx, BackupOptions{Type: BackupTypeFull})
	if err != nil {
		t.Fatalf("CreateBackup failed: %v", err)
	}

	// Restore
	err = manager.Restore(ctx, RestoreOptions{
		BackupID:  info.ID,
		TargetDir: restoreDir,
		Verify:    true,
	})
	if err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	// Verify restored files
	restoredSSTDir := filepath.Join(restoreDir, "sst")
	entries, err := os.ReadDir(restoredSSTDir)
	if err != nil {
		t.Fatalf("Failed to read restored sst dir: %v", err)
	}

	if len(entries) != 2 {
		t.Errorf("Expected 2 sst files, got %d", len(entries))
	}

	// Verify content
	data, _ := os.ReadFile(filepath.Join(restoreDir, "sst", "000001.sst"))
	if string(data) != "sstable1" {
		t.Errorf("Restored content mismatch")
	}
}

func TestManager_Restore_NoBackupID(t *testing.T) {
	manager := NewManager(ManagerOptions{
		Backend: NewLocalBackend(t.TempDir()),
	})

	ctx := context.Background()

	err := manager.Restore(ctx, RestoreOptions{
		TargetDir: "/tmp/restore",
	})
	if err == nil {
		t.Error("Expected error with no backup ID")
	}
}

func TestManager_Restore_NoTargetDir(t *testing.T) {
	manager := NewManager(ManagerOptions{
		Backend: NewLocalBackend(t.TempDir()),
	})

	ctx := context.Background()

	err := manager.Restore(ctx, RestoreOptions{
		BackupID: "backup-123",
	})
	if err == nil {
		t.Error("Expected error with no target dir")
	}
}

func TestManager_DeleteBackup(t *testing.T) {
	engine := newMockEngine(t)
	backend := NewLocalBackend(t.TempDir())

	manager := NewManager(ManagerOptions{
		Engine:  engine,
		Backend: backend,
	})

	ctx := context.Background()

	// Create backup
	info, _ := manager.CreateBackup(ctx, BackupOptions{})

	// Delete it
	err := manager.DeleteBackup(ctx, info.ID)
	if err != nil {
		t.Fatalf("DeleteBackup failed: %v", err)
	}

	// Verify it's gone
	_, err = manager.GetBackup(ctx, info.ID)
	if err == nil {
		t.Error("Backup should be deleted")
	}
}

func TestManager_Stats(t *testing.T) {
	engine := newMockEngine(t)
	backend := NewLocalBackend(t.TempDir())

	manager := NewManager(ManagerOptions{
		Engine:  engine,
		Backend: backend,
	})

	ctx := context.Background()

	// Create backup
	manager.CreateBackup(ctx, BackupOptions{})

	stats := manager.Stats()
	if stats.BackupsCreated != 1 {
		t.Errorf("Expected 1 backup created, got %d", stats.BackupsCreated)
	}

	if stats.BytesBackedUp == 0 {
		t.Error("Expected non-zero bytes backed up")
	}
}

// ============================================================================
// Incremental Backup Tests
// ============================================================================

func TestManager_CreateBackup_Incremental(t *testing.T) {
	engine := newMockEngine(t)
	backend := NewLocalBackend(t.TempDir())

	manager := NewManager(ManagerOptions{
		Engine:  engine,
		Backend: backend,
	})

	ctx := context.Background()

	// Create full backup first
	full, err := manager.CreateBackup(ctx, BackupOptions{Type: BackupTypeFull})
	if err != nil {
		t.Fatalf("Full backup failed: %v", err)
	}

	// Modify a file
	os.WriteFile(filepath.Join(engine.dataDir, "sst", "000003.sst"), []byte("newfile"), 0644)

	// Create incremental
	incr, err := manager.CreateBackup(ctx, BackupOptions{Type: BackupTypeIncremental})
	if err != nil {
		t.Fatalf("Incremental backup failed: %v", err)
	}

	if incr.ParentID != full.ID {
		t.Errorf("Expected parent %s, got %s", full.ID, incr.ParentID)
	}

	if incr.Type != BackupTypeIncremental {
		t.Errorf("Expected type incremental, got %s", incr.Type)
	}
}

func TestManager_CreateBackup_Incremental_NoParent(t *testing.T) {
	engine := newMockEngine(t)
	backend := NewLocalBackend(t.TempDir())

	manager := NewManager(ManagerOptions{
		Engine:  engine,
		Backend: backend,
	})

	ctx := context.Background()

	// Try incremental without any full backup
	_, err := manager.CreateBackup(ctx, BackupOptions{Type: BackupTypeIncremental})
	if err == nil {
		t.Error("Incremental without parent should fail")
	}
}

// ============================================================================
// Backend URL Parsing Tests
// ============================================================================

func TestParseBackendURL_Local(t *testing.T) {
	tests := []string{
		"/tmp/backups",
		"./backups",
		"file:///tmp/backups",
	}

	for _, url := range tests {
		backend, err := ParseBackendURL(url, nil)
		if err != nil {
			t.Errorf("ParseBackendURL(%s) failed: %v", url, err)
		}
		if backend.Type() != "local" {
			t.Errorf("Expected local backend for %s", url)
		}
	}
}

func TestParseBackendURL_S3(t *testing.T) {
	backend, err := ParseBackendURL("s3://mybucket/prefix", map[string]string{
		"region": "us-east-1",
	})
	if err != nil {
		t.Fatalf("ParseBackendURL failed: %v", err)
	}

	if backend.Type() != "s3" {
		t.Errorf("Expected s3 backend")
	}

	s3 := backend.(*S3Backend)
	if s3.Bucket() != "mybucket" {
		t.Errorf("Expected bucket 'mybucket', got '%s'", s3.Bucket())
	}
}

func TestParseBackendURL_Unsupported(t *testing.T) {
	_, err := ParseBackendURL("gs://bucket/prefix", nil)
	if err == nil {
		t.Error("Expected error for unsupported GCS")
	}

	_, err = ParseBackendURL("az://container/prefix", nil)
	if err == nil {
		t.Error("Expected error for unsupported Azure")
	}
}

// ============================================================================
// Backup Info Tests
// ============================================================================

func TestBackupInfo_Checksum(t *testing.T) {
	engine := newMockEngine(t)
	backend := NewLocalBackend(t.TempDir())

	manager := NewManager(ManagerOptions{
		Engine:  engine,
		Backend: backend,
	})

	ctx := context.Background()

	info, _ := manager.CreateBackup(ctx, BackupOptions{})

	if info.Checksum == "" {
		t.Error("Backup should have checksum")
	}

	// All file checksums should be populated
	for _, f := range info.Files {
		if f.Checksum == "" {
			t.Errorf("File %s should have checksum", f.Name)
		}
	}
}

// ============================================================================
// Benchmarks
// ============================================================================

func BenchmarkBackup_Small(b *testing.B) {
	engine := &mockEngine{
		dataDir: b.TempDir(),
		seqNum:  1,
	}

	// Create small test files
	os.MkdirAll(filepath.Join(engine.dataDir, "sst"), 0755)
	os.WriteFile(filepath.Join(engine.dataDir, "sst", "test.sst"), make([]byte, 1024), 0644)

	backend := NewLocalBackend(b.TempDir())
	manager := NewManager(ManagerOptions{
		Engine:  engine,
		Backend: backend,
	})

	ctx := context.Background()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		manager.CreateBackup(ctx, BackupOptions{Type: BackupTypeFull})
	}
}
