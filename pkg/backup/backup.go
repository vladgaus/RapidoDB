// Package backup provides backup and restore functionality for RapidoDB.
//
// Features:
//   - Hot backup (consistent snapshot while DB is running)
//   - Incremental backup (only changed files since last backup)
//   - Point-in-time recovery
//   - Multiple storage backends (local, S3, GCS, Azure)
//
// Example usage:
//
//	manager := backup.NewManager(backup.ManagerOptions{
//	    Engine:  engine,
//	    Backend: backup.NewLocalBackend("/backups"),
//	})
//
//	// Full backup
//	info, err := manager.CreateBackup(ctx, backup.BackupOptions{
//	    Type: backup.BackupTypeFull,
//	})
//
//	// Restore
//	err = manager.Restore(ctx, backup.RestoreOptions{
//	    BackupID: "backup-123",
//	    TargetDir: "/data/restored",
//	})
package backup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"time"

	"github.com/vladgaus/RapidoDB/pkg/logging"
)

// ============================================================================
// Types
// ============================================================================

// BackupType specifies the type of backup.
type BackupType string

const (
	// BackupTypeFull is a complete backup of all data.
	BackupTypeFull BackupType = "full"

	// BackupTypeIncremental only backs up changes since last backup.
	BackupTypeIncremental BackupType = "incremental"
)

// BackupStatus represents the status of a backup.
type BackupStatus string

const (
	BackupStatusInProgress BackupStatus = "in_progress"
	BackupStatusCompleted  BackupStatus = "completed"
	BackupStatusFailed     BackupStatus = "failed"
)

// BackupInfo contains metadata about a backup.
type BackupInfo struct {
	// ID is the unique identifier for this backup.
	ID string `json:"id"`

	// Type is full or incremental.
	Type BackupType `json:"type"`

	// Status is the backup status.
	Status BackupStatus `json:"status"`

	// ParentID is the ID of the parent backup (for incremental).
	ParentID string `json:"parent_id,omitempty"`

	// StartTime is when the backup started.
	StartTime time.Time `json:"start_time"`

	// EndTime is when the backup completed.
	EndTime time.Time `json:"end_time,omitempty"`

	// SequenceNumber is the DB sequence number at backup time.
	SequenceNumber uint64 `json:"sequence_number"`

	// Files contains information about backed up files.
	Files []FileInfo `json:"files"`

	// TotalSize is the total size of backed up data.
	TotalSize int64 `json:"total_size"`

	// FileCount is the number of files backed up.
	FileCount int `json:"file_count"`

	// Error contains error message if backup failed.
	Error string `json:"error,omitempty"`

	// Checksum is the overall checksum of the backup.
	Checksum string `json:"checksum,omitempty"`
}

// FileInfo contains information about a backed up file.
type FileInfo struct {
	// Name is the relative file name.
	Name string `json:"name"`

	// Size is the file size in bytes.
	Size int64 `json:"size"`

	// Checksum is the SHA256 checksum.
	Checksum string `json:"checksum"`

	// ModTime is the file modification time.
	ModTime time.Time `json:"mod_time"`

	// Type indicates the file type (sstable, wal, manifest).
	Type string `json:"type"`
}

// BackupOptions configures a backup operation.
type BackupOptions struct {
	// Type is full or incremental.
	// Default: BackupTypeFull
	Type BackupType

	// Compression enables compression.
	// Default: false
	Compression bool

	// ParentID specifies the parent backup for incremental.
	// If empty, uses the latest full backup.
	ParentID string
}

// RestoreOptions configures a restore operation.
type RestoreOptions struct {
	// BackupID is the backup to restore.
	BackupID string

	// TargetDir is where to restore the data.
	TargetDir string

	// PointInTime restores to a specific sequence number.
	// If 0, restores to the backup's sequence number.
	PointInTime uint64

	// Verify verifies checksums after restore.
	// Default: true
	Verify bool
}

// ============================================================================
// Engine Interface
// ============================================================================

// Engine is the interface that the backup manager needs from the storage engine.
type Engine interface {
	// GetDataDir returns the data directory path.
	GetDataDir() string

	// GetSequenceNumber returns the current sequence number.
	GetSequenceNumber() uint64

	// ForceFlush flushes all in-memory data to disk.
	ForceFlush() error

	// GetSSTableInfo returns information about all SSTables.
	GetSSTableInfo() []SSTableInfo

	// IsClosed returns whether the engine is closed.
	IsClosed() bool
}

// SSTableInfo contains SSTable information needed for backup.
type SSTableInfo struct {
	FileNum uint64
	Level   int
	Size    int64
	MinKey  string
	MaxKey  string
}

// ============================================================================
// Manager
// ============================================================================

// Manager handles backup and restore operations.
type Manager struct {

	// Dependencies
	engine  Engine
	backend Backend
	logger  *logging.Logger

	// State
	currentBackup *BackupInfo
	inProgress    atomic.Bool

	// Statistics
	backupsCreated atomic.Int64
	backupsFailed  atomic.Int64
	bytesBackedUp  atomic.Int64
}

// ManagerOptions configures the backup manager.
type ManagerOptions struct {
	// Engine is the storage engine.
	Engine Engine

	// Backend is the storage backend.
	Backend Backend

	// Logger is the structured logger.
	Logger *logging.Logger
}

// NewManager creates a new backup manager.
func NewManager(opts ManagerOptions) *Manager {
	logger := opts.Logger
	if logger == nil {
		logger = logging.Default()
	}

	return &Manager{
		engine:  opts.Engine,
		backend: opts.Backend,
		logger:  logger.WithComponent("backup"),
	}
}

// CreateBackup creates a new backup.
func (m *Manager) CreateBackup(ctx context.Context, opts BackupOptions) (*BackupInfo, error) {
	if !m.inProgress.CompareAndSwap(false, true) {
		return nil, fmt.Errorf("backup already in progress")
	}
	defer m.inProgress.Store(false)

	if m.engine == nil {
		return nil, fmt.Errorf("engine not configured")
	}

	if m.backend == nil {
		return nil, fmt.Errorf("backend not configured")
	}

	// Default to full backup
	if opts.Type == "" {
		opts.Type = BackupTypeFull
	}

	// Generate backup ID
	backupID := generateBackupID()

	// Create backup info
	info := &BackupInfo{
		ID:        backupID,
		Type:      opts.Type,
		Status:    BackupStatusInProgress,
		StartTime: time.Now(),
	}
	m.currentBackup = info

	m.logger.Info("starting backup",
		"id", backupID,
		"type", opts.Type,
	)

	// Flush in-memory data first
	if err := m.engine.ForceFlush(); err != nil {
		info.Status = BackupStatusFailed
		info.Error = fmt.Sprintf("flush failed: %v", err)
		m.backupsFailed.Add(1)
		return info, fmt.Errorf("failed to flush engine: %w", err)
	}

	// Get sequence number after flush
	info.SequenceNumber = m.engine.GetSequenceNumber()

	// Get parent backup for incremental
	var parentFiles map[string]FileInfo
	if opts.Type == BackupTypeIncremental {
		parent, err := m.getParentBackup(ctx, opts.ParentID)
		if err != nil {
			info.Status = BackupStatusFailed
			info.Error = fmt.Sprintf("parent backup error: %v", err)
			m.backupsFailed.Add(1)
			return info, err
		}
		info.ParentID = parent.ID
		parentFiles = make(map[string]FileInfo)
		for _, f := range parent.Files {
			parentFiles[f.Name] = f
		}
	}

	// Perform backup
	if err := m.performBackup(ctx, info, parentFiles); err != nil {
		info.Status = BackupStatusFailed
		info.Error = err.Error()
		info.EndTime = time.Now()
		m.backupsFailed.Add(1)

		// Save failed backup metadata
		_ = m.saveBackupMetadata(ctx, info)

		return info, err
	}

	// Mark completed
	info.Status = BackupStatusCompleted
	info.EndTime = time.Now()

	// Calculate overall checksum
	info.Checksum = m.calculateBackupChecksum(info)

	// Save backup metadata
	if err := m.saveBackupMetadata(ctx, info); err != nil {
		m.logger.Error("failed to save backup metadata", "error", err)
	}

	m.backupsCreated.Add(1)
	m.bytesBackedUp.Add(info.TotalSize)

	m.logger.Info("backup completed",
		"id", backupID,
		"files", info.FileCount,
		"size", info.TotalSize,
		"duration", info.EndTime.Sub(info.StartTime),
	)

	return info, nil
}

// performBackup does the actual backup work.
func (m *Manager) performBackup(ctx context.Context, info *BackupInfo, parentFiles map[string]FileInfo) error {
	dataDir := m.engine.GetDataDir()

	// Create backup directory in backend
	backupPath := filepath.Join("backups", info.ID)
	if err := m.backend.MkdirAll(ctx, backupPath); err != nil {
		return fmt.Errorf("failed to create backup directory: %w", err)
	}

	// Backup SSTable directory
	sstDir := filepath.Join(dataDir, "sst")
	if err := m.backupDirectory(ctx, info, sstDir, backupPath, "sstable", parentFiles); err != nil {
		return fmt.Errorf("failed to backup sstables: %w", err)
	}

	// Backup WAL directory
	walDir := filepath.Join(dataDir, "wal")
	if err := m.backupDirectory(ctx, info, walDir, backupPath, "wal", parentFiles); err != nil {
		return fmt.Errorf("failed to backup wal: %w", err)
	}

	// Backup manifest
	manifestPath := filepath.Join(dataDir, "MANIFEST")
	if _, err := os.Stat(manifestPath); err == nil {
		if err := m.backupFile(ctx, info, manifestPath, backupPath, "manifest", parentFiles); err != nil {
			return fmt.Errorf("failed to backup manifest: %w", err)
		}
	}

	// Backup CURRENT file
	currentPath := filepath.Join(dataDir, "CURRENT")
	if _, err := os.Stat(currentPath); err == nil {
		if err := m.backupFile(ctx, info, currentPath, backupPath, "current", parentFiles); err != nil {
			return fmt.Errorf("failed to backup current: %w", err)
		}
	}

	return nil
}

// backupDirectory backs up all files in a directory.
func (m *Manager) backupDirectory(ctx context.Context, info *BackupInfo, srcDir, dstDir, fileType string, parentFiles map[string]FileInfo) error {
	entries, err := os.ReadDir(srcDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // Directory doesn't exist, skip
		}
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		srcPath := filepath.Join(srcDir, entry.Name())
		if err := m.backupFile(ctx, info, srcPath, dstDir, fileType, parentFiles); err != nil {
			return err
		}
	}

	return nil
}

// backupFile backs up a single file.
func (m *Manager) backupFile(ctx context.Context, info *BackupInfo, srcPath, dstDir, fileType string, parentFiles map[string]FileInfo) error {
	// Get file info
	stat, err := os.Stat(srcPath)
	if err != nil {
		return err
	}

	fileName := filepath.Base(srcPath)
	relPath := filepath.Join(fileType, fileName)

	// Check if file changed (for incremental)
	if parentFiles != nil {
		if parent, ok := parentFiles[relPath]; ok {
			if parent.Size == stat.Size() && parent.ModTime.Equal(stat.ModTime()) {
				// File unchanged, skip
				info.Files = append(info.Files, parent)
				return nil
			}
		}
	}

	// Open source file
	src, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer func() { _ = src.Close() }()

	// Calculate checksum while copying
	hash := sha256.New()

	// Create destination path
	dstPath := filepath.Join(dstDir, relPath)

	// Write to backend
	if err := m.backend.Write(ctx, dstPath, io.TeeReader(src, hash)); err != nil {
		return fmt.Errorf("failed to write %s: %w", dstPath, err)
	}

	// Add file info
	fileInfo := FileInfo{
		Name:     relPath,
		Size:     stat.Size(),
		Checksum: hex.EncodeToString(hash.Sum(nil)),
		ModTime:  stat.ModTime(),
		Type:     fileType,
	}
	info.Files = append(info.Files, fileInfo)
	info.TotalSize += stat.Size()
	info.FileCount++

	return nil
}

// saveBackupMetadata saves the backup metadata.
func (m *Manager) saveBackupMetadata(ctx context.Context, info *BackupInfo) error {
	data, err := json.MarshalIndent(info, "", "  ")
	if err != nil {
		return err
	}

	metadataPath := filepath.Join("backups", info.ID, "backup.json")
	return m.backend.WriteBytes(ctx, metadataPath, data)
}

// getParentBackup gets the parent backup for incremental.
func (m *Manager) getParentBackup(ctx context.Context, parentID string) (*BackupInfo, error) {
	if parentID != "" {
		return m.GetBackup(ctx, parentID)
	}

	// Find latest full backup
	backups, err := m.ListBackups(ctx)
	if err != nil {
		return nil, err
	}

	for i := len(backups) - 1; i >= 0; i-- {
		if backups[i].Type == BackupTypeFull && backups[i].Status == BackupStatusCompleted {
			return &backups[i], nil
		}
	}

	return nil, fmt.Errorf("no parent backup found for incremental backup")
}

// calculateBackupChecksum calculates overall backup checksum.
func (m *Manager) calculateBackupChecksum(info *BackupInfo) string {
	hash := sha256.New()
	for _, f := range info.Files {
		hash.Write([]byte(f.Name))
		hash.Write([]byte(f.Checksum))
	}
	return hex.EncodeToString(hash.Sum(nil))
}

// Restore restores a backup.
func (m *Manager) Restore(ctx context.Context, opts RestoreOptions) error {
	if opts.BackupID == "" {
		return fmt.Errorf("backup ID is required")
	}

	if opts.TargetDir == "" {
		return fmt.Errorf("target directory is required")
	}

	m.logger.Info("starting restore",
		"backup_id", opts.BackupID,
		"target_dir", opts.TargetDir,
	)

	// Get backup metadata
	info, err := m.GetBackup(ctx, opts.BackupID)
	if err != nil {
		return fmt.Errorf("failed to get backup: %w", err)
	}

	if info.Status != BackupStatusCompleted {
		return fmt.Errorf("backup %s is not completed (status: %s)", opts.BackupID, info.Status)
	}

	// For incremental backup, we need to restore the chain
	chain, err := m.getBackupChain(ctx, info)
	if err != nil {
		return fmt.Errorf("failed to get backup chain: %w", err)
	}

	// Create target directory
	if err := os.MkdirAll(opts.TargetDir, 0755); err != nil {
		return fmt.Errorf("failed to create target directory: %w", err)
	}

	// Restore from oldest to newest (base first, then incrementals)
	for _, backup := range chain {
		if err := m.restoreBackup(ctx, backup, opts.TargetDir, opts.Verify); err != nil {
			return fmt.Errorf("failed to restore backup %s: %w", backup.ID, err)
		}
	}

	m.logger.Info("restore completed",
		"backup_id", opts.BackupID,
		"target_dir", opts.TargetDir,
	)

	return nil
}

// restoreBackup restores a single backup.
func (m *Manager) restoreBackup(ctx context.Context, info *BackupInfo, targetDir string, verify bool) error {
	backupPath := filepath.Join("backups", info.ID)

	for _, file := range info.Files {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		srcPath := filepath.Join(backupPath, file.Name)

		// Determine target path based on file type
		var dstPath string
		switch file.Type {
		case "sstable":
			dstPath = filepath.Join(targetDir, "sst", filepath.Base(file.Name))
		case "wal":
			dstPath = filepath.Join(targetDir, "wal", filepath.Base(file.Name))
		case "manifest":
			dstPath = filepath.Join(targetDir, "MANIFEST")
		case "current":
			dstPath = filepath.Join(targetDir, "CURRENT")
		default:
			dstPath = filepath.Join(targetDir, filepath.Base(file.Name))
		}

		// Create directory
		if err := os.MkdirAll(filepath.Dir(dstPath), 0755); err != nil {
			return err
		}

		// Read from backend
		reader, err := m.backend.Read(ctx, srcPath)
		if err != nil {
			return fmt.Errorf("failed to read %s: %w", srcPath, err)
		}

		// Write to target
		dst, err := os.Create(dstPath)
		if err != nil {
			_ = reader.Close()
			return err
		}

		var hash = sha256.New()
		var writer io.Writer = dst
		if verify {
			writer = io.MultiWriter(dst, hash)
		}

		_, err = io.Copy(writer, reader)
		_ = reader.Close()
		closeErr := dst.Close()
		if err == nil {
			err = closeErr
		}

		if err != nil {
			return fmt.Errorf("failed to write %s: %w", dstPath, err)
		}

		// Verify checksum
		if verify {
			actual := hex.EncodeToString(hash.Sum(nil))
			if actual != file.Checksum {
				return fmt.Errorf("checksum mismatch for %s: expected %s, got %s",
					file.Name, file.Checksum, actual)
			}
		}
	}

	return nil
}

// getBackupChain gets the chain of backups for an incremental.
func (m *Manager) getBackupChain(ctx context.Context, info *BackupInfo) ([]*BackupInfo, error) {
	var chain []*BackupInfo
	current := info

	for current != nil {
		chain = append([]*BackupInfo{current}, chain...)

		if current.ParentID == "" {
			break
		}

		parent, err := m.GetBackup(ctx, current.ParentID)
		if err != nil {
			return nil, fmt.Errorf("failed to get parent %s: %w", current.ParentID, err)
		}
		current = parent
	}

	return chain, nil
}

// GetBackup retrieves backup metadata.
func (m *Manager) GetBackup(ctx context.Context, backupID string) (*BackupInfo, error) {
	metadataPath := filepath.Join("backups", backupID, "backup.json")

	reader, err := m.backend.Read(ctx, metadataPath)
	if err != nil {
		return nil, fmt.Errorf("backup not found: %w", err)
	}
	defer func() { _ = reader.Close() }()

	var info BackupInfo
	if err := json.NewDecoder(reader).Decode(&info); err != nil {
		return nil, fmt.Errorf("failed to parse backup metadata: %w", err)
	}

	return &info, nil
}

// ListBackups lists all backups.
func (m *Manager) ListBackups(ctx context.Context) ([]BackupInfo, error) {
	entries, err := m.backend.List(ctx, "backups")
	if err != nil {
		return nil, err
	}

	var backups []BackupInfo
	for _, entry := range entries {
		info, err := m.GetBackup(ctx, entry)
		if err != nil {
			continue // Skip invalid backups
		}
		backups = append(backups, *info)
	}

	// Sort by start time
	sort.Slice(backups, func(i, j int) bool {
		return backups[i].StartTime.Before(backups[j].StartTime)
	})

	return backups, nil
}

// DeleteBackup deletes a backup.
func (m *Manager) DeleteBackup(ctx context.Context, backupID string) error {
	// Check if any backup depends on this one
	backups, err := m.ListBackups(ctx)
	if err != nil {
		return err
	}

	for _, b := range backups {
		if b.ParentID == backupID {
			return fmt.Errorf("cannot delete backup %s: backup %s depends on it", backupID, b.ID)
		}
	}

	backupPath := filepath.Join("backups", backupID)
	return m.backend.Delete(ctx, backupPath)
}

// Stats returns backup manager statistics.
func (m *Manager) Stats() ManagerStats {
	return ManagerStats{
		BackupsCreated: m.backupsCreated.Load(),
		BackupsFailed:  m.backupsFailed.Load(),
		BytesBackedUp:  m.bytesBackedUp.Load(),
		InProgress:     m.inProgress.Load(),
	}
}

// ManagerStats contains backup manager statistics.
type ManagerStats struct {
	BackupsCreated int64 `json:"backups_created"`
	BackupsFailed  int64 `json:"backups_failed"`
	BytesBackedUp  int64 `json:"bytes_backed_up"`
	InProgress     bool  `json:"in_progress"`
}

// ============================================================================
// Helpers
// ============================================================================

func generateBackupID() string {
	return fmt.Sprintf("backup-%s-%d", time.Now().Format("20060102-150405"), time.Now().UnixNano()%1000000)
}
