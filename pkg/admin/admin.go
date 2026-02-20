// Package admin provides an HTTP API for administrative operations.
//
// Endpoints:
//   - POST /admin/compact     - Trigger manual compaction
//   - POST /admin/flush       - Force MemTable flush
//   - GET  /admin/sstables    - List all SSTables with sizes
//   - GET  /admin/levels      - Level statistics
//   - POST /admin/config      - Hot reload config
//   - GET  /admin/properties  - DB properties
//   - DELETE /admin/range     - Delete key range
//
// Example usage:
//
//	adminServer := admin.NewServer(admin.Options{
//	    Host:   "127.0.0.1",
//	    Port:   9091,
//	    Engine: engine,
//	})
//	adminServer.Start()
//	defer adminServer.Close()
package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/vladgaus/RapidoDB/pkg/logging"
	"github.com/vladgaus/RapidoDB/pkg/lsm"
)

// Server is the HTTP admin API server.
type Server struct {

	// Configuration
	opts Options

	// Dependencies
	engine *lsm.Engine
	logger *logging.Logger

	// Backup manager (optional)
	backupManager BackupManager

	// Import/Export manager (optional)
	importExportManager ImportExportManager

	// HTTP server
	server   *http.Server
	listener net.Listener
	addr     string

	// Config reload callback
	configReloadFn func() error

	// State
	started atomic.Bool
	closed  atomic.Bool
}

// BackupManager is the interface for backup operations.
type BackupManager interface {
	CreateBackup(ctx context.Context, opts BackupOptions) (*BackupInfo, error)
	Restore(ctx context.Context, opts RestoreOptions) error
	ListBackups(ctx context.Context) ([]BackupInfo, error)
	GetBackup(ctx context.Context, id string) (*BackupInfo, error)
	DeleteBackup(ctx context.Context, id string) error
	Stats() BackupManagerStats
}

// ImportExportManager is the interface for import/export operations.
type ImportExportManager interface {
	ImportCSV(ctx context.Context, path string, opts ImportCSVOptions) (*ImportStats, error)
	ImportJSON(ctx context.Context, path string, opts ImportJSONOptions) (*ImportStats, error)
	ExportCSV(ctx context.Context, path string, opts ExportOptions) (*ExportStats, error)
	ExportJSON(ctx context.Context, path string, opts ExportOptions) (*ExportStats, error)
	Stats() ImportExportStats
}

// ImportCSVOptions for CSV import.
type ImportCSVOptions struct {
	HasHeader   bool   `json:"has_header"`
	KeyColumn   int    `json:"key_column"`
	ValueColumn int    `json:"value_column"`
	Delimiter   string `json:"delimiter"`
	KeyPrefix   string `json:"key_prefix"`
	SkipErrors  bool   `json:"skip_errors"`
}

// ImportJSONOptions for JSON import.
type ImportJSONOptions struct {
	KeyField   string `json:"key_field"`
	ValueField string `json:"value_field"`
	KeyPrefix  string `json:"key_prefix"`
	SkipErrors bool   `json:"skip_errors"`
}

// ExportOptions for export operations.
type ExportOptions struct {
	StartKey      string `json:"start_key"`
	EndKey        string `json:"end_key"`
	KeyPrefix     string `json:"key_prefix"`
	Limit         int64  `json:"limit"`
	IncludeHeader bool   `json:"include_header"`
}

// ImportStats contains import statistics.
type ImportStats struct {
	RecordsTotal     int64   `json:"records_total"`
	RecordsImported  int64   `json:"records_imported"`
	RecordsFailed    int64   `json:"records_failed"`
	BytesWritten     int64   `json:"bytes_written"`
	DurationMs       int64   `json:"duration_ms"`
	RecordsPerSecond float64 `json:"records_per_second"`
}

// ExportStats contains export statistics.
type ExportStats struct {
	RecordsExported  int64   `json:"records_exported"`
	BytesWritten     int64   `json:"bytes_written"`
	DurationMs       int64   `json:"duration_ms"`
	RecordsPerSecond float64 `json:"records_per_second"`
}

// ImportExportStats contains overall statistics.
type ImportExportStats struct {
	TotalImports int64 `json:"total_imports"`
	TotalExports int64 `json:"total_exports"`
	InProgress   bool  `json:"in_progress"`
}

// BackupOptions for creating backups.
type BackupOptions struct {
	Type     string `json:"type"` // "full" or "incremental"
	ParentID string `json:"parent_id,omitempty"`
}

// RestoreOptions for restoring backups.
type RestoreOptions struct {
	BackupID  string `json:"backup_id"`
	TargetDir string `json:"target_dir"`
	Verify    bool   `json:"verify"`
}

// BackupInfo contains backup metadata.
type BackupInfo struct {
	ID             string    `json:"id"`
	Type           string    `json:"type"`
	Status         string    `json:"status"`
	ParentID       string    `json:"parent_id,omitempty"`
	StartTime      time.Time `json:"start_time"`
	EndTime        time.Time `json:"end_time,omitempty"`
	SequenceNumber uint64    `json:"sequence_number"`
	TotalSize      int64     `json:"total_size"`
	FileCount      int       `json:"file_count"`
	Error          string    `json:"error,omitempty"`
}

// BackupManagerStats contains backup statistics.
type BackupManagerStats struct {
	BackupsCreated int64 `json:"backups_created"`
	BackupsFailed  int64 `json:"backups_failed"`
	BytesBackedUp  int64 `json:"bytes_backed_up"`
	InProgress     bool  `json:"in_progress"`
}

// Options configures the admin server.
type Options struct {
	// Host is the address to bind to.
	// Default: "127.0.0.1" (localhost only for security)
	Host string

	// Port is the port to listen on.
	// Default: 9091
	Port int

	// Engine is the LSM storage engine.
	Engine *lsm.Engine

	// Logger is the structured logger.
	Logger *logging.Logger

	// Version is the server version string.
	Version string

	// ReadTimeout is the read timeout.
	// Default: 30s
	ReadTimeout time.Duration

	// WriteTimeout is the write timeout.
	// Default: 30s
	WriteTimeout time.Duration

	// AuthToken is an optional bearer token for authentication.
	// If empty, no authentication is required.
	AuthToken string
}

// DefaultOptions returns sensible defaults.
func DefaultOptions() Options {
	return Options{
		Host:         "127.0.0.1",
		Port:         9091,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}
}

// NewServer creates a new admin server.
func NewServer(opts Options) *Server {
	if opts.Host == "" {
		opts.Host = "127.0.0.1"
	}
	if opts.Port == 0 {
		opts.Port = 9091
	}
	if opts.ReadTimeout == 0 {
		opts.ReadTimeout = 30 * time.Second
	}
	if opts.WriteTimeout == 0 {
		opts.WriteTimeout = 30 * time.Second
	}

	logger := opts.Logger
	if logger == nil {
		logger = logging.Default()
	}

	s := &Server{
		opts:   opts,
		engine: opts.Engine,
		logger: logger.WithComponent("admin"),
	}

	return s
}

// Start starts the admin server.
func (s *Server) Start() error {
	if s.started.Swap(true) {
		return fmt.Errorf("admin server already started")
	}

	addr := fmt.Sprintf("%s:%d", s.opts.Host, s.opts.Port)

	var lc net.ListenConfig
	listener, err := lc.Listen(context.Background(), "tcp", addr)
	if err != nil {
		s.started.Store(false)
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	s.listener = listener
	s.addr = listener.Addr().String()

	// Setup HTTP server
	mux := http.NewServeMux()
	s.registerHandlers(mux)

	s.server = &http.Server{
		Handler:      s.authMiddleware(mux),
		ReadTimeout:  s.opts.ReadTimeout,
		WriteTimeout: s.opts.WriteTimeout,
	}

	// Start serving
	go func() {
		if err := s.server.Serve(listener); err != nil && err != http.ErrServerClosed {
			s.logger.Error("admin server error", "error", err)
		}
	}()

	s.logger.Info("admin server started", "addr", s.addr)
	return nil
}

// Close stops the admin server.
func (s *Server) Close() error {
	if s.closed.Swap(true) {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if s.server != nil {
		return s.server.Shutdown(ctx)
	}
	return nil
}

// Addr returns the server address.
func (s *Server) Addr() string {
	return s.addr
}

// SetConfigReloadFn sets the callback function for config reloading.
func (s *Server) SetConfigReloadFn(fn func() error) {
	s.configReloadFn = fn
}

// registerHandlers registers all HTTP handlers.
func (s *Server) registerHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/admin/compact", s.handleCompact)
	mux.HandleFunc("/admin/flush", s.handleFlush)
	mux.HandleFunc("/admin/sstables", s.handleSSTables)
	mux.HandleFunc("/admin/levels", s.handleLevels)
	mux.HandleFunc("/admin/config", s.handleConfig)
	mux.HandleFunc("/admin/properties", s.handleProperties)
	mux.HandleFunc("/admin/range", s.handleRange)
	mux.HandleFunc("/admin/stats", s.handleStats)

	// Backup endpoints
	mux.HandleFunc("/admin/backup", s.handleBackup)
	mux.HandleFunc("/admin/backup/list", s.handleBackupList)
	mux.HandleFunc("/admin/backup/restore", s.handleBackupRestore)

	// Import/Export endpoints
	mux.HandleFunc("/admin/import/csv", s.handleImportCSV)
	mux.HandleFunc("/admin/import/json", s.handleImportJSON)
	mux.HandleFunc("/admin/export/csv", s.handleExportCSV)
	mux.HandleFunc("/admin/export/json", s.handleExportJSON)
	mux.HandleFunc("/admin/import/stats", s.handleImportExportStats)
}

// SetBackupManager sets the backup manager.
func (s *Server) SetBackupManager(bm BackupManager) {
	s.backupManager = bm
}

// SetImportExportManager sets the import/export manager.
func (s *Server) SetImportExportManager(iem ImportExportManager) {
	s.importExportManager = iem
}

// authMiddleware adds authentication if configured.
func (s *Server) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip auth if no token configured
		if s.opts.AuthToken == "" {
			next.ServeHTTP(w, r)
			return
		}

		// Check Authorization header
		auth := r.Header.Get("Authorization")
		expected := "Bearer " + s.opts.AuthToken

		if auth != expected {
			s.writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}

		next.ServeHTTP(w, r)
	})
}

// ============================================================================
// Response Types
// ============================================================================

// Response is a generic API response.
type Response struct {
	Success bool        `json:"success"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

// CompactResponse is the response for compact operation.
type CompactResponse struct {
	Triggered bool   `json:"triggered"`
	Message   string `json:"message"`
}

// FlushResponse is the response for flush operation.
type FlushResponse struct {
	Flushed bool   `json:"flushed"`
	Message string `json:"message"`
}

// SSTableInfo contains information about an SSTable.
type SSTableInfo struct {
	FileNum uint64 `json:"file_num"`
	Level   int    `json:"level"`
	Size    int64  `json:"size"`
	SizeHR  string `json:"size_hr"` // Human-readable
	MinKey  string `json:"min_key"`
	MaxKey  string `json:"max_key"`
	NumKeys int64  `json:"num_keys"`
	MinSeq  uint64 `json:"min_seq"`
	MaxSeq  uint64 `json:"max_seq"`
}

// SSTablesResponse contains all SSTable information.
type SSTablesResponse struct {
	TotalCount  int           `json:"total_count"`
	TotalSize   int64         `json:"total_size"`
	TotalSizeHR string        `json:"total_size_hr"`
	Tables      []SSTableInfo `json:"tables"`
}

// LevelInfo contains information about a level.
type LevelInfo struct {
	Level        int     `json:"level"`
	NumFiles     int     `json:"num_files"`
	Size         int64   `json:"size"`
	SizeHR       string  `json:"size_hr"`
	TargetSize   int64   `json:"target_size"`
	TargetSizeHR string  `json:"target_size_hr"`
	Score        float64 `json:"score"` // Size / TargetSize
}

// LevelsResponse contains level statistics.
type LevelsResponse struct {
	NumLevels   int         `json:"num_levels"`
	TotalSize   int64       `json:"total_size"`
	TotalSizeHR string      `json:"total_size_hr"`
	TotalFiles  int         `json:"total_files"`
	Levels      []LevelInfo `json:"levels"`
}

// PropertiesResponse contains database properties.
type PropertiesResponse struct {
	Version            string `json:"version"`
	DataDir            string `json:"data_dir"`
	MemTableSize       int64  `json:"memtable_size"`
	MemTableSizeHR     string `json:"memtable_size_hr"`
	ImmutableCount     int    `json:"immutable_memtables"`
	SSTableCount       int    `json:"sstable_count"`
	TotalSize          int64  `json:"total_size"`
	TotalSizeHR        string `json:"total_size_hr"`
	CompactionsPending int    `json:"compactions_pending"`
	CompactionsTotal   int64  `json:"compactions_total"`
	BytesCompacted     int64  `json:"bytes_compacted"`
	BytesCompactedHR   string `json:"bytes_compacted_hr"`
	WriteStalled       bool   `json:"write_stalled"`
	SequenceNumber     uint64 `json:"sequence_number"`
}

// RangeDeleteRequest is the request for range delete.
type RangeDeleteRequest struct {
	StartKey string `json:"start_key"`
	EndKey   string `json:"end_key"`
}

// RangeDeleteResponse is the response for range delete.
type RangeDeleteResponse struct {
	Deleted int64  `json:"deleted"`
	Message string `json:"message"`
}

// StatsResponse contains general statistics.
type StatsResponse struct {
	Uptime       string `json:"uptime,omitempty"`
	TotalReads   int64  `json:"total_reads"`
	TotalWrites  int64  `json:"total_writes"`
	TotalDeletes int64  `json:"total_deletes"`
	BytesRead    int64  `json:"bytes_read"`
	BytesWritten int64  `json:"bytes_written"`
	CacheHits    int64  `json:"cache_hits"`
	CacheMisses  int64  `json:"cache_misses"`
	HitRate      string `json:"hit_rate"`
}

// ============================================================================
// Helper Functions
// ============================================================================

func (s *Server) writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func (s *Server) writeSuccess(w http.ResponseWriter, data interface{}) {
	s.writeJSON(w, http.StatusOK, Response{
		Success: true,
		Data:    data,
	})
}

func (s *Server) writeError(w http.ResponseWriter, status int, message string) {
	s.writeJSON(w, status, Response{
		Success: false,
		Error:   message,
	})
}

func (s *Server) writeMessage(w http.ResponseWriter, message string) {
	s.writeJSON(w, http.StatusOK, Response{
		Success: true,
		Message: message,
	})
}

// humanReadableSize converts bytes to human-readable format.
func humanReadableSize(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// ============================================================================
// Handler Implementations
// ============================================================================

// handleCompact triggers manual compaction.
func (s *Server) handleCompact(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if s.engine == nil {
		s.writeError(w, http.StatusServiceUnavailable, "engine not available")
		return
	}

	s.logger.Info("manual compaction triggered")
	s.engine.TriggerCompaction()

	s.writeSuccess(w, CompactResponse{
		Triggered: true,
		Message:   "compaction triggered",
	})
}

// handleFlush forces a MemTable flush.
func (s *Server) handleFlush(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if s.engine == nil {
		s.writeError(w, http.StatusServiceUnavailable, "engine not available")
		return
	}

	s.logger.Info("manual flush triggered")

	if err := s.engine.ForceFlush(); err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeSuccess(w, FlushResponse{
		Flushed: true,
		Message: "flush completed",
	})
}

// handleSSTables lists all SSTables.
func (s *Server) handleSSTables(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if s.engine == nil {
		s.writeError(w, http.StatusServiceUnavailable, "engine not available")
		return
	}

	tables := s.engine.GetSSTableInfo()

	response := SSTablesResponse{
		Tables: make([]SSTableInfo, 0, len(tables)),
	}

	for _, t := range tables {
		info := SSTableInfo{
			FileNum: t.FileNum,
			Level:   t.Level,
			Size:    t.Size,
			SizeHR:  humanReadableSize(t.Size),
			MinKey:  t.MinKey,
			MaxKey:  t.MaxKey,
			NumKeys: t.NumKeys,
			MinSeq:  t.MinSeq,
			MaxSeq:  t.MaxSeq,
		}
		response.Tables = append(response.Tables, info)
		response.TotalSize += t.Size
	}

	response.TotalCount = len(response.Tables)
	response.TotalSizeHR = humanReadableSize(response.TotalSize)

	s.writeSuccess(w, response)
}

// handleLevels returns level statistics.
func (s *Server) handleLevels(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if s.engine == nil {
		s.writeError(w, http.StatusServiceUnavailable, "engine not available")
		return
	}

	levels := s.engine.GetLevelInfo()

	response := LevelsResponse{
		NumLevels: len(levels),
		Levels:    make([]LevelInfo, 0, len(levels)),
	}

	for _, l := range levels {
		info := LevelInfo{
			Level:        l.Level,
			NumFiles:     l.NumFiles,
			Size:         l.Size,
			SizeHR:       humanReadableSize(l.Size),
			TargetSize:   l.TargetSize,
			TargetSizeHR: humanReadableSize(l.TargetSize),
			Score:        l.Score,
		}
		response.Levels = append(response.Levels, info)
		response.TotalFiles += l.NumFiles
		response.TotalSize += l.Size
	}

	response.TotalSizeHR = humanReadableSize(response.TotalSize)

	s.writeSuccess(w, response)
}

// handleConfig handles config reload.
func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if s.configReloadFn == nil {
		s.writeError(w, http.StatusNotImplemented, "config reload not configured")
		return
	}

	s.logger.Info("config reload triggered via admin API")

	if err := s.configReloadFn(); err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeMessage(w, "configuration reloaded")
}

// handleProperties returns database properties.
func (s *Server) handleProperties(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if s.engine == nil {
		s.writeError(w, http.StatusServiceUnavailable, "engine not available")
		return
	}

	props := s.engine.GetProperties()

	response := PropertiesResponse{
		Version:          s.opts.Version,
		DataDir:          props.DataDir,
		MemTableSize:     props.MemTableSize,
		MemTableSizeHR:   humanReadableSize(props.MemTableSize),
		ImmutableCount:   props.ImmutableCount,
		SSTableCount:     props.SSTableCount,
		TotalSize:        props.TotalSize,
		TotalSizeHR:      humanReadableSize(props.TotalSize),
		CompactionsTotal: props.CompactionsTotal,
		BytesCompacted:   props.BytesCompacted,
		BytesCompactedHR: humanReadableSize(props.BytesCompacted),
		WriteStalled:     props.WriteStalled,
		SequenceNumber:   props.SequenceNumber,
	}

	s.writeSuccess(w, response)
}

// handleRange handles key range deletion.
func (s *Server) handleRange(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if s.engine == nil {
		s.writeError(w, http.StatusServiceUnavailable, "engine not available")
		return
	}

	var req RangeDeleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.StartKey == "" || req.EndKey == "" {
		s.writeError(w, http.StatusBadRequest, "start_key and end_key are required")
		return
	}

	if req.StartKey >= req.EndKey {
		s.writeError(w, http.StatusBadRequest, "start_key must be less than end_key")
		return
	}

	s.logger.Info("range delete triggered",
		"start_key", req.StartKey,
		"end_key", req.EndKey,
	)

	count, err := s.engine.DeleteRange(r.Context(), []byte(req.StartKey), []byte(req.EndKey))
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeSuccess(w, RangeDeleteResponse{
		Deleted: count,
		Message: fmt.Sprintf("deleted %d keys", count),
	})
}

// handleStats returns server statistics.
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if s.engine == nil {
		s.writeError(w, http.StatusServiceUnavailable, "engine not available")
		return
	}

	stats := s.engine.GetStats()

	response := StatsResponse{
		TotalReads:   stats.TotalReads,
		TotalWrites:  stats.TotalWrites,
		TotalDeletes: stats.TotalDeletes,
		CacheHits:    stats.CacheHits,
		CacheMisses:  stats.CacheMisses,
		BytesRead:    stats.BytesRead,
		BytesWritten: stats.BytesWritten,
	}

	// Calculate hit rate
	total := stats.CacheHits + stats.CacheMisses
	if total > 0 {
		response.HitRate = fmt.Sprintf("%.2f%%", float64(stats.CacheHits)/float64(total)*100)
	} else {
		response.HitRate = "N/A"
	}

	s.writeSuccess(w, response)
}

// ============================================================================
// Backup Handlers
// ============================================================================

// BackupRequest is the request for creating a backup.
type BackupRequest struct {
	Type     string `json:"type"` // "full" or "incremental"
	ParentID string `json:"parent_id,omitempty"`
}

// BackupResponse contains backup creation response.
type BackupResponse struct {
	BackupID       string    `json:"backup_id"`
	Type           string    `json:"type"`
	Status         string    `json:"status"`
	StartTime      time.Time `json:"start_time"`
	SequenceNumber uint64    `json:"sequence_number"`
	TotalSize      int64     `json:"total_size"`
	TotalSizeHR    string    `json:"total_size_hr"`
	FileCount      int       `json:"file_count"`
}

// handleBackup handles POST /admin/backup (create), GET /admin/backup (get), DELETE (delete).
func (s *Server) handleBackup(w http.ResponseWriter, r *http.Request) {
	if s.backupManager == nil {
		s.writeError(w, http.StatusServiceUnavailable, "backup not configured")
		return
	}

	switch r.Method {
	case http.MethodPost:
		s.handleBackupCreate(w, r)
	case http.MethodGet:
		s.handleBackupGet(w, r)
	case http.MethodDelete:
		s.handleBackupDelete(w, r)
	default:
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// handleBackupCreate creates a new backup.
func (s *Server) handleBackupCreate(w http.ResponseWriter, r *http.Request) {
	var req BackupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		req.Type = "full" // Default
	}

	if req.Type == "" {
		req.Type = "full"
	}

	s.logger.Info("backup requested", "type", req.Type)

	info, err := s.backupManager.CreateBackup(r.Context(), BackupOptions(req))
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeSuccess(w, BackupResponse{
		BackupID:       info.ID,
		Type:           info.Type,
		Status:         info.Status,
		StartTime:      info.StartTime,
		SequenceNumber: info.SequenceNumber,
		TotalSize:      info.TotalSize,
		TotalSizeHR:    humanReadableSize(info.TotalSize),
		FileCount:      info.FileCount,
	})
}

// handleBackupGet gets a backup by ID.
func (s *Server) handleBackupGet(w http.ResponseWriter, r *http.Request) {
	backupID := r.URL.Query().Get("id")
	if backupID == "" {
		s.writeError(w, http.StatusBadRequest, "backup id required")
		return
	}

	info, err := s.backupManager.GetBackup(r.Context(), backupID)
	if err != nil {
		s.writeError(w, http.StatusNotFound, err.Error())
		return
	}

	s.writeSuccess(w, info)
}

// handleBackupDelete deletes a backup.
func (s *Server) handleBackupDelete(w http.ResponseWriter, r *http.Request) {
	backupID := r.URL.Query().Get("id")
	if backupID == "" {
		s.writeError(w, http.StatusBadRequest, "backup id required")
		return
	}

	if err := s.backupManager.DeleteBackup(r.Context(), backupID); err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeMessage(w, "backup deleted")
}

// BackupListResponse contains list of backups.
type BackupListResponse struct {
	Backups []BackupInfo `json:"backups"`
	Count   int          `json:"count"`
}

// handleBackupList lists all backups.
func (s *Server) handleBackupList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if s.backupManager == nil {
		s.writeError(w, http.StatusServiceUnavailable, "backup not configured")
		return
	}

	backups, err := s.backupManager.ListBackups(r.Context())
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeSuccess(w, BackupListResponse{
		Backups: backups,
		Count:   len(backups),
	})
}

// RestoreRequest is the request for restoring a backup.
type RestoreRequest struct {
	BackupID  string `json:"backup_id"`
	TargetDir string `json:"target_dir"`
	Verify    bool   `json:"verify"`
}

// handleBackupRestore restores a backup.
func (s *Server) handleBackupRestore(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if s.backupManager == nil {
		s.writeError(w, http.StatusServiceUnavailable, "backup not configured")
		return
	}

	var req RestoreRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.BackupID == "" {
		s.writeError(w, http.StatusBadRequest, "backup_id required")
		return
	}

	if req.TargetDir == "" {
		s.writeError(w, http.StatusBadRequest, "target_dir required")
		return
	}

	s.logger.Info("restore requested", "backup_id", req.BackupID, "target_dir", req.TargetDir)

	if err := s.backupManager.Restore(r.Context(), RestoreOptions(req)); err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeMessage(w, "restore completed")
}

// ============================================================================
// Import/Export Handlers
// ============================================================================

// ImportCSVRequest is the request for CSV import.
type ImportCSVRequest struct {
	Path        string `json:"path"`
	HasHeader   bool   `json:"has_header"`
	KeyColumn   int    `json:"key_column"`
	ValueColumn int    `json:"value_column"`
	Delimiter   string `json:"delimiter"`
	KeyPrefix   string `json:"key_prefix"`
	SkipErrors  bool   `json:"skip_errors"`
}

// handleImportCSV handles POST /admin/import/csv.
func (s *Server) handleImportCSV(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if s.importExportManager == nil {
		s.writeError(w, http.StatusServiceUnavailable, "import/export not configured")
		return
	}

	var req ImportCSVRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.Path == "" {
		s.writeError(w, http.StatusBadRequest, "path is required")
		return
	}

	s.logger.Info("CSV import requested", "path", req.Path)

	stats, err := s.importExportManager.ImportCSV(r.Context(), req.Path, ImportCSVOptions{
		HasHeader:   req.HasHeader,
		KeyColumn:   req.KeyColumn,
		ValueColumn: req.ValueColumn,
		Delimiter:   req.Delimiter,
		KeyPrefix:   req.KeyPrefix,
		SkipErrors:  req.SkipErrors,
	})
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeSuccess(w, stats)
}

// ImportJSONRequest is the request for JSON import.
type ImportJSONRequest struct {
	Path       string `json:"path"`
	KeyField   string `json:"key_field"`
	ValueField string `json:"value_field"`
	KeyPrefix  string `json:"key_prefix"`
	SkipErrors bool   `json:"skip_errors"`
}

// handleImportJSON handles POST /admin/import/json.
func (s *Server) handleImportJSON(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if s.importExportManager == nil {
		s.writeError(w, http.StatusServiceUnavailable, "import/export not configured")
		return
	}

	var req ImportJSONRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.Path == "" {
		s.writeError(w, http.StatusBadRequest, "path is required")
		return
	}

	s.logger.Info("JSON import requested", "path", req.Path)

	stats, err := s.importExportManager.ImportJSON(r.Context(), req.Path, ImportJSONOptions{
		KeyField:   req.KeyField,
		ValueField: req.ValueField,
		KeyPrefix:  req.KeyPrefix,
		SkipErrors: req.SkipErrors,
	})
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeSuccess(w, stats)
}

// ExportRequest is the request for export operations.
type ExportRequest struct {
	Path          string `json:"path"`
	StartKey      string `json:"start_key"`
	EndKey        string `json:"end_key"`
	KeyPrefix     string `json:"key_prefix"`
	Limit         int64  `json:"limit"`
	IncludeHeader bool   `json:"include_header"`
}

// handleExportCSV handles POST /admin/export/csv.
func (s *Server) handleExportCSV(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if s.importExportManager == nil {
		s.writeError(w, http.StatusServiceUnavailable, "import/export not configured")
		return
	}

	var req ExportRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.Path == "" {
		s.writeError(w, http.StatusBadRequest, "path is required")
		return
	}

	s.logger.Info("CSV export requested", "path", req.Path)

	stats, err := s.importExportManager.ExportCSV(r.Context(), req.Path, ExportOptions{
		StartKey:      req.StartKey,
		EndKey:        req.EndKey,
		KeyPrefix:     req.KeyPrefix,
		Limit:         req.Limit,
		IncludeHeader: req.IncludeHeader,
	})
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeSuccess(w, stats)
}

// handleExportJSON handles POST /admin/export/json.
func (s *Server) handleExportJSON(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if s.importExportManager == nil {
		s.writeError(w, http.StatusServiceUnavailable, "import/export not configured")
		return
	}

	var req ExportRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.Path == "" {
		s.writeError(w, http.StatusBadRequest, "path is required")
		return
	}

	s.logger.Info("JSON export requested", "path", req.Path)

	stats, err := s.importExportManager.ExportJSON(r.Context(), req.Path, ExportOptions{
		StartKey:  req.StartKey,
		EndKey:    req.EndKey,
		KeyPrefix: req.KeyPrefix,
		Limit:     req.Limit,
	})
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeSuccess(w, stats)
}

// handleImportExportStats handles GET /admin/import/stats.
func (s *Server) handleImportExportStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if s.importExportManager == nil {
		s.writeError(w, http.StatusServiceUnavailable, "import/export not configured")
		return
	}

	stats := s.importExportManager.Stats()
	s.writeSuccess(w, stats)
}
