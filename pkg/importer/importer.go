// Package importer provides bulk import and export functionality for RapidoDB.
//
// Supported formats:
//   - CSV: key,value format with optional headers
//   - JSON Lines: {"key": "...", "value": "..."} per line
//   - SSTable: Direct SSTable file import (fastest)
//
// Example usage:
//
//	imp := importer.New(importer.Options{Engine: engine})
//
//	// Import from CSV
//	stats, err := imp.ImportCSV(ctx, "data.csv", importer.CSVOptions{})
//
//	// Export to JSON
//	stats, err := imp.ExportJSON(ctx, "export.jsonl", importer.ExportOptions{})
package importer

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vladgaus/RapidoDB/pkg/logging"
)

// ============================================================================
// Types
// ============================================================================

// Format specifies the import/export format.
type Format string

const (
	FormatCSV       Format = "csv"
	FormatJSON      Format = "json"
	FormatJSONLines Format = "jsonl"
	FormatSSTable   Format = "sstable"
)

// Engine is the interface the importer needs from the storage engine.
type Engine interface {
	Put(key, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
	Scan(start, end []byte) Iterator
	ForceFlush() error
}

// Iterator is the interface for scanning keys.
type Iterator interface {
	Valid() bool
	Key() []byte
	Value() []byte
	Next()
	Close()
	Error() error
}

// ImportStats contains statistics about an import operation.
type ImportStats struct {
	RecordsTotal     int64         `json:"records_total"`
	RecordsImported  int64         `json:"records_imported"`
	RecordsFailed    int64         `json:"records_failed"`
	RecordsSkipped   int64         `json:"records_skipped"`
	BytesRead        int64         `json:"bytes_read"`
	BytesWritten     int64         `json:"bytes_written"`
	Duration         time.Duration `json:"duration"`
	RecordsPerSecond float64       `json:"records_per_second"`
}

// ExportStats contains statistics about an export operation.
type ExportStats struct {
	RecordsTotal     int64         `json:"records_total"`
	RecordsExported  int64         `json:"records_exported"`
	RecordsFailed    int64         `json:"records_failed"`
	BytesWritten     int64         `json:"bytes_written"`
	Duration         time.Duration `json:"duration"`
	RecordsPerSecond float64       `json:"records_per_second"`
}

// ============================================================================
// Importer
// ============================================================================

// Importer handles bulk import and export operations.
type Importer struct {
	engine Engine
	logger *logging.Logger

	// State
	inProgress atomic.Bool

	// Statistics
	totalImports atomic.Int64
	totalExports atomic.Int64
}

// Options configures the importer.
type Options struct {
	Engine Engine
	Logger *logging.Logger
}

// New creates a new importer.
func New(opts Options) *Importer {
	logger := opts.Logger
	if logger == nil {
		logger = logging.Default()
	}

	return &Importer{
		engine: opts.Engine,
		logger: logger.WithComponent("importer"),
	}
}

// ============================================================================
// CSV Import
// ============================================================================

// CSVOptions configures CSV import.
type CSVOptions struct {
	HasHeader   bool
	KeyColumn   int
	ValueColumn int
	Delimiter   rune
	SkipErrors  bool
	BatchSize   int
	KeyPrefix   string
}

// ImportCSV imports data from a CSV file.
func (i *Importer) ImportCSV(ctx context.Context, path string, opts CSVOptions) (*ImportStats, error) {
	if !i.inProgress.CompareAndSwap(false, true) {
		return nil, fmt.Errorf("import already in progress")
	}
	defer i.inProgress.Store(false)

	if i.engine == nil {
		return nil, fmt.Errorf("engine not configured")
	}

	if opts.Delimiter == 0 {
		opts.Delimiter = ','
	}
	if opts.BatchSize == 0 {
		opts.BatchSize = 1000
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer func() { _ = file.Close() }()

	return i.importCSVReader(ctx, file, opts)
}

// ImportCSVReader imports from a CSV reader.
func (i *Importer) ImportCSVReader(ctx context.Context, r io.Reader, opts CSVOptions) (*ImportStats, error) {
	if !i.inProgress.CompareAndSwap(false, true) {
		return nil, fmt.Errorf("import already in progress")
	}
	defer i.inProgress.Store(false)

	if i.engine == nil {
		return nil, fmt.Errorf("engine not configured")
	}

	if opts.Delimiter == 0 {
		opts.Delimiter = ','
	}
	if opts.BatchSize == 0 {
		opts.BatchSize = 1000
	}

	return i.importCSVReader(ctx, r, opts)
}

func (i *Importer) importCSVReader(ctx context.Context, r io.Reader, opts CSVOptions) (*ImportStats, error) {
	start := time.Now()
	stats := &ImportStats{}

	reader := csv.NewReader(r)
	reader.Comma = opts.Delimiter
	reader.FieldsPerRecord = -1

	// Handle default value column (when both are 0, assume key=0, value=1)
	keyCol := opts.KeyColumn
	valueCol := opts.ValueColumn
	if keyCol == 0 && valueCol == 0 {
		valueCol = 1 // Default: key in column 0, value in column 1
	}

	if opts.HasHeader {
		_, err := reader.Read()
		if err != nil && err != io.EOF {
			return stats, fmt.Errorf("failed to read header: %w", err)
		}
	}

	i.logger.Info("starting CSV import")

	batchCount := 0
	for {
		select {
		case <-ctx.Done():
			stats.Duration = time.Since(start)
			return stats, ctx.Err()
		default:
		}

		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			stats.RecordsFailed++
			if opts.SkipErrors {
				continue
			}
			stats.Duration = time.Since(start)
			return stats, fmt.Errorf("CSV parse error at record %d: %w", stats.RecordsTotal, err)
		}

		stats.RecordsTotal++

		if keyCol >= len(record) || valueCol >= len(record) {
			stats.RecordsFailed++
			if opts.SkipErrors {
				continue
			}
			stats.Duration = time.Since(start)
			return stats, fmt.Errorf("invalid column index at record %d", stats.RecordsTotal)
		}

		key := opts.KeyPrefix + record[keyCol]
		value := record[valueCol]

		stats.BytesRead += int64(len(key) + len(value))

		if err := i.engine.Put([]byte(key), []byte(value)); err != nil {
			stats.RecordsFailed++
			if opts.SkipErrors {
				continue
			}
			stats.Duration = time.Since(start)
			return stats, fmt.Errorf("put failed at record %d: %w", stats.RecordsTotal, err)
		}

		stats.RecordsImported++
		stats.BytesWritten += int64(len(key) + len(value))
		batchCount++

		if batchCount >= opts.BatchSize {
			_ = i.engine.ForceFlush()
			batchCount = 0
		}
	}

	_ = i.engine.ForceFlush()

	stats.Duration = time.Since(start)
	if stats.Duration > 0 {
		stats.RecordsPerSecond = float64(stats.RecordsImported) / stats.Duration.Seconds()
	}

	i.totalImports.Add(1)
	i.logger.Info("CSV import completed",
		"records", stats.RecordsImported,
		"failed", stats.RecordsFailed,
		"duration", stats.Duration,
	)

	return stats, nil
}

// ============================================================================
// JSON Import
// ============================================================================

// JSONOptions configures JSON import.
type JSONOptions struct {
	KeyField   string
	ValueField string
	SkipErrors bool
	BatchSize  int
	KeyPrefix  string
}

// JSONRecord is a single record in JSON Lines format.
type JSONRecord struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// ImportJSON imports data from a JSON Lines file.
func (i *Importer) ImportJSON(ctx context.Context, path string, opts JSONOptions) (*ImportStats, error) {
	if !i.inProgress.CompareAndSwap(false, true) {
		return nil, fmt.Errorf("import already in progress")
	}
	defer i.inProgress.Store(false)

	if i.engine == nil {
		return nil, fmt.Errorf("engine not configured")
	}

	if opts.KeyField == "" {
		opts.KeyField = "key"
	}
	if opts.ValueField == "" {
		opts.ValueField = "value"
	}
	if opts.BatchSize == 0 {
		opts.BatchSize = 1000
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer func() { _ = file.Close() }()

	return i.importJSONReader(ctx, file, opts)
}

// ImportJSONReader imports from a JSON Lines reader.
func (i *Importer) ImportJSONReader(ctx context.Context, r io.Reader, opts JSONOptions) (*ImportStats, error) {
	if !i.inProgress.CompareAndSwap(false, true) {
		return nil, fmt.Errorf("import already in progress")
	}
	defer i.inProgress.Store(false)

	if i.engine == nil {
		return nil, fmt.Errorf("engine not configured")
	}

	if opts.KeyField == "" {
		opts.KeyField = "key"
	}
	if opts.ValueField == "" {
		opts.ValueField = "value"
	}
	if opts.BatchSize == 0 {
		opts.BatchSize = 1000
	}

	return i.importJSONReader(ctx, r, opts)
}

func (i *Importer) importJSONReader(ctx context.Context, r io.Reader, opts JSONOptions) (*ImportStats, error) {
	start := time.Now()
	stats := &ImportStats{}

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 1024*1024), 10*1024*1024)

	i.logger.Info("starting JSON import")

	batchCount := 0
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			stats.Duration = time.Since(start)
			return stats, ctx.Err()
		default:
		}

		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		stats.RecordsTotal++
		stats.BytesRead += int64(len(line))

		var record map[string]interface{}
		if err := json.Unmarshal(line, &record); err != nil {
			stats.RecordsFailed++
			if opts.SkipErrors {
				continue
			}
			stats.Duration = time.Since(start)
			return stats, fmt.Errorf("JSON parse error at line %d: %w", stats.RecordsTotal, err)
		}

		keyVal, ok := record[opts.KeyField]
		if !ok {
			stats.RecordsFailed++
			if opts.SkipErrors {
				continue
			}
			stats.Duration = time.Since(start)
			return stats, fmt.Errorf("missing key field '%s' at line %d", opts.KeyField, stats.RecordsTotal)
		}

		valueVal, ok := record[opts.ValueField]
		if !ok {
			stats.RecordsFailed++
			if opts.SkipErrors {
				continue
			}
			stats.Duration = time.Since(start)
			return stats, fmt.Errorf("missing value field '%s' at line %d", opts.ValueField, stats.RecordsTotal)
		}

		key := opts.KeyPrefix + fmt.Sprintf("%v", keyVal)
		value := fmt.Sprintf("%v", valueVal)

		if err := i.engine.Put([]byte(key), []byte(value)); err != nil {
			stats.RecordsFailed++
			if opts.SkipErrors {
				continue
			}
			stats.Duration = time.Since(start)
			return stats, fmt.Errorf("put failed at line %d: %w", stats.RecordsTotal, err)
		}

		stats.RecordsImported++
		stats.BytesWritten += int64(len(key) + len(value))
		batchCount++

		if batchCount >= opts.BatchSize {
			_ = i.engine.ForceFlush()
			batchCount = 0
		}
	}

	if err := scanner.Err(); err != nil {
		stats.Duration = time.Since(start)
		return stats, fmt.Errorf("scanner error: %w", err)
	}

	_ = i.engine.ForceFlush()

	stats.Duration = time.Since(start)
	if stats.Duration > 0 {
		stats.RecordsPerSecond = float64(stats.RecordsImported) / stats.Duration.Seconds()
	}

	i.totalImports.Add(1)
	i.logger.Info("JSON import completed",
		"records", stats.RecordsImported,
		"failed", stats.RecordsFailed,
		"duration", stats.Duration,
	)

	return stats, nil
}

// ============================================================================
// CSV Export
// ============================================================================

// ExportOptions configures export operations.
type ExportOptions struct {
	StartKey      string
	EndKey        string
	KeyPrefix     string
	Limit         int64
	IncludeHeader bool
	Delimiter     rune
}

// ExportCSV exports data to a CSV file.
func (i *Importer) ExportCSV(ctx context.Context, path string, opts ExportOptions) (*ExportStats, error) {
	if !i.inProgress.CompareAndSwap(false, true) {
		return nil, fmt.Errorf("export already in progress")
	}
	defer i.inProgress.Store(false)

	if i.engine == nil {
		return nil, fmt.Errorf("engine not configured")
	}

	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("failed to create file: %w", err)
	}
	defer func() { _ = file.Close() }()

	return i.exportCSVWriter(ctx, file, opts)
}

// ExportCSVWriter exports to a CSV writer.
func (i *Importer) ExportCSVWriter(ctx context.Context, w io.Writer, opts ExportOptions) (*ExportStats, error) {
	if !i.inProgress.CompareAndSwap(false, true) {
		return nil, fmt.Errorf("export already in progress")
	}
	defer i.inProgress.Store(false)

	if i.engine == nil {
		return nil, fmt.Errorf("engine not configured")
	}

	return i.exportCSVWriter(ctx, w, opts)
}

func (i *Importer) exportCSVWriter(ctx context.Context, w io.Writer, opts ExportOptions) (*ExportStats, error) {
	start := time.Now()
	stats := &ExportStats{}

	if opts.Delimiter == 0 {
		opts.Delimiter = ','
	}

	writer := csv.NewWriter(w)
	writer.Comma = opts.Delimiter

	if opts.IncludeHeader {
		if err := writer.Write([]string{"key", "value"}); err != nil {
			return stats, fmt.Errorf("failed to write header: %w", err)
		}
	}

	startKey := []byte(opts.StartKey)
	if opts.KeyPrefix != "" && opts.StartKey == "" {
		startKey = []byte(opts.KeyPrefix)
	}

	var endKey []byte
	if opts.EndKey != "" {
		endKey = []byte(opts.EndKey)
	} else if opts.KeyPrefix != "" {
		endKey = prefixEnd([]byte(opts.KeyPrefix))
	}

	iter := i.engine.Scan(startKey, endKey)
	defer iter.Close()

	i.logger.Info("starting CSV export")

	for iter.Valid() {
		select {
		case <-ctx.Done():
			writer.Flush()
			stats.Duration = time.Since(start)
			return stats, ctx.Err()
		default:
		}

		stats.RecordsTotal++

		key := string(iter.Key())
		value := string(iter.Value())

		if opts.KeyPrefix != "" && len(key) >= len(opts.KeyPrefix) {
			if key[:len(opts.KeyPrefix)] != opts.KeyPrefix {
				iter.Next()
				continue
			}
		}

		if err := writer.Write([]string{key, value}); err != nil {
			stats.RecordsFailed++
			iter.Next()
			continue
		}

		stats.RecordsExported++
		stats.BytesWritten += int64(len(key) + len(value) + 3)

		if opts.Limit > 0 && stats.RecordsExported >= opts.Limit {
			break
		}

		iter.Next()
	}

	writer.Flush()

	if err := iter.Error(); err != nil {
		stats.Duration = time.Since(start)
		return stats, fmt.Errorf("iterator error: %w", err)
	}

	stats.Duration = time.Since(start)
	if stats.Duration > 0 {
		stats.RecordsPerSecond = float64(stats.RecordsExported) / stats.Duration.Seconds()
	}

	i.totalExports.Add(1)
	i.logger.Info("CSV export completed",
		"records", stats.RecordsExported,
		"duration", stats.Duration,
	)

	return stats, nil
}

// ============================================================================
// JSON Export
// ============================================================================

// ExportJSON exports data to a JSON Lines file.
func (i *Importer) ExportJSON(ctx context.Context, path string, opts ExportOptions) (*ExportStats, error) {
	if !i.inProgress.CompareAndSwap(false, true) {
		return nil, fmt.Errorf("export already in progress")
	}
	defer i.inProgress.Store(false)

	if i.engine == nil {
		return nil, fmt.Errorf("engine not configured")
	}

	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("failed to create file: %w", err)
	}
	defer func() { _ = file.Close() }()

	return i.exportJSONWriter(ctx, file, opts)
}

// ExportJSONWriter exports to a JSON Lines writer.
func (i *Importer) ExportJSONWriter(ctx context.Context, w io.Writer, opts ExportOptions) (*ExportStats, error) {
	if !i.inProgress.CompareAndSwap(false, true) {
		return nil, fmt.Errorf("export already in progress")
	}
	defer i.inProgress.Store(false)

	if i.engine == nil {
		return nil, fmt.Errorf("engine not configured")
	}

	return i.exportJSONWriter(ctx, w, opts)
}

func (i *Importer) exportJSONWriter(ctx context.Context, w io.Writer, opts ExportOptions) (*ExportStats, error) {
	start := time.Now()
	stats := &ExportStats{}

	bufWriter := bufio.NewWriter(w)

	encoder := json.NewEncoder(bufWriter)

	startKey := []byte(opts.StartKey)
	if opts.KeyPrefix != "" && opts.StartKey == "" {
		startKey = []byte(opts.KeyPrefix)
	}

	var endKey []byte
	if opts.EndKey != "" {
		endKey = []byte(opts.EndKey)
	} else if opts.KeyPrefix != "" {
		endKey = prefixEnd([]byte(opts.KeyPrefix))
	}

	iter := i.engine.Scan(startKey, endKey)
	defer iter.Close()

	i.logger.Info("starting JSON export")

	for iter.Valid() {
		select {
		case <-ctx.Done():
			_ = bufWriter.Flush()
			stats.Duration = time.Since(start)
			return stats, ctx.Err()
		default:
		}

		stats.RecordsTotal++

		key := string(iter.Key())
		value := string(iter.Value())

		if opts.KeyPrefix != "" && len(key) >= len(opts.KeyPrefix) {
			if key[:len(opts.KeyPrefix)] != opts.KeyPrefix {
				iter.Next()
				continue
			}
		}

		record := JSONRecord{Key: key, Value: value}

		if err := encoder.Encode(record); err != nil {
			stats.RecordsFailed++
			iter.Next()
			continue
		}

		stats.RecordsExported++
		stats.BytesWritten += int64(len(key) + len(value) + 20)

		if opts.Limit > 0 && stats.RecordsExported >= opts.Limit {
			break
		}

		iter.Next()
	}

	if err := bufWriter.Flush(); err != nil {
		stats.Duration = time.Since(start)
		return stats, fmt.Errorf("flush error: %w", err)
	}

	if err := iter.Error(); err != nil {
		stats.Duration = time.Since(start)
		return stats, fmt.Errorf("iterator error: %w", err)
	}

	stats.Duration = time.Since(start)
	if stats.Duration > 0 {
		stats.RecordsPerSecond = float64(stats.RecordsExported) / stats.Duration.Seconds()
	}

	i.totalExports.Add(1)
	i.logger.Info("JSON export completed",
		"records", stats.RecordsExported,
		"duration", stats.Duration,
	)

	return stats, nil
}

// ============================================================================
// Streaming Import
// ============================================================================

// StreamImporter handles streaming imports for large datasets.
type StreamImporter struct {
	engine    Engine
	logger    *logging.Logger
	batchSize int
	batch     []KeyValue
	stats     ImportStats
	mu        sync.Mutex
}

// KeyValue is a key-value pair for streaming import.
type KeyValue struct {
	Key   []byte
	Value []byte
}

// NewStreamImporter creates a new streaming importer.
func (i *Importer) NewStreamImporter(batchSize int) *StreamImporter {
	if batchSize <= 0 {
		batchSize = 1000
	}
	return &StreamImporter{
		engine:    i.engine,
		logger:    i.logger,
		batchSize: batchSize,
		batch:     make([]KeyValue, 0, batchSize),
	}
}

// Write writes a key-value pair to the stream.
func (s *StreamImporter) Write(key, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.batch = append(s.batch, KeyValue{Key: key, Value: value})
	s.stats.RecordsTotal++

	if len(s.batch) >= s.batchSize {
		return s.flushLocked()
	}

	return nil
}

// Flush flushes any pending writes.
func (s *StreamImporter) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushLocked()
}

func (s *StreamImporter) flushLocked() error { //nolint:unparam // error return kept for API consistency
	if len(s.batch) == 0 {
		return nil
	}

	for _, kv := range s.batch {
		if err := s.engine.Put(kv.Key, kv.Value); err != nil {
			s.stats.RecordsFailed++
			continue
		}
		s.stats.RecordsImported++
		s.stats.BytesWritten += int64(len(kv.Key) + len(kv.Value))
	}

	_ = s.engine.ForceFlush()
	s.batch = s.batch[:0]
	return nil
}

// Stats returns import statistics.
func (s *StreamImporter) Stats() ImportStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}

// ============================================================================
// Stats
// ============================================================================

// Stats returns overall importer statistics.
func (i *Importer) Stats() ImporterStats {
	return ImporterStats{
		TotalImports: i.totalImports.Load(),
		TotalExports: i.totalExports.Load(),
		InProgress:   i.inProgress.Load(),
	}
}

// ImporterStats contains overall statistics.
type ImporterStats struct {
	TotalImports int64 `json:"total_imports"`
	TotalExports int64 `json:"total_exports"`
	InProgress   bool  `json:"in_progress"`
}

// ============================================================================
// Helpers
// ============================================================================

func prefixEnd(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}

	end := make([]byte, len(prefix))
	copy(end, prefix)

	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return end[:i+1]
		}
	}

	return nil
}
