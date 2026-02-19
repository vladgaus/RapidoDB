package importer

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/vladgaus/RapidoDB/pkg/logging"
)

// ============================================================================
// SSTable Import
// ============================================================================

// SSTableImporter handles direct SSTable file imports.
type SSTableImporter struct {
	dataDir string
	logger  *logging.Logger
}

// SSTableImportOptions configures SSTable import.
type SSTableImportOptions struct {
	DataDir string
	Logger  *logging.Logger
}

// NewSSTableImporter creates a new SSTable importer.
func NewSSTableImporter(opts SSTableImportOptions) *SSTableImporter {
	logger := opts.Logger
	if logger == nil {
		logger = logging.Default()
	}

	return &SSTableImporter{
		dataDir: opts.DataDir,
		logger:  logger.WithComponent("sstable-import"),
	}
}

// SSTableImportStats contains SSTable import statistics.
type SSTableImportStats struct {
	FilesImported int64         `json:"files_imported"`
	FilesFailed   int64         `json:"files_failed"`
	TotalSize     int64         `json:"total_size"`
	Duration      time.Duration `json:"duration"`
}

// ImportSSTable imports an SSTable file directly into the database.
func (s *SSTableImporter) ImportSSTable(ctx context.Context, sstPath string) (*SSTableImportStats, error) {
	start := time.Now()
	stats := &SSTableImportStats{}

	info, err := os.Stat(sstPath)
	if err != nil {
		return stats, fmt.Errorf("file not found: %w", err)
	}

	if filepath.Ext(sstPath) != ".sst" {
		return stats, fmt.Errorf("file must have .sst extension")
	}

	sstDir := filepath.Join(s.dataDir, "sst")
	if err := os.MkdirAll(sstDir, 0755); err != nil {
		return stats, fmt.Errorf("failed to create sst directory: %w", err)
	}

	fileNum := s.nextFileNum()
	destPath := filepath.Join(sstDir, fmt.Sprintf("%06d.sst", fileNum))

	s.logger.Info("importing SSTable",
		"source", sstPath,
		"dest", destPath,
		"size", info.Size(),
	)

	if err := copyFile(sstPath, destPath); err != nil {
		stats.FilesFailed++
		return stats, fmt.Errorf("failed to copy SSTable: %w", err)
	}

	stats.FilesImported++
	stats.TotalSize = info.Size()
	stats.Duration = time.Since(start)

	s.logger.Info("SSTable import completed",
		"file", destPath,
		"size", info.Size(),
		"duration", stats.Duration,
	)

	return stats, nil
}

// ImportSSTables imports multiple SSTable files.
func (s *SSTableImporter) ImportSSTables(ctx context.Context, paths []string) (*SSTableImportStats, error) {
	start := time.Now()
	stats := &SSTableImportStats{}

	for _, path := range paths {
		select {
		case <-ctx.Done():
			stats.Duration = time.Since(start)
			return stats, ctx.Err()
		default:
		}

		fileStats, err := s.ImportSSTable(ctx, path)
		if err != nil {
			stats.FilesFailed++
			s.logger.Error("failed to import SSTable", "path", path, "error", err)
			continue
		}

		stats.FilesImported += fileStats.FilesImported
		stats.TotalSize += fileStats.TotalSize
	}

	stats.Duration = time.Since(start)
	return stats, nil
}

// ImportDirectory imports all SSTable files from a directory.
func (s *SSTableImporter) ImportDirectory(ctx context.Context, dir string) (*SSTableImportStats, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory: %w", err)
	}

	var paths []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if filepath.Ext(entry.Name()) == ".sst" {
			paths = append(paths, filepath.Join(dir, entry.Name()))
		}
	}

	sort.Strings(paths)

	return s.ImportSSTables(ctx, paths)
}

func (s *SSTableImporter) nextFileNum() uint64 {
	sstDir := filepath.Join(s.dataDir, "sst")
	entries, err := os.ReadDir(sstDir)
	if err != nil {
		return 1
	}

	var maxNum uint64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if filepath.Ext(name) != ".sst" {
			continue
		}
		var num uint64
		_, _ = fmt.Sscanf(name, "%d.sst", &num)
		if num > maxNum {
			maxNum = num
		}
	}

	return maxNum + 1
}

func copyFile(src, dst string) (err error) {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = srcFile.Close() }()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = os.Remove(dst)
		}
	}()

	_, err = io.Copy(dstFile, srcFile)
	if err != nil {
		_ = dstFile.Close()
		return err
	}

	if err = dstFile.Sync(); err != nil {
		_ = dstFile.Close()
		return err
	}

	return dstFile.Close()
}

// ============================================================================
// SSTable Builder
// ============================================================================

// SSTableBuilder builds SSTable files from sorted key-value pairs.
type SSTableBuilder struct {
	file       *os.File
	writer     *bufWriter
	blockSize  int
	indexBlock []indexEntry
	dataOffset int64
	entryCount int64
	minKey     []byte
	maxKey     []byte
	closed     bool
}

type bufWriter struct {
	w   io.Writer
	buf []byte
	n   int
}

func newBufWriter(w io.Writer, size int) *bufWriter {
	return &bufWriter{w: w, buf: make([]byte, size)}
}

func (b *bufWriter) Write(p []byte) (n int, err error) {
	if len(p) > len(b.buf)-b.n {
		if err := b.Flush(); err != nil {
			return 0, err
		}
		if len(p) > len(b.buf) {
			return b.w.Write(p)
		}
	}
	n = copy(b.buf[b.n:], p)
	b.n += n
	return n, nil
}

func (b *bufWriter) Flush() error {
	if b.n == 0 {
		return nil
	}
	_, err := b.w.Write(b.buf[:b.n])
	b.n = 0
	return err
}

type indexEntry struct {
	key    []byte
	offset int64
}

// SSTableBuilderOptions configures the SSTable builder.
type SSTableBuilderOptions struct {
	BlockSize int
}

// NewSSTableBuilder creates a new SSTable builder.
func NewSSTableBuilder(path string, opts SSTableBuilderOptions) (*SSTableBuilder, error) {
	if opts.BlockSize == 0 {
		opts.BlockSize = 4 * 1024
	}

	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	return &SSTableBuilder{
		file:      file,
		writer:    newBufWriter(file, 64*1024),
		blockSize: opts.BlockSize,
	}, nil
}

// Add adds a key-value pair. Keys must be added in sorted order.
func (b *SSTableBuilder) Add(key, value []byte) error {
	if b.closed {
		return fmt.Errorf("builder is closed")
	}

	if b.minKey == nil {
		b.minKey = append([]byte{}, key...)
	}
	b.maxKey = append(b.maxKey[:0], key...)

	entry := make([]byte, 8+len(key)+len(value))
	binary.LittleEndian.PutUint32(entry[0:4], uint32(len(key)))
	binary.LittleEndian.PutUint32(entry[4:8], uint32(len(value)))
	copy(entry[8:], key)
	copy(entry[8+len(key):], value)

	if len(b.indexBlock) == 0 || b.dataOffset-b.indexBlock[len(b.indexBlock)-1].offset >= int64(b.blockSize) {
		b.indexBlock = append(b.indexBlock, indexEntry{
			key:    append([]byte{}, key...),
			offset: b.dataOffset,
		})
	}

	if _, err := b.writer.Write(entry); err != nil {
		return err
	}

	b.dataOffset += int64(len(entry))
	b.entryCount++

	return nil
}

// Finish completes the SSTable and writes the footer.
func (b *SSTableBuilder) Finish() error {
	if b.closed {
		return fmt.Errorf("builder already closed")
	}
	b.closed = true

	if err := b.writer.Flush(); err != nil {
		return err
	}

	indexOffset := b.dataOffset

	for _, entry := range b.indexBlock {
		idx := make([]byte, 4+len(entry.key)+8)
		binary.LittleEndian.PutUint32(idx[0:4], uint32(len(entry.key)))
		copy(idx[4:], entry.key)
		binary.LittleEndian.PutUint64(idx[4+len(entry.key):], uint64(entry.offset))

		if _, err := b.file.Write(idx); err != nil {
			return err
		}
	}

	footer := make([]byte, 24)
	binary.LittleEndian.PutUint64(footer[0:8], uint64(indexOffset))
	binary.LittleEndian.PutUint64(footer[8:16], uint64(b.entryCount))
	binary.LittleEndian.PutUint64(footer[16:24], 0x5353544142454C00)

	if _, err := b.file.Write(footer); err != nil {
		return err
	}

	return b.file.Sync()
}

// Close closes the builder.
func (b *SSTableBuilder) Close() error {
	if !b.closed {
		_ = b.Finish()
	}
	return b.file.Close()
}

// Stats returns builder statistics.
func (b *SSTableBuilder) Stats() SSTableBuilderStats {
	return SSTableBuilderStats{
		EntryCount: b.entryCount,
		DataSize:   b.dataOffset,
		MinKey:     string(b.minKey),
		MaxKey:     string(b.maxKey),
	}
}

// SSTableBuilderStats contains builder statistics.
type SSTableBuilderStats struct {
	EntryCount int64  `json:"entry_count"`
	DataSize   int64  `json:"data_size"`
	MinKey     string `json:"min_key"`
	MaxKey     string `json:"max_key"`
}
