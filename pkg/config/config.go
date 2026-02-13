// Package config provides configuration management for RapidoDB.
// It supports JSON-based configuration with sensible defaults optimized
// for different workload patterns.
package config

import (
	"encoding/json"
	"os"
	"time"
)

// CompactionStrategy defines the compaction algorithm to use.
type CompactionStrategy string

const (
	// LeveledCompaction uses RocksDB-style leveled compaction.
	// Best for read-heavy workloads with good space efficiency.
	LeveledCompaction CompactionStrategy = "leveled"

	// TieredCompaction uses universal/tiered compaction.
	// Best for write-heavy workloads with lower write amplification.
	TieredCompaction CompactionStrategy = "tiered"

	// FIFOCompaction uses time-based compaction with TTL.
	// Best for time-series data or cache-like workloads.
	FIFOCompaction CompactionStrategy = "fifo"
)

// Config holds all configuration for RapidoDB engine.
type Config struct {
	// DataDir is the directory where all data files are stored.
	DataDir string `json:"data_dir"`

	// MemTable configuration
	MemTable MemTableConfig `json:"memtable"`

	// WAL configuration
	WAL WALConfig `json:"wal"`

	// SSTable configuration
	SSTable SSTableConfig `json:"sstable"`

	// Compaction configuration
	Compaction CompactionConfig `json:"compaction"`

	// BloomFilter configuration
	BloomFilter BloomFilterConfig `json:"bloom_filter"`

	// Server configuration
	Server ServerConfig `json:"server"`

	// Health check configuration
	Health HealthConfig `json:"health"`
}

// MemTableConfig holds MemTable-specific configuration.
type MemTableConfig struct {
	// MaxSize is the maximum size of a single MemTable in bytes.
	// When exceeded, the MemTable becomes immutable and is flushed.
	// Default: 64MB (like RocksDB)
	MaxSize int64 `json:"max_size"`

	// MaxMemTables is the maximum number of MemTables (active + immutable)
	// before write stalling occurs.
	// Default: 4
	MaxMemTables int `json:"max_memtables"`

	// Type specifies the MemTable implementation.
	// Currently only "skiplist" is supported.
	Type string `json:"type"`
}

// WALConfig holds Write-Ahead Log configuration.
type WALConfig struct {
	// Enabled determines if WAL is used for durability.
	// Disabling improves performance but risks data loss on crash.
	// Default: true
	Enabled bool `json:"enabled"`

	// SyncOnWrite forces fsync after each write for maximum durability.
	// Default: false (batch sync for better performance)
	SyncOnWrite bool `json:"sync_on_write"`

	// MaxSize is the maximum size of a single WAL file.
	// Default: 128MB
	MaxSize int64 `json:"max_size"`

	// SyncInterval is the interval between automatic syncs when SyncOnWrite is false.
	// Default: 1 second
	SyncInterval time.Duration `json:"sync_interval"`
}

// SSTableConfig holds SSTable format configuration.
type SSTableConfig struct {
	// BlockSize is the size of data blocks within SSTables.
	// Smaller blocks = better random read, larger = better sequential.
	// Default: 4KB
	BlockSize int `json:"block_size"`

	// SparseIndexInterval is how many keys between sparse index entries.
	// Lower = more memory, faster lookups. Higher = less memory.
	// Default: 16
	SparseIndexInterval int `json:"sparse_index_interval"`

	// Compression specifies the compression algorithm.
	// Options: "none", "snappy", "zstd"
	// Default: "none" (for educational clarity)
	Compression string `json:"compression"`

	// TargetFileSize is the target size for SSTable files.
	// Default: 64MB
	TargetFileSize int64 `json:"target_file_size"`
}

// CompactionConfig holds compaction-specific configuration.
type CompactionConfig struct {
	// Strategy determines the compaction algorithm.
	// Default: "leveled"
	Strategy CompactionStrategy `json:"strategy"`

	// Leveled compaction specific settings
	Leveled LeveledCompactionConfig `json:"leveled"`

	// Tiered compaction specific settings
	Tiered TieredCompactionConfig `json:"tiered"`

	// FIFO compaction specific settings
	FIFO FIFOCompactionConfig `json:"fifo"`

	// MaxBackgroundCompactions is the number of concurrent compaction workers.
	// Default: 4
	MaxBackgroundCompactions int `json:"max_background_compactions"`

	// CompactionRateLimitMB limits compaction I/O in MB/s. 0 = unlimited.
	// Default: 0
	CompactionRateLimitMB int `json:"compaction_rate_limit_mb"`
}

// LeveledCompactionConfig holds leveled compaction settings.
type LeveledCompactionConfig struct {
	// NumLevels is the total number of levels (excluding L0).
	// Default: 7
	NumLevels int `json:"num_levels"`

	// L0CompactionTrigger is number of L0 files that trigger compaction.
	// Default: 4
	L0CompactionTrigger int `json:"l0_compaction_trigger"`

	// L0StopWritesTrigger is number of L0 files that cause write stall.
	// Default: 12
	L0StopWritesTrigger int `json:"l0_stop_writes_trigger"`

	// BaseLevelSize is the target size for L1 in bytes.
	// Default: 256MB
	BaseLevelSize int64 `json:"base_level_size"`

	// LevelSizeMultiplier is the size ratio between adjacent levels.
	// Default: 10
	LevelSizeMultiplier int `json:"level_size_multiplier"`
}

// TieredCompactionConfig holds tiered/universal compaction settings.
type TieredCompactionConfig struct {
	// MaxTiers is the maximum number of sorted runs (tiers).
	// Default: 6
	MaxTiers int `json:"max_tiers"`

	// SizeRatio triggers compaction when adjacent tier size ratio exceeds this.
	// Default: 1 (100% - tiers should be similar size)
	SizeRatio int `json:"size_ratio"`

	// MaxSizeAmplificationPercent triggers full compaction when space amp exceeds this.
	// Default: 200 (200%)
	MaxSizeAmplificationPercent int `json:"max_size_amplification_percent"`

	// MinMergeWidth is the minimum number of tiers to compact together.
	// Default: 2
	MinMergeWidth int `json:"min_merge_width"`
}

// FIFOCompactionConfig holds FIFO compaction settings.
type FIFOCompactionConfig struct {
	// MaxTableFilesSizeBytes is the total size limit for all SST files.
	// Oldest files are deleted when exceeded.
	// Default: 1GB
	MaxTableFilesSizeBytes int64 `json:"max_table_files_size_bytes"`

	// TTL is the time-to-live for SST files.
	// Files older than TTL are deleted regardless of size.
	// Default: 0 (disabled)
	TTL time.Duration `json:"ttl"`
}

// BloomFilterConfig holds Bloom filter settings.
type BloomFilterConfig struct {
	// Enabled determines if Bloom filters are created for SSTables.
	// Default: true
	Enabled bool `json:"enabled"`

	// BitsPerKey is the number of bits per key in the filter.
	// Higher = lower false positive rate, more memory.
	// 10 bits ≈ 1% false positive rate.
	// Default: 10
	BitsPerKey int `json:"bits_per_key"`
}

// ServerConfig holds TCP server settings.
type ServerConfig struct {
	// Host is the address to bind to.
	// Default: "0.0.0.0"
	Host string `json:"host"`

	// Port is the port to listen on.
	// Default: 11211 (Memcached default)
	Port int `json:"port"`

	// MaxConnections is the maximum number of concurrent connections.
	// Default: 1000
	MaxConnections int `json:"max_connections"`

	// ReadTimeout is the timeout for read operations.
	// Default: 30 seconds
	ReadTimeout time.Duration `json:"read_timeout"`

	// WriteTimeout is the timeout for write operations.
	// Default: 30 seconds
	WriteTimeout time.Duration `json:"write_timeout"`
}

// HealthConfig holds health check server settings.
type HealthConfig struct {
	// Enabled determines if the health HTTP server is started.
	// Default: true
	Enabled bool `json:"enabled"`

	// Host is the address to bind the health server to.
	// Default: "0.0.0.0"
	Host string `json:"host"`

	// Port is the port for the health HTTP server.
	// Default: 8080
	Port int `json:"port"`

	// DiskPath is the path to monitor for disk health.
	// Default: same as DataDir
	DiskPath string `json:"disk_path"`

	// DiskWarningPercent is the disk usage percentage that triggers warning.
	// Default: 80
	DiskWarningPercent float64 `json:"disk_warning_percent"`

	// DiskCriticalPercent is the disk usage percentage that triggers critical.
	// Default: 95
	DiskCriticalPercent float64 `json:"disk_critical_percent"`

	// MemoryMaxHeapMB is the max heap size in MB before warning (0 = no limit).
	// Default: 0
	MemoryMaxHeapMB int64 `json:"memory_max_heap_mb"`
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		DataDir: "./rapidodb_data",
		MemTable: MemTableConfig{
			MaxSize:      64 * 1024 * 1024, // 64MB
			MaxMemTables: 4,
			Type:         "skiplist",
		},
		WAL: WALConfig{
			Enabled:      true,
			SyncOnWrite:  false,
			MaxSize:      128 * 1024 * 1024, // 128MB
			SyncInterval: time.Second,
		},
		SSTable: SSTableConfig{
			BlockSize:           4 * 1024, // 4KB
			SparseIndexInterval: 16,
			Compression:         "none",
			TargetFileSize:      64 * 1024 * 1024, // 64MB
		},
		Compaction: CompactionConfig{
			Strategy: LeveledCompaction,
			Leveled: LeveledCompactionConfig{
				NumLevels:           7,
				L0CompactionTrigger: 4,
				L0StopWritesTrigger: 12,
				BaseLevelSize:       256 * 1024 * 1024, // 256MB
				LevelSizeMultiplier: 10,
			},
			Tiered: TieredCompactionConfig{
				MaxTiers:                    6,
				SizeRatio:                   1,
				MaxSizeAmplificationPercent: 200,
				MinMergeWidth:               2,
			},
			FIFO: FIFOCompactionConfig{
				MaxTableFilesSizeBytes: 1024 * 1024 * 1024, // 1GB
				TTL:                    0,
			},
			MaxBackgroundCompactions: 4,
			CompactionRateLimitMB:    0,
		},
		BloomFilter: BloomFilterConfig{
			Enabled:    true,
			BitsPerKey: 10,
		},
		Server: ServerConfig{
			Host:           "0.0.0.0",
			Port:           11211,
			MaxConnections: 1000,
			ReadTimeout:    30 * time.Second,
			WriteTimeout:   30 * time.Second,
		},
		Health: HealthConfig{
			Enabled:             true,
			Host:                "0.0.0.0",
			Port:                8080,
			DiskPath:            "", // Will default to DataDir
			DiskWarningPercent:  80.0,
			DiskCriticalPercent: 95.0,
			MemoryMaxHeapMB:     0, // No limit
		},
	}
}

// LoadFromFile loads configuration from a JSON file.
// Missing fields are filled with defaults.
func LoadFromFile(path string) (*Config, error) {
	cfg := DefaultConfig()

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}

// SaveToFile saves the configuration to a JSON file.
func (c *Config) SaveToFile(path string) error {
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(path, data, 0644)
}

// Validate checks if the configuration is valid.
func (c *Config) Validate() error {
	if c.DataDir == "" {
		return ErrInvalidConfig("data_dir cannot be empty")
	}

	if c.MemTable.MaxSize <= 0 {
		return ErrInvalidConfig("memtable.max_size must be positive")
	}

	if c.MemTable.MaxMemTables <= 0 {
		return ErrInvalidConfig("memtable.max_memtables must be positive")
	}

	if c.SSTable.BlockSize <= 0 {
		return ErrInvalidConfig("sstable.block_size must be positive")
	}

	if c.BloomFilter.Enabled && c.BloomFilter.BitsPerKey <= 0 {
		return ErrInvalidConfig("bloom_filter.bits_per_key must be positive when enabled")
	}

	return nil
}

// ErrInvalidConfig represents a configuration validation error.
type ErrInvalidConfig string

func (e ErrInvalidConfig) Error() string {
	return "invalid config: " + string(e)
}
