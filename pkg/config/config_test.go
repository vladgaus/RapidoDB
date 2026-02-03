package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	// Verify key defaults
	if cfg.DataDir == "" {
		t.Error("DataDir should not be empty")
	}

	if cfg.MemTable.MaxSize != 64*1024*1024 {
		t.Errorf("MemTable.MaxSize should be 64MB, got %d", cfg.MemTable.MaxSize)
	}

	if cfg.WAL.Enabled != true {
		t.Error("WAL should be enabled by default")
	}

	if cfg.Compaction.Strategy != LeveledCompaction {
		t.Errorf("Default compaction strategy should be leveled, got %s", cfg.Compaction.Strategy)
	}

	if cfg.BloomFilter.BitsPerKey != 10 {
		t.Errorf("BloomFilter.BitsPerKey should be 10, got %d", cfg.BloomFilter.BitsPerKey)
	}

	if cfg.Server.Port != 11211 {
		t.Errorf("Server.Port should be 11211, got %d", cfg.Server.Port)
	}
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(*Config)
		wantErr bool
	}{
		{
			name:    "valid default config",
			modify:  func(c *Config) {},
			wantErr: false,
		},
		{
			name:    "empty data dir",
			modify:  func(c *Config) { c.DataDir = "" },
			wantErr: true,
		},
		{
			name:    "zero memtable size",
			modify:  func(c *Config) { c.MemTable.MaxSize = 0 },
			wantErr: true,
		},
		{
			name:    "negative memtable size",
			modify:  func(c *Config) { c.MemTable.MaxSize = -1 },
			wantErr: true,
		},
		{
			name:    "zero block size",
			modify:  func(c *Config) { c.SSTable.BlockSize = 0 },
			wantErr: true,
		},
		{
			name:    "bloom filter enabled with zero bits",
			modify:  func(c *Config) { c.BloomFilter.BitsPerKey = 0 },
			wantErr: true,
		},
		{
			name: "bloom filter disabled with zero bits",
			modify: func(c *Config) {
				c.BloomFilter.Enabled = false
				c.BloomFilter.BitsPerKey = 0
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tt.modify(cfg)

			err := cfg.Validate()
			if tt.wantErr && err == nil {
				t.Error("expected validation error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected validation error: %v", err)
			}
		})
	}
}

func TestConfigLoadSave(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "rapidodb-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	configPath := filepath.Join(tmpDir, "config.yaml")

	// Create custom config
	original := DefaultConfig()
	original.DataDir = "/custom/data/dir"
	original.MemTable.MaxSize = 128 * 1024 * 1024
	original.Compaction.Strategy = TieredCompaction
	original.WAL.SyncInterval = 5 * time.Second
	original.Server.Port = 12345

	// Save config
	if err := original.SaveToFile(configPath); err != nil {
		t.Fatalf("failed to save config: %v", err)
	}

	// Load config
	loaded, err := LoadFromFile(configPath)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	// Verify loaded values
	if loaded.DataDir != original.DataDir {
		t.Errorf("DataDir mismatch: expected %s, got %s", original.DataDir, loaded.DataDir)
	}

	if loaded.MemTable.MaxSize != original.MemTable.MaxSize {
		t.Errorf("MemTable.MaxSize mismatch: expected %d, got %d", original.MemTable.MaxSize, loaded.MemTable.MaxSize)
	}

	if loaded.Compaction.Strategy != original.Compaction.Strategy {
		t.Errorf("Compaction.Strategy mismatch: expected %s, got %s", original.Compaction.Strategy, loaded.Compaction.Strategy)
	}

	if loaded.Server.Port != original.Server.Port {
		t.Errorf("Server.Port mismatch: expected %d, got %d", original.Server.Port, loaded.Server.Port)
	}
}

func TestLoadFromFileNotFound(t *testing.T) {
	_, err := LoadFromFile("/nonexistent/path/config.yaml")
	if err == nil {
		t.Error("expected error loading nonexistent file")
	}
}

func TestCompactionStrategy(t *testing.T) {
	strategies := []CompactionStrategy{
		LeveledCompaction,
		TieredCompaction,
		FIFOCompaction,
	}

	for _, s := range strategies {
		if s == "" {
			t.Error("CompactionStrategy should not be empty")
		}
	}
}
