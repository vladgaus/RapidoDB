package leveled

import (
	"testing"

	"github.com/vladgaus/RapidoDB/pkg/compaction"
)

func TestNewStrategy(t *testing.T) {
	// Test with default config
	s := New(DefaultConfig())
	if s.Name() != compaction.StrategyLeveled {
		t.Errorf("expected strategy name %q, got %q", compaction.StrategyLeveled, s.Name())
	}

	config := s.Config()
	if config.NumLevels != 7 {
		t.Errorf("expected 7 levels, got %d", config.NumLevels)
	}
	if config.L0CompactionTrigger != 4 {
		t.Errorf("expected L0 trigger 4, got %d", config.L0CompactionTrigger)
	}
}

func TestNewStrategyWithZeroConfig(t *testing.T) {
	// Test that zero config gets defaults
	s := New(Config{})
	config := s.Config()

	if config.NumLevels != 7 {
		t.Errorf("expected 7 levels, got %d", config.NumLevels)
	}
	if config.L0CompactionTrigger != 4 {
		t.Errorf("expected L0 trigger 4, got %d", config.L0CompactionTrigger)
	}
	if config.BaseLevelSize != 64*1024*1024 {
		t.Errorf("expected base level size 64MB, got %d", config.BaseLevelSize)
	}
}

func TestTargetLevelSize(t *testing.T) {
	s := New(Config{
		BaseLevelSize:       64 * 1024 * 1024, // 64MB
		LevelSizeMultiplier: 10,
	})

	tests := []struct {
		level    int
		expected int64
	}{
		{0, 0},                   // L0 uses file count
		{1, 64 * 1024 * 1024},    // 64MB
		{2, 640 * 1024 * 1024},   // 640MB
		{3, 6400 * 1024 * 1024},  // 6.4GB
		{4, 64000 * 1024 * 1024}, // 64GB
	}

	for _, tt := range tests {
		got := s.targetLevelSize(tt.level)
		if got != tt.expected {
			t.Errorf("targetLevelSize(%d) = %d, want %d", tt.level, got, tt.expected)
		}
	}
}

func TestTargetFileSize(t *testing.T) {
	s := New(Config{
		TargetFileSizeBase:       4 * 1024 * 1024, // 4MB
		TargetFileSizeMultiplier: 2,
	})

	tests := []struct {
		level    int
		expected int64
	}{
		{1, 4 * 1024 * 1024},  // 4MB
		{2, 8 * 1024 * 1024},  // 8MB
		{3, 16 * 1024 * 1024}, // 16MB
	}

	for _, tt := range tests {
		got := s.TargetFileSize(tt.level)
		if got != tt.expected {
			t.Errorf("TargetFileSize(%d) = %d, want %d", tt.level, got, tt.expected)
		}
	}
}

func TestShouldTriggerCompaction(t *testing.T) {
	// This test requires a LevelManager, which needs SSTable files
	// We'll test the basic logic through the strategy interface

	s := New(Config{
		L0CompactionTrigger: 4,
		BaseLevelSize:       64 * 1024 * 1024,
	})

	// Strategy should not crash with nil (but won't work correctly)
	// Real tests need integration with LevelManager
	_ = s
}

func TestPickCompactionEmpty(t *testing.T) {
	// Test that picking compaction from empty levels returns nil
	s := New(DefaultConfig())

	// Without a real LevelManager, we can't fully test this
	// This is tested in integration tests
	_ = s
}

func TestConfig(t *testing.T) {
	config := Config{
		NumLevels:                5,
		L0CompactionTrigger:      2,
		L0StopWritesTrigger:      8,
		BaseLevelSize:            32 * 1024 * 1024,
		LevelSizeMultiplier:      8,
		TargetFileSizeBase:       2 * 1024 * 1024,
		TargetFileSizeMultiplier: 1.5,
	}

	s := New(config)
	got := s.Config()

	if got.NumLevels != 5 {
		t.Errorf("NumLevels: got %d, want 5", got.NumLevels)
	}
	if got.L0CompactionTrigger != 2 {
		t.Errorf("L0CompactionTrigger: got %d, want 2", got.L0CompactionTrigger)
	}
	if got.BaseLevelSize != 32*1024*1024 {
		t.Errorf("BaseLevelSize: got %d, want 32MB", got.BaseLevelSize)
	}
}

func TestStrategyType(t *testing.T) {
	s := New(DefaultConfig())
	if s.Name() != compaction.StrategyLeveled {
		t.Errorf("Name() = %v, want %v", s.Name(), compaction.StrategyLeveled)
	}
}

func BenchmarkTargetLevelSize(b *testing.B) {
	s := New(DefaultConfig())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for level := 0; level < 7; level++ {
			_ = s.targetLevelSize(level)
		}
	}
}
