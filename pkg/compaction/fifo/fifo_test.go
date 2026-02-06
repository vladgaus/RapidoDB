package fifo

import (
	"testing"

	"github.com/rapidodb/rapidodb/pkg/compaction"
)

func TestNewStrategy(t *testing.T) {
	s := New(DefaultConfig())
	if s.Name() != compaction.StrategyFIFO {
		t.Errorf("expected strategy name %q, got %q", compaction.StrategyFIFO, s.Name())
	}
}

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if config.MaxTableFilesSize != 1024*1024*1024 {
		t.Errorf("expected MaxTableFilesSize 1GB, got %d", config.MaxTableFilesSize)
	}
	if config.TTLSeconds != 0 {
		t.Errorf("expected TTLSeconds 0, got %d", config.TTLSeconds)
	}
	if config.MaxFilesToDeletePerCycle != 10 {
		t.Errorf("expected MaxFilesToDeletePerCycle 10, got %d", config.MaxFilesToDeletePerCycle)
	}
	if config.L0StopWritesTrigger != 100 {
		t.Errorf("expected L0StopWritesTrigger 100, got %d", config.L0StopWritesTrigger)
	}
}

func TestNewStrategyWithZeroConfig(t *testing.T) {
	s := New(Config{})
	config := s.Config()

	// Should use defaults
	if config.MaxTableFilesSize != 1024*1024*1024 {
		t.Errorf("expected MaxTableFilesSize 1GB, got %d", config.MaxTableFilesSize)
	}
	if config.MaxFilesToDeletePerCycle != 10 {
		t.Errorf("expected MaxFilesToDeletePerCycle 10, got %d", config.MaxFilesToDeletePerCycle)
	}
	if config.L0StopWritesTrigger != 100 {
		t.Errorf("expected L0StopWritesTrigger 100, got %d", config.L0StopWritesTrigger)
	}
}

func TestConfigRetrieval(t *testing.T) {
	config := Config{
		MaxTableFilesSize:        512 * 1024 * 1024, // 512MB
		TTLSeconds:               3600,               // 1 hour
		MaxFilesToDeletePerCycle: 5,
		L0StopWritesTrigger:      50,
	}

	s := New(config)
	got := s.Config()

	if got.MaxTableFilesSize != 512*1024*1024 {
		t.Errorf("MaxTableFilesSize: got %d, want 512MB", got.MaxTableFilesSize)
	}
	if got.TTLSeconds != 3600 {
		t.Errorf("TTLSeconds: got %d, want 3600", got.TTLSeconds)
	}
	if got.MaxFilesToDeletePerCycle != 5 {
		t.Errorf("MaxFilesToDeletePerCycle: got %d, want 5", got.MaxFilesToDeletePerCycle)
	}
	if got.L0StopWritesTrigger != 50 {
		t.Errorf("L0StopWritesTrigger: got %d, want 50", got.L0StopWritesTrigger)
	}
}

func TestStrategyName(t *testing.T) {
	s := New(DefaultConfig())
	if s.Name() != compaction.StrategyFIFO {
		t.Errorf("Name() = %v, want %v", s.Name(), compaction.StrategyFIFO)
	}
}

func TestIsDeletionTask(t *testing.T) {
	tests := []struct {
		name     string
		task     *compaction.Task
		expected bool
	}{
		{
			name:     "nil task",
			task:     nil,
			expected: false,
		},
		{
			name: "deletion task",
			task: &compaction.Task{
				Level:       0,
				TargetLevel: -1, // Deletion marker
			},
			expected: true,
		},
		{
			name: "regular compaction task to L1",
			task: &compaction.Task{
				Level:       0,
				TargetLevel: 1,
			},
			expected: false,
		},
		{
			name: "regular compaction task to L0",
			task: &compaction.Task{
				Level:       0,
				TargetLevel: 0,
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsDeletionTask(tt.task)
			if got != tt.expected {
				t.Errorf("IsDeletionTask() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestTTLConfig(t *testing.T) {
	// Test with various TTL configurations
	tests := []struct {
		name       string
		ttlSeconds int64
	}{
		{"no TTL", 0},
		{"1 hour", 3600},
		{"1 day", 86400},
		{"1 week", 604800},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := Config{
				TTLSeconds: tt.ttlSeconds,
			}
			s := New(config)
			got := s.Config()

			if got.TTLSeconds != tt.ttlSeconds {
				t.Errorf("TTLSeconds: got %d, want %d", got.TTLSeconds, tt.ttlSeconds)
			}
		})
	}
}

func TestPickCompactionWithNoFiles(t *testing.T) {
	s := New(DefaultConfig())
	// Without a real LevelManager with files, PickCompaction should return nil
	// Actual behavior is tested in integration tests
	_ = s
}

func TestGetStatsEmpty(t *testing.T) {
	s := New(DefaultConfig())
	// GetStats with empty LevelManager is tested via integration tests
	_ = s
}

func TestSizeBasedDeletionConfig(t *testing.T) {
	// Test different size limits
	sizes := []int64{
		100 * 1024 * 1024,  // 100MB
		500 * 1024 * 1024,  // 500MB
		1024 * 1024 * 1024, // 1GB
		5 * 1024 * 1024 * 1024, // 5GB
	}

	for _, size := range sizes {
		s := New(Config{
			MaxTableFilesSize: size,
		})
		config := s.Config()
		if config.MaxTableFilesSize != size {
			t.Errorf("MaxTableFilesSize: got %d, want %d", config.MaxTableFilesSize, size)
		}
	}
}

func BenchmarkNewStrategy(b *testing.B) {
	config := DefaultConfig()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = New(config)
	}
}

func BenchmarkIsDeletionTask(b *testing.B) {
	task := &compaction.Task{
		Level:       0,
		TargetLevel: -1,
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = IsDeletionTask(task)
	}
}
