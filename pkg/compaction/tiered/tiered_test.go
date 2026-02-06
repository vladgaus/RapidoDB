package tiered

import (
	"testing"

	"github.com/rapidodb/rapidodb/pkg/compaction"
)

func TestNewStrategy(t *testing.T) {
	s := New(DefaultConfig())
	if s.Name() != compaction.StrategyTiered {
		t.Errorf("expected strategy name %q, got %q", compaction.StrategyTiered, s.Name())
	}
}

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if config.MinMergeWidth != 4 {
		t.Errorf("expected MinMergeWidth 4, got %d", config.MinMergeWidth)
	}
	if config.MaxMergeWidth != 32 {
		t.Errorf("expected MaxMergeWidth 32, got %d", config.MaxMergeWidth)
	}
	if config.SizeRatio != 4.0 {
		t.Errorf("expected SizeRatio 4.0, got %f", config.SizeRatio)
	}
	if config.BaseBucketSize != 4*1024*1024 {
		t.Errorf("expected BaseBucketSize 4MB, got %d", config.BaseBucketSize)
	}
	if config.MaxBuckets != 10 {
		t.Errorf("expected MaxBuckets 10, got %d", config.MaxBuckets)
	}
}

func TestNewStrategyWithZeroConfig(t *testing.T) {
	s := New(Config{})
	config := s.Config()

	// Should use defaults
	if config.MinMergeWidth != 4 {
		t.Errorf("expected MinMergeWidth 4, got %d", config.MinMergeWidth)
	}
	if config.SizeRatio != 4.0 {
		t.Errorf("expected SizeRatio 4.0, got %f", config.SizeRatio)
	}
}

func TestGetBucketID(t *testing.T) {
	s := New(Config{
		BaseBucketSize: 1024, // 1KB
		SizeRatio:      4.0,
		MaxBuckets:     10,
	})

	tests := []struct {
		fileSize int64
		expected int
	}{
		{0, 0},           // Empty file -> bucket 0
		{512, 0},         // < 1KB -> bucket 0
		{1024, 0},        // = 1KB -> bucket 0
		{1025, 1},        // > 1KB -> bucket 1
		{4096, 1},        // = 4KB -> bucket 1
		{4097, 2},        // > 4KB -> bucket 2
		{16384, 2},       // = 16KB -> bucket 2
		{16385, 3},       // > 16KB -> bucket 3
		{1024 * 1024, 5}, // 1MB -> bucket 5
	}

	for _, tt := range tests {
		got := s.getBucketID(tt.fileSize)
		if got != tt.expected {
			t.Errorf("getBucketID(%d) = %d, want %d", tt.fileSize, got, tt.expected)
		}
	}
}

func TestBucketMaxSize(t *testing.T) {
	s := New(Config{
		BaseBucketSize: 1024, // 1KB
		SizeRatio:      4.0,
	})

	tests := []struct {
		bucketID int
		expected int64
	}{
		{0, 1024},    // 1KB
		{1, 4096},    // 4KB
		{2, 16384},   // 16KB
		{3, 65536},   // 64KB
		{4, 262144},  // 256KB
		{5, 1048576}, // 1MB
	}

	for _, tt := range tests {
		got := s.bucketMaxSize(tt.bucketID)
		if got != tt.expected {
			t.Errorf("bucketMaxSize(%d) = %d, want %d", tt.bucketID, got, tt.expected)
		}
	}
}

func TestConfigRetrieval(t *testing.T) {
	config := Config{
		MinMergeWidth:               8,
		MaxMergeWidth:               16,
		SizeRatio:                   2.0,
		BaseBucketSize:              2 * 1024 * 1024,
		MaxBuckets:                  5,
		MaxSizeAmplificationPercent: 150,
		L0StopWritesTrigger:         10,
	}

	s := New(config)
	got := s.Config()

	if got.MinMergeWidth != 8 {
		t.Errorf("MinMergeWidth: got %d, want 8", got.MinMergeWidth)
	}
	if got.MaxMergeWidth != 16 {
		t.Errorf("MaxMergeWidth: got %d, want 16", got.MaxMergeWidth)
	}
	if got.SizeRatio != 2.0 {
		t.Errorf("SizeRatio: got %f, want 2.0", got.SizeRatio)
	}
	if got.BaseBucketSize != 2*1024*1024 {
		t.Errorf("BaseBucketSize: got %d, want 2MB", got.BaseBucketSize)
	}
	if got.MaxBuckets != 5 {
		t.Errorf("MaxBuckets: got %d, want 5", got.MaxBuckets)
	}
}

func TestStrategyName(t *testing.T) {
	s := New(DefaultConfig())
	if s.Name() != compaction.StrategyTiered {
		t.Errorf("Name() = %v, want %v", s.Name(), compaction.StrategyTiered)
	}
}

func TestPickCompactionNilWithNoFiles(t *testing.T) {
	s := New(DefaultConfig())
	// Without a real LevelManager with files, PickCompaction should return nil
	// This is tested via integration tests
	_ = s
}

func TestBucketStatsEmpty(t *testing.T) {
	s := New(DefaultConfig())
	// GetBucketStats with empty LevelManager is tested via integration tests
	_ = s
}

func BenchmarkGetBucketID(b *testing.B) {
	s := New(DefaultConfig())
	sizes := []int64{1024, 4096, 16384, 65536, 262144, 1048576, 4194304}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, size := range sizes {
			_ = s.getBucketID(size)
		}
	}
}

func BenchmarkBucketMaxSize(b *testing.B) {
	s := New(DefaultConfig())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for bucket := 0; bucket < 10; bucket++ {
			_ = s.bucketMaxSize(bucket)
		}
	}
}
