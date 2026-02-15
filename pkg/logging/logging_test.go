package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// ============================================================================
// Logger Tests
// ============================================================================

func TestNew(t *testing.T) {
	var buf bytes.Buffer
	logger := New(Options{
		Level:  LevelInfo,
		Format: FormatJSON,
		Output: &buf,
	})

	if logger == nil {
		t.Fatal("New returned nil")
	}
}

func TestLogLevels(t *testing.T) {
	var buf bytes.Buffer
	logger := New(Options{
		Level:  LevelInfo,
		Format: FormatJSON,
		Output: &buf,
	})

	// Debug should not be logged at Info level
	logger.Debug("debug message")
	if buf.Len() > 0 {
		t.Error("Debug message should not be logged at Info level")
	}

	// Info should be logged
	logger.Info("info message")
	if buf.Len() == 0 {
		t.Error("Info message should be logged")
	}
	buf.Reset()

	// Warn should be logged
	logger.Warn("warn message")
	if buf.Len() == 0 {
		t.Error("Warn message should be logged")
	}
	buf.Reset()

	// Error should be logged
	logger.Error("error message")
	if buf.Len() == 0 {
		t.Error("Error message should be logged")
	}
}

func TestSetLevel(t *testing.T) {
	var buf bytes.Buffer
	logger := New(Options{
		Level:  LevelInfo,
		Format: FormatJSON,
		Output: &buf,
	})

	// Debug not logged initially
	logger.Debug("debug1")
	if buf.Len() > 0 {
		t.Error("Debug should not be logged at Info level")
	}

	// Change to Debug level
	logger.SetLevel(LevelDebug)

	// Now Debug should be logged
	logger.Debug("debug2")
	if buf.Len() == 0 {
		t.Error("Debug should be logged after SetLevel")
	}
}

func TestGetLevel(t *testing.T) {
	logger := New(Options{Level: LevelWarn})

	if logger.GetLevel() != LevelWarn {
		t.Errorf("Expected LevelWarn, got %v", logger.GetLevel())
	}

	logger.SetLevel(LevelError)

	if logger.GetLevel() != LevelError {
		t.Errorf("Expected LevelError, got %v", logger.GetLevel())
	}
}

func TestJSONFormat(t *testing.T) {
	var buf bytes.Buffer
	logger := New(Options{
		Level:  LevelInfo,
		Format: FormatJSON,
		Output: &buf,
	})

	logger.Info("test message", "key", "value")

	var logEntry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}

	if logEntry["msg"] != "test message" {
		t.Errorf("Expected msg 'test message', got %v", logEntry["msg"])
	}

	if logEntry["key"] != "value" {
		t.Errorf("Expected key 'value', got %v", logEntry["key"])
	}

	if logEntry["level"] != "INFO" {
		t.Errorf("Expected level 'INFO', got %v", logEntry["level"])
	}
}

func TestTextFormat(t *testing.T) {
	var buf bytes.Buffer
	logger := New(Options{
		Level:  LevelInfo,
		Format: FormatText,
		Output: &buf,
	})

	logger.Info("test message", "key", "value")

	output := buf.String()
	if !strings.Contains(output, "test message") {
		t.Error("Output should contain message")
	}
	if !strings.Contains(output, "key=value") {
		t.Error("Output should contain key=value")
	}
}

func TestWith(t *testing.T) {
	var buf bytes.Buffer
	logger := New(Options{
		Level:  LevelInfo,
		Format: FormatJSON,
		Output: &buf,
	})

	childLogger := logger.With("service", "rapidodb")
	childLogger.Info("test message")

	var logEntry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}

	if logEntry["service"] != "rapidodb" {
		t.Errorf("Expected service 'rapidodb', got %v", logEntry["service"])
	}
}

func TestWithRequestID(t *testing.T) {
	var buf bytes.Buffer
	logger := New(Options{
		Level:  LevelInfo,
		Format: FormatJSON,
		Output: &buf,
	})

	reqLogger := logger.WithRequestID("req-12345")
	reqLogger.Info("request handled")

	var logEntry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}

	if logEntry["request_id"] != "req-12345" {
		t.Errorf("Expected request_id 'req-12345', got %v", logEntry["request_id"])
	}
}

func TestWithComponent(t *testing.T) {
	var buf bytes.Buffer
	logger := New(Options{
		Level:  LevelInfo,
		Format: FormatJSON,
		Output: &buf,
	})

	compLogger := logger.WithComponent("compactor")
	compLogger.Info("compaction started")

	var logEntry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}

	if logEntry["component"] != "compactor" {
		t.Errorf("Expected component 'compactor', got %v", logEntry["component"])
	}
}

func TestConcurrentLogging(t *testing.T) {
	var buf bytes.Buffer
	logger := New(Options{
		Level:  LevelInfo,
		Format: FormatJSON,
		Output: &buf,
	})

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			logger.Info("concurrent log", "n", n)
		}(i)
	}
	wg.Wait()

	// Count log lines
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 100 {
		t.Errorf("Expected 100 log lines, got %d", len(lines))
	}
}

// ============================================================================
// Level Tests
// ============================================================================

func TestLevelString(t *testing.T) {
	tests := []struct {
		level    Level
		expected string
	}{
		{LevelDebug, "DEBUG"},
		{LevelInfo, "INFO"},
		{LevelWarn, "WARN"},
		{LevelError, "ERROR"},
	}

	for _, tt := range tests {
		if got := tt.level.String(); got != tt.expected {
			t.Errorf("Level(%d).String() = %s, want %s", tt.level, got, tt.expected)
		}
	}
}

func TestParseLevel(t *testing.T) {
	tests := []struct {
		input    string
		expected Level
	}{
		{"debug", LevelDebug},
		{"DEBUG", LevelDebug},
		{"info", LevelInfo},
		{"INFO", LevelInfo},
		{"warn", LevelWarn},
		{"warning", LevelWarn},
		{"error", LevelError},
		{"ERROR", LevelError},
		{"", LevelInfo},
		{"invalid", LevelInfo},
	}

	for _, tt := range tests {
		if got := ParseLevel(tt.input); got != tt.expected {
			t.Errorf("ParseLevel(%q) = %d, want %d", tt.input, got, tt.expected)
		}
	}
}

// ============================================================================
// Format Tests
// ============================================================================

func TestFormatString(t *testing.T) {
	if FormatJSON.String() != "json" {
		t.Errorf("FormatJSON.String() = %s, want json", FormatJSON.String())
	}
	if FormatText.String() != "text" {
		t.Errorf("FormatText.String() = %s, want text", FormatText.String())
	}
}

func TestParseFormat(t *testing.T) {
	tests := []struct {
		input    string
		expected Format
	}{
		{"json", FormatJSON},
		{"JSON", FormatJSON},
		{"text", FormatText},
		{"TEXT", FormatText},
		{"", FormatJSON},
		{"invalid", FormatJSON},
	}

	for _, tt := range tests {
		if got := ParseFormat(tt.input); got != tt.expected {
			t.Errorf("ParseFormat(%q) = %d, want %d", tt.input, got, tt.expected)
		}
	}
}

// ============================================================================
// Context Tests
// ============================================================================

func TestContextWithRequestID(t *testing.T) {
	ctx := context.Background()
	ctx = ContextWithRequestID(ctx, "test-request-123")

	id := RequestIDFromContext(ctx)
	if id != "test-request-123" {
		t.Errorf("Expected request ID 'test-request-123', got %s", id)
	}
}

func TestRequestIDFromContext_Empty(t *testing.T) {
	ctx := context.Background()
	id := RequestIDFromContext(ctx)
	if id != "" {
		t.Errorf("Expected empty request ID, got %s", id)
	}
}

func TestLoggerContext(t *testing.T) {
	var buf bytes.Buffer
	logger := New(Options{
		Level:  LevelInfo,
		Format: FormatJSON,
		Output: &buf,
	})

	ctx := context.Background()
	ctx = logger.WithContext(ctx)

	retrieved := FromContext(ctx)
	if retrieved == nil {
		t.Fatal("FromContext returned nil")
	}

	retrieved.Info("from context")
	if buf.Len() == 0 {
		t.Error("Logger from context should work")
	}
}

// ============================================================================
// Request ID Generator Tests
// ============================================================================

func TestRequestIDGenerator(t *testing.T) {
	gen := NewRequestIDGenerator()

	ids := make(map[string]bool)
	for i := 0; i < 1000; i++ {
		id := gen.Generate()
		if ids[id] {
			t.Errorf("Duplicate ID generated: %s", id)
		}
		ids[id] = true
	}
}

func TestRequestIDGeneratorShort(t *testing.T) {
	gen := NewRequestIDGenerator()

	id := gen.GenerateShort()
	if len(id) < 8 {
		t.Errorf("Short ID too short: %s", id)
	}
}

func TestGenerateRequestID(t *testing.T) {
	id1 := GenerateRequestID()
	id2 := GenerateRequestID()

	if id1 == id2 {
		t.Error("Generated IDs should be unique")
	}
}

// ============================================================================
// Request Logger Tests
// ============================================================================

func TestRequestLogger(t *testing.T) {
	var buf bytes.Buffer
	base := New(Options{
		Level:  LevelDebug,
		Format: FormatJSON,
		Output: &buf,
	})

	reqLogger := NewRequestLogger(base, "req-001")

	if reqLogger.RequestID() != "req-001" {
		t.Errorf("Expected request ID 'req-001', got %s", reqLogger.RequestID())
	}

	reqLogger.Start("GET")
	reqLogger.Success("GET", "status", 200)

	if buf.Len() == 0 {
		t.Error("Request logger should produce output")
	}
}

func TestRequestLoggerDuration(t *testing.T) {
	var buf bytes.Buffer
	base := New(Options{
		Level:  LevelDebug,
		Format: FormatJSON,
		Output: &buf,
	})

	reqLogger := NewRequestLogger(base, "req-001")
	time.Sleep(10 * time.Millisecond)

	duration := reqLogger.Duration()
	if duration < 10*time.Millisecond {
		t.Errorf("Duration should be at least 10ms, got %v", duration)
	}
}

// ============================================================================
// Rotating File Tests
// ============================================================================

func TestRotatingFile(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.log")

	rf, err := NewRotatingFile(RotatingFileOptions{
		Path:       logPath,
		MaxSize:    1, // 1MB
		MaxBackups: 3,
	})
	if err != nil {
		t.Fatalf("Failed to create rotating file: %v", err)
	}
	defer rf.Close()

	// Write some data
	data := []byte("test log entry\n")
	n, err := rf.Write(data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(data) {
		t.Errorf("Expected to write %d bytes, wrote %d", len(data), n)
	}

	// Verify file exists
	if _, err := os.Stat(logPath); os.IsNotExist(err) {
		t.Error("Log file should exist")
	}
}

func TestRotatingFileRotation(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.log")

	rf, err := NewRotatingFile(RotatingFileOptions{
		Path:       logPath,
		MaxSize:    1, // 1MB - we'll force rotation
		MaxBackups: 3,
	})
	if err != nil {
		t.Fatalf("Failed to create rotating file: %v", err)
	}
	defer rf.Close()

	// Write some data
	rf.Write([]byte("initial data\n"))

	// Force rotation
	if err := rf.Rotate(); err != nil {
		t.Fatalf("Rotation failed: %v", err)
	}

	// Write more data
	rf.Write([]byte("after rotation\n"))

	// Check that rotated file exists
	matches, err := filepath.Glob(filepath.Join(tmpDir, "test.*.log"))
	if err != nil {
		t.Fatalf("Glob failed: %v", err)
	}
	if len(matches) < 1 {
		t.Error("Should have at least one rotated file")
	}
}

// ============================================================================
// MultiWriter Tests
// ============================================================================

func TestMultiWriter(t *testing.T) {
	var buf1, buf2 bytes.Buffer

	mw := NewMultiWriter(&buf1, &buf2)

	data := []byte("test data")
	n, err := mw.Write(data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(data) {
		t.Errorf("Expected %d bytes, wrote %d", len(data), n)
	}

	if buf1.String() != "test data" {
		t.Error("buf1 should contain 'test data'")
	}
	if buf2.String() != "test data" {
		t.Error("buf2 should contain 'test data'")
	}
}

// ============================================================================
// Utility Function Tests
// ============================================================================

func TestErr(t *testing.T) {
	attr := Err(os.ErrNotExist)
	if attr.Key != "error" {
		t.Errorf("Expected key 'error', got %s", attr.Key)
	}
}

func TestDuration(t *testing.T) {
	attr := Duration("latency", 100*time.Millisecond)
	if attr.Key != "latency_ms" {
		t.Errorf("Expected key 'latency_ms', got %s", attr.Key)
	}
	if attr.Value.Float64() != 100 {
		t.Errorf("Expected 100ms, got %f", attr.Value.Float64())
	}
}

func TestCaller(t *testing.T) {
	attr := Caller(0)
	if attr.Key != "caller" {
		t.Errorf("Expected key 'caller', got %s", attr.Key)
	}
	val := attr.Value.String()
	if !strings.Contains(val, "logging_test.go") {
		t.Errorf("Expected caller to contain 'logging_test.go', got %s", val)
	}
}

// ============================================================================
// Default Logger Tests
// ============================================================================

func TestDefault(t *testing.T) {
	logger := Default()
	if logger == nil {
		t.Fatal("Default() returned nil")
	}

	// Should return same instance
	logger2 := Default()
	if logger != logger2 {
		t.Error("Default() should return same instance")
	}
}

func TestSetDefault(t *testing.T) {
	var buf bytes.Buffer
	customLogger := New(Options{
		Level:  LevelDebug,
		Format: FormatJSON,
		Output: &buf,
	})

	SetDefault(customLogger)

	// Package-level functions should use custom logger
	Info("test from default")

	if buf.Len() == 0 {
		t.Error("Custom default logger should be used")
	}
}

// ============================================================================
// Benchmark Tests
// ============================================================================

func BenchmarkLoggerInfo(b *testing.B) {
	var buf bytes.Buffer
	logger := New(Options{
		Level:  LevelInfo,
		Format: FormatJSON,
		Output: &buf,
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logger.Info("benchmark message", "iteration", i)
	}
}

func BenchmarkLoggerWithFields(b *testing.B) {
	var buf bytes.Buffer
	logger := New(Options{
		Level:  LevelInfo,
		Format: FormatJSON,
		Output: &buf,
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logger.Info("benchmark",
			"field1", "value1",
			"field2", 123,
			"field3", true,
			"field4", 3.14,
		)
	}
}

func BenchmarkRequestIDGenerate(b *testing.B) {
	gen := NewRequestIDGenerator()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		gen.Generate()
	}
}
