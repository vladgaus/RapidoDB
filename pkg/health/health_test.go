package health

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// ============================================================================
// HealthChecker Tests
// ============================================================================

func TestNewHealthChecker(t *testing.T) {
	opts := DefaultOptions()
	hc := NewHealthChecker(opts)

	if hc == nil {
		t.Fatal("NewHealthChecker returned nil")
	}

	// Should start as live but not ready
	if !hc.IsLive() {
		t.Error("Expected IsLive to be true initially")
	}

	if hc.IsReady() {
		t.Error("Expected IsReady to be false initially")
	}
}

func TestHealthChecker_LiveReady(t *testing.T) {
	hc := NewHealthChecker(DefaultOptions())

	// Test SetLive
	hc.SetLive(false)
	if hc.IsLive() {
		t.Error("Expected IsLive to be false after SetLive(false)")
	}

	hc.SetLive(true)
	if !hc.IsLive() {
		t.Error("Expected IsLive to be true after SetLive(true)")
	}

	// Test SetReady
	hc.SetReady(true)
	if !hc.IsReady() {
		t.Error("Expected IsReady to be true after SetReady(true)")
	}

	hc.SetReady(false)
	if hc.IsReady() {
		t.Error("Expected IsReady to be false after SetReady(false)")
	}
}

func TestHealthChecker_Check(t *testing.T) {
	hc := NewHealthChecker(Options{
		Version:       "test/1.0.0",
		CacheDuration: 10 * time.Millisecond,
	})

	// Add a simple checker
	hc.RegisterChecker(NewMemoryChecker())

	// Set ready
	hc.SetReady(true)

	report := hc.Check()

	if report.Status != StatusHealthy {
		t.Errorf("Expected healthy status, got %s", report.Status)
	}

	if report.Version != "test/1.0.0" {
		t.Errorf("Expected version test/1.0.0, got %s", report.Version)
	}

	if len(report.Checks) != 1 {
		t.Errorf("Expected 1 check, got %d", len(report.Checks))
	}

	if report.Checks[0].Name != "memory" {
		t.Errorf("Expected memory check, got %s", report.Checks[0].Name)
	}
}

func TestHealthChecker_CheckNotReady(t *testing.T) {
	hc := NewHealthChecker(DefaultOptions())
	hc.RegisterChecker(NewMemoryChecker())

	// Not ready - should be degraded
	report := hc.Check()

	if report.Status != StatusDegraded {
		t.Errorf("Expected degraded status when not ready, got %s", report.Status)
	}
}

func TestHealthChecker_CheckNotLive(t *testing.T) {
	hc := NewHealthChecker(DefaultOptions())
	hc.SetLive(false)
	hc.SetReady(true)

	report := hc.Check()

	if report.Status != StatusUnhealthy {
		t.Errorf("Expected unhealthy status when not live, got %s", report.Status)
	}
}

func TestHealthChecker_Uptime(t *testing.T) {
	hc := NewHealthChecker(DefaultOptions())

	time.Sleep(10 * time.Millisecond)

	uptime := hc.Uptime()
	if uptime < 10*time.Millisecond {
		t.Errorf("Expected uptime >= 10ms, got %v", uptime)
	}
}

func TestHealthChecker_Cache(t *testing.T) {
	hc := NewHealthChecker(Options{
		Version:       "test/1.0.0",
		CacheDuration: 100 * time.Millisecond,
	})

	hc.SetReady(true)

	// First call
	report1 := hc.Check()
	time1 := report1.Timestamp

	// Immediate second call should be cached
	report2 := hc.Check()
	if report2.Timestamp != time1 {
		t.Error("Expected cached result")
	}

	// Wait for cache to expire
	time.Sleep(110 * time.Millisecond)

	report3 := hc.Check()
	if report3.Timestamp.Equal(time1) {
		t.Error("Expected fresh result after cache expiry")
	}
}

// ============================================================================
// Memory Checker Tests
// ============================================================================

func TestMemoryChecker(t *testing.T) {
	mc := NewMemoryChecker()

	if mc.Name() != "memory" {
		t.Errorf("Expected name 'memory', got %s", mc.Name())
	}

	result := mc.Check()

	if result.Status != StatusHealthy && result.Status != StatusDegraded {
		t.Errorf("Unexpected status: %s", result.Status)
	}

	// Check that details are populated
	if result.Details == nil {
		t.Error("Expected details to be populated")
	}

	if _, ok := result.Details["heap_alloc_bytes"]; !ok {
		t.Error("Expected heap_alloc_bytes in details")
	}

	if _, ok := result.Details["goroutines"]; !ok {
		t.Error("Expected goroutines in details")
	}
}

// ============================================================================
// Disk Checker Tests
// ============================================================================

func TestDiskChecker(t *testing.T) {
	// Check current directory (should always exist)
	dc := NewDiskChecker(".")

	if dc.Name() != "disk" {
		t.Errorf("Expected name 'disk', got %s", dc.Name())
	}

	result := dc.Check()

	// Should be healthy or degraded (unless disk is actually full)
	if result.Status == StatusUnhealthy && result.Message != "" {
		// Only fail if it's not a "Path does not exist" error
		if result.Details != nil {
			t.Logf("Disk check result: %s - %s", result.Status, result.Message)
		}
	}

	// Check details
	if result.Details != nil {
		if _, ok := result.Details["total_bytes"]; !ok {
			t.Error("Expected total_bytes in details")
		}
	}
}

func TestDiskChecker_NonExistentPath(t *testing.T) {
	dc := NewDiskChecker("/nonexistent/path/that/should/not/exist")

	result := dc.Check()

	if result.Status != StatusUnhealthy {
		t.Errorf("Expected unhealthy for non-existent path, got %s", result.Status)
	}
}

// ============================================================================
// Engine Checker Tests
// ============================================================================

type mockEngine struct {
	closed bool
}

func (m *mockEngine) IsClosed() bool {
	return m.closed
}

func TestEngineChecker(t *testing.T) {
	engine := &mockEngine{closed: false}
	ec := NewEngineChecker(engine)

	if ec.Name() != "engine" {
		t.Errorf("Expected name 'engine', got %s", ec.Name())
	}

	result := ec.Check()

	if result.Status != StatusHealthy {
		t.Errorf("Expected healthy, got %s", result.Status)
	}
}

func TestEngineChecker_Closed(t *testing.T) {
	engine := &mockEngine{closed: true}
	ec := NewEngineChecker(engine)

	result := ec.Check()

	if result.Status != StatusUnhealthy {
		t.Errorf("Expected unhealthy for closed engine, got %s", result.Status)
	}
}

func TestEngineChecker_Nil(t *testing.T) {
	ec := NewEngineChecker(nil)

	result := ec.Check()

	if result.Status != StatusUnhealthy {
		t.Errorf("Expected unhealthy for nil engine, got %s", result.Status)
	}
}

// ============================================================================
// Server Checker Tests
// ============================================================================

type mockServerStats struct {
	active int64
	total  uint64
	hits   uint64
	misses uint64
}

func (m *mockServerStats) ActiveConnections() int64 { return m.active }
func (m *mockServerStats) TotalConnections() uint64 { return m.total }
func (m *mockServerStats) GetHits() uint64          { return m.hits }
func (m *mockServerStats) GetMisses() uint64        { return m.misses }

func TestServerChecker(t *testing.T) {
	stats := &mockServerStats{
		active: 10,
		total:  100,
		hits:   1000,
		misses: 100,
	}

	sc := NewServerChecker(stats, 100)

	if sc.Name() != "server" {
		t.Errorf("Expected name 'server', got %s", sc.Name())
	}

	result := sc.Check()

	if result.Status != StatusHealthy {
		t.Errorf("Expected healthy, got %s", result.Status)
	}

	// Check hit rate in details
	if result.Details != nil {
		hitRate, ok := result.Details["hit_rate_percent"]
		if !ok {
			t.Error("Expected hit_rate_percent in details")
		}
		// 1000 hits / 1100 total = ~90.9%
		if hitRate.(float64) < 90 || hitRate.(float64) > 92 {
			t.Errorf("Expected hit rate ~90.9%%, got %.1f%%", hitRate.(float64))
		}
	}
}

func TestServerChecker_HighConnections(t *testing.T) {
	stats := &mockServerStats{
		active: 95,
		total:  100,
	}

	sc := NewServerChecker(stats, 100)
	result := sc.Check()

	if result.Status != StatusDegraded {
		t.Errorf("Expected degraded for high connections, got %s", result.Status)
	}
}

// ============================================================================
// Custom Checker Tests
// ============================================================================

func TestCustomChecker(t *testing.T) {
	checker := NewCustomChecker("custom", func() (Status, string, map[string]interface{}) {
		return StatusHealthy, "All good", map[string]interface{}{"custom_key": "custom_value"}
	})

	if checker.Name() != "custom" {
		t.Errorf("Expected name 'custom', got %s", checker.Name())
	}

	result := checker.Check()

	if result.Status != StatusHealthy {
		t.Errorf("Expected healthy, got %s", result.Status)
	}

	if result.Details["custom_key"] != "custom_value" {
		t.Error("Expected custom_key in details")
	}
}

// ============================================================================
// HTTP Server Tests
// ============================================================================

func TestHTTPServer_HealthEndpoint(t *testing.T) {
	hc := NewHealthChecker(Options{Version: "test/1.0"})
	hc.SetReady(true)
	hc.RegisterChecker(NewMemoryChecker())

	server := NewHTTPServer(hc, DefaultHTTPServerOptions())

	// Create test request
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	server.handleHealth(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d", w.Code)
	}

	// Parse as generic map since we have custom JSON marshaling
	var response map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	if response["status"] != "healthy" {
		t.Errorf("Expected status healthy, got %v", response["status"])
	}

	if response["version"] != "test/1.0" {
		t.Errorf("Expected version test/1.0, got %v", response["version"])
	}

	// Check uptime is a string
	if _, ok := response["uptime"].(string); !ok {
		t.Error("Expected uptime to be a string")
	}
}

func TestHTTPServer_LiveEndpoint(t *testing.T) {
	hc := NewHealthChecker(DefaultOptions())
	server := NewHTTPServer(hc, DefaultHTTPServerOptions())

	// Test live
	req := httptest.NewRequest(http.MethodGet, "/health/live", nil)
	w := httptest.NewRecorder()

	server.handleLive(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d", w.Code)
	}

	// Test not live
	hc.SetLive(false)
	w = httptest.NewRecorder()
	server.handleLive(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected 503, got %d", w.Code)
	}
}

func TestHTTPServer_ReadyEndpoint(t *testing.T) {
	hc := NewHealthChecker(DefaultOptions())
	server := NewHTTPServer(hc, DefaultHTTPServerOptions())

	// Test not ready (default)
	req := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	w := httptest.NewRecorder()

	server.handleReady(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected 503 when not ready, got %d", w.Code)
	}

	// Test ready
	hc.SetReady(true)
	w = httptest.NewRecorder()
	server.handleReady(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected 200 when ready, got %d", w.Code)
	}
}

func TestHTTPServer_UnhealthyStatus(t *testing.T) {
	hc := NewHealthChecker(DefaultOptions())
	hc.SetLive(false) // This makes it unhealthy

	server := NewHTTPServer(hc, DefaultHTTPServerOptions())

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	server.handleHealth(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected 503 for unhealthy, got %d", w.Code)
	}
}

func TestHTTPServer_MethodNotAllowed(t *testing.T) {
	hc := NewHealthChecker(DefaultOptions())
	server := NewHTTPServer(hc, DefaultHTTPServerOptions())

	req := httptest.NewRequest(http.MethodPost, "/health", nil)
	w := httptest.NewRecorder()

	server.handleHealth(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected 405, got %d", w.Code)
	}
}

func TestHTTPServer_HeadRequest(t *testing.T) {
	hc := NewHealthChecker(DefaultOptions())
	hc.SetReady(true)
	server := NewHTTPServer(hc, DefaultHTTPServerOptions())

	req := httptest.NewRequest(http.MethodHead, "/health", nil)
	w := httptest.NewRecorder()

	server.handleHealth(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d", w.Code)
	}

	// HEAD should have empty body
	if w.Body.Len() != 0 {
		t.Errorf("Expected empty body for HEAD request, got %d bytes", w.Body.Len())
	}
}

// ============================================================================
// JSON Marshaling Tests
// ============================================================================

func TestCheckResult_MarshalJSON(t *testing.T) {
	result := CheckResult{
		Name:      "test",
		Status:    StatusHealthy,
		Message:   "OK",
		Duration:  150 * time.Millisecond,
		Timestamp: time.Now(),
	}

	data, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	// Check duration is in milliseconds
	if parsed["duration_ms"].(float64) != 150 {
		t.Errorf("Expected duration_ms=150, got %v", parsed["duration_ms"])
	}
}

func TestHealthReport_MarshalJSON(t *testing.T) {
	report := HealthReport{
		Status:    StatusHealthy,
		Uptime:    time.Hour + 30*time.Minute,
		Timestamp: time.Now(),
		Version:   "test/1.0",
	}

	data, err := json.Marshal(report)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	// Check uptime is a human-readable string
	uptime, ok := parsed["uptime"].(string)
	if !ok {
		t.Fatal("uptime should be a string")
	}

	if uptime != "1h30m0s" {
		t.Errorf("Expected uptime '1h30m0s', got %s", uptime)
	}
}
