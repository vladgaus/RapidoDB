package metrics

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// ============================================================================
// Counter Tests
// ============================================================================

func TestNewCounter(t *testing.T) {
	c := NewCounter(CounterOpts{
		Name: "test_counter",
		Help: "A test counter",
	})

	if c == nil {
		t.Fatal("NewCounter returned nil")
	}

	desc := c.Describe()
	if desc.Name != "test_counter" {
		t.Errorf("Expected name test_counter, got %s", desc.Name)
	}
	if desc.Type != TypeCounter {
		t.Errorf("Expected type counter, got %s", desc.Type)
	}
}

func TestCounter_Inc(t *testing.T) {
	c := NewCounter(CounterOpts{Name: "test"})

	c.Inc()
	c.Inc()
	c.Inc()

	if c.Value() != 3 {
		t.Errorf("Expected 3, got %d", c.Value())
	}
}

func TestCounter_Add(t *testing.T) {
	c := NewCounter(CounterOpts{Name: "test"})

	c.Add(5)
	c.Add(10)

	if c.Value() != 15 {
		t.Errorf("Expected 15, got %d", c.Value())
	}
}

func TestCounter_Concurrent(t *testing.T) {
	c := NewCounter(CounterOpts{Name: "test"})

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				c.Inc()
			}
		}()
	}
	wg.Wait()

	if c.Value() != 10000 {
		t.Errorf("Expected 10000, got %d", c.Value())
	}
}

func TestCounterVec(t *testing.T) {
	cv := NewCounterVec(CounterVecOpts{
		Name:   "test_counter_vec",
		Help:   "Test counter with labels",
		Labels: []string{"method", "status"},
	})

	cv.WithLabelValues("GET", "200").Inc()
	cv.WithLabelValues("GET", "200").Inc()
	cv.WithLabelValues("POST", "500").Inc()

	values := cv.Collect()
	if len(values) != 2 {
		t.Errorf("Expected 2 label combinations, got %d", len(values))
	}
}

// ============================================================================
// Gauge Tests
// ============================================================================

func TestNewGauge(t *testing.T) {
	g := NewGauge(GaugeOpts{
		Name: "test_gauge",
		Help: "A test gauge",
	})

	if g == nil {
		t.Fatal("NewGauge returned nil")
	}

	desc := g.Describe()
	if desc.Type != TypeGauge {
		t.Errorf("Expected type gauge, got %s", desc.Type)
	}
}

func TestGauge_Set(t *testing.T) {
	g := NewGauge(GaugeOpts{Name: "test"})

	g.Set(42.5)

	if g.Value() != 42.5 {
		t.Errorf("Expected 42.5, got %f", g.Value())
	}
}

func TestGauge_IncDec(t *testing.T) {
	g := NewGauge(GaugeOpts{Name: "test"})

	g.Inc()
	g.Inc()
	g.Dec()

	if g.Value() != 1 {
		t.Errorf("Expected 1, got %f", g.Value())
	}
}

func TestGauge_Add(t *testing.T) {
	g := NewGauge(GaugeOpts{Name: "test"})

	g.Add(10.5)
	g.Add(-3.2)

	expected := 7.3
	if g.Value() < expected-0.01 || g.Value() > expected+0.01 {
		t.Errorf("Expected ~%f, got %f", expected, g.Value())
	}
}

func TestGauge_Concurrent(t *testing.T) {
	g := NewGauge(GaugeOpts{Name: "test"})

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				g.Add(1)
			}
		}()
	}
	wg.Wait()

	if g.Value() != 10000 {
		t.Errorf("Expected 10000, got %f", g.Value())
	}
}

func TestGaugeFunc(t *testing.T) {
	counter := 0
	gf := NewGaugeFunc(GaugeFuncOpts{
		Name: "test_func",
		Help: "Test gauge function",
		Func: func() float64 {
			counter++
			return float64(counter)
		},
	})

	values := gf.Collect()
	if values[0].Value != 1 {
		t.Errorf("Expected 1, got %f", values[0].Value)
	}

	values = gf.Collect()
	if values[0].Value != 2 {
		t.Errorf("Expected 2, got %f", values[0].Value)
	}
}

func TestGaugeVec(t *testing.T) {
	gv := NewGaugeVec(GaugeVecOpts{
		Name:   "test_gauge_vec",
		Help:   "Test gauge with labels",
		Labels: []string{"host"},
	})

	gv.WithLabelValues("server1").Set(100)
	gv.WithLabelValues("server2").Set(200)

	values := gv.Collect()
	if len(values) != 2 {
		t.Errorf("Expected 2 label combinations, got %d", len(values))
	}
}

// ============================================================================
// Histogram Tests
// ============================================================================

func TestNewHistogram(t *testing.T) {
	h := NewHistogram(HistogramOpts{
		Name:    "test_histogram",
		Help:    "A test histogram",
		Buckets: []float64{0.1, 0.5, 1},
	})

	if h == nil {
		t.Fatal("NewHistogram returned nil")
	}

	desc := h.Describe()
	if desc.Type != TypeHistogram {
		t.Errorf("Expected type histogram, got %s", desc.Type)
	}
}

func TestHistogram_Observe(t *testing.T) {
	h := NewHistogram(HistogramOpts{
		Name:    "test",
		Buckets: []float64{0.1, 0.5, 1},
	})

	h.Observe(0.05) // <= 0.1
	h.Observe(0.3)  // <= 0.5
	h.Observe(0.7)  // <= 1
	h.Observe(2.0)  // <= +Inf

	values := h.Collect()

	// Should have 4 buckets + sum + count = 6 values
	if len(values) != 6 {
		t.Errorf("Expected 6 values, got %d", len(values))
	}

	// Check bucket values (cumulative)
	// le=0.1: 1, le=0.5: 2, le=1: 3, le=+Inf: 4
	expectedBuckets := []float64{1, 2, 3, 4}
	for i, expected := range expectedBuckets {
		if values[i].Value != expected {
			t.Errorf("Bucket %d: expected %f, got %f", i, expected, values[i].Value)
		}
	}
}

func TestHistogram_ObserveDuration(t *testing.T) {
	h := NewHistogram(HistogramOpts{
		Name:    "test",
		Buckets: []float64{0.001, 0.01, 0.1},
	})

	start := time.Now()
	time.Sleep(5 * time.Millisecond)
	h.ObserveDuration(start)

	values := h.Collect()

	// Should have observed ~5ms
	// Find _sum value
	var sum float64
	for _, v := range values {
		if v.Suffix == "_sum" {
			sum = v.Value
			break
		}
	}

	if sum < 0.004 || sum > 0.02 {
		t.Errorf("Expected sum ~0.005, got %f", sum)
	}
}

func TestHistogramVec(t *testing.T) {
	hv := NewHistogramVec(HistogramVecOpts{
		Name:    "test_histogram_vec",
		Help:    "Test histogram with labels",
		Labels:  []string{"method"},
		Buckets: []float64{0.1, 0.5, 1},
	})

	hv.WithLabelValues("GET").Observe(0.05)
	hv.WithLabelValues("POST").Observe(0.3)

	values := hv.Collect()

	// Should have 2 label combinations * (4 buckets + sum + count) = 12 values
	if len(values) != 12 {
		t.Errorf("Expected 12 values, got %d", len(values))
	}
}

// ============================================================================
// Registry Tests
// ============================================================================

func TestNewRegistry(t *testing.T) {
	r := NewRegistry()

	if r == nil {
		t.Fatal("NewRegistry returned nil")
	}
}

func TestRegistry_Register(t *testing.T) {
	r := NewRegistry()

	c := NewCounter(CounterOpts{Name: "test"})
	err := r.Register(c)

	if err != nil {
		t.Errorf("Register failed: %v", err)
	}

	// Double registration should fail
	err = r.Register(c)
	if err == nil {
		t.Error("Double registration should fail")
	}
}

func TestRegistry_Unregister(t *testing.T) {
	r := NewRegistry()

	c := NewCounter(CounterOpts{Name: "test"})
	r.MustRegister(c)

	if !r.Unregister("test") {
		t.Error("Unregister should return true")
	}

	if r.Unregister("test") {
		t.Error("Second unregister should return false")
	}
}

func TestRegistry_Get(t *testing.T) {
	r := NewRegistry()

	c := NewCounter(CounterOpts{Name: "test"})
	r.MustRegister(c)

	got := r.Get("test")
	if got != c {
		t.Error("Get returned wrong metric")
	}

	if r.Get("nonexistent") != nil {
		t.Error("Get should return nil for nonexistent metric")
	}
}

func TestRegistry_WritePrometheus(t *testing.T) {
	r := NewRegistry()

	c := NewCounter(CounterOpts{
		Name: "test_counter",
		Help: "A test counter",
	})
	c.Add(42)
	r.MustRegister(c)

	g := NewGauge(GaugeOpts{
		Name: "test_gauge",
		Help: "A test gauge",
	})
	g.Set(3.14)
	r.MustRegister(g)

	var buf bytes.Buffer
	err := r.WritePrometheus(&buf)

	if err != nil {
		t.Errorf("WritePrometheus failed: %v", err)
	}

	output := buf.String()

	// Check expected content
	if !strings.Contains(output, "# HELP test_counter A test counter") {
		t.Error("Missing counter HELP line")
	}
	if !strings.Contains(output, "# TYPE test_counter counter") {
		t.Error("Missing counter TYPE line")
	}
	if !strings.Contains(output, "test_counter 42") {
		t.Error("Missing counter value")
	}
	if !strings.Contains(output, "test_gauge 3.14") {
		t.Error("Missing gauge value")
	}
}

func TestRegistry_Handler(t *testing.T) {
	r := NewRegistry()

	c := NewCounter(CounterOpts{Name: "test_counter"})
	c.Inc()
	r.MustRegister(c)

	handler := r.Handler()

	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", rec.Code)
	}

	contentType := rec.Header().Get("Content-Type")
	if !strings.HasPrefix(contentType, "text/plain") {
		t.Errorf("Unexpected content type: %s", contentType)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "test_counter 1") {
		t.Error("Response should contain counter value")
	}
}

// ============================================================================
// RapiDoDBMetrics Tests
// ============================================================================

func TestNewRapiDoDBMetrics(t *testing.T) {
	m := NewRapiDoDBMetrics()

	if m == nil {
		t.Fatal("NewRapiDoDBMetrics returned nil")
	}

	if m.WritesTotal == nil {
		t.Error("WritesTotal should not be nil")
	}
	if m.ReadLatency == nil {
		t.Error("ReadLatency should not be nil")
	}
}

func TestRapiDoDBMetrics_Operations(t *testing.T) {
	m := NewRapiDoDBMetrics()

	m.WritesTotal.Inc()
	m.WritesTotal.Inc()
	m.ReadsTotal.Inc()

	if m.WritesTotal.Value() != 2 {
		t.Errorf("Expected 2 writes, got %d", m.WritesTotal.Value())
	}
	if m.ReadsTotal.Value() != 1 {
		t.Errorf("Expected 1 read, got %d", m.ReadsTotal.Value())
	}
}

func TestRapiDoDBMetrics_SetVersion(t *testing.T) {
	m := NewRapiDoDBMetrics()

	m.SetVersion("1.0.0")

	values := m.Info.Collect()
	if len(values) != 1 {
		t.Errorf("Expected 1 info value, got %d", len(values))
	}
	if values[0].Labels["version"] != "1.0.0" {
		t.Errorf("Expected version 1.0.0, got %s", values[0].Labels["version"])
	}
}

func TestRapiDoDBMetrics_Handler(t *testing.T) {
	m := NewRapiDoDBMetrics()
	m.SetVersion("1.0.0")
	m.WritesTotal.Add(100)
	m.ActiveConnections.Set(5)

	handler := m.Handler()

	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	body := rec.Body.String()

	// Check for expected metrics
	expectations := []string{
		"rapidodb_writes_total 100",
		"rapidodb_active_connections 5",
		"rapidodb_info{version=\"1.0.0\"} 1",
		"rapidodb_goroutines",
		"rapidodb_heap_alloc_bytes",
	}

	for _, exp := range expectations {
		if !strings.Contains(body, exp) {
			t.Errorf("Missing expected metric: %s", exp)
		}
	}
}

// ============================================================================
// Metrics Server Tests
// ============================================================================

func TestNewServer(t *testing.T) {
	s := NewServer(ServerOptions{
		Host: "127.0.0.1",
		Port: 9191,
	})

	if s == nil {
		t.Fatal("NewServer returned nil")
	}

	if s.Addr() != "127.0.0.1:9191" {
		t.Errorf("Expected addr 127.0.0.1:9191, got %s", s.Addr())
	}
}

func TestServer_StartClose(t *testing.T) {
	s := NewServer(ServerOptions{
		Host: "127.0.0.1",
		Port: 9192,
	})

	err := s.Start()
	if err != nil {
		t.Errorf("Start failed: %v", err)
	}

	// Give server time to start
	time.Sleep(10 * time.Millisecond)

	err = s.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

// ============================================================================
// Helper Function Tests
// ============================================================================

func TestFormatValue(t *testing.T) {
	tests := []struct {
		input    float64
		expected string
	}{
		{0, "0"},
		{42, "42"},
		{3.14, "3.14"},
		{100.0, "100"},
	}

	for _, tt := range tests {
		got := formatValue(tt.input)
		if got != tt.expected {
			t.Errorf("formatValue(%v): expected %s, got %s", tt.input, tt.expected, got)
		}
	}
}

func TestLabelsKey(t *testing.T) {
	tests := []struct {
		input    []string
		expected string
	}{
		{[]string{}, ""},
		{[]string{"a"}, "a"},
		{[]string{"a", "b"}, "a\x00b"},
	}

	for _, tt := range tests {
		got := labelsKey(tt.input)
		if got != tt.expected {
			t.Errorf("labelsKey(%v): expected %q, got %q", tt.input, tt.expected, got)
		}
	}
}

// ============================================================================
// Benchmark Tests
// ============================================================================

func BenchmarkCounter_Inc(b *testing.B) {
	c := NewCounter(CounterOpts{Name: "bench"})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Inc()
	}
}

func BenchmarkGauge_Set(b *testing.B) {
	g := NewGauge(GaugeOpts{Name: "bench"})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g.Set(float64(i))
	}
}

func BenchmarkHistogram_Observe(b *testing.B) {
	h := NewHistogram(HistogramOpts{Name: "bench"})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Observe(float64(i) * 0.001)
	}
}

func BenchmarkCounterVec_Inc(b *testing.B) {
	cv := NewCounterVec(CounterVecOpts{
		Name:   "bench",
		Labels: []string{"method"},
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cv.WithLabelValues("GET").Inc()
	}
}
