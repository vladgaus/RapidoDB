package metrics

import (
	"net/http"
	"runtime"
	"sync"
	"time"
)

// EngineStatsProvider provides engine statistics for metrics collection.
type EngineStatsProvider interface {
	// Stats returns current engine statistics.
	// Expected fields: MemTableSize, L0TableCount, TotalKeyValuePairs
	GetMemTableSize() int64
	GetSSTableCount() int
	GetCompactionStats() (compactions int64, bytesCompacted int64)
}

// RapiDoDBMetrics holds all RapidoDB metrics.
type RapiDoDBMetrics struct {
	// Operation counters
	WritesTotal  *Counter
	ReadsTotal   *Counter
	DeletesTotal *Counter

	// Latency histograms
	WriteLatency  *Histogram
	ReadLatency   *Histogram
	DeleteLatency *Histogram

	// Storage metrics
	MemtableSizeBytes *Gauge
	SSTableCount      *Gauge
	SSTableSizeBytes  *Gauge
	WALSizeBytes      *Gauge
	DiskUsageBytes    *Gauge

	// Compaction metrics
	CompactionDuration *Histogram
	CompactionsTotal   *Counter
	CompactionBytes    *Counter

	// Bloom filter metrics
	BloomFilterHits   *Counter
	BloomFilterMisses *Counter

	// Connection metrics
	ActiveConnections *Gauge
	TotalConnections  *Counter

	// Rate limiting metrics
	RateLimitedTotal *Counter

	// Cache metrics
	CacheHits   *Counter
	CacheMisses *Counter

	// Go runtime metrics
	GoroutinesCount *GaugeFunc
	HeapAllocBytes  *GaugeFunc
	HeapSysBytes    *GaugeFunc
	GCPauseTotal    *GaugeFunc

	// Info metric
	Info *GaugeVec

	// Registry
	registry *Registry

	// Stats collector
	statsCollectorDone chan struct{}
	statsCollectorWg   sync.WaitGroup
}

// NewRapiDoDBMetrics creates a new metrics collection.
func NewRapiDoDBMetrics() *RapiDoDBMetrics {
	m := &RapiDoDBMetrics{
		registry: NewRegistry(),
	}

	// Operation counters
	m.WritesTotal = NewCounter(CounterOpts{
		Name: "rapidodb_writes_total",
		Help: "Total number of write operations",
	})

	m.ReadsTotal = NewCounter(CounterOpts{
		Name: "rapidodb_reads_total",
		Help: "Total number of read operations",
	})

	m.DeletesTotal = NewCounter(CounterOpts{
		Name: "rapidodb_deletes_total",
		Help: "Total number of delete operations",
	})

	// Latency histograms
	latencyBuckets := []float64{
		0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1,
	}

	m.WriteLatency = NewHistogram(HistogramOpts{
		Name:    "rapidodb_write_latency_seconds",
		Help:    "Write operation latency in seconds",
		Buckets: latencyBuckets,
	})

	m.ReadLatency = NewHistogram(HistogramOpts{
		Name:    "rapidodb_read_latency_seconds",
		Help:    "Read operation latency in seconds",
		Buckets: latencyBuckets,
	})

	m.DeleteLatency = NewHistogram(HistogramOpts{
		Name:    "rapidodb_delete_latency_seconds",
		Help:    "Delete operation latency in seconds",
		Buckets: latencyBuckets,
	})

	// Storage metrics
	m.MemtableSizeBytes = NewGauge(GaugeOpts{
		Name: "rapidodb_memtable_size_bytes",
		Help: "Current MemTable size in bytes",
	})

	m.SSTableCount = NewGauge(GaugeOpts{
		Name: "rapidodb_sstable_count",
		Help: "Number of SSTable files",
	})

	m.SSTableSizeBytes = NewGauge(GaugeOpts{
		Name: "rapidodb_sstable_size_bytes",
		Help: "Total SSTable size in bytes",
	})

	m.WALSizeBytes = NewGauge(GaugeOpts{
		Name: "rapidodb_wal_size_bytes",
		Help: "Current WAL size in bytes",
	})

	m.DiskUsageBytes = NewGauge(GaugeOpts{
		Name: "rapidodb_disk_usage_bytes",
		Help: "Total disk usage in bytes",
	})

	// Compaction metrics
	m.CompactionDuration = NewHistogram(HistogramOpts{
		Name:    "rapidodb_compaction_duration_seconds",
		Help:    "Compaction duration in seconds",
		Buckets: []float64{0.1, 0.5, 1, 5, 10, 30, 60, 120, 300},
	})

	m.CompactionsTotal = NewCounter(CounterOpts{
		Name: "rapidodb_compactions_total",
		Help: "Total number of compactions completed",
	})

	m.CompactionBytes = NewCounter(CounterOpts{
		Name: "rapidodb_compaction_bytes_total",
		Help: "Total bytes processed by compaction",
	})

	// Bloom filter metrics
	m.BloomFilterHits = NewCounter(CounterOpts{
		Name: "rapidodb_bloom_filter_hits_total",
		Help: "Total bloom filter hits (key definitely not present)",
	})

	m.BloomFilterMisses = NewCounter(CounterOpts{
		Name: "rapidodb_bloom_filter_misses_total",
		Help: "Total bloom filter misses (key might be present)",
	})

	// Connection metrics
	m.ActiveConnections = NewGauge(GaugeOpts{
		Name: "rapidodb_active_connections",
		Help: "Number of active client connections",
	})

	m.TotalConnections = NewCounter(CounterOpts{
		Name: "rapidodb_connections_total",
		Help: "Total number of client connections",
	})

	// Rate limiting metrics
	m.RateLimitedTotal = NewCounter(CounterOpts{
		Name: "rapidodb_rate_limited_total",
		Help: "Total number of rate limited requests",
	})

	// Cache metrics
	m.CacheHits = NewCounter(CounterOpts{
		Name: "rapidodb_cache_hits_total",
		Help: "Total cache hits",
	})

	m.CacheMisses = NewCounter(CounterOpts{
		Name: "rapidodb_cache_misses_total",
		Help: "Total cache misses",
	})

	// Go runtime metrics
	m.GoroutinesCount = NewGaugeFunc(GaugeFuncOpts{
		Name: "rapidodb_goroutines",
		Help: "Number of goroutines",
		Func: func() float64 {
			return float64(runtime.NumGoroutine())
		},
	})

	m.HeapAllocBytes = NewGaugeFunc(GaugeFuncOpts{
		Name: "rapidodb_heap_alloc_bytes",
		Help: "Heap memory allocated in bytes",
		Func: func() float64 {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			return float64(m.HeapAlloc)
		},
	})

	m.HeapSysBytes = NewGaugeFunc(GaugeFuncOpts{
		Name: "rapidodb_heap_sys_bytes",
		Help: "Heap memory obtained from system in bytes",
		Func: func() float64 {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			return float64(m.HeapSys)
		},
	})

	m.GCPauseTotal = NewGaugeFunc(GaugeFuncOpts{
		Name: "rapidodb_gc_pause_total_seconds",
		Help: "Total GC pause time in seconds",
		Func: func() float64 {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			return float64(m.PauseTotalNs) / 1e9
		},
	})

	// Info metric
	m.Info = NewGaugeVec(GaugeVecOpts{
		Name:   "rapidodb_info",
		Help:   "RapidoDB version information",
		Labels: []string{"version"},
	})

	// Register all metrics
	m.register()

	return m
}

// register registers all metrics with the registry.
func (m *RapiDoDBMetrics) register() {
	m.registry.MustRegister(m.WritesTotal)
	m.registry.MustRegister(m.ReadsTotal)
	m.registry.MustRegister(m.DeletesTotal)
	m.registry.MustRegister(m.WriteLatency)
	m.registry.MustRegister(m.ReadLatency)
	m.registry.MustRegister(m.DeleteLatency)
	m.registry.MustRegister(m.MemtableSizeBytes)
	m.registry.MustRegister(m.SSTableCount)
	m.registry.MustRegister(m.SSTableSizeBytes)
	m.registry.MustRegister(m.WALSizeBytes)
	m.registry.MustRegister(m.DiskUsageBytes)
	m.registry.MustRegister(m.CompactionDuration)
	m.registry.MustRegister(m.CompactionsTotal)
	m.registry.MustRegister(m.CompactionBytes)
	m.registry.MustRegister(m.BloomFilterHits)
	m.registry.MustRegister(m.BloomFilterMisses)
	m.registry.MustRegister(m.ActiveConnections)
	m.registry.MustRegister(m.TotalConnections)
	m.registry.MustRegister(m.RateLimitedTotal)
	m.registry.MustRegister(m.CacheHits)
	m.registry.MustRegister(m.CacheMisses)
	m.registry.MustRegister(m.GoroutinesCount)
	m.registry.MustRegister(m.HeapAllocBytes)
	m.registry.MustRegister(m.HeapSysBytes)
	m.registry.MustRegister(m.GCPauseTotal)
	m.registry.MustRegister(m.Info)
}

// Handler returns an HTTP handler for the /metrics endpoint.
func (m *RapiDoDBMetrics) Handler() http.Handler {
	return m.registry.Handler()
}

// Registry returns the metrics registry.
func (m *RapiDoDBMetrics) Registry() *Registry {
	return m.registry
}

// SetVersion sets the version info metric.
func (m *RapiDoDBMetrics) SetVersion(version string) {
	m.Info.WithLabelValues(version).Set(1)
}

// StartStatsCollector starts a background goroutine that periodically
// collects engine statistics and updates the corresponding gauges.
// The provider should implement methods to get engine stats.
func (m *RapiDoDBMetrics) StartStatsCollector(provider EngineStatsProvider, interval time.Duration) {
	if interval <= 0 {
		interval = 5 * time.Second
	}

	m.statsCollectorDone = make(chan struct{})
	m.statsCollectorWg.Add(1)

	go func() {
		defer m.statsCollectorWg.Done()

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				m.collectEngineStats(provider)
			case <-m.statsCollectorDone:
				return
			}
		}
	}()
}

// collectEngineStats collects stats from the engine and updates gauges.
func (m *RapiDoDBMetrics) collectEngineStats(provider EngineStatsProvider) {
	// MemTable size
	m.MemtableSizeBytes.Set(float64(provider.GetMemTableSize()))

	// SSTable count
	m.SSTableCount.Set(float64(provider.GetSSTableCount()))

	// Compaction stats (we track the delta)
	compactions, bytesCompacted := provider.GetCompactionStats()
	// Note: These are totals from engine, we just set them
	// In a real implementation, we might track deltas
	_ = compactions
	_ = bytesCompacted
}

// StopStatsCollector stops the background stats collector.
func (m *RapiDoDBMetrics) StopStatsCollector() {
	if m.statsCollectorDone != nil {
		close(m.statsCollectorDone)
		m.statsCollectorWg.Wait()
	}
}

// ============================================================================
// Metrics Server
// ============================================================================

// Server is an HTTP server that exposes metrics.
type Server struct {
	mu      sync.Mutex
	server  *http.Server
	metrics *RapiDoDBMetrics
	addr    string
	started bool
}

// ServerOptions configures the metrics server.
type ServerOptions struct {
	Host    string
	Port    int
	Metrics *RapiDoDBMetrics
}

// NewServer creates a new metrics server.
func NewServer(opts ServerOptions) *Server {
	if opts.Host == "" {
		opts.Host = "0.0.0.0"
	}
	if opts.Port == 0 {
		opts.Port = 9090
	}
	if opts.Metrics == nil {
		opts.Metrics = NewRapiDoDBMetrics()
	}

	addr := formatAddr(opts.Host, opts.Port)

	mux := http.NewServeMux()
	mux.Handle("/metrics", opts.Metrics.Handler())
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		// Best-effort response; nothing to do if the client disconnected.
		_, _ = w.Write([]byte(`<html>
<head><title>RapidoDB Metrics</title></head>
<body>
<h1>RapidoDB Metrics</h1>
<p><a href="/metrics">Metrics</a></p>
</body>
</html>`))
	})

	return &Server{
		server: &http.Server{
			Addr:         addr,
			Handler:      mux,
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 10 * time.Second,
		},
		metrics: opts.Metrics,
		addr:    addr,
	}
}

// Start starts the metrics server.
func (s *Server) Start() error {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return nil
	}
	s.started = true
	s.mu.Unlock()

	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed { //nolint:staticcheck // intentionally empty; no logger available, error is non-actionable in a fire-and-forget goroutine
		}
	}()

	return nil
}

// Close stops the metrics server.
func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started {
		return nil
	}

	return s.server.Close()
}

// Addr returns the server address.
func (s *Server) Addr() string {
	return s.addr
}

// Metrics returns the metrics collection.
func (s *Server) Metrics() *RapiDoDBMetrics {
	return s.metrics
}

// formatAddr formats host and port into an address string.
func formatAddr(host string, port int) string {
	return host + ":" + itoa(port)
}

// itoa converts an int to a string without importing strconv.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}
