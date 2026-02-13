package health

import (
	"fmt"
	"runtime"
	"time"
)

// ============================================================================
// Memory Checker
// ============================================================================

// MemoryChecker checks system memory health.
type MemoryChecker struct {
	// WarningThresholdPercent triggers degraded status.
	WarningThresholdPercent float64

	// CriticalThresholdPercent triggers unhealthy status.
	CriticalThresholdPercent float64

	// MaxHeapBytes is the maximum heap size before warning.
	// 0 means no limit.
	MaxHeapBytes uint64
}

// NewMemoryChecker creates a memory checker with default thresholds.
func NewMemoryChecker() *MemoryChecker {
	return &MemoryChecker{
		WarningThresholdPercent:  80.0,
		CriticalThresholdPercent: 95.0,
		MaxHeapBytes:             0, // No limit by default
	}
}

// Name returns the checker name.
func (c *MemoryChecker) Name() string {
	return "memory"
}

// Check performs the memory health check.
func (c *MemoryChecker) Check() CheckResult {
	start := time.Now()

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// Calculate heap usage percentage (relative to system memory if available)
	heapUsedMB := float64(m.HeapAlloc) / 1024 / 1024
	heapSysMB := float64(m.HeapSys) / 1024 / 1024

	status := StatusHealthy
	message := fmt.Sprintf("Heap: %.1f MB used, %.1f MB sys", heapUsedMB, heapSysMB)

	// Check heap size limit
	if c.MaxHeapBytes > 0 && m.HeapAlloc > c.MaxHeapBytes {
		status = StatusDegraded
		message = fmt.Sprintf("Heap exceeds limit: %.1f MB > %.1f MB",
			heapUsedMB, float64(c.MaxHeapBytes)/1024/1024)
	}

	// Check GC pressure (if GC is running too often)
	if m.NumGC > 0 {
		gcRate := float64(m.NumGC) / time.Since(time.Unix(0, int64(m.LastGC))).Seconds()
		if gcRate > 10 { // More than 10 GCs per second is concerning
			status = StatusDegraded
			message += fmt.Sprintf(", high GC rate: %.1f/s", gcRate)
		}
	}

	return CheckResult{
		Name:      c.Name(),
		Status:    status,
		Message:   message,
		Duration:  time.Since(start),
		Timestamp: time.Now(),
		Details: map[string]interface{}{
			"heap_alloc_bytes":  m.HeapAlloc,
			"heap_sys_bytes":    m.HeapSys,
			"heap_objects":      m.HeapObjects,
			"gc_runs":           m.NumGC,
			"gc_pause_total_ns": m.PauseTotalNs,
			"goroutines":        runtime.NumGoroutine(),
		},
	}
}

// ============================================================================
// Engine Checker
// ============================================================================

// EngineStateProvider provides engine state information.
type EngineStateProvider interface {
	// IsClosed returns true if the engine is closed.
	IsClosed() bool
}

// EngineChecker checks the storage engine health.
type EngineChecker struct {
	engine EngineStateProvider
}

// NewEngineChecker creates an engine checker.
func NewEngineChecker(engine EngineStateProvider) *EngineChecker {
	return &EngineChecker{
		engine: engine,
	}
}

// Name returns the checker name.
func (c *EngineChecker) Name() string {
	return "engine"
}

// Check performs the engine health check.
func (c *EngineChecker) Check() CheckResult {
	start := time.Now()

	if c.engine == nil {
		return CheckResult{
			Name:      c.Name(),
			Status:    StatusUnhealthy,
			Message:   "Engine not initialized",
			Duration:  time.Since(start),
			Timestamp: time.Now(),
		}
	}

	if c.engine.IsClosed() {
		return CheckResult{
			Name:      c.Name(),
			Status:    StatusUnhealthy,
			Message:   "Engine is closed",
			Duration:  time.Since(start),
			Timestamp: time.Now(),
		}
	}

	return CheckResult{
		Name:      c.Name(),
		Status:    StatusHealthy,
		Message:   "Engine is running",
		Duration:  time.Since(start),
		Timestamp: time.Now(),
	}
}

// ============================================================================
// Server Stats Checker
// ============================================================================

// ServerStatsProvider provides server statistics.
type ServerStatsProvider interface {
	// ActiveConnections returns current active connection count.
	ActiveConnections() int64

	// TotalConnections returns total connections since start.
	TotalConnections() uint64

	// GetHits returns total cache hits.
	GetHits() uint64

	// GetMisses returns total cache misses.
	GetMisses() uint64
}

// ServerChecker checks the server health.
type ServerChecker struct {
	stats            ServerStatsProvider
	maxConnections   int64
	warningThreshold float64 // Connection count as % of max
}

// NewServerChecker creates a server checker.
func NewServerChecker(stats ServerStatsProvider, maxConnections int) *ServerChecker {
	return &ServerChecker{
		stats:            stats,
		maxConnections:   int64(maxConnections),
		warningThreshold: 80.0,
	}
}

// Name returns the checker name.
func (c *ServerChecker) Name() string {
	return "server"
}

// Check performs the server health check.
func (c *ServerChecker) Check() CheckResult {
	start := time.Now()

	if c.stats == nil {
		return CheckResult{
			Name:      c.Name(),
			Status:    StatusUnhealthy,
			Message:   "Server stats not available",
			Duration:  time.Since(start),
			Timestamp: time.Now(),
		}
	}

	activeConns := c.stats.ActiveConnections()
	totalConns := c.stats.TotalConnections()
	hits := c.stats.GetHits()
	misses := c.stats.GetMisses()

	// Calculate hit rate
	var hitRate float64
	if hits+misses > 0 {
		hitRate = float64(hits) / float64(hits+misses) * 100
	}

	status := StatusHealthy
	message := fmt.Sprintf("%d active connections", activeConns)

	// Check connection usage
	if c.maxConnections > 0 {
		connPercent := float64(activeConns) / float64(c.maxConnections) * 100
		if connPercent >= 95 {
			status = StatusDegraded
			message = fmt.Sprintf("Near connection limit: %d/%d (%.1f%%)",
				activeConns, c.maxConnections, connPercent)
		} else if connPercent >= c.warningThreshold {
			status = StatusDegraded
			message = fmt.Sprintf("High connection usage: %d/%d (%.1f%%)",
				activeConns, c.maxConnections, connPercent)
		}
	}

	return CheckResult{
		Name:      c.Name(),
		Status:    status,
		Message:   message,
		Duration:  time.Since(start),
		Timestamp: time.Now(),
		Details: map[string]interface{}{
			"active_connections": activeConns,
			"total_connections":  totalConns,
			"max_connections":    c.maxConnections,
			"get_hits":           hits,
			"get_misses":         misses,
			"hit_rate_percent":   hitRate,
		},
	}
}

// ============================================================================
// Custom Checker (for user-defined checks)
// ============================================================================

// CustomChecker allows users to define custom health checks.
type CustomChecker struct {
	name    string
	checkFn func() (Status, string, map[string]interface{})
}

// NewCustomChecker creates a custom health checker.
func NewCustomChecker(name string, checkFn func() (Status, string, map[string]interface{})) *CustomChecker {
	return &CustomChecker{
		name:    name,
		checkFn: checkFn,
	}
}

// Name returns the checker name.
func (c *CustomChecker) Name() string {
	return c.name
}

// Check performs the custom health check.
func (c *CustomChecker) Check() CheckResult {
	start := time.Now()

	status, message, details := c.checkFn()

	return CheckResult{
		Name:      c.name,
		Status:    status,
		Message:   message,
		Duration:  time.Since(start),
		Timestamp: time.Now(),
		Details:   details,
	}
}
