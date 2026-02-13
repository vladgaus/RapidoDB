// Package health provides health check functionality for RapidoDB.
//
// The package implements Kubernetes-style health probes:
//
//   - /health        - Full health status with details
//   - /health/live   - Liveness probe (is the process alive?)
//   - /health/ready  - Readiness probe (is it ready to serve traffic?)
//
// Health checks include disk space, memory usage, WAL status, and engine state.
// All implementations use only Go standard library (zero dependencies).
package health

import (
	"encoding/json"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Status represents the health status of a component or the overall system.
type Status string

const (
	// StatusHealthy indicates the component is functioning normally.
	StatusHealthy Status = "healthy"

	// StatusDegraded indicates the component is functioning but with issues.
	StatusDegraded Status = "degraded"

	// StatusUnhealthy indicates the component is not functioning.
	StatusUnhealthy Status = "unhealthy"
)

// CheckResult represents the result of a single health check.
type CheckResult struct {
	// Name is the identifier of the check.
	Name string `json:"name"`

	// Status is the health status.
	Status Status `json:"status"`

	// Message provides additional context.
	Message string `json:"message,omitempty"`

	// Duration is how long the check took.
	Duration time.Duration `json:"duration_ms"`

	// Timestamp is when the check was performed.
	Timestamp time.Time `json:"timestamp"`

	// Details contains check-specific data.
	Details map[string]interface{} `json:"details,omitempty"`
}

// MarshalJSON customizes JSON output for CheckResult.
func (r CheckResult) MarshalJSON() ([]byte, error) {
	type Alias CheckResult
	return json.Marshal(&struct {
		*Alias
		Duration int64 `json:"duration_ms"`
	}{
		Alias:    (*Alias)(&r),
		Duration: r.Duration.Milliseconds(),
	})
}

// HealthReport is the complete health status report.
type HealthReport struct {
	// Status is the overall health status.
	Status Status `json:"status"`

	// Uptime is how long the service has been running.
	Uptime time.Duration `json:"uptime"`

	// Timestamp is when the report was generated.
	Timestamp time.Time `json:"timestamp"`

	// Version is the service version.
	Version string `json:"version"`

	// Checks contains individual check results.
	Checks []CheckResult `json:"checks"`
}

// MarshalJSON customizes JSON output for HealthReport.
func (r HealthReport) MarshalJSON() ([]byte, error) {
	type Alias HealthReport
	return json.Marshal(&struct {
		*Alias
		Uptime string `json:"uptime"`
	}{
		Alias:  (*Alias)(&r),
		Uptime: r.Uptime.Round(time.Second).String(),
	})
}

// Checker performs health checks on the system.
type Checker interface {
	// Name returns the checker's identifier.
	Name() string

	// Check performs the health check and returns the result.
	Check() CheckResult
}

// HealthChecker manages health checks for the service.
type HealthChecker struct {
	mu sync.RWMutex

	// checkers is the list of registered health checkers.
	checkers []Checker

	// startTime is when the service started.
	startTime time.Time

	// version is the service version string.
	version string

	// live indicates if the service is alive (for liveness probe).
	live atomic.Bool

	// ready indicates if the service is ready (for readiness probe).
	ready atomic.Bool

	// cached health report for rate limiting
	cachedReport    *HealthReport
	cachedTime      time.Time
	cacheDuration   time.Duration
	cacheInProgress atomic.Bool
}

// Options configures the HealthChecker.
type Options struct {
	// Version is the service version string.
	Version string

	// CacheDuration is how long to cache health check results.
	// Default: 1 second
	CacheDuration time.Duration
}

// DefaultOptions returns sensible default options.
func DefaultOptions() Options {
	return Options{
		Version:       "RapidoDB/1.0.0",
		CacheDuration: time.Second,
	}
}

// NewHealthChecker creates a new health checker.
func NewHealthChecker(opts Options) *HealthChecker {
	if opts.CacheDuration == 0 {
		opts.CacheDuration = time.Second
	}

	hc := &HealthChecker{
		checkers:      make([]Checker, 0),
		startTime:     time.Now(),
		version:       opts.Version,
		cacheDuration: opts.CacheDuration,
	}

	// Service starts as live but not ready
	hc.live.Store(true)
	hc.ready.Store(false)

	return hc
}

// RegisterChecker adds a health checker.
func (h *HealthChecker) RegisterChecker(checker Checker) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.checkers = append(h.checkers, checker)
}

// SetLive sets the liveness status.
func (h *HealthChecker) SetLive(live bool) {
	h.live.Store(live)
}

// SetReady sets the readiness status.
func (h *HealthChecker) SetReady(ready bool) {
	h.ready.Store(ready)
}

// IsLive returns true if the service is alive.
func (h *HealthChecker) IsLive() bool {
	return h.live.Load()
}

// IsReady returns true if the service is ready to serve traffic.
func (h *HealthChecker) IsReady() bool {
	return h.ready.Load()
}

// Check performs all health checks and returns a full report.
func (h *HealthChecker) Check() HealthReport {
	// Check cache
	h.mu.RLock()
	if h.cachedReport != nil && time.Since(h.cachedTime) < h.cacheDuration {
		report := *h.cachedReport
		h.mu.RUnlock()
		return report
	}
	h.mu.RUnlock()

	// Prevent thundering herd
	if !h.cacheInProgress.CompareAndSwap(false, true) {
		// Another goroutine is updating, return stale or empty
		h.mu.RLock()
		if h.cachedReport != nil {
			report := *h.cachedReport
			h.mu.RUnlock()
			return report
		}
		h.mu.RUnlock()
	} else {
		defer h.cacheInProgress.Store(false)
	}

	// Perform checks
	h.mu.RLock()
	checkers := make([]Checker, len(h.checkers))
	copy(checkers, h.checkers)
	h.mu.RUnlock()

	results := make([]CheckResult, 0, len(checkers))
	overallStatus := StatusHealthy

	for _, checker := range checkers {
		result := checker.Check()
		results = append(results, result)

		// Aggregate status (worst wins)
		if result.Status == StatusUnhealthy {
			overallStatus = StatusUnhealthy
		} else if result.Status == StatusDegraded && overallStatus != StatusUnhealthy {
			overallStatus = StatusDegraded
		}
	}

	// If not ready, mark as degraded at minimum
	if !h.ready.Load() && overallStatus == StatusHealthy {
		overallStatus = StatusDegraded
	}

	// If not live, mark as unhealthy
	if !h.live.Load() {
		overallStatus = StatusUnhealthy
	}

	report := HealthReport{
		Status:    overallStatus,
		Uptime:    time.Since(h.startTime),
		Timestamp: time.Now(),
		Version:   h.version,
		Checks:    results,
	}

	// Update cache
	h.mu.Lock()
	h.cachedReport = &report
	h.cachedTime = time.Now()
	h.mu.Unlock()

	return report
}

// Uptime returns how long the service has been running.
func (h *HealthChecker) Uptime() time.Duration {
	return time.Since(h.startTime)
}

// Version returns the service version.
func (h *HealthChecker) Version() string {
	return h.version
}

// MemoryStats returns current memory statistics.
func MemoryStats() runtime.MemStats {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m
}
