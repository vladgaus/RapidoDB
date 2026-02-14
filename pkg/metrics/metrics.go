// Package metrics provides Prometheus-compatible metrics for RapidoDB.
//
// The package implements metrics collection and exposition in Prometheus
// text format, allowing integration with Prometheus, Grafana, and other
// monitoring tools.
//
// Features:
//   - Counter, Gauge, and Histogram metric types
//   - Labels support for dimensional metrics
//   - Prometheus text format output
//   - HTTP handler for /metrics endpoint
//   - Zero dependencies (Go stdlib only)
//
// Example usage:
//
//	registry := metrics.NewRegistry()
//	registry.MustRegister(metrics.NewCounter(metrics.CounterOpts{
//	    Name: "rapidodb_requests_total",
//	    Help: "Total number of requests",
//	}))
//
//	http.Handle("/metrics", registry.Handler())
package metrics

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// MetricType represents the type of metric.
type MetricType string

const (
	TypeCounter   MetricType = "counter"
	TypeGauge     MetricType = "gauge"
	TypeHistogram MetricType = "histogram"
	TypeSummary   MetricType = "summary"
)

// Metric is the interface for all metric types.
type Metric interface {
	// Describe returns the metric descriptor.
	Describe() Desc

	// Collect returns the current metric values.
	Collect() []MetricValue
}

// Desc describes a metric.
type Desc struct {
	// Name is the metric name (e.g., "rapidodb_writes_total").
	Name string

	// Help is a description of the metric.
	Help string

	// Type is the metric type.
	Type MetricType

	// Labels are the label names for this metric.
	Labels []string
}

// MetricValue represents a single metric value.
type MetricValue struct {
	// Labels are the label values (in same order as Desc.Labels).
	Labels map[string]string

	// Value is the metric value.
	Value float64

	// Suffix is appended to the metric name (e.g., "_total", "_bucket").
	Suffix string

	// ExtraLabels are additional labels (e.g., "le" for histogram buckets).
	ExtraLabels map[string]string
}

// Registry holds all registered metrics.
type Registry struct {
	mu      sync.RWMutex
	metrics map[string]Metric
	order   []string // Maintain registration order
}

// NewRegistry creates a new metrics registry.
func NewRegistry() *Registry {
	return &Registry{
		metrics: make(map[string]Metric),
		order:   make([]string, 0),
	}
}

// Register adds a metric to the registry.
func (r *Registry) Register(m Metric) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	desc := m.Describe()
	if _, exists := r.metrics[desc.Name]; exists {
		return fmt.Errorf("metric %s already registered", desc.Name)
	}

	r.metrics[desc.Name] = m
	r.order = append(r.order, desc.Name)
	return nil
}

// MustRegister adds a metric and panics on error.
func (r *Registry) MustRegister(m Metric) {
	if err := r.Register(m); err != nil {
		panic(err)
	}
}

// Unregister removes a metric from the registry.
func (r *Registry) Unregister(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.metrics[name]; !exists {
		return false
	}

	delete(r.metrics, name)

	// Remove from order
	for i, n := range r.order {
		if n == name {
			r.order = append(r.order[:i], r.order[i+1:]...)
			break
		}
	}

	return true
}

// Get returns a metric by name.
func (r *Registry) Get(name string) Metric {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.metrics[name]
}

// Gather collects all metrics.
func (r *Registry) Gather() []GatheredMetric {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]GatheredMetric, 0, len(r.metrics))

	for _, name := range r.order {
		m := r.metrics[name]
		result = append(result, GatheredMetric{
			Desc:   m.Describe(),
			Values: m.Collect(),
		})
	}

	return result
}

// GatheredMetric holds a metric descriptor and its values.
type GatheredMetric struct {
	Desc   Desc
	Values []MetricValue
}

// WritePrometheus writes all metrics in Prometheus text format.
func (r *Registry) WritePrometheus(w io.Writer) error {
	gathered := r.Gather()

	for _, gm := range gathered {
		// Write HELP line
		if gm.Desc.Help != "" {
			if _, err := fmt.Fprintf(w, "# HELP %s %s\n", gm.Desc.Name, escapeHelp(gm.Desc.Help)); err != nil {
				return err
			}
		}

		// Write TYPE line
		if _, err := fmt.Fprintf(w, "# TYPE %s %s\n", gm.Desc.Name, gm.Desc.Type); err != nil {
			return err
		}

		// Write values
		for _, v := range gm.Values {
			name := gm.Desc.Name + v.Suffix
			labels := formatLabels(v.Labels, v.ExtraLabels)

			if labels != "" {
				if _, err := fmt.Fprintf(w, "%s{%s} %s\n", name, labels, formatValue(v.Value)); err != nil {
					return err
				}
			} else {
				if _, err := fmt.Fprintf(w, "%s %s\n", name, formatValue(v.Value)); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// Handler returns an HTTP handler for the /metrics endpoint.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_ = r.WritePrometheus(w)
	})
}

// escapeHelp escapes special characters in help text.
func escapeHelp(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "\n", "\\n")
	return s
}

// formatLabels formats labels for Prometheus output.
func formatLabels(labels map[string]string, extra map[string]string) string {
	if len(labels) == 0 && len(extra) == 0 {
		return ""
	}

	// Combine and sort labels
	all := make(map[string]string)
	for k, v := range labels {
		all[k] = v
	}
	for k, v := range extra {
		all[k] = v
	}

	keys := make([]string, 0, len(all))
	for k := range all {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%q", k, all[k]))
	}

	return strings.Join(parts, ",")
}

// formatValue formats a float64 for Prometheus output.
func formatValue(v float64) string {
	// Handle special values
	if v != v { // NaN
		return "NaN"
	}
	if v > 1e308 { // +Inf
		return "+Inf"
	}
	if v < -1e308 { // -Inf
		return "-Inf"
	}

	// Format with appropriate precision
	if v == float64(int64(v)) {
		return fmt.Sprintf("%d", int64(v))
	}
	return fmt.Sprintf("%g", v)
}

// ============================================================================
// Default Registry
// ============================================================================

var defaultRegistry = NewRegistry()

// DefaultRegistry returns the default global registry.
func DefaultRegistry() *Registry {
	return defaultRegistry
}

// Register adds a metric to the default registry.
func Register(m Metric) error {
	return defaultRegistry.Register(m)
}

// MustRegister adds a metric to the default registry and panics on error.
func MustRegister(m Metric) {
	defaultRegistry.MustRegister(m)
}

// Handler returns an HTTP handler for the default registry.
func Handler() http.Handler {
	return defaultRegistry.Handler()
}

// ============================================================================
// Timestamp helper
// ============================================================================

// Now returns the current time in seconds since epoch.
func Now() float64 {
	return float64(time.Now().UnixNano()) / 1e9
}
