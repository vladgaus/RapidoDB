package metrics

import (
	"math"
	"sync"
	"sync/atomic"
)

// Gauge is a metric that can go up and down.
type Gauge struct {
	desc  Desc
	value atomic.Uint64 // Stored as uint64 bits of float64
}

// GaugeOpts configures a gauge.
type GaugeOpts struct {
	Name string
	Help string
}

// NewGauge creates a new gauge.
func NewGauge(opts GaugeOpts) *Gauge {
	return &Gauge{
		desc: Desc{
			Name: opts.Name,
			Help: opts.Help,
			Type: TypeGauge,
		},
	}
}

// Describe returns the gauge descriptor.
func (g *Gauge) Describe() Desc {
	return g.desc
}

// Collect returns the current gauge value.
func (g *Gauge) Collect() []MetricValue {
	return []MetricValue{{
		Value: g.Value(),
	}}
}

// Set sets the gauge to the given value.
func (g *Gauge) Set(v float64) {
	g.value.Store(math.Float64bits(v))
}

// Inc increments the gauge by 1.
func (g *Gauge) Inc() {
	g.Add(1)
}

// Dec decrements the gauge by 1.
func (g *Gauge) Dec() {
	g.Add(-1)
}

// Add adds the given value to the gauge.
func (g *Gauge) Add(v float64) {
	for {
		old := g.value.Load()
		new := math.Float64bits(math.Float64frombits(old) + v)
		if g.value.CompareAndSwap(old, new) {
			return
		}
	}
}

// Sub subtracts the given value from the gauge.
func (g *Gauge) Sub(v float64) {
	g.Add(-v)
}

// Value returns the current gauge value.
func (g *Gauge) Value() float64 {
	return math.Float64frombits(g.value.Load())
}

// SetToCurrentTime sets the gauge to the current Unix timestamp.
func (g *Gauge) SetToCurrentTime() {
	g.Set(Now())
}

// ============================================================================
// GaugeVec - Gauge with labels
// ============================================================================

// GaugeVec is a collection of gauges with labels.
type GaugeVec struct {
	desc   Desc
	mu     sync.RWMutex
	gauges map[string]*labeledGauge
}

// labeledGauge is a gauge with label values.
type labeledGauge struct {
	labels map[string]string
	value  atomic.Uint64
}

// GaugeVecOpts configures a gauge vector.
type GaugeVecOpts struct {
	Name   string
	Help   string
	Labels []string
}

// NewGaugeVec creates a new gauge vector.
func NewGaugeVec(opts GaugeVecOpts) *GaugeVec {
	return &GaugeVec{
		desc: Desc{
			Name:   opts.Name,
			Help:   opts.Help,
			Type:   TypeGauge,
			Labels: opts.Labels,
		},
		gauges: make(map[string]*labeledGauge),
	}
}

// Describe returns the gauge vector descriptor.
func (gv *GaugeVec) Describe() Desc {
	return gv.desc
}

// Collect returns all gauge values.
func (gv *GaugeVec) Collect() []MetricValue {
	gv.mu.RLock()
	defer gv.mu.RUnlock()

	values := make([]MetricValue, 0, len(gv.gauges))
	for _, g := range gv.gauges {
		values = append(values, MetricValue{
			Labels: g.labels,
			Value:  math.Float64frombits(g.value.Load()),
		})
	}
	return values
}

// WithLabelValues returns a gauge for the given label values.
func (gv *GaugeVec) WithLabelValues(values ...string) *GaugeVecEntry {
	key := labelsKey(values)

	gv.mu.RLock()
	g, exists := gv.gauges[key]
	gv.mu.RUnlock()

	if exists {
		return &GaugeVecEntry{gauge: g}
	}

	gv.mu.Lock()
	defer gv.mu.Unlock()

	// Double-check
	if g, exists = gv.gauges[key]; exists {
		return &GaugeVecEntry{gauge: g}
	}

	// Create new gauge
	labels := make(map[string]string)
	for i, name := range gv.desc.Labels {
		if i < len(values) {
			labels[name] = values[i]
		}
	}

	g = &labeledGauge{labels: labels}
	gv.gauges[key] = g

	return &GaugeVecEntry{gauge: g}
}

// GaugeVecEntry is a single entry in a gauge vector.
type GaugeVecEntry struct {
	gauge *labeledGauge
}

// Set sets the gauge to the given value.
func (e *GaugeVecEntry) Set(v float64) {
	e.gauge.value.Store(math.Float64bits(v))
}

// Inc increments the gauge by 1.
func (e *GaugeVecEntry) Inc() {
	e.Add(1)
}

// Dec decrements the gauge by 1.
func (e *GaugeVecEntry) Dec() {
	e.Add(-1)
}

// Add adds the given value to the gauge.
func (e *GaugeVecEntry) Add(v float64) {
	for {
		old := e.gauge.value.Load()
		new := math.Float64bits(math.Float64frombits(old) + v)
		if e.gauge.value.CompareAndSwap(old, new) {
			return
		}
	}
}

// ============================================================================
// GaugeFunc - Gauge from a function
// ============================================================================

// GaugeFunc is a gauge that gets its value from a function.
type GaugeFunc struct {
	desc Desc
	fn   func() float64
}

// GaugeFuncOpts configures a gauge function.
type GaugeFuncOpts struct {
	Name string
	Help string
	Func func() float64
}

// NewGaugeFunc creates a new gauge function.
func NewGaugeFunc(opts GaugeFuncOpts) *GaugeFunc {
	return &GaugeFunc{
		desc: Desc{
			Name: opts.Name,
			Help: opts.Help,
			Type: TypeGauge,
		},
		fn: opts.Func,
	}
}

// Describe returns the gauge descriptor.
func (g *GaugeFunc) Describe() Desc {
	return g.desc
}

// Collect returns the current gauge value.
func (g *GaugeFunc) Collect() []MetricValue {
	return []MetricValue{{
		Value: g.fn(),
	}}
}
