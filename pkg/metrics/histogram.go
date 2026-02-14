package metrics

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Histogram tracks the distribution of values.
type Histogram struct {
	desc    Desc
	buckets []float64

	mu         sync.Mutex
	counts     []atomic.Uint64
	sum        atomic.Uint64 // Stored as uint64 bits of float64
	totalCount atomic.Uint64
}

// HistogramOpts configures a histogram.
type HistogramOpts struct {
	Name    string
	Help    string
	Buckets []float64
}

// DefaultBuckets are the default histogram buckets for latencies.
var DefaultBuckets = []float64{
	0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
}

// NewHistogram creates a new histogram.
func NewHistogram(opts HistogramOpts) *Histogram {
	buckets := opts.Buckets
	if len(buckets) == 0 {
		buckets = DefaultBuckets
	}

	// Sort buckets
	sorted := make([]float64, len(buckets))
	copy(sorted, buckets)
	sort.Float64s(sorted)

	// Add +Inf bucket if not present
	if len(sorted) == 0 || sorted[len(sorted)-1] != math.Inf(1) {
		sorted = append(sorted, math.Inf(1))
	}

	h := &Histogram{
		desc: Desc{
			Name: opts.Name,
			Help: opts.Help,
			Type: TypeHistogram,
		},
		buckets: sorted,
		counts:  make([]atomic.Uint64, len(sorted)),
	}

	return h
}

// Describe returns the histogram descriptor.
func (h *Histogram) Describe() Desc {
	return h.desc
}

// Collect returns the histogram values.
func (h *Histogram) Collect() []MetricValue {
	h.mu.Lock()
	defer h.mu.Unlock()

	values := make([]MetricValue, 0, len(h.buckets)+2)

	// Bucket values (cumulative)
	var cumulative uint64
	for i, bound := range h.buckets {
		cumulative += h.counts[i].Load()
		values = append(values, MetricValue{
			Suffix: "_bucket",
			Value:  float64(cumulative),
			ExtraLabels: map[string]string{
				"le": formatBound(bound),
			},
		})
	}

	// Sum
	values = append(values, MetricValue{
		Suffix: "_sum",
		Value:  math.Float64frombits(h.sum.Load()),
	})

	// Count
	values = append(values, MetricValue{
		Suffix: "_count",
		Value:  float64(h.totalCount.Load()),
	})

	return values
}

// Observe adds a single observation to the histogram.
func (h *Histogram) Observe(v float64) {
	// Find bucket
	idx := sort.SearchFloat64s(h.buckets, v)
	if idx < len(h.buckets) {
		h.counts[idx].Add(1)
	}

	// Update sum and count
	h.totalCount.Add(1)

	// Atomic add to sum
	for {
		old := h.sum.Load()
		new := math.Float64bits(math.Float64frombits(old) + v)
		if h.sum.CompareAndSwap(old, new) {
			break
		}
	}
}

// ObserveDuration observes the duration since the given start time.
func (h *Histogram) ObserveDuration(start time.Time) {
	h.Observe(time.Since(start).Seconds())
}

// formatBound formats a bucket bound for Prometheus output.
func formatBound(v float64) string {
	if math.IsInf(v, 1) {
		return "+Inf"
	}
	return fmt.Sprintf("%g", v)
}

// ============================================================================
// HistogramVec - Histogram with labels
// ============================================================================

// HistogramVec is a collection of histograms with labels.
type HistogramVec struct {
	desc       Desc
	buckets    []float64
	mu         sync.RWMutex
	histograms map[string]*labeledHistogram
}

// labeledHistogram is a histogram with label values.
type labeledHistogram struct {
	labels     map[string]string
	counts     []atomic.Uint64
	sum        atomic.Uint64
	totalCount atomic.Uint64
}

// HistogramVecOpts configures a histogram vector.
type HistogramVecOpts struct {
	Name    string
	Help    string
	Labels  []string
	Buckets []float64
}

// NewHistogramVec creates a new histogram vector.
func NewHistogramVec(opts HistogramVecOpts) *HistogramVec {
	buckets := opts.Buckets
	if len(buckets) == 0 {
		buckets = DefaultBuckets
	}

	sorted := make([]float64, len(buckets))
	copy(sorted, buckets)
	sort.Float64s(sorted)

	if len(sorted) == 0 || sorted[len(sorted)-1] != math.Inf(1) {
		sorted = append(sorted, math.Inf(1))
	}

	return &HistogramVec{
		desc: Desc{
			Name:   opts.Name,
			Help:   opts.Help,
			Type:   TypeHistogram,
			Labels: opts.Labels,
		},
		buckets:    sorted,
		histograms: make(map[string]*labeledHistogram),
	}
}

// Describe returns the histogram vector descriptor.
func (hv *HistogramVec) Describe() Desc {
	return hv.desc
}

// Collect returns all histogram values.
func (hv *HistogramVec) Collect() []MetricValue {
	hv.mu.RLock()
	defer hv.mu.RUnlock()

	values := make([]MetricValue, 0, (len(hv.buckets)+2)*len(hv.histograms))

	for _, h := range hv.histograms {
		// Bucket values (cumulative)
		var cumulative uint64
		for i, bound := range hv.buckets {
			cumulative += h.counts[i].Load()
			values = append(values, MetricValue{
				Labels: h.labels,
				Suffix: "_bucket",
				Value:  float64(cumulative),
				ExtraLabels: map[string]string{
					"le": formatBound(bound),
				},
			})
		}

		// Sum
		values = append(values, MetricValue{
			Labels: h.labels,
			Suffix: "_sum",
			Value:  math.Float64frombits(h.sum.Load()),
		})

		// Count
		values = append(values, MetricValue{
			Labels: h.labels,
			Suffix: "_count",
			Value:  float64(h.totalCount.Load()),
		})
	}

	return values
}

// WithLabelValues returns a histogram for the given label values.
func (hv *HistogramVec) WithLabelValues(values ...string) *HistogramVecEntry {
	key := labelsKey(values)

	hv.mu.RLock()
	h, exists := hv.histograms[key]
	hv.mu.RUnlock()

	if exists {
		return &HistogramVecEntry{histogram: h, buckets: hv.buckets}
	}

	hv.mu.Lock()
	defer hv.mu.Unlock()

	// Double-check
	if h, exists = hv.histograms[key]; exists {
		return &HistogramVecEntry{histogram: h, buckets: hv.buckets}
	}

	// Create new histogram
	labels := make(map[string]string)
	for i, name := range hv.desc.Labels {
		if i < len(values) {
			labels[name] = values[i]
		}
	}

	h = &labeledHistogram{
		labels: labels,
		counts: make([]atomic.Uint64, len(hv.buckets)),
	}
	hv.histograms[key] = h

	return &HistogramVecEntry{histogram: h, buckets: hv.buckets}
}

// HistogramVecEntry is a single entry in a histogram vector.
type HistogramVecEntry struct {
	histogram *labeledHistogram
	buckets   []float64
}

// Observe adds a single observation to the histogram.
func (e *HistogramVecEntry) Observe(v float64) {
	// Find bucket
	idx := sort.SearchFloat64s(e.buckets, v)
	if idx < len(e.buckets) {
		e.histogram.counts[idx].Add(1)
	}

	// Update sum and count
	e.histogram.totalCount.Add(1)

	for {
		old := e.histogram.sum.Load()
		new := math.Float64bits(math.Float64frombits(old) + v)
		if e.histogram.sum.CompareAndSwap(old, new) {
			break
		}
	}
}

// ObserveDuration observes the duration since the given start time.
func (e *HistogramVecEntry) ObserveDuration(start time.Time) {
	e.Observe(time.Since(start).Seconds())
}

// ============================================================================
// Timer helper
// ============================================================================

// Timer is a helper for timing operations.
type Timer struct {
	histogram *Histogram
	start     time.Time
}

// NewTimer starts a new timer for the given histogram.
func NewTimer(h *Histogram) *Timer {
	return &Timer{
		histogram: h,
		start:     time.Now(),
	}
}

// ObserveDuration records the duration since the timer was created.
func (t *Timer) ObserveDuration() time.Duration {
	d := time.Since(t.start)
	t.histogram.Observe(d.Seconds())
	return d
}
