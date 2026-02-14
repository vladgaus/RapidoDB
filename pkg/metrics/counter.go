package metrics

import (
	"sync"
	"sync/atomic"
)

// Counter is a monotonically increasing metric.
type Counter struct {
	desc  Desc
	value atomic.Uint64
}

// CounterOpts configures a counter.
type CounterOpts struct {
	Name string
	Help string
}

// NewCounter creates a new counter.
func NewCounter(opts CounterOpts) *Counter {
	return &Counter{
		desc: Desc{
			Name: opts.Name,
			Help: opts.Help,
			Type: TypeCounter,
		},
	}
}

// Describe returns the counter descriptor.
func (c *Counter) Describe() Desc {
	return c.desc
}

// Collect returns the current counter value.
func (c *Counter) Collect() []MetricValue {
	return []MetricValue{{
		Value: float64(c.value.Load()),
	}}
}

// Inc increments the counter by 1.
func (c *Counter) Inc() {
	c.value.Add(1)
}

// Add adds the given value to the counter.
func (c *Counter) Add(v uint64) {
	c.value.Add(v)
}

// Value returns the current counter value.
func (c *Counter) Value() uint64 {
	return c.value.Load()
}

// ============================================================================
// CounterVec - Counter with labels
// ============================================================================

// CounterVec is a collection of counters with labels.
type CounterVec struct {
	desc     Desc
	mu       sync.RWMutex
	counters map[string]*labeledCounter
}

// labeledCounter is a counter with label values.
type labeledCounter struct {
	labels map[string]string
	value  atomic.Uint64
}

// CounterVecOpts configures a counter vector.
type CounterVecOpts struct {
	Name   string
	Help   string
	Labels []string
}

// NewCounterVec creates a new counter vector.
func NewCounterVec(opts CounterVecOpts) *CounterVec {
	return &CounterVec{
		desc: Desc{
			Name:   opts.Name,
			Help:   opts.Help,
			Type:   TypeCounter,
			Labels: opts.Labels,
		},
		counters: make(map[string]*labeledCounter),
	}
}

// Describe returns the counter vector descriptor.
func (cv *CounterVec) Describe() Desc {
	return cv.desc
}

// Collect returns all counter values.
func (cv *CounterVec) Collect() []MetricValue {
	cv.mu.RLock()
	defer cv.mu.RUnlock()

	values := make([]MetricValue, 0, len(cv.counters))
	for _, c := range cv.counters {
		values = append(values, MetricValue{
			Labels: c.labels,
			Value:  float64(c.value.Load()),
		})
	}
	return values
}

// WithLabelValues returns a counter for the given label values.
func (cv *CounterVec) WithLabelValues(values ...string) *CounterVecEntry {
	key := labelsKey(values)

	cv.mu.RLock()
	c, exists := cv.counters[key]
	cv.mu.RUnlock()

	if exists {
		return &CounterVecEntry{counter: c}
	}

	cv.mu.Lock()
	defer cv.mu.Unlock()

	// Double-check
	if c, exists = cv.counters[key]; exists {
		return &CounterVecEntry{counter: c}
	}

	// Create new counter
	labels := make(map[string]string)
	for i, name := range cv.desc.Labels {
		if i < len(values) {
			labels[name] = values[i]
		}
	}

	c = &labeledCounter{labels: labels}
	cv.counters[key] = c

	return &CounterVecEntry{counter: c}
}

// CounterVecEntry is a single entry in a counter vector.
type CounterVecEntry struct {
	counter *labeledCounter
}

// Inc increments the counter by 1.
func (e *CounterVecEntry) Inc() {
	e.counter.value.Add(1)
}

// Add adds the given value to the counter.
func (e *CounterVecEntry) Add(v uint64) {
	e.counter.value.Add(v)
}

// labelsKey creates a unique key from label values.
func labelsKey(values []string) string {
	if len(values) == 0 {
		return ""
	}
	if len(values) == 1 {
		return values[0]
	}

	total := 0
	for _, v := range values {
		total += len(v) + 1
	}

	b := make([]byte, 0, total)
	for i, v := range values {
		if i > 0 {
			b = append(b, '\x00')
		}
		b = append(b, v...)
	}
	return string(b)
}
