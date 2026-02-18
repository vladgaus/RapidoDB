package tracing

import (
	"context"
	"sync"
	"time"
)

// ============================================================================
// Tracer
// ============================================================================

// Tracer creates spans for distributed tracing.
type Tracer struct {
	mu sync.RWMutex

	// Configuration
	serviceName string
	exporter    Exporter
	sampler     Sampler
	enabled     bool

	// Resource attributes
	resource []Attribute

	// Statistics
	spansCreated  int64
	spansSampled  int64
	spansExported int64
}

// TracerOptions configures the tracer.
type TracerOptions struct {
	// ServiceName is the name of the service being traced.
	ServiceName string

	// Exporter receives completed spans.
	Exporter Exporter

	// Sampler determines which traces to sample.
	// Default: AlwaysSample()
	Sampler Sampler

	// Enabled determines if tracing is active.
	// Default: true
	Enabled bool

	// Resource attributes to add to all spans.
	Resource []Attribute
}

// DefaultTracerOptions returns sensible defaults.
func DefaultTracerOptions() TracerOptions {
	return TracerOptions{
		ServiceName: "rapidodb",
		Sampler:     AlwaysSample(),
		Enabled:     true,
	}
}

// NewTracer creates a new tracer.
func NewTracer(opts TracerOptions) *Tracer {
	if opts.Sampler == nil {
		opts.Sampler = AlwaysSample()
	}
	if opts.ServiceName == "" {
		opts.ServiceName = "rapidodb"
	}

	// Enable by default when an exporter is provided or ServiceName is set
	// This handles the common case where users configure a tracer and expect it to work
	enabled := opts.Enabled
	if !enabled && (opts.Exporter != nil || opts.ServiceName != "rapidodb") {
		enabled = true
	}

	return &Tracer{
		serviceName: opts.ServiceName,
		exporter:    opts.Exporter,
		sampler:     opts.Sampler,
		enabled:     enabled,
		resource:    opts.Resource,
	}
}

// Start creates a new span.
func (t *Tracer) Start(ctx context.Context, name string, opts ...SpanOption) (context.Context, *Span) {
	if !t.enabled {
		return ctx, noopSpan
	}

	// Apply options
	cfg := spanConfig{
		kind: SpanKindInternal,
	}
	for _, opt := range opts {
		opt.apply(&cfg)
	}

	// Get parent context
	parent := SpanContextFromContext(ctx)

	// Generate IDs
	var traceID TraceID
	var parentSpanID SpanID
	if parent.IsValid() {
		traceID = parent.TraceID
		parentSpanID = parent.SpanID
	} else {
		traceID = GenerateTraceID()
	}
	spanID := GenerateSpanID()

	// Determine sampling
	var flags TraceFlags
	if parent.IsValid() {
		// Inherit parent's sampling decision
		flags = parent.TraceFlags
	} else {
		// Make sampling decision for new trace
		params := SamplingParameters{
			TraceID: traceID,
			Name:    name,
			Kind:    cfg.kind,
		}
		if t.sampler.ShouldSample(params).Decision == RecordAndSample {
			flags = FlagsSampled
		}
	}

	// Create span context
	spanCtx := SpanContext{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: flags,
	}

	// Create parent context for span
	parentCtx := SpanContext{
		TraceID: traceID,
		SpanID:  parentSpanID,
	}

	// Create span
	span := &Span{
		name:        name,
		spanContext: spanCtx,
		parent:      parentCtx,
		startTime:   time.Now(),
		kind:        cfg.kind,
		attributes:  cfg.attributes,
		tracer:      t,
	}

	// Update stats
	t.mu.Lock()
	t.spansCreated++
	if flags.IsSampled() {
		t.spansSampled++
	}
	t.mu.Unlock()

	// Return context with span
	return ContextWithSpan(ctx, span), span
}

// export sends a span to the exporter.
func (t *Tracer) export(span *Span) {
	if t.exporter == nil || !span.spanContext.TraceFlags.IsSampled() {
		return
	}

	data := span.snapshot()

	// Add resource attributes
	if len(t.resource) > 0 {
		data.Attributes = append(t.resource, data.Attributes...)
	}

	// Add service name
	data.Attributes = append([]Attribute{
		String("service.name", t.serviceName),
	}, data.Attributes...)

	t.exporter.Export(data)

	t.mu.Lock()
	t.spansExported++
	t.mu.Unlock()
}

// SetEnabled enables or disables tracing.
func (t *Tracer) SetEnabled(enabled bool) {
	t.mu.Lock()
	t.enabled = enabled
	t.mu.Unlock()
}

// IsEnabled returns whether tracing is enabled.
func (t *Tracer) IsEnabled() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.enabled
}

// Stats returns tracer statistics.
func (t *Tracer) Stats() TracerStats {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return TracerStats{
		SpansCreated:  t.spansCreated,
		SpansSampled:  t.spansSampled,
		SpansExported: t.spansExported,
	}
}

// TracerStats contains tracer statistics.
type TracerStats struct {
	SpansCreated  int64
	SpansSampled  int64
	SpansExported int64
}

// Shutdown shuts down the tracer and flushes any pending spans.
func (t *Tracer) Shutdown() error {
	if t.exporter != nil {
		return t.exporter.Shutdown()
	}
	return nil
}

// ============================================================================
// Span Options
// ============================================================================

// SpanOption configures a span.
type SpanOption interface {
	apply(*spanConfig)
}

type spanConfig struct {
	kind       SpanKind
	attributes []Attribute
}

type spanOptionFunc func(*spanConfig)

func (f spanOptionFunc) apply(cfg *spanConfig) { f(cfg) }

// WithSpanKind sets the span kind.
func WithSpanKind(kind SpanKind) SpanOption {
	return spanOptionFunc(func(cfg *spanConfig) {
		cfg.kind = kind
	})
}

// WithAttributes sets initial attributes.
func WithAttributes(attrs ...Attribute) SpanOption {
	return spanOptionFunc(func(cfg *spanConfig) {
		cfg.attributes = append(cfg.attributes, attrs...)
	})
}

// ============================================================================
// Global Tracer
// ============================================================================

var (
	globalTracer     *Tracer
	globalTracerOnce sync.Once
	globalTracerMu   sync.RWMutex
)

// SetGlobalTracer sets the global tracer.
func SetGlobalTracer(t *Tracer) {
	globalTracerMu.Lock()
	globalTracer = t
	globalTracerMu.Unlock()
}

// GetGlobalTracer returns the global tracer.
func GetGlobalTracer() *Tracer {
	globalTracerMu.RLock()
	t := globalTracer
	globalTracerMu.RUnlock()

	if t == nil {
		globalTracerOnce.Do(func() {
			globalTracer = NewTracer(DefaultTracerOptions())
		})
		return globalTracer
	}
	return t
}

// Start creates a span using the global tracer.
func Start(ctx context.Context, name string, opts ...SpanOption) (context.Context, *Span) {
	return GetGlobalTracer().Start(ctx, name, opts...)
}

// ============================================================================
// Common Span Names
// ============================================================================

const (
	SpanGet          = "rapidodb.get"
	SpanPut          = "rapidodb.put"
	SpanDelete       = "rapidodb.delete"
	SpanScan         = "rapidodb.scan"
	SpanCompaction   = "rapidodb.compaction"
	SpanFlush        = "rapidodb.flush"
	SpanWALWrite     = "rapidodb.wal.write"
	SpanSSTableRead  = "rapidodb.sstable.read"
	SpanSSTableWrite = "rapidodb.sstable.write"
	SpanBloomCheck   = "rapidodb.bloom.check"
)

// ============================================================================
// Common Attribute Keys
// ============================================================================

const (
	AttrDBSystem     = "db.system"
	AttrDBOperation  = "db.operation"
	AttrDBKey        = "db.key"
	AttrDBKeySize    = "db.key.size"
	AttrDBValueSize  = "db.value.size"
	AttrDBBytesRead  = "db.bytes.read"
	AttrDBBytesWrite = "db.bytes.write"
	AttrDBLevel      = "db.level"
	AttrDBTableCount = "db.table.count"
)
