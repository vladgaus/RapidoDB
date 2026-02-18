// Package tracing provides distributed tracing for RapidoDB.
//
// Features:
//   - OpenTelemetry-compatible trace context
//   - W3C Trace Context propagation
//   - Spans for operations (Get, Put, Delete, Compaction)
//   - Jaeger/Zipkin compatible export
//   - Configurable sampling
//   - Zero external dependencies
//
// Example usage:
//
//	tracer := tracing.NewTracer(tracing.TracerOptions{
//	    ServiceName: "rapidodb",
//	    Exporter:    tracing.NewJSONExporter(os.Stdout),
//	    Sampler:     tracing.AlwaysSample(),
//	})
//
//	ctx, span := tracer.Start(ctx, "Get")
//	defer span.End()
//	span.SetAttribute("key", keyName)
package tracing

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"sync/atomic"
	"time"
)

// ============================================================================
// Trace and Span IDs
// ============================================================================

// TraceID is a unique identifier for a trace (16 bytes / 128 bits).
type TraceID [16]byte

// SpanID is a unique identifier for a span (8 bytes / 64 bits).
type SpanID [8]byte

// String returns the hex string representation.
func (t TraceID) String() string {
	return hex.EncodeToString(t[:])
}

// IsValid returns true if the trace ID is not zero.
func (t TraceID) IsValid() bool {
	for _, b := range t {
		if b != 0 {
			return true
		}
	}
	return false
}

// String returns the hex string representation.
func (s SpanID) String() string {
	return hex.EncodeToString(s[:])
}

// IsValid returns true if the span ID is not zero.
func (s SpanID) IsValid() bool {
	for _, b := range s {
		if b != 0 {
			return true
		}
	}
	return false
}

// GenerateTraceID generates a new random trace ID.
func GenerateTraceID() TraceID {
	var id TraceID
	_, _ = rand.Read(id[:])
	return id
}

// GenerateSpanID generates a new random span ID.
func GenerateSpanID() SpanID {
	var id SpanID
	_, _ = rand.Read(id[:])
	return id
}

// ParseTraceID parses a hex string into a TraceID.
func ParseTraceID(s string) (TraceID, error) {
	var id TraceID
	if len(s) != 32 {
		return id, ErrInvalidTraceID
	}
	_, err := hex.Decode(id[:], []byte(s))
	return id, err
}

// ParseSpanID parses a hex string into a SpanID.
func ParseSpanID(s string) (SpanID, error) {
	var id SpanID
	if len(s) != 16 {
		return id, ErrInvalidSpanID
	}
	_, err := hex.Decode(id[:], []byte(s))
	return id, err
}

// ============================================================================
// Span Context
// ============================================================================

// SpanContext contains the trace context that propagates across boundaries.
type SpanContext struct {
	TraceID    TraceID
	SpanID     SpanID
	TraceFlags TraceFlags
	TraceState string // W3C tracestate header value
	Remote     bool   // True if context was propagated from remote
}

// TraceFlags contains trace flags.
type TraceFlags byte

const (
	// FlagsSampled indicates the trace is sampled.
	FlagsSampled TraceFlags = 1 << iota
)

// IsSampled returns true if the sampled flag is set.
func (f TraceFlags) IsSampled() bool {
	return f&FlagsSampled != 0
}

// IsValid returns true if the span context has valid trace and span IDs.
func (sc SpanContext) IsValid() bool {
	return sc.TraceID.IsValid() && sc.SpanID.IsValid()
}

// ============================================================================
// Span Status and Kind
// ============================================================================

// SpanKind describes the relationship between the span and its parent.
type SpanKind int

const (
	SpanKindInternal SpanKind = iota
	SpanKindServer
	SpanKindClient
	SpanKindProducer
	SpanKindConsumer
)

func (k SpanKind) String() string {
	switch k {
	case SpanKindServer:
		return "SERVER"
	case SpanKindClient:
		return "CLIENT"
	case SpanKindProducer:
		return "PRODUCER"
	case SpanKindConsumer:
		return "CONSUMER"
	default:
		return "INTERNAL"
	}
}

// StatusCode represents the status of a span.
type StatusCode int

const (
	StatusUnset StatusCode = iota
	StatusOK
	StatusError
)

func (s StatusCode) String() string {
	switch s {
	case StatusOK:
		return "OK"
	case StatusError:
		return "ERROR"
	default:
		return "UNSET"
	}
}

// Status represents the status of a span.
type Status struct {
	Code        StatusCode
	Description string
}

// ============================================================================
// Attributes
// ============================================================================

// Attribute represents a key-value pair.
type Attribute struct {
	Key   string
	Value interface{}
}

// String creates a string attribute.
func String(key, value string) Attribute {
	return Attribute{Key: key, Value: value}
}

// Int creates an int attribute.
func Int(key string, value int) Attribute {
	return Attribute{Key: key, Value: value}
}

// Int64 creates an int64 attribute.
func Int64(key string, value int64) Attribute {
	return Attribute{Key: key, Value: value}
}

// Float64 creates a float64 attribute.
func Float64(key string, value float64) Attribute {
	return Attribute{Key: key, Value: value}
}

// Bool creates a bool attribute.
func Bool(key string, value bool) Attribute {
	return Attribute{Key: key, Value: value}
}

// ============================================================================
// Events
// ============================================================================

// Event represents a span event (annotation).
type Event struct {
	Name       string
	Timestamp  time.Time
	Attributes []Attribute
}

// ============================================================================
// Span
// ============================================================================

// Span represents a single operation within a trace.
type Span struct {
	mu sync.RWMutex

	// Identity
	name        string
	spanContext SpanContext
	parent      SpanContext

	// Timing
	startTime time.Time
	endTime   time.Time
	ended     atomic.Bool

	// Metadata
	kind       SpanKind
	status     Status
	attributes []Attribute
	events     []Event

	// Tracer reference
	tracer *Tracer
}

// SpanContext returns the span's context.
func (s *Span) SpanContext() SpanContext {
	return s.spanContext
}

// IsRecording returns true if the span is recording events.
func (s *Span) IsRecording() bool {
	return !s.ended.Load() && s.spanContext.TraceFlags.IsSampled()
}

// SetName sets the span name.
func (s *Span) SetName(name string) {
	if !s.IsRecording() {
		return
	}
	s.mu.Lock()
	s.name = name
	s.mu.Unlock()
}

// SetStatus sets the span status.
func (s *Span) SetStatus(code StatusCode, description string) {
	if !s.IsRecording() {
		return
	}
	s.mu.Lock()
	s.status = Status{Code: code, Description: description}
	s.mu.Unlock()
}

// SetAttributes sets attributes on the span.
func (s *Span) SetAttributes(attrs ...Attribute) {
	if !s.IsRecording() {
		return
	}
	s.mu.Lock()
	s.attributes = append(s.attributes, attrs...)
	s.mu.Unlock()
}

// SetAttribute sets a single attribute.
func (s *Span) SetAttribute(key string, value interface{}) {
	s.SetAttributes(Attribute{Key: key, Value: value})
}

// AddEvent adds an event to the span.
func (s *Span) AddEvent(name string, attrs ...Attribute) {
	if !s.IsRecording() {
		return
	}
	s.mu.Lock()
	s.events = append(s.events, Event{
		Name:       name,
		Timestamp:  time.Now(),
		Attributes: attrs,
	})
	s.mu.Unlock()
}

// RecordError records an error as an event.
func (s *Span) RecordError(err error, attrs ...Attribute) {
	if err == nil || !s.IsRecording() {
		return
	}

	attrs = append(attrs,
		String("exception.type", typeOf(err)),
		String("exception.message", err.Error()),
	)

	s.AddEvent("exception", attrs...)
	s.SetStatus(StatusError, err.Error())
}

// End marks the span as ended.
func (s *Span) End() {
	if s.ended.Swap(true) {
		return // Already ended
	}

	s.mu.Lock()
	s.endTime = time.Now()
	s.mu.Unlock()

	// Export the span
	if s.tracer != nil && s.tracer.exporter != nil {
		s.tracer.export(s)
	}
}

// Duration returns the span duration.
func (s *Span) Duration() time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.endTime.IsZero() {
		return time.Since(s.startTime)
	}
	return s.endTime.Sub(s.startTime)
}

// snapshot returns a copy of span data for export.
func (s *Span) snapshot() SpanData {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return SpanData{
		Name:        s.name,
		SpanContext: s.spanContext,
		Parent:      s.parent,
		Kind:        s.kind,
		StartTime:   s.startTime,
		EndTime:     s.endTime,
		Status:      s.status,
		Attributes:  append([]Attribute(nil), s.attributes...),
		Events:      append([]Event(nil), s.events...),
	}
}

// SpanData is an immutable snapshot of span data for export.
type SpanData struct {
	Name        string
	SpanContext SpanContext
	Parent      SpanContext
	Kind        SpanKind
	StartTime   time.Time
	EndTime     time.Time
	Status      Status
	Attributes  []Attribute
	Events      []Event
}

// ============================================================================
// No-op Span
// ============================================================================

// noopSpan is a span that does nothing (used when tracing is disabled).
var noopSpan = &Span{
	spanContext: SpanContext{},
}

func init() {
	noopSpan.ended.Store(true) // Mark as ended so IsRecording returns false
}

// ============================================================================
// Context Integration
// ============================================================================

type spanContextKey struct{}

// ContextWithSpan returns a new context with the span attached.
func ContextWithSpan(ctx context.Context, span *Span) context.Context {
	return context.WithValue(ctx, spanContextKey{}, span)
}

// SpanFromContext returns the span from context, or nil if not present.
func SpanFromContext(ctx context.Context) *Span {
	if span, ok := ctx.Value(spanContextKey{}).(*Span); ok {
		return span
	}
	return nil
}

// SpanContextFromContext returns the span context from context.
func SpanContextFromContext(ctx context.Context) SpanContext {
	if span := SpanFromContext(ctx); span != nil {
		return span.SpanContext()
	}
	return SpanContext{}
}

// ============================================================================
// Helper Functions
// ============================================================================

func typeOf(i interface{}) string {
	if i == nil {
		return "nil"
	}
	return formatType(i)
}

func formatType(i interface{}) string {
	switch i.(type) {
	case error:
		return "error"
	default:
		return "unknown"
	}
}
