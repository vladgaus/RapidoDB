package tracing

import (
	"context"
	"strings"
)

// ============================================================================
// W3C Trace Context Propagation
// ============================================================================

// W3C Trace Context headers
const (
	TraceparentHeader = "traceparent"
	TracestateHeader  = "tracestate"
)

// Propagator injects and extracts trace context from carriers.
type Propagator interface {
	// Inject injects trace context into a carrier.
	Inject(ctx context.Context, carrier Carrier)

	// Extract extracts trace context from a carrier.
	Extract(ctx context.Context, carrier Carrier) context.Context

	// Fields returns the header names used by this propagator.
	Fields() []string
}

// Carrier is a storage medium for trace context.
type Carrier interface {
	// Get returns the value for a key.
	Get(key string) string

	// Set sets a key-value pair.
	Set(key, value string)

	// Keys returns all keys.
	Keys() []string
}

// ============================================================================
// Map Carrier
// ============================================================================

// MapCarrier is a Carrier backed by a map.
type MapCarrier map[string]string

// Get returns the value for a key.
func (c MapCarrier) Get(key string) string {
	return c[strings.ToLower(key)]
}

// Set sets a key-value pair.
func (c MapCarrier) Set(key, value string) {
	c[strings.ToLower(key)] = value
}

// Keys returns all keys.
func (c MapCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

// ============================================================================
// Header Carrier
// ============================================================================

// HeaderCarrier wraps an http.Header-like map.
type HeaderCarrier map[string][]string

// Get returns the value for a key.
func (c HeaderCarrier) Get(key string) string {
	if v, ok := c[key]; ok && len(v) > 0 {
		return v[0]
	}
	return ""
}

// Set sets a key-value pair.
func (c HeaderCarrier) Set(key, value string) {
	c[key] = []string{value}
}

// Keys returns all keys.
func (c HeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

// ============================================================================
// W3C Trace Context Propagator
// ============================================================================

// TraceContextPropagator implements W3C Trace Context propagation.
type TraceContextPropagator struct{}

// NewTraceContextPropagator creates a new W3C Trace Context propagator.
func NewTraceContextPropagator() *TraceContextPropagator {
	return &TraceContextPropagator{}
}

// Inject injects trace context into a carrier.
// Format: traceparent: 00-{trace-id}-{span-id}-{flags}
func (p *TraceContextPropagator) Inject(ctx context.Context, carrier Carrier) {
	sc := SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return
	}

	// Build traceparent: version-traceid-spanid-flags
	// Version 00, flags: 01 if sampled
	flags := "00"
	if sc.TraceFlags.IsSampled() {
		flags = "01"
	}

	traceparent := "00-" + sc.TraceID.String() + "-" + sc.SpanID.String() + "-" + flags
	carrier.Set(TraceparentHeader, traceparent)

	// Include tracestate if present
	if sc.TraceState != "" {
		carrier.Set(TracestateHeader, sc.TraceState)
	}
}

// Extract extracts trace context from a carrier.
func (p *TraceContextPropagator) Extract(ctx context.Context, carrier Carrier) context.Context {
	traceparent := carrier.Get(TraceparentHeader)
	if traceparent == "" {
		return ctx
	}

	sc, ok := parseTraceparent(traceparent)
	if !ok {
		return ctx
	}

	// Get tracestate
	sc.TraceState = carrier.Get(TracestateHeader)
	sc.Remote = true

	// Create a span with the extracted context
	span := &Span{
		spanContext: sc,
	}
	span.ended.Store(true) // Mark as ended so it won't record

	return ContextWithSpan(ctx, span)
}

// Fields returns the headers used by this propagator.
func (p *TraceContextPropagator) Fields() []string {
	return []string{TraceparentHeader, TracestateHeader}
}

// parseTraceparent parses a W3C traceparent header.
// Format: version-traceid-spanid-flags (e.g., "00-xxx-yyy-01")
func parseTraceparent(s string) (SpanContext, bool) {
	var sc SpanContext

	// Split by dash
	parts := strings.Split(s, "-")
	if len(parts) != 4 {
		return sc, false
	}

	// Validate version
	if parts[0] != "00" {
		return sc, false
	}

	// Parse trace ID (32 hex chars)
	traceID, err := ParseTraceID(parts[1])
	if err != nil {
		return sc, false
	}

	// Parse span ID (16 hex chars)
	spanID, err := ParseSpanID(parts[2])
	if err != nil {
		return sc, false
	}

	// Parse flags
	var flags TraceFlags
	if len(parts[3]) >= 2 && parts[3][1] == '1' {
		flags = FlagsSampled
	}

	sc.TraceID = traceID
	sc.SpanID = spanID
	sc.TraceFlags = flags

	return sc, true
}

// ============================================================================
// Composite Propagator
// ============================================================================

// CompositePropagator combines multiple propagators.
type CompositePropagator struct {
	propagators []Propagator
}

// NewCompositePropagator creates a composite propagator.
func NewCompositePropagator(propagators ...Propagator) *CompositePropagator {
	return &CompositePropagator{propagators: propagators}
}

// Inject injects trace context using all propagators.
func (p *CompositePropagator) Inject(ctx context.Context, carrier Carrier) {
	for _, prop := range p.propagators {
		prop.Inject(ctx, carrier)
	}
}

// Extract extracts trace context using all propagators.
func (p *CompositePropagator) Extract(ctx context.Context, carrier Carrier) context.Context {
	for _, prop := range p.propagators {
		ctx = prop.Extract(ctx, carrier)
	}
	return ctx
}

// Fields returns all fields from all propagators.
func (p *CompositePropagator) Fields() []string {
	var fields []string
	seen := make(map[string]bool)
	for _, prop := range p.propagators {
		for _, f := range prop.Fields() {
			if !seen[f] {
				seen[f] = true
				fields = append(fields, f)
			}
		}
	}
	return fields
}

// ============================================================================
// B3 Propagator (Zipkin)
// ============================================================================

// B3 header names
const (
	B3TraceIDHeader      = "X-B3-TraceId"
	B3SpanIDHeader       = "X-B3-SpanId"
	B3ParentSpanIDHeader = "X-B3-ParentSpanId"
	B3SampledHeader      = "X-B3-Sampled"
	B3SingleHeader       = "b3"
)

// B3Propagator implements Zipkin B3 propagation.
type B3Propagator struct {
	singleHeader bool
}

// NewB3Propagator creates a new B3 propagator.
func NewB3Propagator(singleHeader bool) *B3Propagator {
	return &B3Propagator{singleHeader: singleHeader}
}

// Inject injects trace context using B3 headers.
func (p *B3Propagator) Inject(ctx context.Context, carrier Carrier) {
	sc := SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return
	}

	if p.singleHeader {
		// Single header format: {TraceId}-{SpanId}-{SamplingState}
		sampled := "0"
		if sc.TraceFlags.IsSampled() {
			sampled = "1"
		}
		b3 := sc.TraceID.String() + "-" + sc.SpanID.String() + "-" + sampled
		carrier.Set(B3SingleHeader, b3)
	} else {
		// Multi-header format
		carrier.Set(B3TraceIDHeader, sc.TraceID.String())
		carrier.Set(B3SpanIDHeader, sc.SpanID.String())
		if sc.TraceFlags.IsSampled() {
			carrier.Set(B3SampledHeader, "1")
		} else {
			carrier.Set(B3SampledHeader, "0")
		}
	}
}

// Extract extracts trace context from B3 headers.
func (p *B3Propagator) Extract(ctx context.Context, carrier Carrier) context.Context {
	// Try single header first
	if b3 := carrier.Get(B3SingleHeader); b3 != "" {
		sc, ok := parseB3SingleHeader(b3)
		if ok {
			sc.Remote = true
			span := &Span{spanContext: sc}
			span.ended.Store(true)
			return ContextWithSpan(ctx, span)
		}
	}

	// Try multi-header
	traceIDStr := carrier.Get(B3TraceIDHeader)
	spanIDStr := carrier.Get(B3SpanIDHeader)

	if traceIDStr == "" || spanIDStr == "" {
		return ctx
	}

	traceID, err := ParseTraceID(traceIDStr)
	if err != nil {
		return ctx
	}

	spanID, err := ParseSpanID(spanIDStr)
	if err != nil {
		return ctx
	}

	var flags TraceFlags
	if carrier.Get(B3SampledHeader) == "1" {
		flags = FlagsSampled
	}

	sc := SpanContext{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: flags,
		Remote:     true,
	}

	span := &Span{spanContext: sc}
	span.ended.Store(true)
	return ContextWithSpan(ctx, span)
}

// Fields returns the headers used by B3.
func (p *B3Propagator) Fields() []string {
	if p.singleHeader {
		return []string{B3SingleHeader}
	}
	return []string{B3TraceIDHeader, B3SpanIDHeader, B3ParentSpanIDHeader, B3SampledHeader}
}

// parseB3SingleHeader parses the single B3 header.
func parseB3SingleHeader(s string) (SpanContext, bool) {
	var sc SpanContext

	parts := strings.Split(s, "-")
	if len(parts) < 2 {
		return sc, false
	}

	traceID, err := ParseTraceID(parts[0])
	if err != nil {
		return sc, false
	}

	spanID, err := ParseSpanID(parts[1])
	if err != nil {
		return sc, false
	}

	sc.TraceID = traceID
	sc.SpanID = spanID

	// Check sampling flag
	if len(parts) >= 3 && parts[2] == "1" {
		sc.TraceFlags = FlagsSampled
	}

	return sc, true
}
