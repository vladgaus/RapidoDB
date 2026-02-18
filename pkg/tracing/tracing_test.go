package tracing

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"
)

// ============================================================================
// Trace/Span ID Tests
// ============================================================================

func TestTraceID(t *testing.T) {
	id := GenerateTraceID()

	if !id.IsValid() {
		t.Error("Generated TraceID should be valid")
	}

	str := id.String()
	if len(str) != 32 {
		t.Errorf("TraceID string should be 32 chars, got %d", len(str))
	}

	// Parse back
	parsed, err := ParseTraceID(str)
	if err != nil {
		t.Errorf("Failed to parse TraceID: %v", err)
	}
	if parsed != id {
		t.Error("Parsed TraceID should match original")
	}
}

func TestSpanID(t *testing.T) {
	id := GenerateSpanID()

	if !id.IsValid() {
		t.Error("Generated SpanID should be valid")
	}

	str := id.String()
	if len(str) != 16 {
		t.Errorf("SpanID string should be 16 chars, got %d", len(str))
	}

	// Parse back
	parsed, err := ParseSpanID(str)
	if err != nil {
		t.Errorf("Failed to parse SpanID: %v", err)
	}
	if parsed != id {
		t.Error("Parsed SpanID should match original")
	}
}

func TestZeroIDs(t *testing.T) {
	var traceID TraceID
	var spanID SpanID

	if traceID.IsValid() {
		t.Error("Zero TraceID should not be valid")
	}
	if spanID.IsValid() {
		t.Error("Zero SpanID should not be valid")
	}
}

func TestInvalidParse(t *testing.T) {
	_, err := ParseTraceID("invalid")
	if err == nil {
		t.Error("Should fail with invalid trace ID")
	}

	_, err = ParseSpanID("invalid")
	if err == nil {
		t.Error("Should fail with invalid span ID")
	}
}

// ============================================================================
// Tracer Tests
// ============================================================================

func TestNewTracer(t *testing.T) {
	tracer := NewTracer(TracerOptions{
		ServiceName: "test-service",
		Sampler:     AlwaysSample(),
	})

	if tracer == nil {
		t.Fatal("NewTracer returned nil")
	}
}

func TestTracerStart(t *testing.T) {
	var buf bytes.Buffer
	tracer := NewTracer(TracerOptions{
		ServiceName: "test-service",
		Exporter:    NewJSONExporter(&buf),
		Sampler:     AlwaysSample(),
	})

	ctx := context.Background()
	_, span := tracer.Start(ctx, "test-operation")

	if span == nil {
		t.Fatal("Start returned nil span")
	}

	if !span.IsRecording() {
		t.Error("Span should be recording")
	}

	sc := span.SpanContext()
	if !sc.TraceID.IsValid() {
		t.Error("Span should have valid TraceID")
	}
	if !sc.SpanID.IsValid() {
		t.Error("Span should have valid SpanID")
	}

	span.End()

	if span.IsRecording() {
		t.Error("Span should not be recording after End")
	}

	// Check exported JSON
	if buf.Len() == 0 {
		t.Error("Exporter should have received span")
	}
}

func TestTracerChildSpan(t *testing.T) {
	tracer := NewTracer(TracerOptions{
		ServiceName: "test-service",
		Sampler:     AlwaysSample(),
	})

	ctx := context.Background()
	ctx, parent := tracer.Start(ctx, "parent")
	defer parent.End()

	_, child := tracer.Start(ctx, "child")
	defer child.End()

	parentSC := parent.SpanContext()
	childSC := child.SpanContext()

	// Should share trace ID
	if parentSC.TraceID != childSC.TraceID {
		t.Error("Child should have same TraceID as parent")
	}

	// Should have different span IDs
	if parentSC.SpanID == childSC.SpanID {
		t.Error("Child should have different SpanID from parent")
	}
}

func TestTracerDisabled(t *testing.T) {
	tracer := NewTracer(TracerOptions{
		ServiceName: "test-service",
		Enabled:     false,
		Sampler:     NeverSample(), // Use NeverSample to ensure disabled
	})
	tracer.SetEnabled(false) // Explicitly disable

	ctx := context.Background()
	_, span := tracer.Start(ctx, "test")

	if span.IsRecording() {
		t.Error("Span should not be recording when tracer is disabled")
	}
}

// ============================================================================
// Span Tests
// ============================================================================

func TestSpanAttributes(t *testing.T) {
	var buf bytes.Buffer
	tracer := NewTracer(TracerOptions{
		ServiceName: "test",
		Exporter:    NewJSONExporter(&buf),
		Sampler:     AlwaysSample(),
	})

	ctx := context.Background()
	_, span := tracer.Start(ctx, "test")

	span.SetAttributes(
		String("key1", "value1"),
		Int("key2", 42),
		Bool("key3", true),
	)

	span.SetAttribute("key4", 3.14)

	span.End()

	// Parse JSON output
	var js JSONSpan
	if err := json.Unmarshal(buf.Bytes(), &js); err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}

	if js.Attributes["key1"] != "value1" {
		t.Error("Should have key1 attribute")
	}
}

func TestSpanEvents(t *testing.T) {
	var buf bytes.Buffer
	tracer := NewTracer(TracerOptions{
		ServiceName: "test",
		Exporter:    NewJSONExporter(&buf),
		Sampler:     AlwaysSample(),
	})

	ctx := context.Background()
	_, span := tracer.Start(ctx, "test")

	span.AddEvent("event1", String("detail", "info"))
	span.AddEvent("event2")

	span.End()

	var js JSONSpan
	if err := json.Unmarshal(buf.Bytes(), &js); err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}

	if len(js.Events) != 2 {
		t.Errorf("Expected 2 events, got %d", len(js.Events))
	}
}

func TestSpanRecordError(t *testing.T) {
	var buf bytes.Buffer
	tracer := NewTracer(TracerOptions{
		ServiceName: "test",
		Exporter:    NewJSONExporter(&buf),
		Sampler:     AlwaysSample(),
	})

	ctx := context.Background()
	_, span := tracer.Start(ctx, "test")

	testErr := errors.New("test error")
	span.RecordError(testErr)

	span.End()

	var js JSONSpan
	if err := json.Unmarshal(buf.Bytes(), &js); err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}

	if js.Status != "ERROR" {
		t.Error("Status should be ERROR after RecordError")
	}

	if len(js.Events) != 1 || js.Events[0].Name != "exception" {
		t.Error("Should have exception event")
	}
}

func TestSpanStatus(t *testing.T) {
	var buf bytes.Buffer
	tracer := NewTracer(TracerOptions{
		ServiceName: "test",
		Exporter:    NewJSONExporter(&buf),
		Sampler:     AlwaysSample(),
	})

	ctx := context.Background()
	_, span := tracer.Start(ctx, "test")

	span.SetStatus(StatusOK, "success")
	span.End()

	var js JSONSpan
	if err := json.Unmarshal(buf.Bytes(), &js); err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}

	if js.Status != "OK" {
		t.Errorf("Expected status OK, got %s", js.Status)
	}
}

func TestSpanDuration(t *testing.T) {
	tracer := NewTracer(TracerOptions{
		ServiceName: "test",
		Sampler:     AlwaysSample(),
	})

	ctx := context.Background()
	_, span := tracer.Start(ctx, "test")

	time.Sleep(10 * time.Millisecond)

	span.End()

	d := span.Duration()
	if d < 10*time.Millisecond {
		t.Errorf("Duration should be at least 10ms, got %v", d)
	}
}

// ============================================================================
// Sampler Tests
// ============================================================================

func TestAlwaysSample(t *testing.T) {
	sampler := AlwaysSample()

	result := sampler.ShouldSample(SamplingParameters{})
	if result.Decision != RecordAndSample {
		t.Error("AlwaysSample should always sample")
	}
}

func TestNeverSample(t *testing.T) {
	sampler := NeverSample()

	result := sampler.ShouldSample(SamplingParameters{})
	if result.Decision != Drop {
		t.Error("NeverSample should never sample")
	}
}

func TestRatioSampler(t *testing.T) {
	// 0% should never sample
	sampler := RatioSampler(0)
	result := sampler.ShouldSample(SamplingParameters{})
	if result.Decision == RecordAndSample {
		t.Error("0% ratio should not sample")
	}

	// 100% should always sample
	sampler = RatioSampler(1.0)
	result = sampler.ShouldSample(SamplingParameters{})
	if result.Decision != RecordAndSample {
		t.Error("100% ratio should always sample")
	}
}

func TestRatioSamplerDistribution(t *testing.T) {
	sampler := RatioSampler(0.5)

	sampled := 0
	total := 10000

	for i := 0; i < total; i++ {
		params := SamplingParameters{
			TraceID: GenerateTraceID(),
		}
		if sampler.ShouldSample(params).Decision == RecordAndSample {
			sampled++
		}
	}

	ratio := float64(sampled) / float64(total)
	// Should be approximately 50% (with reasonable tolerance for random variance)
	if ratio < 0.45 || ratio > 0.55 {
		t.Errorf("Ratio sampler produced %.2f%% sampled, expected ~50%%", ratio*100)
	}
}

// ============================================================================
// Exporter Tests
// ============================================================================

func TestJSONExporter(t *testing.T) {
	var buf bytes.Buffer
	exporter := NewJSONExporter(&buf)

	span := SpanData{
		Name: "test-span",
		SpanContext: SpanContext{
			TraceID:    GenerateTraceID(),
			SpanID:     GenerateSpanID(),
			TraceFlags: FlagsSampled,
		},
		Kind:      SpanKindServer,
		StartTime: time.Now(),
		EndTime:   time.Now().Add(100 * time.Millisecond),
		Status:    Status{Code: StatusOK},
		Attributes: []Attribute{
			String("key", "value"),
		},
	}

	exporter.Export(span)

	if buf.Len() == 0 {
		t.Error("Exporter should produce output")
	}

	var js JSONSpan
	if err := json.Unmarshal(buf.Bytes(), &js); err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}

	if js.Name != "test-span" {
		t.Errorf("Expected name 'test-span', got %s", js.Name)
	}
}

func TestMultiExporter(t *testing.T) {
	var buf1, buf2 bytes.Buffer
	exp1 := NewJSONExporter(&buf1)
	exp2 := NewJSONExporter(&buf2)

	multi := NewMultiExporter(exp1, exp2)

	span := SpanData{
		Name: "test",
		SpanContext: SpanContext{
			TraceID:    GenerateTraceID(),
			SpanID:     GenerateSpanID(),
			TraceFlags: FlagsSampled,
		},
		StartTime: time.Now(),
		EndTime:   time.Now(),
	}

	multi.Export(span)

	if buf1.Len() == 0 || buf2.Len() == 0 {
		t.Error("Both exporters should receive span")
	}
}

// ============================================================================
// Propagation Tests
// ============================================================================

func TestTraceContextPropagation(t *testing.T) {
	tracer := NewTracer(TracerOptions{
		ServiceName: "test",
		Sampler:     AlwaysSample(),
	})

	propagator := NewTraceContextPropagator()

	// Create a span
	ctx := context.Background()
	ctx, span := tracer.Start(ctx, "test")
	defer span.End()

	// Inject into carrier
	carrier := make(MapCarrier)
	propagator.Inject(ctx, carrier)

	traceparent := carrier.Get(TraceparentHeader)
	if traceparent == "" {
		t.Error("Should have injected traceparent")
	}

	// Extract from carrier
	newCtx := propagator.Extract(context.Background(), carrier)

	extractedSC := SpanContextFromContext(newCtx)
	originalSC := span.SpanContext()

	if extractedSC.TraceID != originalSC.TraceID {
		t.Error("Extracted TraceID should match")
	}
	if extractedSC.SpanID != originalSC.SpanID {
		t.Error("Extracted SpanID should match")
	}
}

func TestB3Propagation(t *testing.T) {
	tracer := NewTracer(TracerOptions{
		ServiceName: "test",
		Sampler:     AlwaysSample(),
	})

	propagator := NewB3Propagator(false)

	ctx := context.Background()
	ctx, span := tracer.Start(ctx, "test")
	defer span.End()

	// Inject
	carrier := make(MapCarrier)
	propagator.Inject(ctx, carrier)

	if carrier.Get(B3TraceIDHeader) == "" {
		t.Error("Should have X-B3-TraceId")
	}
	if carrier.Get(B3SpanIDHeader) == "" {
		t.Error("Should have X-B3-SpanId")
	}

	// Extract
	newCtx := propagator.Extract(context.Background(), carrier)
	extractedSC := SpanContextFromContext(newCtx)

	if extractedSC.TraceID != span.SpanContext().TraceID {
		t.Error("Extracted TraceID should match")
	}
}

func TestB3SingleHeaderPropagation(t *testing.T) {
	tracer := NewTracer(TracerOptions{
		ServiceName: "test",
		Sampler:     AlwaysSample(),
	})

	propagator := NewB3Propagator(true)

	ctx := context.Background()
	ctx, span := tracer.Start(ctx, "test")
	defer span.End()

	carrier := make(MapCarrier)
	propagator.Inject(ctx, carrier)

	b3 := carrier.Get(B3SingleHeader)
	if b3 == "" {
		t.Error("Should have b3 header")
	}
}

// ============================================================================
// Context Tests
// ============================================================================

func TestContextWithSpan(t *testing.T) {
	tracer := NewTracer(TracerOptions{
		ServiceName: "test",
		Sampler:     AlwaysSample(),
	})

	ctx := context.Background()
	ctx, span := tracer.Start(ctx, "test")
	defer span.End()

	// Get span from context
	retrieved := SpanFromContext(ctx)
	if retrieved != span {
		t.Error("Should retrieve same span from context")
	}

	// Get span context
	sc := SpanContextFromContext(ctx)
	if sc.TraceID != span.SpanContext().TraceID {
		t.Error("SpanContext should match")
	}
}

func TestSpanFromEmptyContext(t *testing.T) {
	span := SpanFromContext(context.Background())
	if span != nil {
		t.Error("Should return nil for empty context")
	}
}

// ============================================================================
// Global Tracer Tests
// ============================================================================

func TestGlobalTracer(t *testing.T) {
	tracer := NewTracer(TracerOptions{
		ServiceName: "global-test",
		Sampler:     AlwaysSample(),
	})

	SetGlobalTracer(tracer)

	retrieved := GetGlobalTracer()
	if retrieved != tracer {
		t.Error("Should get same tracer")
	}
}

func TestStartWithGlobalTracer(t *testing.T) {
	tracer := NewTracer(TracerOptions{
		ServiceName: "global-test",
		Sampler:     AlwaysSample(),
	})
	SetGlobalTracer(tracer)

	ctx, span := Start(context.Background(), "test")
	defer span.End()

	if !span.SpanContext().IsValid() {
		t.Error("Global Start should create valid span")
	}

	_ = ctx // Use ctx
}

// ============================================================================
// Concurrent Tests
// ============================================================================

func TestConcurrentSpans(t *testing.T) {
	var buf bytes.Buffer
	tracer := NewTracer(TracerOptions{
		ServiceName: "test",
		Exporter:    NewJSONExporter(&buf),
		Sampler:     AlwaysSample(),
	})

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			ctx := context.Background()
			_, span := tracer.Start(ctx, "concurrent")
			span.SetAttribute("n", n)
			span.End()
		}(i)
	}
	wg.Wait()

	stats := tracer.Stats()
	if stats.SpansCreated != 100 {
		t.Errorf("Expected 100 spans, got %d", stats.SpansCreated)
	}
}

// ============================================================================
// Benchmark Tests
// ============================================================================

func BenchmarkSpanCreation(b *testing.B) {
	tracer := NewTracer(TracerOptions{
		ServiceName: "bench",
		Exporter:    NoopExporter(),
		Sampler:     AlwaysSample(),
	})

	ctx := context.Background()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, span := tracer.Start(ctx, "bench")
		span.End()
	}
}

func BenchmarkSpanWithAttributes(b *testing.B) {
	tracer := NewTracer(TracerOptions{
		ServiceName: "bench",
		Exporter:    NoopExporter(),
		Sampler:     AlwaysSample(),
	})

	ctx := context.Background()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, span := tracer.Start(ctx, "bench")
		span.SetAttributes(
			String("key1", "value1"),
			Int("key2", 42),
			Bool("key3", true),
		)
		span.End()
	}
}

func BenchmarkNoopSpan(b *testing.B) {
	tracer := NewTracer(TracerOptions{
		ServiceName: "bench",
		Enabled:     false,
	})

	ctx := context.Background()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, span := tracer.Start(ctx, "bench")
		span.SetAttribute("key", "value")
		span.End()
	}
}

func BenchmarkGenerateTraceID(b *testing.B) {
	for i := 0; i < b.N; i++ {
		GenerateTraceID()
	}
}
