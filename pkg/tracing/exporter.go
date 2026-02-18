package tracing

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// ============================================================================
// Exporter Interface
// ============================================================================

// Exporter exports spans to a backend.
type Exporter interface {
	// Export exports a span.
	Export(span SpanData)

	// Shutdown shuts down the exporter.
	Shutdown() error
}

// ============================================================================
// JSON Exporter (stdout/file)
// ============================================================================

// JSONExporter exports spans as JSON to a writer.
type JSONExporter struct {
	mu      sync.Mutex
	writer  io.Writer
	encoder *json.Encoder
	closed  atomic.Bool
}

// JSONSpan is the JSON representation of a span.
type JSONSpan struct {
	TraceID      string                 `json:"trace_id"`
	SpanID       string                 `json:"span_id"`
	ParentSpanID string                 `json:"parent_span_id,omitempty"`
	Name         string                 `json:"name"`
	Kind         string                 `json:"kind"`
	StartTime    string                 `json:"start_time"`
	EndTime      string                 `json:"end_time"`
	DurationMs   float64                `json:"duration_ms"`
	Status       string                 `json:"status"`
	StatusMsg    string                 `json:"status_message,omitempty"`
	Attributes   map[string]interface{} `json:"attributes,omitempty"`
	Events       []JSONEvent            `json:"events,omitempty"`
}

// JSONEvent is the JSON representation of an event.
type JSONEvent struct {
	Name       string                 `json:"name"`
	Timestamp  string                 `json:"timestamp"`
	Attributes map[string]interface{} `json:"attributes,omitempty"`
}

// NewJSONExporter creates a new JSON exporter.
func NewJSONExporter(w io.Writer) *JSONExporter {
	return &JSONExporter{
		writer:  w,
		encoder: json.NewEncoder(w),
	}
}

// Export exports a span as JSON.
func (e *JSONExporter) Export(span SpanData) {
	if e.closed.Load() {
		return
	}

	// Convert to JSON format
	js := JSONSpan{
		TraceID:    span.SpanContext.TraceID.String(),
		SpanID:     span.SpanContext.SpanID.String(),
		Name:       span.Name,
		Kind:       span.Kind.String(),
		StartTime:  span.StartTime.Format(time.RFC3339Nano),
		EndTime:    span.EndTime.Format(time.RFC3339Nano),
		DurationMs: float64(span.EndTime.Sub(span.StartTime).Nanoseconds()) / 1e6,
		Status:     span.Status.Code.String(),
		StatusMsg:  span.Status.Description,
	}

	if span.Parent.SpanID.IsValid() {
		js.ParentSpanID = span.Parent.SpanID.String()
	}

	// Convert attributes
	if len(span.Attributes) > 0 {
		js.Attributes = make(map[string]interface{})
		for _, attr := range span.Attributes {
			js.Attributes[attr.Key] = attr.Value
		}
	}

	// Convert events
	if len(span.Events) > 0 {
		js.Events = make([]JSONEvent, len(span.Events))
		for i, evt := range span.Events {
			je := JSONEvent{
				Name:      evt.Name,
				Timestamp: evt.Timestamp.Format(time.RFC3339Nano),
			}
			if len(evt.Attributes) > 0 {
				je.Attributes = make(map[string]interface{})
				for _, attr := range evt.Attributes {
					je.Attributes[attr.Key] = attr.Value
				}
			}
			js.Events[i] = je
		}
	}

	e.mu.Lock()
	_ = e.encoder.Encode(js)
	e.mu.Unlock()
}

// Shutdown closes the exporter.
func (e *JSONExporter) Shutdown() error {
	e.closed.Store(true)
	return nil
}

// ============================================================================
// Batch Exporter
// ============================================================================

// BatchExporter batches spans before sending them.
type BatchExporter struct {
	mu       sync.Mutex
	exporter Exporter
	batch    []SpanData
	batchMax int
	timeout  time.Duration
	timer    *time.Timer
	closed   atomic.Bool
	done     chan struct{}
}

// BatchExporterOptions configures the batch exporter.
type BatchExporterOptions struct {
	// MaxBatchSize is the maximum number of spans to batch.
	// Default: 100
	MaxBatchSize int

	// BatchTimeout is the maximum time to wait before flushing.
	// Default: 5s
	BatchTimeout time.Duration
}

// NewBatchExporter creates a new batch exporter.
func NewBatchExporter(exporter Exporter, opts BatchExporterOptions) *BatchExporter {
	if opts.MaxBatchSize <= 0 {
		opts.MaxBatchSize = 100
	}
	if opts.BatchTimeout <= 0 {
		opts.BatchTimeout = 5 * time.Second
	}

	b := &BatchExporter{
		exporter: exporter,
		batch:    make([]SpanData, 0, opts.MaxBatchSize),
		batchMax: opts.MaxBatchSize,
		timeout:  opts.BatchTimeout,
		done:     make(chan struct{}),
	}

	b.timer = time.AfterFunc(opts.BatchTimeout, b.flush)
	return b
}

// Export adds a span to the batch.
func (b *BatchExporter) Export(span SpanData) {
	if b.closed.Load() {
		return
	}

	b.mu.Lock()
	b.batch = append(b.batch, span)

	if len(b.batch) >= b.batchMax {
		batch := b.batch
		b.batch = make([]SpanData, 0, b.batchMax)
		b.timer.Reset(b.timeout)
		b.mu.Unlock()

		// Export batch
		for _, s := range batch {
			b.exporter.Export(s)
		}
		return
	}

	b.mu.Unlock()
}

// flush flushes the current batch.
func (b *BatchExporter) flush() {
	b.mu.Lock()
	if len(b.batch) == 0 {
		b.timer.Reset(b.timeout)
		b.mu.Unlock()
		return
	}

	batch := b.batch
	b.batch = make([]SpanData, 0, b.batchMax)
	b.timer.Reset(b.timeout)
	b.mu.Unlock()

	for _, s := range batch {
		b.exporter.Export(s)
	}
}

// Shutdown shuts down the batch exporter.
func (b *BatchExporter) Shutdown() error {
	if b.closed.Swap(true) {
		return nil
	}

	b.timer.Stop()
	b.flush()
	close(b.done)
	return b.exporter.Shutdown()
}

// ============================================================================
// Jaeger Thrift Exporter (HTTP)
// ============================================================================

// JaegerExporter exports spans to Jaeger via HTTP/Thrift.
type JaegerExporter struct {
	endpoint string
	client   *http.Client
	closed   atomic.Bool
}

// JaegerExporterOptions configures the Jaeger exporter.
type JaegerExporterOptions struct {
	// Endpoint is the Jaeger collector HTTP endpoint.
	// Default: http://localhost:14268/api/traces
	Endpoint string

	// HTTPClient is the HTTP client to use.
	// Default: http.DefaultClient with 10s timeout
	HTTPClient *http.Client
}

// NewJaegerExporter creates a new Jaeger exporter.
func NewJaegerExporter(opts JaegerExporterOptions) *JaegerExporter {
	if opts.Endpoint == "" {
		opts.Endpoint = "http://localhost:14268/api/traces"
	}
	if opts.HTTPClient == nil {
		opts.HTTPClient = &http.Client{
			Timeout: 10 * time.Second,
		}
	}

	return &JaegerExporter{
		endpoint: opts.Endpoint,
		client:   opts.HTTPClient,
	}
}

// JaegerSpan is the Jaeger span format.
type JaegerSpan struct {
	TraceIDLow    uint64            `json:"traceIdLow"`
	TraceIDHigh   uint64            `json:"traceIdHigh"`
	SpanID        uint64            `json:"spanId"`
	ParentSpanID  uint64            `json:"parentSpanId,omitempty"`
	OperationName string            `json:"operationName"`
	References    []JaegerReference `json:"references,omitempty"`
	Flags         int               `json:"flags"`
	StartTime     int64             `json:"startTime"` // microseconds
	Duration      int64             `json:"duration"`  // microseconds
	Tags          []JaegerTag       `json:"tags,omitempty"`
	Logs          []JaegerLog       `json:"logs,omitempty"`
	Process       *JaegerProcess    `json:"process,omitempty"`
}

// JaegerReference is a span reference.
type JaegerReference struct {
	RefType     string `json:"refType"`
	TraceIDLow  uint64 `json:"traceIdLow"`
	TraceIDHigh uint64 `json:"traceIdHigh"`
	SpanID      uint64 `json:"spanId"`
}

// JaegerTag is a span tag.
type JaegerTag struct {
	Key   string      `json:"key"`
	Type  string      `json:"type"`
	Value interface{} `json:"value"`
}

// JaegerLog is a span log.
type JaegerLog struct {
	Timestamp int64       `json:"timestamp"` // microseconds
	Fields    []JaegerTag `json:"fields"`
}

// JaegerProcess describes the traced process.
type JaegerProcess struct {
	ServiceName string      `json:"serviceName"`
	Tags        []JaegerTag `json:"tags,omitempty"`
}

// JaegerBatch is a batch of spans.
type JaegerBatch struct {
	Process *JaegerProcess `json:"process"`
	Spans   []JaegerSpan   `json:"spans"`
}

// Export exports a span to Jaeger.
func (e *JaegerExporter) Export(span SpanData) {
	if e.closed.Load() {
		return
	}

	// Convert span to Jaeger format
	js := e.convertSpan(span)

	// Create batch
	batch := JaegerBatch{
		Process: &JaegerProcess{
			ServiceName: getServiceName(span.Attributes),
		},
		Spans: []JaegerSpan{js},
	}

	// Send to Jaeger
	data, err := json.Marshal(batch)
	if err != nil {
		return
	}

	req, err := http.NewRequestWithContext(context.Background(), "POST", e.endpoint, bytes.NewReader(data))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := e.client.Do(req)
	if err != nil {
		return
	}
	defer func() { _ = resp.Body.Close() }()
}

func (e *JaegerExporter) convertSpan(span SpanData) JaegerSpan {
	// Convert trace ID to high/low
	var traceIDHigh, traceIDLow uint64
	for i := 0; i < 8; i++ {
		traceIDHigh = (traceIDHigh << 8) | uint64(span.SpanContext.TraceID[i])
		traceIDLow = (traceIDLow << 8) | uint64(span.SpanContext.TraceID[i+8])
	}

	// Convert span ID
	var spanID uint64
	for i := 0; i < 8; i++ {
		spanID = (spanID << 8) | uint64(span.SpanContext.SpanID[i])
	}

	// Convert parent span ID
	var parentSpanID uint64
	if span.Parent.SpanID.IsValid() {
		for i := 0; i < 8; i++ {
			parentSpanID = (parentSpanID << 8) | uint64(span.Parent.SpanID[i])
		}
	}

	js := JaegerSpan{
		TraceIDHigh:   traceIDHigh,
		TraceIDLow:    traceIDLow,
		SpanID:        spanID,
		ParentSpanID:  parentSpanID,
		OperationName: span.Name,
		StartTime:     span.StartTime.UnixMicro(),
		Duration:      span.EndTime.Sub(span.StartTime).Microseconds(),
		Flags:         1, // Sampled
	}

	// Convert attributes to tags
	for _, attr := range span.Attributes {
		js.Tags = append(js.Tags, convertToJaegerTag(attr))
	}

	// Add span kind
	js.Tags = append(js.Tags, JaegerTag{
		Key:   "span.kind",
		Type:  "string",
		Value: span.Kind.String(),
	})

	// Convert events to logs
	for _, evt := range span.Events {
		log := JaegerLog{
			Timestamp: evt.Timestamp.UnixMicro(),
		}
		log.Fields = append(log.Fields, JaegerTag{
			Key:   "event",
			Type:  "string",
			Value: evt.Name,
		})
		for _, attr := range evt.Attributes {
			log.Fields = append(log.Fields, convertToJaegerTag(attr))
		}
		js.Logs = append(js.Logs, log)
	}

	return js
}

// Shutdown shuts down the exporter.
func (e *JaegerExporter) Shutdown() error {
	e.closed.Store(true)
	return nil
}

// ============================================================================
// Zipkin Exporter (HTTP)
// ============================================================================

// ZipkinExporter exports spans to Zipkin via HTTP.
type ZipkinExporter struct {
	endpoint string
	client   *http.Client
	closed   atomic.Bool
}

// ZipkinExporterOptions configures the Zipkin exporter.
type ZipkinExporterOptions struct {
	// Endpoint is the Zipkin collector HTTP endpoint.
	// Default: http://localhost:9411/api/v2/spans
	Endpoint string

	// HTTPClient is the HTTP client to use.
	HTTPClient *http.Client
}

// NewZipkinExporter creates a new Zipkin exporter.
func NewZipkinExporter(opts ZipkinExporterOptions) *ZipkinExporter {
	if opts.Endpoint == "" {
		opts.Endpoint = "http://localhost:9411/api/v2/spans"
	}
	if opts.HTTPClient == nil {
		opts.HTTPClient = &http.Client{
			Timeout: 10 * time.Second,
		}
	}

	return &ZipkinExporter{
		endpoint: opts.Endpoint,
		client:   opts.HTTPClient,
	}
}

// ZipkinSpan is the Zipkin v2 span format.
type ZipkinSpan struct {
	TraceID       string             `json:"traceId"`
	ID            string             `json:"id"`
	ParentID      string             `json:"parentId,omitempty"`
	Name          string             `json:"name"`
	Kind          string             `json:"kind,omitempty"`
	Timestamp     int64              `json:"timestamp"` // microseconds
	Duration      int64              `json:"duration"`  // microseconds
	LocalEndpoint *ZipkinEndpoint    `json:"localEndpoint,omitempty"`
	Tags          map[string]string  `json:"tags,omitempty"`
	Annotations   []ZipkinAnnotation `json:"annotations,omitempty"`
}

// ZipkinEndpoint describes a network endpoint.
type ZipkinEndpoint struct {
	ServiceName string `json:"serviceName"`
}

// ZipkinAnnotation is an event annotation.
type ZipkinAnnotation struct {
	Timestamp int64  `json:"timestamp"` // microseconds
	Value     string `json:"value"`
}

// Export exports a span to Zipkin.
func (e *ZipkinExporter) Export(span SpanData) {
	if e.closed.Load() {
		return
	}

	// Convert to Zipkin format
	zs := ZipkinSpan{
		TraceID:   span.SpanContext.TraceID.String(),
		ID:        span.SpanContext.SpanID.String(),
		Name:      span.Name,
		Timestamp: span.StartTime.UnixMicro(),
		Duration:  span.EndTime.Sub(span.StartTime).Microseconds(),
		LocalEndpoint: &ZipkinEndpoint{
			ServiceName: getServiceName(span.Attributes),
		},
	}

	if span.Parent.SpanID.IsValid() {
		zs.ParentID = span.Parent.SpanID.String()
	}

	// Map span kind
	switch span.Kind {
	case SpanKindServer:
		zs.Kind = "SERVER"
	case SpanKindClient:
		zs.Kind = "CLIENT"
	case SpanKindProducer:
		zs.Kind = "PRODUCER"
	case SpanKindConsumer:
		zs.Kind = "CONSUMER"
	}

	// Convert attributes to tags
	if len(span.Attributes) > 0 {
		zs.Tags = make(map[string]string)
		for _, attr := range span.Attributes {
			zs.Tags[attr.Key] = fmt.Sprintf("%v", attr.Value)
		}
	}

	// Convert events to annotations
	for _, evt := range span.Events {
		zs.Annotations = append(zs.Annotations, ZipkinAnnotation{
			Timestamp: evt.Timestamp.UnixMicro(),
			Value:     evt.Name,
		})
	}

	// Send to Zipkin
	data, err := json.Marshal([]ZipkinSpan{zs})
	if err != nil {
		return
	}

	req, err := http.NewRequestWithContext(context.Background(), "POST", e.endpoint, bytes.NewReader(data))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := e.client.Do(req)
	if err != nil {
		return
	}
	defer func() { _ = resp.Body.Close() }()
}

// Shutdown shuts down the exporter.
func (e *ZipkinExporter) Shutdown() error {
	e.closed.Store(true)
	return nil
}

// ============================================================================
// Multi-Exporter
// ============================================================================

// MultiExporter exports to multiple backends.
type MultiExporter struct {
	exporters []Exporter
}

// NewMultiExporter creates an exporter that sends to multiple backends.
func NewMultiExporter(exporters ...Exporter) *MultiExporter {
	return &MultiExporter{exporters: exporters}
}

// Export exports to all backends.
func (m *MultiExporter) Export(span SpanData) {
	for _, e := range m.exporters {
		e.Export(span)
	}
}

// Shutdown shuts down all exporters.
func (m *MultiExporter) Shutdown() error {
	var lastErr error
	for _, e := range m.exporters {
		if err := e.Shutdown(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// ============================================================================
// No-op Exporter
// ============================================================================

type noopExporter struct{}

// NoopExporter returns an exporter that does nothing.
func NoopExporter() Exporter {
	return noopExporter{}
}

func (noopExporter) Export(_ SpanData) {}
func (noopExporter) Shutdown() error   { return nil }

// ============================================================================
// Helper Functions
// ============================================================================

func getServiceName(attrs []Attribute) string {
	for _, attr := range attrs {
		if attr.Key == "service.name" {
			if s, ok := attr.Value.(string); ok {
				return s
			}
		}
	}
	return "unknown"
}

func convertToJaegerTag(attr Attribute) JaegerTag {
	tag := JaegerTag{Key: attr.Key}

	switch v := attr.Value.(type) {
	case string:
		tag.Type = "string"
		tag.Value = v
	case bool:
		tag.Type = "bool"
		tag.Value = v
	case int, int32, int64:
		tag.Type = "int64"
		tag.Value = v
	case float32, float64:
		tag.Type = "float64"
		tag.Value = v
	default:
		tag.Type = "string"
		tag.Value = fmt.Sprintf("%v", v)
	}

	return tag
}
