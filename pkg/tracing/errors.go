package tracing

import "errors"

var (
	// ErrInvalidTraceID is returned when a trace ID string is invalid.
	ErrInvalidTraceID = errors.New("invalid trace ID")

	// ErrInvalidSpanID is returned when a span ID string is invalid.
	ErrInvalidSpanID = errors.New("invalid span ID")

	// ErrExporterClosed is returned when exporting to a closed exporter.
	ErrExporterClosed = errors.New("exporter is closed")
)
