package logging

import (
	"crypto/rand"
	"encoding/hex"
	"sync/atomic"
	"time"
)

// RequestIDGenerator generates unique request IDs.
type RequestIDGenerator struct {
	counter atomic.Uint64
	prefix  string
}

// NewRequestIDGenerator creates a new request ID generator.
func NewRequestIDGenerator() *RequestIDGenerator {
	// Generate a random prefix for this instance
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	prefix := hex.EncodeToString(b)

	return &RequestIDGenerator{
		prefix: prefix,
	}
}

// Generate generates a new unique request ID.
// Format: <prefix>-<counter>-<timestamp_hex>
// Example: "a1b2c3d4-00000001-019abc12"
func (g *RequestIDGenerator) Generate() string {
	counter := g.counter.Add(1)
	ts := time.Now().UnixMilli() & 0xFFFFFFFF // Lower 32 bits

	return g.prefix + "-" +
		hexPadded(counter, 8) + "-" +
		hexPadded(uint64(ts), 8)
}

// GenerateShort generates a shorter request ID.
// Format: <prefix><counter_hex>
// Example: "a1b2c3d4f5e6"
func (g *RequestIDGenerator) GenerateShort() string {
	counter := g.counter.Add(1)
	return g.prefix + hexPadded(counter, 4)
}

// hexPadded converts a uint64 to hex with padding.
func hexPadded(n uint64, minLen int) string {
	const hexDigits = "0123456789abcdef"

	var buf [16]byte
	pos := len(buf)

	for n > 0 || pos > len(buf)-minLen {
		pos--
		buf[pos] = hexDigits[n&0xf]
		n >>= 4
	}

	return string(buf[pos:])
}

// ============================================================================
// Global request ID generator
// ============================================================================

var globalRequestIDGen = NewRequestIDGenerator()

// GenerateRequestID generates a new unique request ID.
func GenerateRequestID() string {
	return globalRequestIDGen.Generate()
}

// GenerateShortRequestID generates a shorter request ID.
func GenerateShortRequestID() string {
	return globalRequestIDGen.GenerateShort()
}

// ============================================================================
// Request scoped logger
// ============================================================================

// RequestLogger wraps a logger with request-scoped attributes.
type RequestLogger struct {
	*Logger
	requestID string
	startTime time.Time
	attrs     []any
}

// NewRequestLogger creates a new request-scoped logger.
func NewRequestLogger(base *Logger, requestID string) *RequestLogger {
	return &RequestLogger{
		Logger:    base.WithRequestID(requestID),
		requestID: requestID,
		startTime: time.Now(),
		attrs:     make([]any, 0),
	}
}

// RequestID returns the request ID.
func (rl *RequestLogger) RequestID() string {
	return rl.requestID
}

// Duration returns the time elapsed since the request started.
func (rl *RequestLogger) Duration() time.Duration {
	return time.Since(rl.startTime)
}

// With adds attributes to the logger.
func (rl *RequestLogger) With(args ...any) *RequestLogger {
	return &RequestLogger{
		Logger:    rl.Logger.With(args...),
		requestID: rl.requestID,
		startTime: rl.startTime,
		attrs:     append(rl.attrs, args...),
	}
}

// Start logs the start of a request.
func (rl *RequestLogger) Start(operation string, args ...any) {
	allArgs := append([]any{"operation", operation}, args...)
	rl.Debug("request started", allArgs...)
}

// End logs the end of a request with duration.
func (rl *RequestLogger) End(operation string, err error, args ...any) {
	duration := rl.Duration()
	allArgs := append([]any{
		"operation", operation,
		"duration_ms", float64(duration.Nanoseconds()) / 1e6,
	}, args...)

	if err != nil {
		allArgs = append(allArgs, "error", err.Error())
		rl.Error("request failed", allArgs...)
	} else {
		rl.Info("request completed", allArgs...)
	}
}

// Success logs a successful operation.
func (rl *RequestLogger) Success(operation string, args ...any) {
	rl.End(operation, nil, args...)
}

// Failure logs a failed operation.
func (rl *RequestLogger) Failure(operation string, err error, args ...any) {
	rl.End(operation, err, args...)
}
