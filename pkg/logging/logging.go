// Package logging provides structured logging for RapidoDB.
//
// Features:
//   - JSON and text log formats
//   - Log levels (debug, info, warn, error)
//   - Request ID tracing
//   - Configurable outputs (stdout, stderr, file)
//   - Log rotation support
//   - Zero dependencies (uses Go 1.21+ log/slog)
//
// Example usage:
//
//	logger := logging.New(logging.Options{
//	    Level:  logging.LevelInfo,
//	    Format: logging.FormatJSON,
//	    Output: os.Stdout,
//	})
//
//	logger.Info("server started", "port", 11211)
//	logger.Error("connection failed", "error", err, "client", clientIP)
package logging

import (
	"context"
	"io"
	"log/slog"
	"os"
	"runtime"
	"sync"
	"time"
)

// Level represents log severity levels.
type Level int

const (
	LevelDebug Level = iota - 4
	LevelInfo  Level = 0
	LevelWarn  Level = 4
	LevelError Level = 8
)

// String returns the string representation of the level.
func (l Level) String() string {
	switch {
	case l < LevelInfo:
		return "DEBUG"
	case l < LevelWarn:
		return "INFO"
	case l < LevelError:
		return "WARN"
	default:
		return "ERROR"
	}
}

// ParseLevel parses a level string.
func ParseLevel(s string) Level {
	switch s {
	case "debug", "DEBUG":
		return LevelDebug
	case "info", "INFO", "":
		return LevelInfo
	case "warn", "WARN", "warning", "WARNING":
		return LevelWarn
	case "error", "ERROR":
		return LevelError
	default:
		return LevelInfo
	}
}

// Format represents the log output format.
type Format int

const (
	FormatJSON Format = iota
	FormatText
)

// String returns the string representation of the format.
func (f Format) String() string {
	switch f {
	case FormatJSON:
		return "json"
	case FormatText:
		return "text"
	default:
		return "json"
	}
}

// ParseFormat parses a format string.
func ParseFormat(s string) Format {
	switch s {
	case "json", "JSON":
		return FormatJSON
	case "text", "TEXT":
		return FormatText
	default:
		return FormatJSON
	}
}

// Options configures the logger.
type Options struct {
	// Level is the minimum log level.
	Level Level

	// Format is the output format (json or text).
	Format Format

	// Output is where logs are written.
	// Default: os.Stdout
	Output io.Writer

	// AddSource includes source file and line in logs.
	AddSource bool

	// TimeFormat is the time format string.
	// Default: RFC3339
	TimeFormat string

	// Component is an optional component name to include.
	Component string
}

// DefaultOptions returns sensible defaults.
func DefaultOptions() Options {
	return Options{
		Level:      LevelInfo,
		Format:     FormatJSON,
		Output:     os.Stdout,
		AddSource:  false,
		TimeFormat: time.RFC3339,
	}
}

// Logger is the structured logger for RapidoDB.
type Logger struct {
	slog    *slog.Logger
	level   *slog.LevelVar
	opts    Options
	mu      sync.RWMutex
	outputs []io.Writer
}

// New creates a new logger with the given options.
func New(opts Options) *Logger {
	if opts.Output == nil {
		opts.Output = os.Stdout
	}
	if opts.TimeFormat == "" {
		opts.TimeFormat = time.RFC3339
	}

	levelVar := &slog.LevelVar{}
	levelVar.Set(slog.Level(opts.Level))

	var handler slog.Handler
	handlerOpts := &slog.HandlerOptions{
		Level:     levelVar,
		AddSource: opts.AddSource,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// Custom time format
			if a.Key == slog.TimeKey {
				if t, ok := a.Value.Any().(time.Time); ok {
					a.Value = slog.StringValue(t.Format(opts.TimeFormat))
				}
			}
			// Map level to uppercase string
			if a.Key == slog.LevelKey {
				level := a.Value.Any().(slog.Level)
				a.Value = slog.StringValue(Level(level).String())
			}
			return a
		},
	}

	switch opts.Format {
	case FormatText:
		handler = slog.NewTextHandler(opts.Output, handlerOpts)
	default:
		handler = slog.NewJSONHandler(opts.Output, handlerOpts)
	}

	l := &Logger{
		slog:    slog.New(handler),
		level:   levelVar,
		opts:    opts,
		outputs: []io.Writer{opts.Output},
	}

	// Add component if specified
	if opts.Component != "" {
		l.slog = l.slog.With("component", opts.Component)
	}

	return l
}

// SetLevel changes the log level dynamically.
func (l *Logger) SetLevel(level Level) {
	l.level.Set(slog.Level(level))
}

// GetLevel returns the current log level.
func (l *Logger) GetLevel() Level {
	return Level(l.level.Level())
}

// With returns a new logger with additional attributes.
func (l *Logger) With(args ...any) *Logger {
	return &Logger{
		slog:    l.slog.With(args...),
		level:   l.level,
		opts:    l.opts,
		outputs: l.outputs,
	}
}

// WithComponent returns a new logger with a component name.
func (l *Logger) WithComponent(component string) *Logger {
	return l.With("component", component)
}

// WithRequestID returns a new logger with a request ID.
func (l *Logger) WithRequestID(requestID string) *Logger {
	return l.With("request_id", requestID)
}

// Debug logs at debug level.
func (l *Logger) Debug(msg string, args ...any) {
	l.slog.Debug(msg, args...)
}

// Info logs at info level.
func (l *Logger) Info(msg string, args ...any) {
	l.slog.Info(msg, args...)
}

// Warn logs at warn level.
func (l *Logger) Warn(msg string, args ...any) {
	l.slog.Warn(msg, args...)
}

// Error logs at error level.
func (l *Logger) Error(msg string, args ...any) {
	l.slog.Error(msg, args...)
}

// DebugContext logs at debug level with context.
func (l *Logger) DebugContext(ctx context.Context, msg string, args ...any) {
	l.slog.DebugContext(ctx, msg, args...)
}

// InfoContext logs at info level with context.
func (l *Logger) InfoContext(ctx context.Context, msg string, args ...any) {
	l.slog.InfoContext(ctx, msg, args...)
}

// WarnContext logs at warn level with context.
func (l *Logger) WarnContext(ctx context.Context, msg string, args ...any) {
	l.slog.WarnContext(ctx, msg, args...)
}

// ErrorContext logs at error level with context.
func (l *Logger) ErrorContext(ctx context.Context, msg string, args ...any) {
	l.slog.ErrorContext(ctx, msg, args...)
}

// Log logs at the specified level.
func (l *Logger) Log(level Level, msg string, args ...any) {
	l.slog.Log(context.Background(), slog.Level(level), msg, args...)
}

// LogContext logs at the specified level with context.
func (l *Logger) LogContext(ctx context.Context, level Level, msg string, args ...any) {
	l.slog.Log(ctx, slog.Level(level), msg, args...)
}

// Handler returns the underlying slog handler.
func (l *Logger) Handler() slog.Handler {
	return l.slog.Handler()
}

// Slog returns the underlying slog.Logger.
func (l *Logger) Slog() *slog.Logger {
	return l.slog
}

// ============================================================================
// Context-based logging
// ============================================================================

type contextKey string

const (
	requestIDKey contextKey = "request_id"
	loggerKey    contextKey = "logger"
)

// WithContext returns a new context with the logger attached.
func (l *Logger) WithContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, loggerKey, l)
}

// FromContext returns the logger from context, or the default logger.
func FromContext(ctx context.Context) *Logger {
	if l, ok := ctx.Value(loggerKey).(*Logger); ok {
		return l
	}
	return defaultLogger
}

// ContextWithRequestID adds a request ID to the context.
func ContextWithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, requestIDKey, requestID)
}

// RequestIDFromContext returns the request ID from context.
func RequestIDFromContext(ctx context.Context) string {
	if id, ok := ctx.Value(requestIDKey).(string); ok {
		return id
	}
	return ""
}

// ============================================================================
// Default logger
// ============================================================================

var (
	defaultLogger     *Logger
	defaultLoggerOnce sync.Once
)

// Default returns the default logger.
func Default() *Logger {
	defaultLoggerOnce.Do(func() {
		defaultLogger = New(DefaultOptions())
	})
	return defaultLogger
}

// SetDefault sets the default logger.
func SetDefault(l *Logger) {
	defaultLogger = l
}

// Debug logs at debug level using the default logger.
func Debug(msg string, args ...any) {
	Default().Debug(msg, args...)
}

// Info logs at info level using the default logger.
func Info(msg string, args ...any) {
	Default().Info(msg, args...)
}

// Warn logs at warn level using the default logger.
func Warn(msg string, args ...any) {
	Default().Warn(msg, args...)
}

// Error logs at error level using the default logger.
func Error(msg string, args ...any) {
	Default().Error(msg, args...)
}

// ============================================================================
// Utility functions
// ============================================================================

// Err returns an error attribute for logging.
func Err(err error) slog.Attr {
	return slog.Any("error", err)
}

// Duration returns a duration attribute in milliseconds.
func Duration(key string, d time.Duration) slog.Attr {
	return slog.Float64(key+"_ms", float64(d.Nanoseconds())/1e6)
}

// Caller returns the caller's file and line.
func Caller(skip int) slog.Attr {
	_, file, line, ok := runtime.Caller(skip + 1)
	if !ok {
		return slog.String("caller", "unknown")
	}
	// Shorten path
	short := file
	for i := len(file) - 1; i > 0; i-- {
		if file[i] == '/' {
			short = file[i+1:]
			break
		}
	}
	return slog.String("caller", short+":"+itoa(line))
}

// itoa converts int to string without strconv.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	neg := i < 0
	if neg {
		i = -i
	}
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}
