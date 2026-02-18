// Package server implements a TCP server with Memcached protocol support.
//
// The server provides a network interface to the RapidoDB storage engine,
// allowing clients to connect using any standard Memcached client library.
//
// Supported commands:
//   - get <key>           - Retrieve a value
//   - gets <key>          - Retrieve with CAS token
//   - set <key> ...       - Store a value
//   - add <key> ...       - Store only if not exists
//   - replace <key> ...   - Store only if exists
//   - delete <key>        - Remove a key
//   - incr <key> <delta>  - Increment numeric value
//   - decr <key> <delta>  - Decrement numeric value
//   - flush_all           - Clear all data
//   - stats               - Server statistics
//   - version             - Server version
//   - quit                - Close connection
package server

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vladgaus/RapidoDB/pkg/logging"
	"github.com/vladgaus/RapidoDB/pkg/lsm"
	"github.com/vladgaus/RapidoDB/pkg/metrics"
	"github.com/vladgaus/RapidoDB/pkg/ratelimit"
	"github.com/vladgaus/RapidoDB/pkg/tracing"
)

// Server is a TCP server that handles Memcached protocol requests.
type Server struct {
	// Configuration
	opts Options

	// Storage engine
	engine *lsm.Engine

	// Network
	listener net.Listener
	addr     string

	// Connection management
	connMu      sync.Mutex
	connections map[*Connection]struct{}
	connCount   atomic.Int64

	// Server state
	started   atomic.Bool
	closed    atomic.Bool
	closeOnce sync.Once

	// Request draining for graceful shutdown
	draining atomic.Bool

	// Rate limiting
	globalLimiter *ratelimit.GlobalLimiter
	clientLimiter *ratelimit.PerClientLimiter

	// Prometheus metrics
	metrics *metrics.RapiDoDBMetrics

	// Structured logging
	logger *logging.Logger

	// Distributed tracing
	tracer *tracing.Tracer

	// Statistics
	stats Stats

	// Shutdown coordination
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Options configures the server.
type Options struct {
	// Host is the address to bind to.
	Host string

	// Port is the port to listen on.
	Port int

	// ReadTimeout is the maximum duration for reading a request.
	ReadTimeout time.Duration

	// WriteTimeout is the maximum duration for writing a response.
	WriteTimeout time.Duration

	// IdleTimeout is the maximum duration a connection can be idle.
	IdleTimeout time.Duration

	// MaxConnections is the maximum number of concurrent connections.
	// 0 means unlimited.
	MaxConnections int

	// MaxKeySize is the maximum key size in bytes.
	MaxKeySize int

	// MaxValueSize is the maximum value size in bytes.
	MaxValueSize int

	// Version string to report.
	Version string

	// RateLimit configures rate limiting.
	RateLimit RateLimitOptions
}

// RateLimitOptions configures rate limiting.
type RateLimitOptions struct {
	// Enabled enables rate limiting.
	Enabled bool

	// GlobalEnabled enables global (server-wide) rate limiting.
	GlobalEnabled bool

	// GlobalRate is the global requests per second limit.
	GlobalRate float64

	// GlobalBurst is the global burst size.
	GlobalBurst int

	// PerClientEnabled enables per-client rate limiting.
	PerClientEnabled bool

	// PerClientRate is the per-client requests per second limit.
	PerClientRate float64

	// PerClientBurst is the per-client burst size.
	PerClientBurst int

	// Algorithm is the rate limiting algorithm ("token_bucket" or "sliding_window").
	Algorithm string

	// MaxIdleTime is how long a client can be idle before cleanup.
	MaxIdleTime time.Duration
}

// DefaultOptions returns sensible default options.
func DefaultOptions() Options {
	return Options{
		Host:           "127.0.0.1",
		Port:           11211,
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   30 * time.Second,
		IdleTimeout:    5 * time.Minute,
		MaxConnections: 10000,
		MaxKeySize:     250,
		MaxValueSize:   1024 * 1024, // 1MB
		Version:        "RapidoDB/1.0.0",
		RateLimit: RateLimitOptions{
			Enabled:          true,
			GlobalEnabled:    true,
			GlobalRate:       10000,
			GlobalBurst:      10000,
			PerClientEnabled: true,
			PerClientRate:    100,
			PerClientBurst:   100,
			Algorithm:        "token_bucket",
			MaxIdleTime:      5 * time.Minute,
		},
	}
}

// Stats holds server statistics.
type Stats struct {
	// Connection stats
	TotalConnections atomic.Uint64
	ActiveConns      atomic.Int64

	// Command stats
	CmdGet    atomic.Uint64
	CmdSet    atomic.Uint64
	CmdDelete atomic.Uint64
	CmdFlush  atomic.Uint64
	CmdOther  atomic.Uint64

	// Hit/miss stats
	GetHits   atomic.Uint64
	GetMisses atomic.Uint64

	// Byte stats
	BytesRead    atomic.Uint64
	BytesWritten atomic.Uint64

	// Rate limit stats
	RateLimitedGlobal atomic.Uint64
	RateLimitedClient atomic.Uint64

	// Timing
	StartTime time.Time
}

// New creates a new server with the given engine and options.
func New(engine *lsm.Engine, opts Options) *Server {
	ctx, cancel := context.WithCancel(context.Background())

	s := &Server{
		opts:        opts,
		engine:      engine,
		connections: make(map[*Connection]struct{}),
		ctx:         ctx,
		cancel:      cancel,
	}

	s.stats.StartTime = time.Now()

	// Initialize rate limiters
	if opts.RateLimit.Enabled {
		// Determine algorithm
		algo := ratelimit.AlgorithmTokenBucket
		if opts.RateLimit.Algorithm == "sliding_window" {
			algo = ratelimit.AlgorithmSlidingWindow
		}

		// Global limiter
		if opts.RateLimit.GlobalEnabled {
			s.globalLimiter = ratelimit.NewGlobalLimiter(ratelimit.Config{
				Algorithm: algo,
				Rate:      opts.RateLimit.GlobalRate,
				Burst:     opts.RateLimit.GlobalBurst,
			})
		}

		// Per-client limiter
		if opts.RateLimit.PerClientEnabled {
			s.clientLimiter = ratelimit.NewPerClientLimiter(ratelimit.Config{
				Algorithm:   algo,
				Rate:        opts.RateLimit.PerClientRate,
				Burst:       opts.RateLimit.PerClientBurst,
				MaxIdleTime: opts.RateLimit.MaxIdleTime,
			})
		}
	}

	return s
}

// SetMetrics sets the Prometheus metrics collector.
// Should be called before Start().
func (s *Server) SetMetrics(m *metrics.RapiDoDBMetrics) {
	s.metrics = m
}

// Metrics returns the Prometheus metrics collector.
func (s *Server) Metrics() *metrics.RapiDoDBMetrics {
	return s.metrics
}

// SetLogger sets the structured logger.
// Should be called before Start().
func (s *Server) SetLogger(l *logging.Logger) {
	s.logger = l.WithComponent("server")
}

// Logger returns the server's logger.
func (s *Server) Logger() *logging.Logger {
	if s.logger == nil {
		return logging.Default()
	}
	return s.logger
}

// SetTracer sets the distributed tracer.
// Should be called before Start().
func (s *Server) SetTracer(t *tracing.Tracer) {
	s.tracer = t
}

// Tracer returns the server's tracer, or nil if not set.
func (s *Server) Tracer() *tracing.Tracer {
	return s.tracer
}

// Start begins listening for connections.
func (s *Server) Start() error {
	if s.started.Swap(true) {
		return fmt.Errorf("server already started")
	}

	addr := fmt.Sprintf("%s:%d", s.opts.Host, s.opts.Port)

	lc := net.ListenConfig{}
	listener, err := lc.Listen(s.ctx, "tcp", addr)
	if err != nil {
		s.started.Store(false)
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	s.listener = listener
	s.addr = listener.Addr().String()

	// Start accept loop
	s.wg.Add(1)
	go s.acceptLoop()

	return nil
}

// Addr returns the server's listen address.
func (s *Server) Addr() string {
	return s.addr
}

// acceptLoop accepts new connections.
func (s *Server) acceptLoop() {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if s.closed.Load() {
				return
			}
			// Backoff and retry on accept error
			time.Sleep(10 * time.Millisecond)
			continue
		}

		// Check connection limit
		if s.opts.MaxConnections > 0 && s.connCount.Load() >= int64(s.opts.MaxConnections) {
			_ = conn.Close()
			continue
		}

		// Handle connection
		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

// handleConnection handles a single client connection.
func (s *Server) handleConnection(netConn net.Conn) {
	defer s.wg.Done()

	// Update stats
	s.stats.TotalConnections.Add(1)
	s.stats.ActiveConns.Add(1)
	s.connCount.Add(1)

	// Update metrics
	if s.metrics != nil {
		s.metrics.TotalConnections.Inc()
		s.metrics.ActiveConnections.Inc()
	}

	defer func() {
		s.stats.ActiveConns.Add(-1)
		s.connCount.Add(-1)
		if s.metrics != nil {
			s.metrics.ActiveConnections.Dec()
		}
	}()

	// Create connection handler
	conn := NewConnection(netConn, s)

	// Track connection
	s.connMu.Lock()
	s.connections[conn] = struct{}{}
	s.connMu.Unlock()

	defer func() {
		s.connMu.Lock()
		delete(s.connections, conn)
		s.connMu.Unlock()
		_ = conn.Close()
	}()

	// Handle requests
	conn.Serve(s.ctx)
}

// Close gracefully shuts down the server.
func (s *Server) Close() error {
	return s.GracefulClose(context.Background())
}

// GracefulClose shuts down the server gracefully, waiting for in-flight requests.
// The context can be used to set a timeout for the graceful shutdown.
func (s *Server) GracefulClose(ctx context.Context) error {
	var err error

	s.closeOnce.Do(func() {
		// Mark as draining first (stop accepting new requests in handlers)
		s.draining.Store(true)

		// Mark as closed
		s.closed.Store(true)

		// Cancel context to signal all goroutines
		s.cancel()

		// Close listener to stop accepting new connections
		if s.listener != nil {
			err = s.listener.Close()
		}

		// Wait for connections to drain with timeout
		drainDone := make(chan struct{})
		go func() {
			s.wg.Wait()
			close(drainDone)
		}()

		select {
		case <-drainDone:
			// All connections finished gracefully
		case <-ctx.Done():
			// Timeout - force close remaining connections
			s.connMu.Lock()
			for conn := range s.connections {
				_ = conn.Close()
			}
			s.connMu.Unlock()
			s.wg.Wait()
		}

		// Cleanup rate limiters
		if s.clientLimiter != nil {
			s.clientLimiter.Close()
		}
	})

	return err
}

// IsDraining returns true if the server is draining connections.
func (s *Server) IsDraining() bool {
	return s.draining.Load()
}

// Stats returns server statistics. Returns a pointer to avoid
// copying atomic values (copylocks).
func (s *Server) Stats() *Stats {
	return &s.stats
}

// Engine returns the storage engine.
func (s *Server) Engine() *lsm.Engine {
	return s.engine
}

// Options returns the server options.
func (s *Server) Options() Options {
	return s.opts
}

// ============================================================================
// Health Check Interface Implementation
// ============================================================================

// ActiveConnections returns the current number of active connections.
// Implements health.ServerStatsProvider.
func (s *Server) ActiveConnections() int64 {
	return s.stats.ActiveConns.Load()
}

// TotalConnections returns the total number of connections since start.
// Implements health.ServerStatsProvider.
func (s *Server) TotalConnections() uint64 {
	return s.stats.TotalConnections.Load()
}

// GetHits returns the number of cache hits.
// Implements health.ServerStatsProvider.
func (s *Server) GetHits() uint64 {
	return s.stats.GetHits.Load()
}

// GetMisses returns the number of cache misses.
// Implements health.ServerStatsProvider.
func (s *Server) GetMisses() uint64 {
	return s.stats.GetMisses.Load()
}

// ============================================================================
// Rate Limiting Interface
// ============================================================================

// CheckRateLimit checks if a request from the given client IP is allowed.
// Returns true if allowed, false if rate limited.
// Also returns a retry-after duration if rate limited.
func (s *Server) CheckRateLimit(clientIP string) (allowed bool, retryAfter time.Duration) {
	// If rate limiting is disabled, always allow
	if !s.opts.RateLimit.Enabled {
		return true, 0
	}

	// Check global rate limit first
	if s.globalLimiter != nil {
		if !s.globalLimiter.Allow() {
			s.stats.RateLimitedGlobal.Add(1)
			if s.metrics != nil {
				s.metrics.RateLimitedTotal.Inc()
			}
			result := s.globalLimiter.Check()
			return false, result.RetryAfter
		}
	}

	// Check per-client rate limit
	if s.clientLimiter != nil {
		if !s.clientLimiter.Allow(clientIP) {
			s.stats.RateLimitedClient.Add(1)
			if s.metrics != nil {
				s.metrics.RateLimitedTotal.Inc()
			}
			result := s.clientLimiter.Check(clientIP)
			return false, result.RetryAfter
		}
	}

	return true, 0
}

// RateLimitStats returns rate limiting statistics.
func (s *Server) RateLimitStats() RateLimitStats {
	stats := RateLimitStats{
		Enabled:       s.opts.RateLimit.Enabled,
		GlobalLimited: s.stats.RateLimitedGlobal.Load(),
		ClientLimited: s.stats.RateLimitedClient.Load(),
	}

	if s.clientLimiter != nil {
		clientStats := s.clientLimiter.Stats()
		stats.ActiveClients = clientStats.ActiveClients
		stats.TotalRequests = clientStats.TotalRequests
		stats.TotalAllowed = clientStats.TotalAllowed
		stats.TotalDenied = clientStats.TotalDenied
	}

	return stats
}

// RateLimitStats holds rate limiting statistics.
type RateLimitStats struct {
	Enabled       bool
	GlobalLimited uint64
	ClientLimited uint64
	ActiveClients int
	TotalRequests uint64
	TotalAllowed  uint64
	TotalDenied   uint64
}

// GlobalLimiter returns the global rate limiter (for testing/monitoring).
func (s *Server) GlobalLimiter() *ratelimit.GlobalLimiter {
	return s.globalLimiter
}

// ClientLimiter returns the per-client rate limiter (for testing/monitoring).
func (s *Server) ClientLimiter() *ratelimit.PerClientLimiter {
	return s.clientLimiter
}
