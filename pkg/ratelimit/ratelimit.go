// Package ratelimit provides rate limiting functionality for RapidoDB.
//
// The package implements multiple rate limiting algorithms:
//   - Token Bucket: Smooth rate limiting with burst support
//   - Sliding Window: Precise rate limiting over time windows
//
// Features:
//   - Per-client (IP-based) rate limiting
//   - Global throughput limits
//   - Backpressure signaling with retry-after
//   - Automatic cleanup of stale client entries
//
// All implementations use only Go standard library (zero dependencies).
package ratelimit

import (
	"sync"
	"time"
)

// Limiter is the interface for rate limiters.
type Limiter interface {
	// Allow checks if a request is allowed.
	// Returns true if allowed, false if rate limited.
	Allow() bool

	// AllowN checks if n requests are allowed.
	AllowN(n int) bool

	// Wait blocks until a request is allowed or context deadline.
	// Returns the time waited.
	Wait() time.Duration

	// Reserve reserves a token and returns when it will be available.
	// Returns zero duration if immediately available.
	Reserve() time.Duration

	// Tokens returns the current number of available tokens.
	Tokens() float64

	// Reset resets the limiter to its initial state.
	Reset()
}

// Result contains the result of a rate limit check.
type Result struct {
	// Allowed indicates if the request is allowed.
	Allowed bool

	// Remaining is the number of requests remaining in the current window.
	Remaining int

	// Limit is the maximum number of requests allowed.
	Limit int

	// RetryAfter is when to retry if rate limited (zero if allowed).
	RetryAfter time.Duration

	// ResetAt is when the rate limit resets.
	ResetAt time.Time
}

// ClientLimiter manages rate limits for multiple clients.
type ClientLimiter interface {
	// GetLimiter returns or creates a limiter for the given client ID.
	GetLimiter(clientID string) Limiter

	// Allow checks if a request from the client is allowed.
	Allow(clientID string) bool

	// Check returns detailed rate limit status for a client.
	Check(clientID string) Result

	// Remove removes a client's limiter.
	Remove(clientID string)

	// Cleanup removes stale client entries.
	Cleanup()

	// Stats returns current statistics.
	Stats() ClientLimiterStats
}

// ClientLimiterStats contains statistics about the client limiter.
type ClientLimiterStats struct {
	// ActiveClients is the number of tracked clients.
	ActiveClients int

	// TotalRequests is the total number of requests processed.
	TotalRequests uint64

	// TotalAllowed is the total number of allowed requests.
	TotalAllowed uint64

	// TotalDenied is the total number of denied requests.
	TotalDenied uint64
}

// Algorithm specifies the rate limiting algorithm.
type Algorithm int

const (
	// AlgorithmTokenBucket uses token bucket algorithm.
	// Good for smooth rate limiting with burst support.
	AlgorithmTokenBucket Algorithm = iota

	// AlgorithmSlidingWindow uses sliding window algorithm.
	// Good for precise rate limiting over time windows.
	AlgorithmSlidingWindow
)

// String returns the string representation of the algorithm.
func (a Algorithm) String() string {
	switch a {
	case AlgorithmTokenBucket:
		return "token_bucket"
	case AlgorithmSlidingWindow:
		return "sliding_window"
	default:
		return "unknown"
	}
}

// Config configures rate limiting behavior.
type Config struct {
	// Algorithm is the rate limiting algorithm to use.
	// Default: AlgorithmTokenBucket
	Algorithm Algorithm

	// Rate is the number of requests allowed per second.
	// Default: 100
	Rate float64

	// Burst is the maximum burst size (token bucket only).
	// Default: same as Rate
	Burst int

	// Window is the time window for sliding window algorithm.
	// Default: 1 second
	Window time.Duration

	// CleanupInterval is how often to clean up stale entries.
	// Default: 1 minute
	CleanupInterval time.Duration

	// MaxIdleTime is how long a client can be idle before cleanup.
	// Default: 5 minutes
	MaxIdleTime time.Duration
}

// DefaultConfig returns sensible default configuration.
func DefaultConfig() Config {
	return Config{
		Algorithm:       AlgorithmTokenBucket,
		Rate:            100,
		Burst:           100,
		Window:          time.Second,
		CleanupInterval: time.Minute,
		MaxIdleTime:     5 * time.Minute,
	}
}

// Validate validates the configuration.
func (c *Config) Validate() error {
	if c.Rate <= 0 {
		c.Rate = 100
	}
	if c.Burst <= 0 {
		c.Burst = int(c.Rate)
	}
	if c.Window <= 0 {
		c.Window = time.Second
	}
	if c.CleanupInterval <= 0 {
		c.CleanupInterval = time.Minute
	}
	if c.MaxIdleTime <= 0 {
		c.MaxIdleTime = 5 * time.Minute
	}
	return nil
}

// GlobalLimiter provides a global rate limiter for the entire server.
type GlobalLimiter struct {
	mu      sync.Mutex
	limiter Limiter
	config  Config
}

// NewGlobalLimiter creates a new global rate limiter.
func NewGlobalLimiter(cfg Config) *GlobalLimiter {
	_ = cfg.Validate()

	var limiter Limiter
	switch cfg.Algorithm {
	case AlgorithmSlidingWindow:
		limiter = NewSlidingWindowLimiter(int(cfg.Rate), cfg.Window)
	default:
		limiter = NewTokenBucket(cfg.Rate, cfg.Burst)
	}

	return &GlobalLimiter{
		limiter: limiter,
		config:  cfg,
	}
}

// Allow checks if a request is allowed globally.
func (g *GlobalLimiter) Allow() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.limiter.Allow()
}

// AllowN checks if n requests are allowed globally.
func (g *GlobalLimiter) AllowN(n int) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.limiter.AllowN(n)
}

// Check returns the current rate limit status.
func (g *GlobalLimiter) Check() Result {
	g.mu.Lock()
	defer g.mu.Unlock()

	tokens := g.limiter.Tokens()
	allowed := tokens >= 1

	var retryAfter time.Duration
	if !allowed {
		retryAfter = g.limiter.Reserve()
	}

	return Result{
		Allowed:    allowed,
		Remaining:  int(tokens),
		Limit:      g.config.Burst,
		RetryAfter: retryAfter,
		ResetAt:    time.Now().Add(time.Second),
	}
}

// Limiter returns the underlying limiter.
func (g *GlobalLimiter) Limiter() Limiter {
	return g.limiter
}
