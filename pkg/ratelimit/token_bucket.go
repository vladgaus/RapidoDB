package ratelimit

import (
	"sync"
	"time"
)

// TokenBucket implements the token bucket rate limiting algorithm.
//
// The token bucket algorithm works by:
// 1. A bucket holds up to 'burst' tokens
// 2. Tokens are added at 'rate' per second
// 3. Each request consumes one token
// 4. If no tokens available, request is denied
//
// This provides smooth rate limiting with burst support.
type TokenBucket struct {
	mu sync.Mutex

	// rate is tokens added per second.
	rate float64

	// burst is the maximum bucket capacity.
	burst float64

	// tokens is the current number of tokens.
	tokens float64

	// lastTime is when tokens were last updated.
	lastTime time.Time
}

// NewTokenBucket creates a new token bucket limiter.
// rate: tokens per second
// burst: maximum tokens (bucket capacity)
func NewTokenBucket(rate float64, burst int) *TokenBucket {
	if rate <= 0 {
		rate = 1
	}
	if burst <= 0 {
		burst = int(rate)
	}

	return &TokenBucket{
		rate:     rate,
		burst:    float64(burst),
		tokens:   float64(burst), // Start full
		lastTime: time.Now(),
	}
}

// Allow checks if a single request is allowed.
func (tb *TokenBucket) Allow() bool {
	return tb.AllowN(1)
}

// AllowN checks if n requests are allowed.
func (tb *TokenBucket) AllowN(n int) bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.refill()

	if tb.tokens >= float64(n) {
		tb.tokens -= float64(n)
		return true
	}

	return false
}

// Wait blocks until a token is available.
// Returns the time waited.
func (tb *TokenBucket) Wait() time.Duration {
	start := time.Now()

	for {
		tb.mu.Lock()
		tb.refill()

		if tb.tokens >= 1 {
			tb.tokens--
			tb.mu.Unlock()
			return time.Since(start)
		}

		// Calculate wait time for one token
		tokensNeeded := 1 - tb.tokens
		waitTime := time.Duration(tokensNeeded / tb.rate * float64(time.Second))
		tb.mu.Unlock()

		// Wait a bit (but not too long to allow for cancellation)
		sleepTime := waitTime
		if sleepTime > 10*time.Millisecond {
			sleepTime = 10 * time.Millisecond
		}
		time.Sleep(sleepTime)
	}
}

// Reserve returns how long until a token will be available.
// Returns zero if tokens are available now.
func (tb *TokenBucket) Reserve() time.Duration {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.refill()

	if tb.tokens >= 1 {
		return 0
	}

	// Calculate time until one token is available
	tokensNeeded := 1 - tb.tokens
	return time.Duration(tokensNeeded / tb.rate * float64(time.Second))
}

// Tokens returns the current number of available tokens.
func (tb *TokenBucket) Tokens() float64 {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.refill()
	return tb.tokens
}

// Reset resets the bucket to full capacity.
func (tb *TokenBucket) Reset() {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.tokens = tb.burst
	tb.lastTime = time.Now()
}

// refill adds tokens based on elapsed time.
// Must be called with lock held.
func (tb *TokenBucket) refill() {
	now := time.Now()
	elapsed := now.Sub(tb.lastTime).Seconds()

	if elapsed > 0 {
		tb.tokens += elapsed * tb.rate
		if tb.tokens > tb.burst {
			tb.tokens = tb.burst
		}
		tb.lastTime = now
	}
}

// Rate returns the token generation rate.
func (tb *TokenBucket) Rate() float64 {
	return tb.rate
}

// Burst returns the bucket capacity.
func (tb *TokenBucket) Burst() int {
	return int(tb.burst)
}

// SetRate changes the token generation rate.
func (tb *TokenBucket) SetRate(rate float64) {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.refill()
	tb.rate = rate
}

// SetBurst changes the bucket capacity.
func (tb *TokenBucket) SetBurst(burst int) {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.burst = float64(burst)
	if tb.tokens > tb.burst {
		tb.tokens = tb.burst
	}
}
