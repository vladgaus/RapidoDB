package ratelimit

import (
	"sync"
	"time"
)

// SlidingWindowLimiter implements sliding window rate limiting.
//
// The sliding window algorithm:
// 1. Tracks requests in fixed time windows
// 2. Uses weighted average of current and previous window
// 3. Provides more accurate rate limiting than fixed windows
//
// For example, with a 1-second window and 100 requests limit:
// - If 80 requests in previous second (ending 0.3s ago)
// - And 30 requests in current second (0.3s elapsed)
// - Weighted count = 80 * 0.7 + 30 = 86 (allowed)
type SlidingWindowLimiter struct {
	mu sync.Mutex

	// limit is the maximum requests per window.
	limit int

	// window is the time window duration.
	window time.Duration

	// prevCount is the request count in the previous window.
	prevCount int

	// currCount is the request count in the current window.
	currCount int

	// windowStart is when the current window started.
	windowStart time.Time
}

// NewSlidingWindowLimiter creates a new sliding window limiter.
// limit: maximum requests per window
// window: time window duration
func NewSlidingWindowLimiter(limit int, window time.Duration) *SlidingWindowLimiter {
	if limit <= 0 {
		limit = 100
	}
	if window <= 0 {
		window = time.Second
	}

	return &SlidingWindowLimiter{
		limit:       limit,
		window:      window,
		windowStart: time.Now(),
	}
}

// Allow checks if a single request is allowed.
func (sw *SlidingWindowLimiter) Allow() bool {
	return sw.AllowN(1)
}

// AllowN checks if n requests are allowed.
func (sw *SlidingWindowLimiter) AllowN(n int) bool {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	sw.slideWindow()

	// Calculate weighted count
	count := sw.weightedCount()

	if count+float64(n) <= float64(sw.limit) {
		sw.currCount += n
		return true
	}

	return false
}

// Wait blocks until a request is allowed.
func (sw *SlidingWindowLimiter) Wait() time.Duration {
	start := time.Now()

	for {
		if sw.Allow() {
			return time.Since(start)
		}

		// Sleep until window moves
		sw.mu.Lock()
		elapsed := time.Since(sw.windowStart)
		sleepTime := sw.window - elapsed
		if sleepTime > 10*time.Millisecond {
			sleepTime = 10 * time.Millisecond
		}
		sw.mu.Unlock()

		if sleepTime > 0 {
			time.Sleep(sleepTime)
		}
	}
}

// Reserve returns how long until a request will be allowed.
func (sw *SlidingWindowLimiter) Reserve() time.Duration {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	sw.slideWindow()

	count := sw.weightedCount()
	if count < float64(sw.limit) {
		return 0
	}

	// Estimate when we'll have capacity
	elapsed := time.Since(sw.windowStart)
	remaining := sw.window - elapsed
	return remaining
}

// Tokens returns the remaining capacity.
func (sw *SlidingWindowLimiter) Tokens() float64 {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	sw.slideWindow()

	count := sw.weightedCount()
	remaining := float64(sw.limit) - count
	if remaining < 0 {
		remaining = 0
	}
	return remaining
}

// Reset resets the limiter.
func (sw *SlidingWindowLimiter) Reset() {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	sw.prevCount = 0
	sw.currCount = 0
	sw.windowStart = time.Now()
}

// slideWindow advances the window if needed.
// Must be called with lock held.
func (sw *SlidingWindowLimiter) slideWindow() {
	now := time.Now()
	elapsed := now.Sub(sw.windowStart)

	// If more than one window has passed
	for elapsed >= sw.window {
		sw.prevCount = sw.currCount
		sw.currCount = 0
		sw.windowStart = sw.windowStart.Add(sw.window)
		elapsed = now.Sub(sw.windowStart)
	}
}

// weightedCount calculates the weighted request count.
// Must be called with lock held.
func (sw *SlidingWindowLimiter) weightedCount() float64 {
	elapsed := time.Since(sw.windowStart)
	weight := float64(elapsed) / float64(sw.window)

	// Previous window contribution decreases as we move through current window
	prevContribution := float64(sw.prevCount) * (1 - weight)
	currContribution := float64(sw.currCount)

	return prevContribution + currContribution
}

// Limit returns the request limit.
func (sw *SlidingWindowLimiter) Limit() int {
	return sw.limit
}

// Window returns the window duration.
func (sw *SlidingWindowLimiter) Window() time.Duration {
	return sw.window
}

// SetLimit changes the request limit.
func (sw *SlidingWindowLimiter) SetLimit(limit int) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	sw.limit = limit
}

// CurrentCount returns the current window's request count.
func (sw *SlidingWindowLimiter) CurrentCount() int {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	sw.slideWindow()
	return sw.currCount
}

// PreviousCount returns the previous window's request count.
func (sw *SlidingWindowLimiter) PreviousCount() int {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	sw.slideWindow()
	return sw.prevCount
}
