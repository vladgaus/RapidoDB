package ratelimit

import (
	"sync"
	"sync/atomic"
	"time"
)

// clientEntry holds a client's limiter and metadata.
type clientEntry struct {
	limiter  Limiter
	lastSeen time.Time
}

// PerClientLimiter manages rate limits for multiple clients.
// Each client (identified by IP or other ID) gets their own limiter.
type PerClientLimiter struct {
	mu sync.RWMutex

	// clients maps client IDs to their limiters.
	clients map[string]*clientEntry

	// config is the rate limit configuration.
	config Config

	// stats tracks usage statistics.
	stats struct {
		totalRequests atomic.Uint64
		totalAllowed  atomic.Uint64
		totalDenied   atomic.Uint64
	}

	// cleanupTicker runs periodic cleanup.
	cleanupTicker *time.Ticker
	cleanupDone   chan struct{}
}

// NewPerClientLimiter creates a new per-client rate limiter.
func NewPerClientLimiter(cfg Config) *PerClientLimiter {
	_ = cfg.Validate()

	pcl := &PerClientLimiter{
		clients:     make(map[string]*clientEntry),
		config:      cfg,
		cleanupDone: make(chan struct{}),
	}

	// Start cleanup goroutine
	pcl.cleanupTicker = time.NewTicker(cfg.CleanupInterval)
	go pcl.cleanupLoop()

	return pcl
}

// GetLimiter returns or creates a limiter for the given client ID.
func (pcl *PerClientLimiter) GetLimiter(clientID string) Limiter {
	// Try read lock first
	pcl.mu.RLock()
	entry, exists := pcl.clients[clientID]
	pcl.mu.RUnlock()

	if exists {
		// Update last seen time
		pcl.mu.Lock()
		entry.lastSeen = time.Now()
		pcl.mu.Unlock()
		return entry.limiter
	}

	// Need to create a new limiter
	pcl.mu.Lock()
	defer pcl.mu.Unlock()

	// Double-check after acquiring write lock
	if entry, exists = pcl.clients[clientID]; exists {
		entry.lastSeen = time.Now()
		return entry.limiter
	}

	// Create new limiter based on algorithm
	var limiter Limiter
	switch pcl.config.Algorithm {
	case AlgorithmSlidingWindow:
		limiter = NewSlidingWindowLimiter(int(pcl.config.Rate), pcl.config.Window)
	default:
		limiter = NewTokenBucket(pcl.config.Rate, pcl.config.Burst)
	}

	pcl.clients[clientID] = &clientEntry{
		limiter:  limiter,
		lastSeen: time.Now(),
	}

	return limiter
}

// Allow checks if a request from the client is allowed.
func (pcl *PerClientLimiter) Allow(clientID string) bool {
	pcl.stats.totalRequests.Add(1)

	limiter := pcl.GetLimiter(clientID)
	allowed := limiter.Allow()

	if allowed {
		pcl.stats.totalAllowed.Add(1)
	} else {
		pcl.stats.totalDenied.Add(1)
	}

	return allowed
}

// AllowN checks if n requests from the client are allowed.
func (pcl *PerClientLimiter) AllowN(clientID string, n int) bool {
	pcl.stats.totalRequests.Add(uint64(n))

	limiter := pcl.GetLimiter(clientID)
	allowed := limiter.AllowN(n)

	if allowed {
		pcl.stats.totalAllowed.Add(uint64(n))
	} else {
		pcl.stats.totalDenied.Add(uint64(n))
	}

	return allowed
}

// Check returns detailed rate limit status for a client.
func (pcl *PerClientLimiter) Check(clientID string) Result {
	limiter := pcl.GetLimiter(clientID)
	tokens := limiter.Tokens()
	allowed := tokens >= 1

	var retryAfter time.Duration
	if !allowed {
		retryAfter = limiter.Reserve()
	}

	return Result{
		Allowed:    allowed,
		Remaining:  int(tokens),
		Limit:      pcl.config.Burst,
		RetryAfter: retryAfter,
		ResetAt:    time.Now().Add(pcl.config.Window),
	}
}

// Remove removes a client's limiter.
func (pcl *PerClientLimiter) Remove(clientID string) {
	pcl.mu.Lock()
	defer pcl.mu.Unlock()
	delete(pcl.clients, clientID)
}

// Cleanup removes stale client entries.
func (pcl *PerClientLimiter) Cleanup() {
	pcl.mu.Lock()
	defer pcl.mu.Unlock()

	now := time.Now()
	for clientID, entry := range pcl.clients {
		if now.Sub(entry.lastSeen) > pcl.config.MaxIdleTime {
			delete(pcl.clients, clientID)
		}
	}
}

// cleanupLoop periodically cleans up stale entries.
func (pcl *PerClientLimiter) cleanupLoop() {
	for {
		select {
		case <-pcl.cleanupTicker.C:
			pcl.Cleanup()
		case <-pcl.cleanupDone:
			return
		}
	}
}

// Close stops the cleanup goroutine.
func (pcl *PerClientLimiter) Close() {
	pcl.cleanupTicker.Stop()
	close(pcl.cleanupDone)
}

// Stats returns current statistics.
func (pcl *PerClientLimiter) Stats() ClientLimiterStats {
	pcl.mu.RLock()
	activeClients := len(pcl.clients)
	pcl.mu.RUnlock()

	return ClientLimiterStats{
		ActiveClients: activeClients,
		TotalRequests: pcl.stats.totalRequests.Load(),
		TotalAllowed:  pcl.stats.totalAllowed.Load(),
		TotalDenied:   pcl.stats.totalDenied.Load(),
	}
}

// ActiveClients returns the number of tracked clients.
func (pcl *PerClientLimiter) ActiveClients() int {
	pcl.mu.RLock()
	defer pcl.mu.RUnlock()
	return len(pcl.clients)
}

// Config returns the current configuration.
func (pcl *PerClientLimiter) Config() Config {
	return pcl.config
}

// Reset resets all client limiters.
func (pcl *PerClientLimiter) Reset() {
	pcl.mu.Lock()
	defer pcl.mu.Unlock()

	for _, entry := range pcl.clients {
		entry.limiter.Reset()
	}
}

// ResetClient resets a specific client's limiter.
func (pcl *PerClientLimiter) ResetClient(clientID string) {
	pcl.mu.RLock()
	entry, exists := pcl.clients[clientID]
	pcl.mu.RUnlock()

	if exists {
		entry.limiter.Reset()
	}
}
