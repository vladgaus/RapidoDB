package ratelimit

import (
	"sync"
	"testing"
	"time"
)

// ============================================================================
// Token Bucket Tests
// ============================================================================

func TestNewTokenBucket(t *testing.T) {
	tb := NewTokenBucket(10, 20)

	if tb == nil {
		t.Fatal("NewTokenBucket returned nil")
	}

	if tb.Rate() != 10 {
		t.Errorf("Expected rate 10, got %f", tb.Rate())
	}

	if tb.Burst() != 20 {
		t.Errorf("Expected burst 20, got %d", tb.Burst())
	}

	// Should start full
	if tb.Tokens() != 20 {
		t.Errorf("Expected 20 tokens, got %f", tb.Tokens())
	}
}

func TestTokenBucket_Allow(t *testing.T) {
	tb := NewTokenBucket(10, 10)

	// Should allow 10 requests (full bucket)
	for i := 0; i < 10; i++ {
		if !tb.Allow() {
			t.Errorf("Request %d should be allowed", i+1)
		}
	}

	// 11th request should be denied
	if tb.Allow() {
		t.Error("11th request should be denied")
	}
}

func TestTokenBucket_AllowN(t *testing.T) {
	tb := NewTokenBucket(10, 10)

	// Should allow 5 requests
	if !tb.AllowN(5) {
		t.Error("Should allow 5 requests")
	}

	// Should have approximately 5 tokens left (timing may add tiny amounts)
	tokens := tb.Tokens()
	if tokens < 4.9 || tokens > 5.1 {
		t.Errorf("Expected ~5 tokens, got %f", tokens)
	}

	// Should not allow 6 more
	if tb.AllowN(6) {
		t.Error("Should not allow 6 requests when only ~5 tokens left")
	}

	// Should still have ~5 tokens (failed request doesn't consume)
	tokens = tb.Tokens()
	if tokens < 4.9 || tokens > 5.2 {
		t.Errorf("Expected ~5 tokens after failed request, got %f", tokens)
	}
}

func TestTokenBucket_Refill(t *testing.T) {
	tb := NewTokenBucket(100, 10) // 100 tokens/sec

	// Drain all tokens
	tb.AllowN(10)

	// Should have approximately 0 tokens (tiny refill may occur)
	tokens := tb.Tokens()
	if tokens > 0.5 {
		t.Errorf("Expected ~0 tokens, got %f", tokens)
	}

	// Wait 50ms - should get ~5 tokens
	time.Sleep(50 * time.Millisecond)

	tokens = tb.Tokens()
	if tokens < 4 || tokens > 7 {
		t.Errorf("Expected ~5 tokens after 50ms, got %f", tokens)
	}
}

func TestTokenBucket_Reset(t *testing.T) {
	tb := NewTokenBucket(10, 10)

	// Drain tokens
	tb.AllowN(10)

	// Reset
	tb.Reset()

	// Should be full again
	if tb.Tokens() != 10 {
		t.Errorf("Expected 10 tokens after reset, got %f", tb.Tokens())
	}
}

func TestTokenBucket_Reserve(t *testing.T) {
	tb := NewTokenBucket(100, 10) // 100 tokens/sec

	// Full bucket - reserve should return 0
	reserve := tb.Reserve()
	if reserve != 0 {
		t.Errorf("Expected 0 reserve time when full, got %v", reserve)
	}

	// Drain bucket
	tb.AllowN(10)

	// Should need to wait ~10ms for one token
	reserve = tb.Reserve()
	if reserve < 5*time.Millisecond || reserve > 15*time.Millisecond {
		t.Errorf("Expected ~10ms reserve time, got %v", reserve)
	}
}

func TestTokenBucket_SetRate(t *testing.T) {
	tb := NewTokenBucket(10, 10)

	tb.SetRate(100)

	if tb.Rate() != 100 {
		t.Errorf("Expected rate 100, got %f", tb.Rate())
	}
}

func TestTokenBucket_SetBurst(t *testing.T) {
	tb := NewTokenBucket(10, 20)

	tb.SetBurst(5)

	if tb.Burst() != 5 {
		t.Errorf("Expected burst 5, got %d", tb.Burst())
	}

	// Tokens should be capped at new burst
	if tb.Tokens() > 5 {
		t.Errorf("Expected tokens <= 5, got %f", tb.Tokens())
	}
}

func TestTokenBucket_Concurrent(t *testing.T) {
	// Use a near-zero rate so that token refill during the test is negligible.
	// Burst of 100 means the bucket starts with exactly 100 tokens.
	tb := NewTokenBucket(0.001, 100)

	var wg sync.WaitGroup
	allowed := make(chan bool, 200)

	// Launch 200 concurrent requests
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			allowed <- tb.Allow()
		}()
	}

	wg.Wait()
	close(allowed)

	// Count allowed requests
	count := 0
	for a := range allowed {
		if a {
			count++
		}
	}

	// Should allow exactly 100 (burst size), no more.
	if count != 100 {
		t.Errorf("Expected 100 allowed, got %d", count)
	}
}

// ============================================================================
// Sliding Window Tests
// ============================================================================

func TestNewSlidingWindowLimiter(t *testing.T) {
	sw := NewSlidingWindowLimiter(100, time.Second)

	if sw == nil {
		t.Fatal("NewSlidingWindowLimiter returned nil")
	}

	if sw.Limit() != 100 {
		t.Errorf("Expected limit 100, got %d", sw.Limit())
	}

	if sw.Window() != time.Second {
		t.Errorf("Expected window 1s, got %v", sw.Window())
	}
}

func TestSlidingWindow_Allow(t *testing.T) {
	sw := NewSlidingWindowLimiter(10, time.Second)

	// Should allow 10 requests
	for i := 0; i < 10; i++ {
		if !sw.Allow() {
			t.Errorf("Request %d should be allowed", i+1)
		}
	}

	// 11th request should be denied
	if sw.Allow() {
		t.Error("11th request should be denied")
	}
}

func TestSlidingWindow_AllowN(t *testing.T) {
	sw := NewSlidingWindowLimiter(10, time.Second)

	if !sw.AllowN(5) {
		t.Error("Should allow 5 requests")
	}

	if !sw.AllowN(5) {
		t.Error("Should allow another 5 requests")
	}

	if sw.AllowN(1) {
		t.Error("Should not allow any more requests")
	}
}

func TestSlidingWindow_WindowSlide(t *testing.T) {
	sw := NewSlidingWindowLimiter(10, 100*time.Millisecond)

	// Use all 10 requests
	sw.AllowN(10)

	// Should be denied
	if sw.Allow() {
		t.Error("Should be denied")
	}

	// Wait for window to slide
	time.Sleep(110 * time.Millisecond)

	// Should be allowed again
	if !sw.Allow() {
		t.Error("Should be allowed after window slide")
	}
}

func TestSlidingWindow_WeightedCount(t *testing.T) {
	sw := NewSlidingWindowLimiter(100, 100*time.Millisecond)

	// Use 80 requests
	sw.AllowN(80)

	// Immediately should have 20 tokens
	tokens := sw.Tokens()
	if tokens < 19 || tokens > 21 {
		t.Errorf("Expected ~20 tokens immediately, got %f", tokens)
	}

	// Wait for window to fully pass
	time.Sleep(110 * time.Millisecond)

	// Now the 80 requests are in the previous window
	// At start of new window: prev=80, curr=0, weight=0
	// weighted = 80 * (1-0) + 0 = 80, remaining = 100 - 80 = 20

	// Wait a bit into new window
	time.Sleep(50 * time.Millisecond)

	// Now: weight ≈ 0.5
	// weighted = 80 * (1-0.5) + 0 = 40, remaining = 100 - 40 = 60
	tokens = sw.Tokens()
	if tokens < 50 || tokens > 70 {
		t.Errorf("Expected ~60 tokens after half of new window, got %f", tokens)
	}
}

func TestSlidingWindow_Reset(t *testing.T) {
	sw := NewSlidingWindowLimiter(10, time.Second)

	sw.AllowN(10)
	sw.Reset()

	// Should be able to make requests again
	if !sw.Allow() {
		t.Error("Should be allowed after reset")
	}
}

func TestSlidingWindow_CurrentCount(t *testing.T) {
	sw := NewSlidingWindowLimiter(100, time.Second)

	sw.AllowN(50)

	if sw.CurrentCount() != 50 {
		t.Errorf("Expected current count 50, got %d", sw.CurrentCount())
	}
}

// ============================================================================
// Per-Client Limiter Tests
// ============================================================================

func TestNewPerClientLimiter(t *testing.T) {
	pcl := NewPerClientLimiter(DefaultConfig())
	defer pcl.Close()

	if pcl == nil {
		t.Fatal("NewPerClientLimiter returned nil")
	}
}

func TestPerClientLimiter_Allow(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Rate = 10
	cfg.Burst = 10

	pcl := NewPerClientLimiter(cfg)
	defer pcl.Close()

	// First 10 requests should be allowed
	for i := 0; i < 10; i++ {
		if !pcl.Allow("client1") {
			t.Errorf("Request %d should be allowed", i+1)
		}
	}

	// 11th request should be denied
	if pcl.Allow("client1") {
		t.Error("11th request should be denied")
	}

	// Different client should still be allowed
	if !pcl.Allow("client2") {
		t.Error("Different client should be allowed")
	}
}

func TestPerClientLimiter_Check(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Rate = 10
	cfg.Burst = 10

	pcl := NewPerClientLimiter(cfg)
	defer pcl.Close()

	// Check full limiter
	result := pcl.Check("client1")

	if !result.Allowed {
		t.Error("Should be allowed initially")
	}

	if result.Remaining != 10 {
		t.Errorf("Expected 10 remaining, got %d", result.Remaining)
	}

	// Use some tokens
	pcl.AllowN("client1", 8)

	result = pcl.Check("client1")

	if result.Remaining != 2 {
		t.Errorf("Expected 2 remaining, got %d", result.Remaining)
	}
}

func TestPerClientLimiter_Stats(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Rate = 10
	cfg.Burst = 10

	pcl := NewPerClientLimiter(cfg)
	defer pcl.Close()

	// Make some requests
	for i := 0; i < 15; i++ {
		pcl.Allow("client1")
	}

	stats := pcl.Stats()

	if stats.ActiveClients != 1 {
		t.Errorf("Expected 1 active client, got %d", stats.ActiveClients)
	}

	if stats.TotalRequests != 15 {
		t.Errorf("Expected 15 total requests, got %d", stats.TotalRequests)
	}

	if stats.TotalAllowed != 10 {
		t.Errorf("Expected 10 allowed, got %d", stats.TotalAllowed)
	}

	if stats.TotalDenied != 5 {
		t.Errorf("Expected 5 denied, got %d", stats.TotalDenied)
	}
}

func TestPerClientLimiter_Remove(t *testing.T) {
	pcl := NewPerClientLimiter(DefaultConfig())
	defer pcl.Close()

	pcl.Allow("client1")
	pcl.Allow("client2")

	if pcl.ActiveClients() != 2 {
		t.Errorf("Expected 2 clients, got %d", pcl.ActiveClients())
	}

	pcl.Remove("client1")

	if pcl.ActiveClients() != 1 {
		t.Errorf("Expected 1 client after remove, got %d", pcl.ActiveClients())
	}
}

func TestPerClientLimiter_Cleanup(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxIdleTime = 50 * time.Millisecond

	pcl := NewPerClientLimiter(cfg)
	defer pcl.Close()

	pcl.Allow("client1")

	time.Sleep(100 * time.Millisecond)

	pcl.Cleanup()

	if pcl.ActiveClients() != 0 {
		t.Errorf("Expected 0 clients after cleanup, got %d", pcl.ActiveClients())
	}
}

func TestPerClientLimiter_ResetClient(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Rate = 10
	cfg.Burst = 10

	pcl := NewPerClientLimiter(cfg)
	defer pcl.Close()

	// Exhaust client's limit
	for i := 0; i < 10; i++ {
		pcl.Allow("client1")
	}

	// Should be denied
	if pcl.Allow("client1") {
		t.Error("Should be denied before reset")
	}

	// Reset client
	pcl.ResetClient("client1")

	// Should be allowed now
	if !pcl.Allow("client1") {
		t.Error("Should be allowed after reset")
	}
}

func TestPerClientLimiter_SlidingWindow(t *testing.T) {
	cfg := Config{
		Algorithm: AlgorithmSlidingWindow,
		Rate:      10,
		Window:    100 * time.Millisecond,
	}
	_ = cfg.Validate()

	pcl := NewPerClientLimiter(cfg)
	defer pcl.Close()

	// Should use sliding window
	for i := 0; i < 10; i++ {
		if !pcl.Allow("client1") {
			t.Errorf("Request %d should be allowed", i+1)
		}
	}

	if pcl.Allow("client1") {
		t.Error("11th request should be denied")
	}
}

func TestPerClientLimiter_Concurrent(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Rate = 100
	cfg.Burst = 100

	pcl := NewPerClientLimiter(cfg)
	defer pcl.Close()

	var wg sync.WaitGroup
	clients := []string{"client1", "client2", "client3"}

	for _, client := range clients {
		wg.Add(1)
		go func(c string) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				pcl.Allow(c)
			}
		}(client)
	}

	wg.Wait()

	stats := pcl.Stats()

	if stats.ActiveClients != 3 {
		t.Errorf("Expected 3 clients, got %d", stats.ActiveClients)
	}

	if stats.TotalRequests != 150 {
		t.Errorf("Expected 150 requests, got %d", stats.TotalRequests)
	}
}

// ============================================================================
// Global Limiter Tests
// ============================================================================

func TestNewGlobalLimiter(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Rate = 100
	cfg.Burst = 100

	gl := NewGlobalLimiter(cfg)

	if gl == nil {
		t.Fatal("NewGlobalLimiter returned nil")
	}
}

func TestGlobalLimiter_Allow(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Rate = 10
	cfg.Burst = 10

	gl := NewGlobalLimiter(cfg)

	for i := 0; i < 10; i++ {
		if !gl.Allow() {
			t.Errorf("Request %d should be allowed", i+1)
		}
	}

	if gl.Allow() {
		t.Error("11th request should be denied")
	}
}

func TestGlobalLimiter_Check(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Rate = 10
	cfg.Burst = 10

	gl := NewGlobalLimiter(cfg)

	result := gl.Check()

	if !result.Allowed {
		t.Error("Should be allowed initially")
	}

	if result.Limit != 10 {
		t.Errorf("Expected limit 10, got %d", result.Limit)
	}
}

// ============================================================================
// Config Tests
// ============================================================================

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Rate != 100 {
		t.Errorf("Expected rate 100, got %f", cfg.Rate)
	}

	if cfg.Burst != 100 {
		t.Errorf("Expected burst 100, got %d", cfg.Burst)
	}

	if cfg.Algorithm != AlgorithmTokenBucket {
		t.Errorf("Expected token bucket algorithm")
	}
}

func TestConfig_Validate(t *testing.T) {
	cfg := Config{
		Rate:  -1, // Invalid
		Burst: 0,  // Invalid
	}

	err := cfg.Validate()

	if err != nil {
		t.Errorf("Validate should not return error: %v", err)
	}

	// Should be corrected to defaults
	if cfg.Rate <= 0 {
		t.Error("Rate should be corrected")
	}

	if cfg.Burst <= 0 {
		t.Error("Burst should be corrected")
	}
}

func TestAlgorithm_String(t *testing.T) {
	if AlgorithmTokenBucket.String() != "token_bucket" {
		t.Error("Wrong string for token bucket")
	}

	if AlgorithmSlidingWindow.String() != "sliding_window" {
		t.Error("Wrong string for sliding window")
	}

	if Algorithm(99).String() != "unknown" {
		t.Error("Wrong string for unknown algorithm")
	}
}

// ============================================================================
// Benchmark Tests
// ============================================================================

func BenchmarkTokenBucket_Allow(b *testing.B) {
	tb := NewTokenBucket(1000000, 1000000) // High limit

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tb.Allow()
	}
}

func BenchmarkSlidingWindow_Allow(b *testing.B) {
	sw := NewSlidingWindowLimiter(1000000, time.Second)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sw.Allow()
	}
}

func BenchmarkPerClientLimiter_Allow(b *testing.B) {
	cfg := DefaultConfig()
	cfg.Rate = 1000000
	cfg.Burst = 1000000

	pcl := NewPerClientLimiter(cfg)
	defer pcl.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pcl.Allow("client1")
	}
}

func BenchmarkPerClientLimiter_MultiClient(b *testing.B) {
	cfg := DefaultConfig()
	cfg.Rate = 1000000
	cfg.Burst = 1000000

	pcl := NewPerClientLimiter(cfg)
	defer pcl.Close()

	clients := make([]string, 100)
	for i := 0; i < 100; i++ {
		clients[i] = string(rune('a' + i%26))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pcl.Allow(clients[i%100])
	}
}
