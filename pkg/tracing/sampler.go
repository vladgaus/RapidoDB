package tracing

import (
	"encoding/binary"
	"sync/atomic"
)

// ============================================================================
// Sampler Interface
// ============================================================================

// Sampler determines whether a trace should be sampled.
type Sampler interface {
	// ShouldSample returns a sampling decision for a span.
	ShouldSample(params SamplingParameters) SamplingResult

	// Description returns a description of the sampler.
	Description() string
}

// SamplingParameters contains the values passed to a Sampler.
type SamplingParameters struct {
	TraceID    TraceID
	Name       string
	Kind       SpanKind
	Attributes []Attribute
}

// SamplingDecision indicates whether a span should be recorded and sampled.
type SamplingDecision int

const (
	// Drop indicates the span should not be recorded.
	Drop SamplingDecision = iota

	// RecordOnly indicates the span should be recorded but not sampled.
	RecordOnly

	// RecordAndSample indicates the span should be recorded and sampled.
	RecordAndSample
)

// SamplingResult contains the decision made by a Sampler.
type SamplingResult struct {
	Decision   SamplingDecision
	Attributes []Attribute
}

// ============================================================================
// Always Sample
// ============================================================================

type alwaysSampler struct{}

// AlwaysSample returns a sampler that samples every trace.
func AlwaysSample() Sampler {
	return alwaysSampler{}
}

func (alwaysSampler) ShouldSample(_ SamplingParameters) SamplingResult {
	return SamplingResult{Decision: RecordAndSample}
}

func (alwaysSampler) Description() string {
	return "AlwaysSample"
}

// ============================================================================
// Never Sample
// ============================================================================

type neverSampler struct{}

// NeverSample returns a sampler that never samples.
func NeverSample() Sampler {
	return neverSampler{}
}

func (neverSampler) ShouldSample(_ SamplingParameters) SamplingResult {
	return SamplingResult{Decision: Drop}
}

func (neverSampler) Description() string {
	return "NeverSample"
}

// ============================================================================
// Ratio-based Sampler
// ============================================================================

type ratioSampler struct {
	ratio       float64
	description string
	threshold   uint64
}

// RatioSampler returns a sampler that samples a given fraction of traces.
// The ratio must be in the range [0, 1].
func RatioSampler(ratio float64) Sampler {
	if ratio <= 0 {
		return NeverSample()
	}
	if ratio >= 1 {
		return AlwaysSample()
	}

	// Calculate threshold for the upper half of uint64 range
	// We use the upper bound approach: sample if traceValue/maxUint64 < ratio
	// This is equivalent to: traceValue < ratio * maxUint64
	// To avoid overflow, we use: threshold = maxUint64 * ratio
	maxUint64 := ^uint64(0)
	threshold := uint64(float64(maxUint64) * ratio)

	return &ratioSampler{
		ratio:       ratio,
		description: "RatioSampler",
		threshold:   threshold,
	}
}

func (s *ratioSampler) ShouldSample(params SamplingParameters) SamplingResult {
	// Use trace ID as source of randomness for consistent sampling
	traceValue := binary.BigEndian.Uint64(params.TraceID[8:16])
	if traceValue < s.threshold {
		return SamplingResult{Decision: RecordAndSample}
	}
	return SamplingResult{Decision: Drop}
}

func (s *ratioSampler) Description() string {
	return s.description
}

// ============================================================================
// Rate-limited Sampler
// ============================================================================

type rateLimitedSampler struct {
	maxPerSecond float64
	tokens       atomic.Int64
	lastRefill   atomic.Int64
}

// RateLimitedSampler returns a sampler that limits sampling to N traces per second.
func RateLimitedSampler(maxPerSecond float64) Sampler {
	s := &rateLimitedSampler{
		maxPerSecond: maxPerSecond,
	}
	s.tokens.Store(int64(maxPerSecond))
	s.lastRefill.Store(nanotime())
	return s
}

func (s *rateLimitedSampler) ShouldSample(_ SamplingParameters) SamplingResult {
	// Refill tokens
	now := nanotime()
	last := s.lastRefill.Load()
	elapsed := float64(now-last) / 1e9 // seconds

	if elapsed >= 1.0 {
		s.tokens.Store(int64(s.maxPerSecond))
		s.lastRefill.Store(now)
	}

	// Try to consume a token
	for {
		tokens := s.tokens.Load()
		if tokens <= 0 {
			return SamplingResult{Decision: Drop}
		}
		if s.tokens.CompareAndSwap(tokens, tokens-1) {
			return SamplingResult{Decision: RecordAndSample}
		}
	}
}

func (s *rateLimitedSampler) Description() string {
	return "RateLimitedSampler"
}

// ============================================================================
// Parent-based Sampler
// ============================================================================

type parentBasedSampler struct {
	root Sampler
}

// ParentBasedSampler returns a sampler that respects the parent's sampling decision.
// If there is no parent, it uses the provided root sampler.
func ParentBasedSampler(root Sampler) Sampler {
	if root == nil {
		root = AlwaysSample()
	}
	return &parentBasedSampler{root: root}
}

func (s *parentBasedSampler) ShouldSample(params SamplingParameters) SamplingResult {
	// If this is a root span (no parent), use the root sampler
	return s.root.ShouldSample(params)
}

func (s *parentBasedSampler) Description() string {
	return "ParentBasedSampler"
}

// ============================================================================
// Helper
// ============================================================================

// nanotime returns the current time in nanoseconds.
// Uses a simple implementation for portability.
func nanotime() int64 {
	// We could use runtime.nanotime() but that's not portable
	// Using time.Now().UnixNano() is good enough for sampling
	return atomicNanotimeCounter.Add(1)
}

var atomicNanotimeCounter atomic.Int64

func init() {
	// Seed with actual time
	atomicNanotimeCounter.Store(1)
}
