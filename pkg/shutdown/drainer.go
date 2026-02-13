package shutdown

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Drainer tracks in-flight operations and provides a way to wait
// for them to complete during graceful shutdown.
type Drainer struct {
	// inFlight is the count of in-flight operations.
	inFlight atomic.Int64

	// draining indicates if we're in draining mode.
	draining atomic.Bool

	// done is closed when all in-flight operations complete.
	done chan struct{}

	// doneOnce ensures done is only closed once.
	doneOnce sync.Once
}

// NewDrainer creates a new drainer.
func NewDrainer() *Drainer {
	return &Drainer{
		done: make(chan struct{}),
	}
}

// Acquire increments the in-flight counter.
// Returns false if draining has started and new operations are not allowed.
func (d *Drainer) Acquire() bool {
	if d.draining.Load() {
		return false
	}

	d.inFlight.Add(1)

	// Double-check after incrementing
	if d.draining.Load() {
		d.Release()
		return false
	}

	return true
}

// Release decrements the in-flight counter.
func (d *Drainer) Release() {
	count := d.inFlight.Add(-1)

	// If we're draining and count reached zero, signal done
	if d.draining.Load() && count <= 0 {
		d.doneOnce.Do(func() {
			close(d.done)
		})
	}
}

// InFlight returns the current number of in-flight operations.
func (d *Drainer) InFlight() int64 {
	return d.inFlight.Load()
}

// StartDraining initiates draining mode.
// After this call, Acquire() will return false.
func (d *Drainer) StartDraining() {
	d.draining.Store(true)

	// If already at zero, signal done immediately
	if d.inFlight.Load() <= 0 {
		d.doneOnce.Do(func() {
			close(d.done)
		})
	}
}

// IsDraining returns true if draining has started.
func (d *Drainer) IsDraining() bool {
	return d.draining.Load()
}

// Wait blocks until all in-flight operations complete or the context is cancelled.
// Returns nil if all operations completed, or context error if cancelled.
func (d *Drainer) Wait(ctx context.Context) error {
	select {
	case <-d.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// WaitWithTimeout is a convenience method that waits with a timeout.
func (d *Drainer) WaitWithTimeout(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return d.Wait(ctx)
}

// DrainAndWait starts draining and waits for completion.
func (d *Drainer) DrainAndWait(ctx context.Context) error {
	d.StartDraining()
	return d.Wait(ctx)
}

// Done returns a channel that is closed when draining is complete.
func (d *Drainer) Done() <-chan struct{} {
	return d.done
}

// RequestTracker provides a convenient way to track request lifecycle.
type RequestTracker struct {
	drainer *Drainer
	active  bool
}

// TrackRequest creates a new request tracker.
// Returns nil if draining has started and the request should be rejected.
func (d *Drainer) TrackRequest() *RequestTracker {
	if !d.Acquire() {
		return nil
	}
	return &RequestTracker{
		drainer: d,
		active:  true,
	}
}

// Done marks the request as complete.
// Safe to call multiple times.
func (t *RequestTracker) Done() {
	if t != nil && t.active {
		t.active = false
		t.drainer.Release()
	}
}
