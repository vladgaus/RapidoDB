package shutdown

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// ============================================================================
// Coordinator Tests
// ============================================================================

func TestNewCoordinator(t *testing.T) {
	c := NewCoordinator(DefaultOptions())

	if c == nil {
		t.Fatal("NewCoordinator returned nil")
	}

	if c.Phase() != PhaseRunning {
		t.Errorf("Expected PhaseRunning, got %v", c.Phase())
	}

	if c.IsShuttingDown() {
		t.Error("Should not be shutting down initially")
	}
}

func TestCoordinator_RegisterHook(t *testing.T) {
	c := NewCoordinator(DefaultOptions())

	called := false
	c.RegisterHook("test", func(ctx context.Context) error {
		called = true
		return nil
	}, PriorityNormal)

	c.Shutdown("test")
	<-c.Done()

	if !called {
		t.Error("Hook was not called")
	}
}

func TestCoordinator_HookOrder(t *testing.T) {
	c := NewCoordinator(DefaultOptions())

	order := make([]string, 0)
	mu := sync.Mutex{}

	addToOrder := func(name string) {
		mu.Lock()
		order = append(order, name)
		mu.Unlock()
	}

	c.RegisterHook("last", func(ctx context.Context) error {
		addToOrder("last")
		return nil
	}, PriorityLast)

	c.RegisterHook("first", func(ctx context.Context) error {
		addToOrder("first")
		return nil
	}, PriorityFirst)

	c.RegisterHook("normal", func(ctx context.Context) error {
		addToOrder("normal")
		return nil
	}, PriorityNormal)

	c.Shutdown("test")
	<-c.Done()

	if len(order) != 3 {
		t.Fatalf("Expected 3 hooks, got %d", len(order))
	}

	if order[0] != "first" || order[1] != "normal" || order[2] != "last" {
		t.Errorf("Wrong order: %v", order)
	}
}

func TestCoordinator_HookError(t *testing.T) {
	c := NewCoordinator(DefaultOptions())

	expectedErr := errors.New("hook error")

	c.RegisterHook("failing", func(ctx context.Context) error {
		return expectedErr
	}, PriorityNormal)

	c.Shutdown("test")
	<-c.Done()

	errs := c.Errors()
	if len(errs) != 1 {
		t.Fatalf("Expected 1 error, got %d", len(errs))
	}
}

func TestCoordinator_HookPanic(t *testing.T) {
	c := NewCoordinator(DefaultOptions())

	c.RegisterHook("panicking", func(ctx context.Context) error {
		panic("test panic")
	}, PriorityNormal)

	// Should not panic
	c.Shutdown("test")
	<-c.Done()

	errs := c.Errors()
	if len(errs) != 1 {
		t.Fatalf("Expected 1 error, got %d", len(errs))
	}
}

func TestCoordinator_Timeout(t *testing.T) {
	c := NewCoordinator(Options{
		Timeout: 100 * time.Millisecond,
	})

	c.RegisterHook("slow", func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
			return nil
		}
	}, PriorityFirst)

	c.RegisterHook("never-runs", func(ctx context.Context) error {
		t.Error("This hook should not run")
		return nil
	}, PriorityLast)

	start := time.Now()
	c.Shutdown("test")
	<-c.Done()
	elapsed := time.Since(start)

	// Should timeout around 100ms, not wait 5 seconds
	if elapsed > 500*time.Millisecond {
		t.Errorf("Timeout did not work, took %v", elapsed)
	}
}

func TestCoordinator_ShutdownOnce(t *testing.T) {
	c := NewCoordinator(DefaultOptions())

	callCount := 0
	c.RegisterHook("counter", func(ctx context.Context) error {
		callCount++
		return nil
	}, PriorityNormal)

	// Call shutdown multiple times
	c.Shutdown("first")
	c.Shutdown("second")
	c.Shutdown("third")
	<-c.Done()

	if callCount != 1 {
		t.Errorf("Hook was called %d times, expected 1", callCount)
	}

	// Reason should be from first call
	if c.Reason() != "first" {
		t.Errorf("Expected reason 'first', got '%s'", c.Reason())
	}
}

func TestCoordinator_RegisterHookFunc(t *testing.T) {
	c := NewCoordinator(DefaultOptions())

	called := false
	c.RegisterHookFunc("simple", func() error {
		called = true
		return nil
	}, PriorityNormal)

	c.Shutdown("test")
	<-c.Done()

	if !called {
		t.Error("Hook was not called")
	}
}

func TestPhase_String(t *testing.T) {
	tests := []struct {
		phase    Phase
		expected string
	}{
		{PhaseRunning, "running"},
		{PhaseShuttingDown, "shutting_down"},
		{PhaseDraining, "draining"},
		{PhaseStopped, "stopped"},
		{Phase(999), "unknown"},
	}

	for _, tt := range tests {
		if tt.phase.String() != tt.expected {
			t.Errorf("Phase %d: expected %s, got %s", tt.phase, tt.expected, tt.phase.String())
		}
	}
}

// ============================================================================
// Drainer Tests
// ============================================================================

func TestNewDrainer(t *testing.T) {
	d := NewDrainer()

	if d == nil {
		t.Fatal("NewDrainer returned nil")
	}

	if d.InFlight() != 0 {
		t.Errorf("Expected 0 in-flight, got %d", d.InFlight())
	}

	if d.IsDraining() {
		t.Error("Should not be draining initially")
	}
}

func TestDrainer_AcquireRelease(t *testing.T) {
	d := NewDrainer()

	if !d.Acquire() {
		t.Error("Acquire should return true")
	}

	if d.InFlight() != 1 {
		t.Errorf("Expected 1 in-flight, got %d", d.InFlight())
	}

	d.Release()

	if d.InFlight() != 0 {
		t.Errorf("Expected 0 in-flight, got %d", d.InFlight())
	}
}

func TestDrainer_AcquireWhileDraining(t *testing.T) {
	d := NewDrainer()

	d.StartDraining()

	if d.Acquire() {
		t.Error("Acquire should return false while draining")
	}
}

func TestDrainer_Wait(t *testing.T) {
	d := NewDrainer()

	// Acquire some operations
	d.Acquire()
	d.Acquire()
	d.Acquire()

	// Start draining
	d.StartDraining()

	// Wait in goroutine
	done := make(chan error)
	go func() {
		done <- d.WaitWithTimeout(time.Second)
	}()

	// Release operations
	d.Release()
	d.Release()
	d.Release()

	// Wait should complete
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Wait returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Error("Wait did not complete in time")
	}
}

func TestDrainer_WaitTimeout(t *testing.T) {
	d := NewDrainer()

	// Acquire operation but don't release
	d.Acquire()
	d.StartDraining()

	err := d.WaitWithTimeout(100 * time.Millisecond)
	if err == nil {
		t.Error("Wait should have timed out")
	}
}

func TestDrainer_WaitNoOperations(t *testing.T) {
	d := NewDrainer()

	d.StartDraining()

	err := d.WaitWithTimeout(100 * time.Millisecond)
	if err != nil {
		t.Errorf("Wait should complete immediately with no in-flight: %v", err)
	}
}

func TestDrainer_TrackRequest(t *testing.T) {
	d := NewDrainer()

	tracker := d.TrackRequest()
	if tracker == nil {
		t.Fatal("TrackRequest returned nil")
	}

	if d.InFlight() != 1 {
		t.Errorf("Expected 1 in-flight, got %d", d.InFlight())
	}

	tracker.Done()

	if d.InFlight() != 0 {
		t.Errorf("Expected 0 in-flight, got %d", d.InFlight())
	}

	// Done is idempotent
	tracker.Done()
	tracker.Done()

	if d.InFlight() != 0 {
		t.Errorf("Expected 0 in-flight after multiple Done calls, got %d", d.InFlight())
	}
}

func TestDrainer_TrackRequestWhileDraining(t *testing.T) {
	d := NewDrainer()

	d.StartDraining()

	tracker := d.TrackRequest()
	if tracker != nil {
		t.Error("TrackRequest should return nil while draining")
	}
}

func TestDrainer_Concurrent(t *testing.T) {
	d := NewDrainer()

	const numGoroutines = 100
	const opsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				if d.Acquire() {
					time.Sleep(time.Microsecond)
					d.Release()
				}
			}
		}()
	}

	// Start draining after a short delay
	time.Sleep(10 * time.Millisecond)
	d.StartDraining()

	// Wait for all goroutines
	wg.Wait()

	// Should eventually drain
	err := d.WaitWithTimeout(time.Second)
	if err != nil {
		t.Errorf("Drain did not complete: %v, in-flight: %d", err, d.InFlight())
	}
}

func TestDrainer_DrainAndWait(t *testing.T) {
	d := NewDrainer()

	d.Acquire()

	go func() {
		time.Sleep(50 * time.Millisecond)
		d.Release()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := d.DrainAndWait(ctx)
	if err != nil {
		t.Errorf("DrainAndWait returned error: %v", err)
	}
}
