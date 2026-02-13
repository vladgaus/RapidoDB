// Package shutdown provides graceful shutdown coordination for RapidoDB.
//
// The package handles:
//   - Signal-based shutdown (SIGTERM, SIGINT)
//   - Ordered shutdown of components (server, engine, etc.)
//   - Timeout-based forced shutdown
//   - Shutdown hooks for cleanup tasks
//
// Example usage:
//
//	coordinator := shutdown.NewCoordinator(shutdown.Options{
//	    Timeout: 30 * time.Second,
//	})
//
//	// Register shutdown hooks (executed in reverse order)
//	coordinator.RegisterHook("health-server", health.Close, shutdown.PriorityFirst)
//	coordinator.RegisterHook("tcp-server", server.GracefulClose, shutdown.PriorityNormal)
//	coordinator.RegisterHook("engine", engine.GracefulClose, shutdown.PriorityLast)
//
//	// Start listening for signals
//	coordinator.ListenForSignals()
//
//	// Wait for shutdown to complete
//	<-coordinator.Done()
package shutdown

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Priority defines the order in which hooks are executed during shutdown.
// Lower priority values execute first.
type Priority int

const (
	// PriorityFirst hooks run first (e.g., stop accepting new connections)
	PriorityFirst Priority = 100

	// PriorityEarly hooks run early (e.g., mark as not ready)
	PriorityEarly Priority = 200

	// PriorityNormal hooks run at normal priority
	PriorityNormal Priority = 500

	// PriorityLate hooks run late (e.g., flush data)
	PriorityLate Priority = 800

	// PriorityLast hooks run last (e.g., close storage engine)
	PriorityLast Priority = 900
)

// Phase represents the current shutdown phase.
type Phase int

const (
	// PhaseRunning indicates normal operation.
	PhaseRunning Phase = iota

	// PhaseShuttingDown indicates shutdown is in progress.
	PhaseShuttingDown

	// PhaseDraining indicates we're draining in-flight requests.
	PhaseDraining

	// PhaseStopped indicates shutdown is complete.
	PhaseStopped
)

// String returns the string representation of a Phase.
func (p Phase) String() string {
	switch p {
	case PhaseRunning:
		return "running"
	case PhaseShuttingDown:
		return "shutting_down"
	case PhaseDraining:
		return "draining"
	case PhaseStopped:
		return "stopped"
	default:
		return "unknown"
	}
}

// Hook is a function that performs cleanup during shutdown.
// It receives a context that may be cancelled if the shutdown timeout is exceeded.
type Hook func(ctx context.Context) error

// HookInfo contains information about a registered hook.
type HookInfo struct {
	Name     string
	Hook     Hook
	Priority Priority
}

// Options configures the shutdown coordinator.
type Options struct {
	// Timeout is the maximum time to wait for graceful shutdown.
	// After this, forced shutdown occurs.
	// Default: 30 seconds
	Timeout time.Duration

	// DrainTimeout is the time to wait for in-flight requests to complete.
	// Default: 10 seconds
	DrainTimeout time.Duration

	// Signals is the list of signals to listen for.
	// Default: SIGINT, SIGTERM
	Signals []os.Signal

	// Logger is an optional logger for shutdown events.
	// If nil, logs to stdout.
	Logger Logger
}

// Logger is an interface for shutdown logging.
type Logger interface {
	Info(msg string, args ...interface{})
	Error(msg string, args ...interface{})
}

// DefaultOptions returns sensible default options.
func DefaultOptions() Options {
	return Options{
		Timeout:      30 * time.Second,
		DrainTimeout: 10 * time.Second,
		Signals:      []os.Signal{syscall.SIGINT, syscall.SIGTERM},
		Logger:       nil,
	}
}

// Coordinator manages the graceful shutdown process.
type Coordinator struct {
	mu sync.RWMutex

	opts Options

	// hooks is the list of registered shutdown hooks.
	hooks []HookInfo

	// phase is the current shutdown phase.
	phase atomic.Int32

	// done is closed when shutdown is complete.
	done chan struct{}

	// shutdownOnce ensures shutdown only runs once.
	shutdownOnce sync.Once

	// signalChan receives OS signals.
	signalChan chan os.Signal

	// reason stores why shutdown was triggered.
	reason string

	// errors stores errors from hooks.
	errors []error

	// startTime tracks when shutdown started.
	startTime time.Time
}

// NewCoordinator creates a new shutdown coordinator.
func NewCoordinator(opts Options) *Coordinator {
	if opts.Timeout == 0 {
		opts.Timeout = 30 * time.Second
	}
	if opts.DrainTimeout == 0 {
		opts.DrainTimeout = 10 * time.Second
	}
	if len(opts.Signals) == 0 {
		opts.Signals = []os.Signal{syscall.SIGINT, syscall.SIGTERM}
	}

	c := &Coordinator{
		opts:       opts,
		hooks:      make([]HookInfo, 0),
		done:       make(chan struct{}),
		signalChan: make(chan os.Signal, 1),
	}

	c.phase.Store(int32(PhaseRunning))

	return c
}

// RegisterHook adds a shutdown hook with the given priority.
// Hooks are executed in priority order (lowest first).
// Multiple hooks with the same priority execute in registration order.
func (c *Coordinator) RegisterHook(name string, hook Hook, priority Priority) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.hooks = append(c.hooks, HookInfo{
		Name:     name,
		Hook:     hook,
		Priority: priority,
	})
}

// RegisterHookFunc is a convenience method that wraps a simple function as a hook.
func (c *Coordinator) RegisterHookFunc(name string, fn func() error, priority Priority) {
	c.RegisterHook(name, func(ctx context.Context) error {
		return fn()
	}, priority)
}

// ListenForSignals starts listening for shutdown signals.
// This should be called after all hooks are registered.
func (c *Coordinator) ListenForSignals() {
	signal.Notify(c.signalChan, c.opts.Signals...)

	go func() {
		sig := <-c.signalChan
		c.Shutdown(fmt.Sprintf("received signal: %v", sig))
	}()
}

// Shutdown initiates graceful shutdown.
// This method can be called multiple times; only the first call has effect.
func (c *Coordinator) Shutdown(reason string) {
	c.shutdownOnce.Do(func() {
		c.startTime = time.Now()
		c.reason = reason
		c.phase.Store(int32(PhaseShuttingDown))

		c.log("Initiating graceful shutdown: %s", reason)

		// Create a timeout context
		ctx, cancel := context.WithTimeout(context.Background(), c.opts.Timeout)
		defer cancel()

		// Sort hooks by priority
		c.mu.RLock()
		hooks := make([]HookInfo, len(c.hooks))
		copy(hooks, c.hooks)
		c.mu.RUnlock()

		sort.SliceStable(hooks, func(i, j int) bool {
			return hooks[i].Priority < hooks[j].Priority
		})

		// Execute hooks
		for _, h := range hooks {
			if ctx.Err() != nil {
				c.logError("Shutdown timeout exceeded, forcing shutdown")
				c.mu.Lock()
				c.errors = append(c.errors, fmt.Errorf("shutdown timeout exceeded"))
				c.mu.Unlock()
				break
			}
			c.executeHook(ctx, h)
		}

		// Mark as stopped
		c.phase.Store(int32(PhaseStopped))
		elapsed := time.Since(c.startTime)
		c.log("Shutdown complete in %v", elapsed.Round(time.Millisecond))

		close(c.done)
	})
}

// executeHook runs a single hook with error handling.
func (c *Coordinator) executeHook(ctx context.Context, h HookInfo) {
	start := time.Now()
	c.log("Running shutdown hook: %s", h.Name)

	// Run hook with panic recovery
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("panic in hook %s: %v", h.Name, r)
			}
		}()
		err = h.Hook(ctx)
	}()

	elapsed := time.Since(start)

	if err != nil {
		c.logError("Hook %s failed (%v): %v", h.Name, elapsed.Round(time.Millisecond), err)
		c.mu.Lock()
		c.errors = append(c.errors, fmt.Errorf("hook %s: %w", h.Name, err))
		c.mu.Unlock()
	} else {
		c.log("Hook %s completed (%v)", h.Name, elapsed.Round(time.Millisecond))
	}
}

// Done returns a channel that is closed when shutdown is complete.
func (c *Coordinator) Done() <-chan struct{} {
	return c.done
}

// Phase returns the current shutdown phase.
func (c *Coordinator) Phase() Phase {
	return Phase(c.phase.Load())
}

// IsShuttingDown returns true if shutdown has been initiated.
func (c *Coordinator) IsShuttingDown() bool {
	return c.Phase() != PhaseRunning
}

// Reason returns the reason for shutdown.
func (c *Coordinator) Reason() string {
	return c.reason
}

// Errors returns any errors that occurred during shutdown.
func (c *Coordinator) Errors() []error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	errors := make([]error, len(c.errors))
	copy(errors, c.errors)
	return errors
}

// log logs an info message.
func (c *Coordinator) log(format string, args ...interface{}) {
	if c.opts.Logger != nil {
		c.opts.Logger.Info(format, args...)
	} else {
		fmt.Printf("[shutdown] "+format+"\n", args...)
	}
}

// logError logs an error message.
func (c *Coordinator) logError(format string, args ...interface{}) {
	if c.opts.Logger != nil {
		c.opts.Logger.Error(format, args...)
	} else {
		fmt.Fprintf(os.Stderr, "[shutdown] ERROR: "+format+"\n", args...)
	}
}

// WaitForSignal blocks until a shutdown signal is received.
// Returns the signal that was received.
func WaitForSignal(signals ...os.Signal) os.Signal {
	if len(signals) == 0 {
		signals = []os.Signal{syscall.SIGINT, syscall.SIGTERM}
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, signals...)
	return <-sigChan
}
