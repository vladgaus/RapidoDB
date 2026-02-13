package health

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"
)

// HTTPServer provides HTTP health check endpoints.
type HTTPServer struct {
	mu sync.Mutex

	// checker is the health checker.
	checker *HealthChecker

	// server is the underlying HTTP server.
	server *http.Server

	// addr is the listen address.
	addr string

	// started indicates if the server is running.
	started bool
}

// HTTPServerOptions configures the HTTP health server.
type HTTPServerOptions struct {
	// Host is the address to bind to.
	Host string

	// Port is the port to listen on.
	Port int

	// ReadTimeout is the maximum duration for reading the entire request.
	ReadTimeout time.Duration

	// WriteTimeout is the maximum duration for writing the response.
	WriteTimeout time.Duration
}

// DefaultHTTPServerOptions returns sensible defaults.
func DefaultHTTPServerOptions() HTTPServerOptions {
	return HTTPServerOptions{
		Host:         "0.0.0.0",
		Port:         8080,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
}

// NewHTTPServer creates a new HTTP health server.
func NewHTTPServer(checker *HealthChecker, opts HTTPServerOptions) *HTTPServer {
	s := &HTTPServer{
		checker: checker,
		addr:    fmt.Sprintf("%s:%d", opts.Host, opts.Port),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/health/", s.handleHealth) // Catch /health/*
	mux.HandleFunc("/health/live", s.handleLive)
	mux.HandleFunc("/health/ready", s.handleReady)
	mux.HandleFunc("/", s.handleRoot)

	s.server = &http.Server{
		Addr:         s.addr,
		Handler:      mux,
		ReadTimeout:  opts.ReadTimeout,
		WriteTimeout: opts.WriteTimeout,
	}

	return s
}

// Start begins listening for HTTP requests.
func (s *HTTPServer) Start() error {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return fmt.Errorf("HTTP health server already started")
	}
	s.started = true
	s.mu.Unlock()

	listener, err := net.Listen("tcp", s.addr) //nolint:noctx // one-time listener creation, context not needed
	if err != nil {
		s.mu.Lock()
		s.started = false
		s.mu.Unlock()
		return fmt.Errorf("failed to listen on %s: %w", s.addr, err)
	}

	// Update addr with actual port (in case port 0 was used)
	s.addr = listener.Addr().String()

	go func() {
		if err := s.server.Serve(listener); err != nil && err != http.ErrServerClosed {
			// Log error but don't panic
			fmt.Printf("Health HTTP server error: %v\n", err)
		}
	}()

	return nil
}

// Addr returns the server's listen address.
func (s *HTTPServer) Addr() string {
	return s.addr
}

// Close gracefully shuts down the HTTP server.
func (s *HTTPServer) Close() error {
	s.mu.Lock()
	if !s.started {
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return s.server.Shutdown(ctx)
}

// handleRoot handles requests to / with a simple redirect hint.
func (s *HTTPServer) handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	response := map[string]interface{}{
		"service": "RapidoDB Health API",
		"endpoints": map[string]string{
			"/health":       "Full health status",
			"/health/live":  "Liveness probe (Kubernetes)",
			"/health/ready": "Readiness probe (Kubernetes)",
		},
	}

	_ = json.NewEncoder(w).Encode(response)
}

// handleHealth handles /health requests with full health report.
func (s *HTTPServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	// Handle subpaths
	switch r.URL.Path {
	case "/health", "/health/":
		// Continue to full health check
	case "/health/live":
		s.handleLive(w, r)
		return
	case "/health/ready":
		s.handleReady(w, r)
		return
	default:
		http.NotFound(w, r)
		return
	}

	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	report := s.checker.Check()

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")

	// Set status code based on health
	switch report.Status {
	case StatusHealthy:
		w.WriteHeader(http.StatusOK)
	case StatusDegraded:
		w.WriteHeader(http.StatusOK) // 200 but degraded
	case StatusUnhealthy:
		w.WriteHeader(http.StatusServiceUnavailable) // 503
	default:
		w.WriteHeader(http.StatusInternalServerError)
	}

	if r.Method == http.MethodHead {
		return
	}

	_ = json.NewEncoder(w).Encode(report)
}

// handleLive handles /health/live requests (Kubernetes liveness probe).
// Returns 200 if the service is alive, 503 otherwise.
func (s *HTTPServer) handleLive(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")

	if s.checker.IsLive() {
		w.WriteHeader(http.StatusOK)
		if r.Method != http.MethodHead {
			response := map[string]interface{}{
				"status": "live",
				"uptime": s.checker.Uptime().Round(time.Second).String(),
			}
			_ = json.NewEncoder(w).Encode(response)
		}
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
		if r.Method != http.MethodHead {
			response := map[string]interface{}{
				"status": "not live",
			}
			_ = json.NewEncoder(w).Encode(response)
		}
	}
}

// handleReady handles /health/ready requests (Kubernetes readiness probe).
// Returns 200 if the service is ready to accept traffic, 503 otherwise.
func (s *HTTPServer) handleReady(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")

	if s.checker.IsReady() {
		w.WriteHeader(http.StatusOK)
		if r.Method != http.MethodHead {
			response := map[string]interface{}{
				"status":  "ready",
				"version": s.checker.Version(),
			}
			_ = json.NewEncoder(w).Encode(response)
		}
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
		if r.Method != http.MethodHead {
			response := map[string]interface{}{
				"status": "not ready",
			}
			_ = json.NewEncoder(w).Encode(response)
		}
	}
}

// Handler returns an http.Handler for use with an existing HTTP server.
// This allows integrating health endpoints into an existing server.
func (s *HTTPServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/health/", s.handleHealth)
	mux.HandleFunc("/health/live", s.handleLive)
	mux.HandleFunc("/health/ready", s.handleReady)
	return mux
}
