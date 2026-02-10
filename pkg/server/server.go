// Package server implements a TCP server with Memcached protocol support.
//
// The server provides a network interface to the RapidoDB storage engine,
// allowing clients to connect using any standard Memcached client library.
//
// Supported commands:
//   - get <key>           - Retrieve a value
//   - gets <key>          - Retrieve with CAS token
//   - set <key> ...       - Store a value
//   - add <key> ...       - Store only if not exists
//   - replace <key> ...   - Store only if exists
//   - delete <key>        - Remove a key
//   - incr <key> <delta>  - Increment numeric value
//   - decr <key> <delta>  - Decrement numeric value
//   - flush_all           - Clear all data
//   - stats               - Server statistics
//   - version             - Server version
//   - quit                - Close connection
package server

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rapidodb/rapidodb/pkg/lsm"
)

// Server is a TCP server that handles Memcached protocol requests.
type Server struct {
	// Configuration
	opts Options

	// Storage engine
	engine *lsm.Engine

	// Network
	listener net.Listener
	addr     string

	// Connection management
	connMu      sync.Mutex
	connections map[*Connection]struct{}
	connCount   atomic.Int64

	// Server state
	started   atomic.Bool
	closed    atomic.Bool
	closeOnce sync.Once

	// Statistics
	stats Stats

	// Shutdown coordination
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Options configures the server.
type Options struct {
	// Host is the address to bind to.
	Host string

	// Port is the port to listen on.
	Port int

	// ReadTimeout is the maximum duration for reading a request.
	ReadTimeout time.Duration

	// WriteTimeout is the maximum duration for writing a response.
	WriteTimeout time.Duration

	// IdleTimeout is the maximum duration a connection can be idle.
	IdleTimeout time.Duration

	// MaxConnections is the maximum number of concurrent connections.
	// 0 means unlimited.
	MaxConnections int

	// MaxKeySize is the maximum key size in bytes.
	MaxKeySize int

	// MaxValueSize is the maximum value size in bytes.
	MaxValueSize int

	// Version string to report.
	Version string
}

// DefaultOptions returns sensible default options.
func DefaultOptions() Options {
	return Options{
		Host:           "127.0.0.1",
		Port:           11211,
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   30 * time.Second,
		IdleTimeout:    5 * time.Minute,
		MaxConnections: 10000,
		MaxKeySize:     250,
		MaxValueSize:   1024 * 1024, // 1MB
		Version:        "RapidoDB/1.0.0",
	}
}

// Stats holds server statistics.
type Stats struct {
	// Connection stats
	TotalConnections atomic.Uint64
	ActiveConns      atomic.Int64

	// Command stats
	CmdGet    atomic.Uint64
	CmdSet    atomic.Uint64
	CmdDelete atomic.Uint64
	CmdFlush  atomic.Uint64
	CmdOther  atomic.Uint64

	// Hit/miss stats
	GetHits   atomic.Uint64
	GetMisses atomic.Uint64

	// Byte stats
	BytesRead    atomic.Uint64
	BytesWritten atomic.Uint64

	// Timing
	StartTime time.Time
}

// New creates a new server with the given engine and options.
func New(engine *lsm.Engine, opts Options) *Server {
	ctx, cancel := context.WithCancel(context.Background())

	s := &Server{
		opts:        opts,
		engine:      engine,
		connections: make(map[*Connection]struct{}),
		ctx:         ctx,
		cancel:      cancel,
	}

	s.stats.StartTime = time.Now()

	return s
}

// Start begins listening for connections.
func (s *Server) Start() error {
	if s.started.Swap(true) {
		return fmt.Errorf("server already started")
	}

	addr := fmt.Sprintf("%s:%d", s.opts.Host, s.opts.Port)

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		s.started.Store(false)
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	s.listener = listener
	s.addr = listener.Addr().String()

	// Start accept loop
	s.wg.Add(1)
	go s.acceptLoop()

	return nil
}

// Addr returns the server's listen address.
func (s *Server) Addr() string {
	return s.addr
}

// acceptLoop accepts new connections.
func (s *Server) acceptLoop() {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if s.closed.Load() {
				return
			}
			// Temporary error, continue
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			return
		}

		// Check connection limit
		if s.opts.MaxConnections > 0 && s.connCount.Load() >= int64(s.opts.MaxConnections) {
			_ = conn.Close()
			continue
		}

		// Handle connection
		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

// handleConnection handles a single client connection.
func (s *Server) handleConnection(netConn net.Conn) {
	defer s.wg.Done()

	// Update stats
	s.stats.TotalConnections.Add(1)
	s.stats.ActiveConns.Add(1)
	s.connCount.Add(1)

	defer func() {
		s.stats.ActiveConns.Add(-1)
		s.connCount.Add(-1)
	}()

	// Create connection handler
	conn := NewConnection(netConn, s)

	// Track connection
	s.connMu.Lock()
	s.connections[conn] = struct{}{}
	s.connMu.Unlock()

	defer func() {
		s.connMu.Lock()
		delete(s.connections, conn)
		s.connMu.Unlock()
		_ = conn.Close()
	}()

	// Handle requests
	conn.Serve(s.ctx)
}

// Close gracefully shuts down the server.
func (s *Server) Close() error {
	var err error

	s.closeOnce.Do(func() {
		s.closed.Store(true)

		// Cancel context to signal all goroutines
		s.cancel()

		// Close listener
		if s.listener != nil {
			err = s.listener.Close()
		}

		// Close all connections
		s.connMu.Lock()
		for conn := range s.connections {
			_ = conn.Close()
		}
		s.connMu.Unlock()

		// Wait for all goroutines
		s.wg.Wait()
	})

	return err
}

// Stats returns server statistics. Returns a pointer to avoid
// copying atomic values (copylocks).
func (s *Server) Stats() *Stats {
	return &s.stats
}

// Engine returns the storage engine.
func (s *Server) Engine() *lsm.Engine {
	return s.engine
}

// Options returns the server options.
func (s *Server) Options() Options {
	return s.opts
}
