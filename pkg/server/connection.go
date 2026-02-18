package server

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/vladgaus/RapidoDB/pkg/tracing"
)

// Connection handles a single client connection.
type Connection struct {
	conn   net.Conn
	server *Server
	parser *Parser
	resp   *ResponseWriter

	mu     sync.Mutex
	closed bool
}

// NewConnection creates a new connection handler.
func NewConnection(conn net.Conn, server *Server) *Connection {
	return &Connection{
		conn:   conn,
		server: server,
		parser: NewParser(conn, server.opts.MaxKeySize, server.opts.MaxValueSize),
		resp:   NewResponseWriter(),
	}
}

// Serve handles requests until the connection is closed.
func (c *Connection) Serve(ctx context.Context) {
	defer func() { _ = c.Close() }()

	// Get client IP for rate limiting
	clientIP := c.clientIP()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Set read deadline
		if c.server.opts.ReadTimeout > 0 {
			_ = c.conn.SetReadDeadline(time.Now().Add(c.server.opts.ReadTimeout))
		}

		// Read and parse command
		cmd, err := c.parser.ReadCommand()
		if err != nil {
			// EOF means client closed connection - exit silently
			if err == io.EOF {
				return
			}
			// Check for closed connection errors
			if isConnectionClosed(err) {
				return
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				// Idle timeout - close connection
				return
			}
			// Send error and continue
			c.resp.Reset()
			switch err {
			case ErrBadCommand:
				c.resp.WriteError()
			case ErrBadKey, ErrValueTooLarge:
				c.resp.WriteClientError(err.Error())
			default:
				c.resp.WriteServerError(err.Error())
			}
			_ = c.write(c.resp.Bytes())
			continue
		}

		// Check rate limit before processing command
		allowed, retryAfter := c.server.CheckRateLimit(clientIP)
		if !allowed {
			c.resp.Reset()
			// Use SERVER_ERROR with retry-after hint for backpressure signaling
			c.resp.WriteServerError("RATE_LIMITED retry_after=" + retryAfter.String())
			_ = c.write(c.resp.Bytes())
			continue
		}

		// Handle command
		c.handleCommand(cmd)
	}
}

// clientIP extracts the client IP address from the connection.
func (c *Connection) clientIP() string {
	addr := c.conn.RemoteAddr()
	if addr == nil {
		return "unknown"
	}

	// Extract IP from host:port
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		return addr.String()
	}
	return host
}

// isConnectionClosed checks if the error indicates a closed connection.
func isConnectionClosed(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "use of closed network connection") ||
		strings.Contains(errStr, "connection reset by peer") ||
		strings.Contains(errStr, "broken pipe")
}

// handleCommand dispatches and executes a command.
func (c *Connection) handleCommand(cmd *Command) {
	c.resp.Reset()

	switch cmd.Name {
	case "get":
		c.handleGet(cmd, false)
	case "gets":
		c.handleGet(cmd, true)
	case "set":
		c.handleSet(cmd)
	case "add":
		c.handleAdd(cmd)
	case "replace":
		c.handleReplace(cmd)
	case "delete":
		c.handleDelete(cmd)
	case "incr":
		c.handleIncr(cmd, true)
	case "decr":
		c.handleIncr(cmd, false)
	case "flush_all":
		c.handleFlushAll(cmd)
	case "stats":
		c.handleStats(cmd)
	case "version":
		c.handleVersion(cmd)
	case "quit":
		_ = c.Close()
		return
	case "verbosity":
		// Accept but ignore
		if !cmd.Noreply {
			c.resp.WriteOK()
		}
	default:
		c.resp.WriteError()
	}

	// Send response
	if !cmd.Noreply && c.resp.buf.Len() > 0 {
		_ = c.write(c.resp.Bytes())
	}
}

// handleGet handles get/gets commands.
func (c *Connection) handleGet(cmd *Command, withCAS bool) {
	start := time.Now()
	c.server.stats.CmdGet.Add(1)

	// Start tracing span
	var span *tracing.Span
	if c.server.tracer != nil {
		_, span = c.server.tracer.Start(context.Background(), tracing.SpanGet,
			tracing.WithSpanKind(tracing.SpanKindServer),
			tracing.WithAttributes(
				tracing.String(tracing.AttrDBSystem, "rapidodb"),
				tracing.String(tracing.AttrDBOperation, "get"),
				tracing.Int(tracing.AttrDBTableCount, len(cmd.Keys)),
			),
		)
		defer span.End()
	}

	var bytesRead int64
	var hits, misses int

	for _, key := range cmd.Keys {
		value, err := c.server.engine.Get([]byte(key))
		if err != nil || value == nil {
			c.server.stats.GetMisses.Add(1)
			if c.server.metrics != nil {
				c.server.metrics.CacheMisses.Inc()
			}
			misses++
			continue
		}

		c.server.stats.GetHits.Add(1)
		if c.server.metrics != nil {
			c.server.metrics.CacheHits.Inc()
		}
		hits++
		bytesRead += int64(len(value))

		// Decode stored format: flags (4 bytes) + data
		if len(value) < 4 {
			continue
		}
		flags := binary.LittleEndian.Uint32(value[:4])
		data := value[4:]

		if withCAS {
			// For CAS, we use a hash of the value as the unique token
			// In a real implementation, this would be a version number
			cas := hashBytes(data)
			c.resp.WriteValueWithCAS(key, flags, data, cas)
		} else {
			c.resp.WriteValue(key, flags, data)
		}
	}

	c.resp.WriteEnd()

	// Update span with results
	if span != nil {
		span.SetAttributes(
			tracing.Int64(tracing.AttrDBBytesRead, bytesRead),
			tracing.Int("cache.hits", hits),
			tracing.Int("cache.misses", misses),
		)
		span.SetStatus(tracing.StatusOK, "")
	}

	// Update metrics
	if c.server.metrics != nil {
		c.server.metrics.ReadsTotal.Add(uint64(len(cmd.Keys)))
		c.server.metrics.ReadLatency.ObserveDuration(start)
	}
}

// handleSet handles set command.
func (c *Connection) handleSet(cmd *Command) {
	start := time.Now()
	c.server.stats.CmdSet.Add(1)

	// Start tracing span
	var span *tracing.Span
	if c.server.tracer != nil {
		_, span = c.server.tracer.Start(context.Background(), tracing.SpanPut,
			tracing.WithSpanKind(tracing.SpanKindServer),
			tracing.WithAttributes(
				tracing.String(tracing.AttrDBSystem, "rapidodb"),
				tracing.String(tracing.AttrDBOperation, "set"),
				tracing.String(tracing.AttrDBKey, cmd.Keys[0]),
				tracing.Int(tracing.AttrDBKeySize, len(cmd.Keys[0])),
				tracing.Int(tracing.AttrDBValueSize, len(cmd.Data)),
			),
		)
		defer span.End()
	}

	// Encode value with flags: flags (4 bytes) + data
	value := make([]byte, 4+len(cmd.Data))
	binary.LittleEndian.PutUint32(value[:4], cmd.Flags)
	copy(value[4:], cmd.Data)

	err := c.server.engine.Put([]byte(cmd.Keys[0]), value)
	if err != nil {
		if span != nil {
			span.RecordError(err)
		}
		c.resp.WriteServerError(err.Error())
		return
	}

	if span != nil {
		span.SetAttributes(tracing.Int64(tracing.AttrDBBytesWrite, int64(len(value))))
		span.SetStatus(tracing.StatusOK, "")
	}

	c.resp.WriteStored()

	// Update metrics
	if c.server.metrics != nil {
		c.server.metrics.WritesTotal.Inc()
		c.server.metrics.WriteLatency.ObserveDuration(start)
	}
}

// handleAdd handles add command (store only if not exists).
func (c *Connection) handleAdd(cmd *Command) {
	start := time.Now()
	c.server.stats.CmdSet.Add(1)

	key := []byte(cmd.Keys[0])

	// Check if key exists
	existing, err := c.server.engine.Get(key)
	if err == nil && existing != nil {
		c.resp.WriteNotStored()
		return
	}

	// Encode and store
	value := make([]byte, 4+len(cmd.Data))
	binary.LittleEndian.PutUint32(value[:4], cmd.Flags)
	copy(value[4:], cmd.Data)

	err = c.server.engine.Put(key, value)
	if err != nil {
		c.resp.WriteServerError(err.Error())
		return
	}

	c.resp.WriteStored()

	// Update metrics
	if c.server.metrics != nil {
		c.server.metrics.WritesTotal.Inc()
		c.server.metrics.WriteLatency.ObserveDuration(start)
	}
}

// handleReplace handles replace command (store only if exists).
func (c *Connection) handleReplace(cmd *Command) {
	start := time.Now()
	c.server.stats.CmdSet.Add(1)

	key := []byte(cmd.Keys[0])

	// Check if key exists
	existing, err := c.server.engine.Get(key)
	if err != nil || existing == nil {
		c.resp.WriteNotStored()
		return
	}

	// Encode and store
	value := make([]byte, 4+len(cmd.Data))
	binary.LittleEndian.PutUint32(value[:4], cmd.Flags)
	copy(value[4:], cmd.Data)

	err = c.server.engine.Put(key, value)
	if err != nil {
		c.resp.WriteServerError(err.Error())
		return
	}

	c.resp.WriteStored()

	// Update metrics
	if c.server.metrics != nil {
		c.server.metrics.WritesTotal.Inc()
		c.server.metrics.WriteLatency.ObserveDuration(start)
	}
}

// handleDelete handles delete command.
func (c *Connection) handleDelete(cmd *Command) {
	start := time.Now()
	c.server.stats.CmdDelete.Add(1)

	key := []byte(cmd.Keys[0])

	// Start tracing span
	var span *tracing.Span
	if c.server.tracer != nil {
		_, span = c.server.tracer.Start(context.Background(), tracing.SpanDelete,
			tracing.WithSpanKind(tracing.SpanKindServer),
			tracing.WithAttributes(
				tracing.String(tracing.AttrDBSystem, "rapidodb"),
				tracing.String(tracing.AttrDBOperation, "delete"),
				tracing.String(tracing.AttrDBKey, cmd.Keys[0]),
				tracing.Int(tracing.AttrDBKeySize, len(cmd.Keys[0])),
			),
		)
		defer span.End()
	}

	// Check if key exists
	existing, err := c.server.engine.Get(key)
	if err != nil || existing == nil {
		if span != nil {
			span.SetStatus(tracing.StatusOK, "not_found")
		}
		c.resp.WriteNotFound()
		return
	}

	err = c.server.engine.Delete(key)
	if err != nil {
		if span != nil {
			span.RecordError(err)
		}
		c.resp.WriteServerError(err.Error())
		return
	}

	if span != nil {
		span.SetStatus(tracing.StatusOK, "")
	}

	c.resp.WriteDeleted()

	// Update metrics
	if c.server.metrics != nil {
		c.server.metrics.DeletesTotal.Inc()
		c.server.metrics.DeleteLatency.ObserveDuration(start)
	}
}

// handleIncr handles incr/decr commands.
func (c *Connection) handleIncr(cmd *Command, increment bool) {
	start := time.Now()
	c.server.stats.CmdOther.Add(1)

	key := []byte(cmd.Keys[0])

	// Get current value
	value, err := c.server.engine.Get(key)
	if err != nil || value == nil {
		c.resp.WriteNotFound()
		return
	}

	// Decode - skip flags (4 bytes)
	if len(value) < 4 {
		c.resp.WriteClientError("cannot increment non-numeric value")
		return
	}
	data := value[4:]

	// Parse as number
	var current uint64
	for _, b := range data {
		if b < '0' || b > '9' {
			c.resp.WriteClientError("cannot increment non-numeric value")
			return
		}
		current = current*10 + uint64(b-'0')
	}

	// Apply delta
	var newVal uint64
	if increment {
		newVal = current + cmd.Delta
	} else {
		if cmd.Delta > current {
			newVal = 0
		} else {
			newVal = current - cmd.Delta
		}
	}

	// Store new value
	newData := []byte(uintToString(newVal))
	newValue := make([]byte, 4+len(newData))
	copy(newValue[:4], value[:4]) // Preserve flags
	copy(newValue[4:], newData)

	err = c.server.engine.Put(key, newValue)
	if err != nil {
		c.resp.WriteServerError(err.Error())
		return
	}

	c.resp.WriteNumber(newVal)

	// Update metrics (incr/decr is a read-modify-write)
	if c.server.metrics != nil {
		c.server.metrics.WritesTotal.Inc()
		c.server.metrics.WriteLatency.ObserveDuration(start)
	}
}

// handleFlushAll handles flush_all command.
func (c *Connection) handleFlushAll(_ *Command) {
	c.server.stats.CmdFlush.Add(1)

	// Note: Real flush_all would need to be implemented in the engine
	// For now, we just acknowledge the command
	// TODO: Implement actual flush in engine

	c.resp.WriteOK()
}

// handleStats handles stats command.
func (c *Connection) handleStats(_ *Command) {
	c.server.stats.CmdOther.Add(1)

	stats := &c.server.stats
	opts := c.server.opts

	// Basic stats
	c.resp.WriteStat("pid", 1) // Process ID - simplified
	c.resp.WriteStat("uptime", int64(time.Since(stats.StartTime).Seconds()))
	c.resp.WriteStat("time", time.Now().Unix())
	c.resp.WriteStat("version", opts.Version)

	// Connection stats
	c.resp.WriteStat("curr_connections", stats.ActiveConns.Load())
	c.resp.WriteStat("total_connections", stats.TotalConnections.Load())

	// Command stats
	c.resp.WriteStat("cmd_get", stats.CmdGet.Load())
	c.resp.WriteStat("cmd_set", stats.CmdSet.Load())
	c.resp.WriteStat("cmd_delete", stats.CmdDelete.Load())
	c.resp.WriteStat("cmd_flush", stats.CmdFlush.Load())

	// Hit/miss stats
	c.resp.WriteStat("get_hits", stats.GetHits.Load())
	c.resp.WriteStat("get_misses", stats.GetMisses.Load())

	// Byte stats
	c.resp.WriteStat("bytes_read", stats.BytesRead.Load())
	c.resp.WriteStat("bytes_written", stats.BytesWritten.Load())

	// Engine stats
	if c.server.engine != nil {
		engineStats := c.server.engine.Stats()
		c.resp.WriteStat("curr_items", engineStats.TotalKeyValuePairs)
		c.resp.WriteStat("bytes", engineStats.MemTableSize)
		c.resp.WriteStat("engine_seqnum", engineStats.SeqNum)
		c.resp.WriteStat("engine_l0_files", engineStats.L0TableCount)
		c.resp.WriteStat("engine_immutable_memtables", engineStats.ImmutableCount)
	}

	c.resp.WriteEnd()
}

// handleVersion handles version command.
func (c *Connection) handleVersion(_ *Command) {
	c.server.stats.CmdOther.Add(1)
	c.resp.WriteVersion(c.server.opts.Version)
}

// write sends data to the client.
func (c *Connection) write(data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return io.ErrClosedPipe
	}

	// Set write deadline
	if c.server.opts.WriteTimeout > 0 {
		_ = c.conn.SetWriteDeadline(time.Now().Add(c.server.opts.WriteTimeout))
	}

	n, err := c.conn.Write(data)
	c.server.stats.BytesWritten.Add(uint64(n))
	return err
}

// Close closes the connection.
func (c *Connection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true

	return c.conn.Close()
}

// hashBytes computes a simple hash for CAS tokens.
func hashBytes(data []byte) uint64 {
	// FNV-1a hash
	var hash uint64 = 14695981039346656037
	for _, b := range data {
		hash ^= uint64(b)
		hash *= 1099511628211
	}
	return hash
}

// uintToString converts uint64 to string without allocation.
func uintToString(n uint64) string {
	if n == 0 {
		return "0"
	}

	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
