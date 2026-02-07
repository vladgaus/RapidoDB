package server

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"time"
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
	defer c.Close()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Set read deadline
		if c.server.opts.ReadTimeout > 0 {
			c.conn.SetReadDeadline(time.Now().Add(c.server.opts.ReadTimeout))
		}

		// Read and parse command
		cmd, err := c.parser.ReadCommand()
		if err != nil {
			if err == io.EOF {
				return
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				// Idle timeout - close connection
				return
			}
			// Send error and continue
			c.resp.Reset()
			if err == ErrBadCommand {
				c.resp.WriteError()
			} else if err == ErrBadKey || err == ErrValueTooLarge {
				c.resp.WriteClientError(err.Error())
			} else {
				c.resp.WriteServerError(err.Error())
			}
			c.write(c.resp.Bytes())
			continue
		}

		// Handle command
		c.handleCommand(cmd)
	}
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
		c.Close()
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
		c.write(c.resp.Bytes())
	}
}

// handleGet handles get/gets commands.
func (c *Connection) handleGet(cmd *Command, withCAS bool) {
	c.server.stats.CmdGet.Add(1)

	for _, key := range cmd.Keys {
		value, err := c.server.engine.Get([]byte(key))
		if err != nil || value == nil {
			c.server.stats.GetMisses.Add(1)
			continue
		}

		c.server.stats.GetHits.Add(1)

		// Decode stored format: flags (4 bytes) + data
		if len(value) < 4 {
			continue
		}
		flags := binary.LittleEndian.Uint32(value[:4])
		data := value[4:]

		if withCAS {
			// For CAS, we use a hash of the value as the unique token
			// In a real implementation, this would be a version number
			cas := uint64(hashBytes(data))
			c.resp.WriteValueWithCAS(key, flags, data, cas)
		} else {
			c.resp.WriteValue(key, flags, data)
		}
	}

	c.resp.WriteEnd()
}

// handleSet handles set command.
func (c *Connection) handleSet(cmd *Command) {
	c.server.stats.CmdSet.Add(1)

	// Encode value with flags: flags (4 bytes) + data
	value := make([]byte, 4+len(cmd.Data))
	binary.LittleEndian.PutUint32(value[:4], cmd.Flags)
	copy(value[4:], cmd.Data)

	err := c.server.engine.Put([]byte(cmd.Keys[0]), value)
	if err != nil {
		c.resp.WriteServerError(err.Error())
		return
	}

	c.resp.WriteStored()
}

// handleAdd handles add command (store only if not exists).
func (c *Connection) handleAdd(cmd *Command) {
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
}

// handleReplace handles replace command (store only if exists).
func (c *Connection) handleReplace(cmd *Command) {
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
}

// handleDelete handles delete command.
func (c *Connection) handleDelete(cmd *Command) {
	c.server.stats.CmdDelete.Add(1)

	key := []byte(cmd.Keys[0])

	// Check if key exists
	existing, err := c.server.engine.Get(key)
	if err != nil || existing == nil {
		c.resp.WriteNotFound()
		return
	}

	err = c.server.engine.Delete(key)
	if err != nil {
		c.resp.WriteServerError(err.Error())
		return
	}

	c.resp.WriteDeleted()
}

// handleIncr handles incr/decr commands.
func (c *Connection) handleIncr(cmd *Command, increment bool) {
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
}

// handleFlushAll handles flush_all command.
func (c *Connection) handleFlushAll(cmd *Command) {
	c.server.stats.CmdFlush.Add(1)

	// Note: Real flush_all would need to be implemented in the engine
	// For now, we just acknowledge the command
	// TODO: Implement actual flush in engine

	c.resp.WriteOK()
}

// handleStats handles stats command.
func (c *Connection) handleStats(cmd *Command) {
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
func (c *Connection) handleVersion(cmd *Command) {
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
		c.conn.SetWriteDeadline(time.Now().Add(c.server.opts.WriteTimeout))
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
