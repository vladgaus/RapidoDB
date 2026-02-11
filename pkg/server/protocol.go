package server

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Command represents a parsed Memcached command.
type Command struct {
	Name    string   // Command name (get, set, delete, etc.)
	Keys    []string // Key(s) for the command
	Flags   uint32   // Flags for storage commands
	Exptime int64    // Expiration time
	Bytes   int      // Number of data bytes (for storage commands)
	CAS     uint64   // CAS unique value
	Noreply bool     // Whether to suppress response
	Delta   uint64   // Delta for incr/decr
	Data    []byte   // Data payload for storage commands
}

// Response codes
const (
	RespStored      = "STORED\r\n"
	RespNotStored   = "NOT_STORED\r\n"
	RespExists      = "EXISTS\r\n"
	RespNotFound    = "NOT_FOUND\r\n"
	RespDeleted     = "DELETED\r\n"
	RespTouched     = "TOUCHED\r\n"
	RespOK          = "OK\r\n"
	RespEnd         = "END\r\n"
	RespError       = "ERROR\r\n"
	RespClientError = "CLIENT_ERROR %s\r\n"
	RespServerError = "SERVER_ERROR %s\r\n"
)

// Protocol errors
var (
	ErrBadCommand    = fmt.Errorf("bad command")
	ErrBadDataChunk  = fmt.Errorf("bad data chunk")
	ErrBadKey        = fmt.Errorf("bad key")
	ErrValueTooLarge = fmt.Errorf("value too large")
)

// Parser parses Memcached protocol commands.
type Parser struct {
	reader     *bufio.Reader
	maxKeySize int
	maxValSize int
}

// NewParser creates a new protocol parser.
func NewParser(r io.Reader, maxKeySize, maxValSize int) *Parser {
	return &Parser{
		reader:     bufio.NewReaderSize(r, 64*1024), // 64KB buffer
		maxKeySize: maxKeySize,
		maxValSize: maxValSize,
	}
}

// ReadCommand reads and parses the next command.
func (p *Parser) ReadCommand() (*Command, error) {
	// Read command line
	line, err := p.reader.ReadString('\n')
	if err != nil {
		// If we got EOF with no data, return EOF
		if err == io.EOF && line == "" {
			return nil, io.EOF
		}
		// If we got some data before EOF, try to process it
		if err == io.EOF && line != "" {
			// Continue processing below
		} else {
			return nil, err
		}
	}

	// Trim CRLF
	line = strings.TrimSuffix(line, "\r\n")
	line = strings.TrimSuffix(line, "\n")

	// Empty line - could be keep-alive or end of input
	if line == "" {
		return nil, io.EOF
	}

	// Split into parts
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return nil, io.EOF
	}

	cmd := &Command{
		Name: strings.ToLower(parts[0]),
	}

	// Parse based on command type
	switch cmd.Name {
	case "get", "gets":
		return p.parseGet(cmd, parts[1:])

	case "set", "add", "replace", "append", "prepend":
		return p.parseStorage(cmd, parts[1:])

	case "cas":
		return p.parseCAS(cmd, parts[1:])

	case "delete":
		return p.parseDelete(cmd, parts[1:])

	case "incr", "decr":
		return p.parseIncr(cmd, parts[1:])

	case "touch":
		return p.parseTouch(cmd, parts[1:])

	case "stats":
		return p.parseStats(cmd, parts[1:])

	case "flush_all":
		return p.parseFlushAll(cmd, parts[1:])

	case "version":
		return cmd, nil

	case "quit":
		return cmd, nil

	case "verbosity":
		return p.parseVerbosity(cmd, parts[1:])

	default:
		return nil, ErrBadCommand
	}
}

// parseGet parses: get <key>* or gets <key>*
func (p *Parser) parseGet(cmd *Command, args []string) (*Command, error) {
	if len(args) == 0 {
		return nil, ErrBadCommand
	}

	for _, key := range args {
		if err := p.validateKey(key); err != nil {
			return nil, err
		}
	}

	cmd.Keys = args
	return cmd, nil
}

// parseStorage parses: <cmd> <key> <flags> <exptime> <bytes> [noreply]
func (p *Parser) parseStorage(cmd *Command, args []string) (*Command, error) {
	if len(args) < 4 {
		return nil, ErrBadCommand
	}

	// Key
	if err := p.validateKey(args[0]); err != nil {
		return nil, err
	}
	cmd.Keys = []string{args[0]}

	// Flags
	flags, err := strconv.ParseUint(args[1], 10, 32)
	if err != nil {
		return nil, ErrBadCommand
	}
	cmd.Flags = uint32(flags)

	// Exptime
	exptime, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return nil, ErrBadCommand
	}
	cmd.Exptime = exptime

	// Bytes
	bytes, err := strconv.Atoi(args[3])
	if err != nil || bytes < 0 {
		return nil, ErrBadCommand
	}
	if bytes > p.maxValSize {
		return nil, ErrValueTooLarge
	}
	cmd.Bytes = bytes

	// Noreply
	if len(args) >= 5 && args[4] == "noreply" {
		cmd.Noreply = true
	}

	// Read data block
	data, err := p.readDataBlock(bytes)
	if err != nil {
		return nil, err
	}
	cmd.Data = data

	return cmd, nil
}

// parseCAS parses: cas <key> <flags> <exptime> <bytes> <cas unique> [noreply]
func (p *Parser) parseCAS(cmd *Command, args []string) (*Command, error) {
	if len(args) < 5 {
		return nil, ErrBadCommand
	}

	// Key
	if err := p.validateKey(args[0]); err != nil {
		return nil, err
	}
	cmd.Keys = []string{args[0]}

	// Flags
	flags, err := strconv.ParseUint(args[1], 10, 32)
	if err != nil {
		return nil, ErrBadCommand
	}
	cmd.Flags = uint32(flags)

	// Exptime
	exptime, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return nil, ErrBadCommand
	}
	cmd.Exptime = exptime

	// Bytes
	bytes, err := strconv.Atoi(args[3])
	if err != nil || bytes < 0 {
		return nil, ErrBadCommand
	}
	if bytes > p.maxValSize {
		return nil, ErrValueTooLarge
	}
	cmd.Bytes = bytes

	// CAS unique
	cas, err := strconv.ParseUint(args[4], 10, 64)
	if err != nil {
		return nil, ErrBadCommand
	}
	cmd.CAS = cas

	// Noreply
	if len(args) >= 6 && args[5] == "noreply" {
		cmd.Noreply = true
	}

	// Read data block
	data, err := p.readDataBlock(bytes)
	if err != nil {
		return nil, err
	}
	cmd.Data = data

	return cmd, nil
}

// parseDelete parses: delete <key> [noreply]
func (p *Parser) parseDelete(cmd *Command, args []string) (*Command, error) {
	if len(args) < 1 {
		return nil, ErrBadCommand
	}

	if err := p.validateKey(args[0]); err != nil {
		return nil, err
	}
	cmd.Keys = []string{args[0]}

	if len(args) >= 2 && args[1] == "noreply" {
		cmd.Noreply = true
	}

	return cmd, nil
}

// parseIncr parses: incr <key> <value> [noreply] or decr <key> <value> [noreply]
func (p *Parser) parseIncr(cmd *Command, args []string) (*Command, error) {
	if len(args) < 2 {
		return nil, ErrBadCommand
	}

	if err := p.validateKey(args[0]); err != nil {
		return nil, err
	}
	cmd.Keys = []string{args[0]}

	delta, err := strconv.ParseUint(args[1], 10, 64)
	if err != nil {
		return nil, ErrBadCommand
	}
	cmd.Delta = delta

	if len(args) >= 3 && args[2] == "noreply" {
		cmd.Noreply = true
	}

	return cmd, nil
}

// parseTouch parses: touch <key> <exptime> [noreply]
func (p *Parser) parseTouch(cmd *Command, args []string) (*Command, error) {
	if len(args) < 2 {
		return nil, ErrBadCommand
	}

	if err := p.validateKey(args[0]); err != nil {
		return nil, err
	}
	cmd.Keys = []string{args[0]}

	exptime, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return nil, ErrBadCommand
	}
	cmd.Exptime = exptime

	if len(args) >= 3 && args[2] == "noreply" {
		cmd.Noreply = true
	}

	return cmd, nil
}

// parseStats parses: stats [args]
func (p *Parser) parseStats(cmd *Command, args []string) (*Command, error) {
	if len(args) > 0 {
		cmd.Keys = args
	}
	return cmd, nil
}

// parseFlushAll parses: flush_all [delay] [noreply]
func (p *Parser) parseFlushAll(cmd *Command, args []string) (*Command, error) {
	if len(args) >= 1 {
		// First arg could be delay or noreply
		if args[0] == "noreply" {
			cmd.Noreply = true
		} else {
			delay, err := strconv.ParseInt(args[0], 10, 64)
			if err == nil {
				cmd.Exptime = delay // Use Exptime for delay
			}
			if len(args) >= 2 && args[1] == "noreply" {
				cmd.Noreply = true
			}
		}
	}
	return cmd, nil
}

// parseVerbosity parses: verbosity <level> [noreply]
func (p *Parser) parseVerbosity(cmd *Command, args []string) (*Command, error) {
	if len(args) >= 2 && args[1] == "noreply" {
		cmd.Noreply = true
	}
	return cmd, nil
}

// readDataBlock reads <bytes> of data plus trailing \r\n
func (p *Parser) readDataBlock(size int) ([]byte, error) {
	// Read exactly size bytes + 2 for \r\n
	data := make([]byte, size+2)
	_, err := io.ReadFull(p.reader, data)
	if err != nil {
		return nil, err
	}

	// Verify trailing \r\n
	if !bytes.HasSuffix(data, []byte("\r\n")) {
		return nil, ErrBadDataChunk
	}

	return data[:size], nil
}

// validateKey checks if a key is valid.
func (p *Parser) validateKey(key string) error {
	if len(key) == 0 {
		return ErrBadKey
	}
	if len(key) > p.maxKeySize {
		return ErrBadKey
	}

	// Keys cannot contain control characters or whitespace
	for _, c := range key {
		if c <= 32 || c == 127 {
			return ErrBadKey
		}
	}

	return nil
}

// ResponseWriter helps format Memcached responses.
type ResponseWriter struct {
	buf bytes.Buffer
}

// NewResponseWriter creates a new response writer.
func NewResponseWriter() *ResponseWriter {
	return &ResponseWriter{}
}

// WriteValue writes a VALUE response line.
func (w *ResponseWriter) WriteValue(key string, flags uint32, data []byte) {
	fmt.Fprintf(&w.buf, "VALUE %s %d %d\r\n", key, flags, len(data))
	w.buf.Write(data)
	w.buf.WriteString("\r\n")
}

// WriteValueWithCAS writes a VALUE response with CAS token.
func (w *ResponseWriter) WriteValueWithCAS(key string, flags uint32, data []byte, cas uint64) {
	fmt.Fprintf(&w.buf, "VALUE %s %d %d %d\r\n", key, flags, len(data), cas)
	w.buf.Write(data)
	w.buf.WriteString("\r\n")
}

// WriteEnd writes the END marker.
func (w *ResponseWriter) WriteEnd() {
	w.buf.WriteString(RespEnd)
}

// WriteStored writes STORED response.
func (w *ResponseWriter) WriteStored() {
	w.buf.WriteString(RespStored)
}

// WriteNotStored writes NOT_STORED response.
func (w *ResponseWriter) WriteNotStored() {
	w.buf.WriteString(RespNotStored)
}

// WriteExists writes EXISTS response.
func (w *ResponseWriter) WriteExists() {
	w.buf.WriteString(RespExists)
}

// WriteNotFound writes NOT_FOUND response.
func (w *ResponseWriter) WriteNotFound() {
	w.buf.WriteString(RespNotFound)
}

// WriteDeleted writes DELETED response.
func (w *ResponseWriter) WriteDeleted() {
	w.buf.WriteString(RespDeleted)
}

// WriteOK writes OK response.
func (w *ResponseWriter) WriteOK() {
	w.buf.WriteString(RespOK)
}

// WriteError writes ERROR response.
func (w *ResponseWriter) WriteError() {
	w.buf.WriteString(RespError)
}

// WriteClientError writes CLIENT_ERROR response.
func (w *ResponseWriter) WriteClientError(msg string) {
	fmt.Fprintf(&w.buf, RespClientError, msg)
}

// WriteServerError writes SERVER_ERROR response.
func (w *ResponseWriter) WriteServerError(msg string) {
	fmt.Fprintf(&w.buf, RespServerError, msg)
}

// WriteNumber writes a numeric response (for incr/decr).
func (w *ResponseWriter) WriteNumber(n uint64) {
	fmt.Fprintf(&w.buf, "%d\r\n", n)
}

// WriteVersion writes VERSION response.
func (w *ResponseWriter) WriteVersion(version string) {
	fmt.Fprintf(&w.buf, "VERSION %s\r\n", version)
}

// WriteStat writes a STAT line.
func (w *ResponseWriter) WriteStat(name string, value interface{}) {
	fmt.Fprintf(&w.buf, "STAT %s %v\r\n", name, value)
}

// Bytes returns the accumulated response bytes.
func (w *ResponseWriter) Bytes() []byte {
	return w.buf.Bytes()
}

// Reset clears the buffer.
func (w *ResponseWriter) Reset() {
	w.buf.Reset()
}
