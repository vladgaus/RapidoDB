package server

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rapidodb/rapidodb/pkg/lsm"
)

// testServer creates a test server with a temporary database.
func testServer(t *testing.T) (*Server, string) {
	t.Helper()

	dir, err := os.MkdirTemp("", "rapidodb-server-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	opts := lsm.DefaultOptions(dir)
	engine, err := lsm.Open(opts)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("failed to open engine: %v", err)
	}

	serverOpts := DefaultOptions()
	serverOpts.Host = "127.0.0.1"
	serverOpts.Port = 0 // Random port
	serverOpts.ReadTimeout = 5 * time.Second
	serverOpts.WriteTimeout = 5 * time.Second

	srv := New(engine, serverOpts)
	if err := srv.Start(); err != nil {
		engine.Close()
		os.RemoveAll(dir)
		t.Fatalf("failed to start server: %v", err)
	}

	return srv, dir
}

// testClient creates a connection to the server.
func testClient(t *testing.T, addr string) net.Conn {
	t.Helper()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	return conn
}

// sendCommand sends a command and returns the response.
func sendCommand(conn net.Conn, cmd string) (string, error) {
	_, err := fmt.Fprintf(conn, "%s\r\n", cmd)
	if err != nil {
		return "", err
	}

	reader := bufio.NewReader(conn)
	var lines []string

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return strings.Join(lines, ""), err
		}
		lines = append(lines, line)

		// Check for termination
		trimmed := strings.TrimSpace(line)
		if trimmed == "END" || trimmed == "STORED" || trimmed == "NOT_STORED" ||
			trimmed == "DELETED" || trimmed == "NOT_FOUND" || trimmed == "EXISTS" ||
			trimmed == "OK" || trimmed == "ERROR" ||
			strings.HasPrefix(trimmed, "VERSION") ||
			strings.HasPrefix(trimmed, "CLIENT_ERROR") ||
			strings.HasPrefix(trimmed, "SERVER_ERROR") {
			break
		}
		// For incr/decr, the response is just a number
		if _, err := fmt.Sscanf(trimmed, "%d", new(uint64)); err == nil {
			break
		}
	}

	return strings.Join(lines, ""), nil
}

// sendStorageCommand sends a storage command (set, add, etc.) with data.
func sendStorageCommand(conn net.Conn, cmd, key string, flags uint32, exptime int64, data string) (string, error) {
	_, err := fmt.Fprintf(conn, "%s %s %d %d %d\r\n%s\r\n", cmd, key, flags, exptime, len(data), data)
	if err != nil {
		return "", err
	}

	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return line, nil
}

func TestServerStartStop(t *testing.T) {
	srv, dir := testServer(t)
	defer os.RemoveAll(dir)
	defer srv.Engine().Close()

	// Verify server is listening
	if srv.Addr() == "" {
		t.Error("server address is empty")
	}

	// Connect to verify it's working
	conn := testClient(t, srv.Addr())
	conn.Close()

	// Stop server
	if err := srv.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

func TestVersion(t *testing.T) {
	srv, dir := testServer(t)
	defer os.RemoveAll(dir)
	defer srv.Engine().Close()
	defer srv.Close()

	conn := testClient(t, srv.Addr())
	defer conn.Close()

	resp, err := sendCommand(conn, "version")
	if err != nil {
		t.Fatalf("version failed: %v", err)
	}

	if !strings.HasPrefix(resp, "VERSION") {
		t.Errorf("unexpected response: %s", resp)
	}
}

func TestSetGet(t *testing.T) {
	srv, dir := testServer(t)
	defer os.RemoveAll(dir)
	defer srv.Engine().Close()
	defer srv.Close()

	conn := testClient(t, srv.Addr())
	defer conn.Close()

	// Set a value
	resp, err := sendStorageCommand(conn, "set", "mykey", 0, 0, "hello")
	if err != nil {
		t.Fatalf("set failed: %v", err)
	}
	if !strings.Contains(resp, "STORED") {
		t.Errorf("set response: %s, want STORED", resp)
	}

	// Get the value
	resp, err = sendCommand(conn, "get mykey")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}

	if !strings.Contains(resp, "VALUE mykey") {
		t.Errorf("get response missing VALUE: %s", resp)
	}
	if !strings.Contains(resp, "hello") {
		t.Errorf("get response missing data: %s", resp)
	}
	if !strings.Contains(resp, "END") {
		t.Errorf("get response missing END: %s", resp)
	}
}

func TestGetMiss(t *testing.T) {
	srv, dir := testServer(t)
	defer os.RemoveAll(dir)
	defer srv.Engine().Close()
	defer srv.Close()

	conn := testClient(t, srv.Addr())
	defer conn.Close()

	resp, err := sendCommand(conn, "get nonexistent")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}

	// Should only return END
	if strings.TrimSpace(resp) != "END" {
		t.Errorf("get miss response: %q, want END", resp)
	}
}

func TestDelete(t *testing.T) {
	srv, dir := testServer(t)
	defer os.RemoveAll(dir)
	defer srv.Engine().Close()
	defer srv.Close()

	conn := testClient(t, srv.Addr())
	defer conn.Close()

	// Set a value
	sendStorageCommand(conn, "set", "delkey", 0, 0, "value")

	// Delete it
	resp, err := sendCommand(conn, "delete delkey")
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	if !strings.Contains(resp, "DELETED") {
		t.Errorf("delete response: %s, want DELETED", resp)
	}

	// Verify it's gone
	resp, err = sendCommand(conn, "get delkey")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if strings.TrimSpace(resp) != "END" {
		t.Errorf("get after delete: %q, want END", resp)
	}
}

func TestDeleteNotFound(t *testing.T) {
	srv, dir := testServer(t)
	defer os.RemoveAll(dir)
	defer srv.Engine().Close()
	defer srv.Close()

	conn := testClient(t, srv.Addr())
	defer conn.Close()

	resp, err := sendCommand(conn, "delete nonexistent")
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	if !strings.Contains(resp, "NOT_FOUND") {
		t.Errorf("delete response: %s, want NOT_FOUND", resp)
	}
}

func TestAdd(t *testing.T) {
	srv, dir := testServer(t)
	defer os.RemoveAll(dir)
	defer srv.Engine().Close()
	defer srv.Close()

	conn := testClient(t, srv.Addr())
	defer conn.Close()

	// Add new key
	resp, err := sendStorageCommand(conn, "add", "newkey", 0, 0, "value")
	if err != nil {
		t.Fatalf("add failed: %v", err)
	}
	if !strings.Contains(resp, "STORED") {
		t.Errorf("add response: %s, want STORED", resp)
	}

	// Try to add again - should fail
	resp, err = sendStorageCommand(conn, "add", "newkey", 0, 0, "value2")
	if err != nil {
		t.Fatalf("add failed: %v", err)
	}
	if !strings.Contains(resp, "NOT_STORED") {
		t.Errorf("add existing response: %s, want NOT_STORED", resp)
	}
}

func TestReplace(t *testing.T) {
	srv, dir := testServer(t)
	defer os.RemoveAll(dir)
	defer srv.Engine().Close()
	defer srv.Close()

	conn := testClient(t, srv.Addr())
	defer conn.Close()

	// Replace non-existent key - should fail
	resp, err := sendStorageCommand(conn, "replace", "nokey", 0, 0, "value")
	if err != nil {
		t.Fatalf("replace failed: %v", err)
	}
	if !strings.Contains(resp, "NOT_STORED") {
		t.Errorf("replace nonexistent response: %s, want NOT_STORED", resp)
	}

	// Set the key
	sendStorageCommand(conn, "set", "nokey", 0, 0, "original")

	// Now replace should work
	resp, err = sendStorageCommand(conn, "replace", "nokey", 0, 0, "replaced")
	if err != nil {
		t.Fatalf("replace failed: %v", err)
	}
	if !strings.Contains(resp, "STORED") {
		t.Errorf("replace response: %s, want STORED", resp)
	}
}

func TestIncrDecr(t *testing.T) {
	srv, dir := testServer(t)
	defer os.RemoveAll(dir)
	defer srv.Engine().Close()
	defer srv.Close()

	conn := testClient(t, srv.Addr())
	defer conn.Close()

	// Set initial numeric value
	sendStorageCommand(conn, "set", "counter", 0, 0, "100")

	// Increment
	resp, err := sendCommand(conn, "incr counter 5")
	if err != nil {
		t.Fatalf("incr failed: %v", err)
	}
	if !strings.Contains(resp, "105") {
		t.Errorf("incr response: %s, want 105", resp)
	}

	// Decrement
	resp, err = sendCommand(conn, "decr counter 10")
	if err != nil {
		t.Fatalf("decr failed: %v", err)
	}
	if !strings.Contains(resp, "95") {
		t.Errorf("decr response: %s, want 95", resp)
	}
}

func TestIncrUnderflow(t *testing.T) {
	srv, dir := testServer(t)
	defer os.RemoveAll(dir)
	defer srv.Engine().Close()
	defer srv.Close()

	conn := testClient(t, srv.Addr())
	defer conn.Close()

	sendStorageCommand(conn, "set", "counter", 0, 0, "5")

	// Decrement by more than value - should floor at 0
	resp, err := sendCommand(conn, "decr counter 10")
	if err != nil {
		t.Fatalf("decr failed: %v", err)
	}
	if !strings.Contains(resp, "0") {
		t.Errorf("decr underflow response: %s, want 0", resp)
	}
}

func TestStats(t *testing.T) {
	srv, dir := testServer(t)
	defer os.RemoveAll(dir)
	defer srv.Engine().Close()
	defer srv.Close()

	conn := testClient(t, srv.Addr())
	defer conn.Close()

	// Do some operations
	sendStorageCommand(conn, "set", "key1", 0, 0, "value1")
	sendCommand(conn, "get key1")
	sendCommand(conn, "get missing")

	// Get stats
	resp, err := sendCommand(conn, "stats")
	if err != nil {
		t.Fatalf("stats failed: %v", err)
	}

	// Verify some stats are present
	if !strings.Contains(resp, "STAT uptime") {
		t.Error("missing uptime stat")
	}
	if !strings.Contains(resp, "STAT cmd_get") {
		t.Error("missing cmd_get stat")
	}
	if !strings.Contains(resp, "STAT cmd_set") {
		t.Error("missing cmd_set stat")
	}
	if !strings.Contains(resp, "END") {
		t.Error("missing END")
	}
}

func TestMultipleKeys(t *testing.T) {
	srv, dir := testServer(t)
	defer os.RemoveAll(dir)
	defer srv.Engine().Close()
	defer srv.Close()

	conn := testClient(t, srv.Addr())
	defer conn.Close()

	// Set multiple keys
	sendStorageCommand(conn, "set", "key1", 0, 0, "value1")
	sendStorageCommand(conn, "set", "key2", 0, 0, "value2")
	sendStorageCommand(conn, "set", "key3", 0, 0, "value3")

	// Get multiple keys at once
	resp, err := sendCommand(conn, "get key1 key2 key3 missing")
	if err != nil {
		t.Fatalf("multi-get failed: %v", err)
	}

	// Should have all three values
	if !strings.Contains(resp, "VALUE key1") {
		t.Error("missing key1")
	}
	if !strings.Contains(resp, "VALUE key2") {
		t.Error("missing key2")
	}
	if !strings.Contains(resp, "VALUE key3") {
		t.Error("missing key3")
	}
	// missing should not appear
	if strings.Contains(resp, "VALUE missing") {
		t.Error("should not contain missing")
	}
}

func TestFlags(t *testing.T) {
	srv, dir := testServer(t)
	defer os.RemoveAll(dir)
	defer srv.Engine().Close()
	defer srv.Close()

	conn := testClient(t, srv.Addr())
	defer conn.Close()

	// Set with flags
	resp, err := sendStorageCommand(conn, "set", "flagkey", 12345, 0, "data")
	if err != nil {
		t.Fatalf("set failed: %v", err)
	}
	if !strings.Contains(resp, "STORED") {
		t.Errorf("set response: %s", resp)
	}

	// Get and verify flags
	resp, err = sendCommand(conn, "get flagkey")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if !strings.Contains(resp, "12345") {
		t.Errorf("flags not preserved: %s", resp)
	}
}

func TestQuit(t *testing.T) {
	srv, dir := testServer(t)
	defer os.RemoveAll(dir)
	defer srv.Engine().Close()
	defer srv.Close()

	conn := testClient(t, srv.Addr())
	defer conn.Close()

	// Send quit
	fmt.Fprintf(conn, "quit\r\n")

	// Connection should be closed
	buf := make([]byte, 1)
	conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	_, err := conn.Read(buf)
	if err == nil {
		t.Error("expected connection to be closed after quit")
	}
}

func TestConcurrentConnections(t *testing.T) {
	srv, dir := testServer(t)
	defer os.RemoveAll(dir)
	defer srv.Engine().Close()
	defer srv.Close()

	// Create multiple concurrent connections
	const numConns = 10
	done := make(chan error, numConns)

	for i := 0; i < numConns; i++ {
		go func(id int) {
			conn := testClient(t, srv.Addr())
			defer conn.Close()

			key := fmt.Sprintf("key%d", id)
			value := fmt.Sprintf("value%d", id)

			// Set
			_, err := sendStorageCommand(conn, "set", key, 0, 0, value)
			if err != nil {
				done <- err
				return
			}

			// Get
			resp, err := sendCommand(conn, "get "+key)
			if err != nil {
				done <- err
				return
			}

			if !strings.Contains(resp, value) {
				done <- fmt.Errorf("got %q, want %q", resp, value)
				return
			}

			done <- nil
		}(i)
	}

	// Wait for all
	for i := 0; i < numConns; i++ {
		if err := <-done; err != nil {
			t.Errorf("connection %d failed: %v", i, err)
		}
	}
}

func BenchmarkSetGet(b *testing.B) {
	dir, _ := os.MkdirTemp("", "rapidodb-bench-*")
	defer os.RemoveAll(dir)

	opts := lsm.DefaultOptions(dir)
	engine, _ := lsm.Open(opts)
	defer engine.Close()

	serverOpts := DefaultOptions()
	serverOpts.Host = "127.0.0.1"
	serverOpts.Port = 0

	srv := New(engine, serverOpts)
	srv.Start()
	defer srv.Close()

	conn, _ := net.Dial("tcp", srv.Addr())
	defer conn.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i)
		value := "benchmark-value"

		// Set
		fmt.Fprintf(conn, "set %s 0 0 %d\r\n%s\r\n", key, len(value), value)
		bufio.NewReader(conn).ReadString('\n')

		// Get
		fmt.Fprintf(conn, "get %s\r\n", key)
		reader := bufio.NewReader(conn)
		for {
			line, _ := reader.ReadString('\n')
			if strings.TrimSpace(line) == "END" {
				break
			}
		}
	}
}
