// RapidoDB CLI - Command-line interface for RapidoDB
//
// Usage:
//
//	rapidodb-cli [options] <command> [arguments]
//
// Commands:
//
//	get <key>              Get a value
//	set <key> <value>      Set a value
//	delete <key>           Delete a key
//	scan [--prefix=]       Scan keys
//	stats                  Show server stats
//	ping                   Check server connection
//	backup                 Trigger backup
//	compact                Trigger compaction
//	flush                  Flush memtable
//	info                   Show server info
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// Build-time variables
var (
	Version   = "dev"
	BuildTime = "unknown"
)

// Config holds CLI configuration
type Config struct {
	Host        string
	Port        int
	Timeout     time.Duration
	AdminPort   int
	Interactive bool
}

func main() {
	cfg := &Config{}

	// Global flags
	flag.StringVar(&cfg.Host, "host", "localhost", "Server host")
	flag.StringVar(&cfg.Host, "h", "localhost", "Server host (shorthand)")
	flag.IntVar(&cfg.Port, "port", 11211, "Server port (memcached protocol)")
	flag.IntVar(&cfg.Port, "p", 11211, "Server port (shorthand)")
	flag.IntVar(&cfg.AdminPort, "admin-port", 9091, "Admin API port")
	flag.DurationVar(&cfg.Timeout, "timeout", 5*time.Second, "Connection timeout")
	flag.BoolVar(&cfg.Interactive, "i", false, "Interactive mode")

	showVersion := flag.Bool("version", false, "Show version")
	showHelp := flag.Bool("help", false, "Show help")

	flag.Usage = printUsage
	flag.Parse()

	if *showVersion {
		fmt.Printf("rapidodb-cli %s\n", Version)
		fmt.Printf("Build Time: %s\n", BuildTime)
		os.Exit(0)
	}

	if *showHelp {
		printUsage()
		os.Exit(0)
	}

	args := flag.Args()

	// Interactive mode
	if cfg.Interactive || len(args) == 0 {
		runInteractive(cfg)
		return
	}

	// Single command mode
	if err := runCommand(cfg, args); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`RapidoDB CLI - Command-line interface for RapidoDB

Usage:
  rapidodb-cli [options] <command> [arguments]

Options:
  -h, --host        Server host (default: localhost)
  -p, --port        Memcached port (default: 11211)
  --admin-port      Admin API port (default: 9091)
  --timeout         Connection timeout (default: 5s)
  -i                Interactive mode
  --version         Show version
  --help            Show this help

Commands:
  get <key>                  Get a value by key
  set <key> <value> [ttl]    Set a key-value pair (ttl in seconds)
  delete <key>               Delete a key
  scan [--prefix=PREFIX]     Scan all keys (optionally with prefix)
  keys [--prefix=PREFIX]     List keys (alias for scan)
  
  stats                      Show memcached stats
  ping                       Check server connection
  info                       Show server info (admin API)
  
  backup [--type=full]       Trigger backup (admin API)
  restore <backup-id> <dir>  Restore backup (admin API)
  backups                    List backups (admin API)
  
  compact [--level=N]        Trigger compaction (admin API)
  flush                      Flush memtable to disk (admin API)
  levels                     Show level stats (admin API)
  sstables                   List SSTables (admin API)
  
  import-csv <file>          Import from CSV (admin API)
  import-json <file>         Import from JSON Lines (admin API)
  export-csv <file>          Export to CSV (admin API)
  export-json <file>         Export to JSON Lines (admin API)

Examples:
  rapidodb-cli get mykey
  rapidodb-cli set user:1 '{"name":"John"}' 3600
  rapidodb-cli scan --prefix=user:
  rapidodb-cli -h 192.168.1.10 stats
  rapidodb-cli -i   # interactive mode

Interactive Mode Commands:
  connect [host:port]        Connect to server
  disconnect                 Disconnect
  help                       Show commands
  exit, quit                 Exit CLI`)
}

// ============================================================================
// Command Execution
// ============================================================================

func runCommand(cfg *Config, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("no command specified")
	}

	cmd := strings.ToLower(args[0])
	cmdArgs := args[1:]

	switch cmd {
	// Data commands (memcached protocol)
	case "get":
		return cmdGet(cfg, cmdArgs)
	case "set", "put":
		return cmdSet(cfg, cmdArgs)
	case "delete", "del", "rm":
		return cmdDelete(cfg, cmdArgs)
	case "scan", "keys":
		return cmdScan(cfg, cmdArgs)
	case "stats":
		return cmdStats(cfg, cmdArgs)
	case "ping":
		return cmdPing(cfg, cmdArgs)

	// Admin API commands
	case "info":
		return cmdInfo(cfg, cmdArgs)
	case "backup":
		return cmdBackup(cfg, cmdArgs)
	case "restore":
		return cmdRestore(cfg, cmdArgs)
	case "backups":
		return cmdBackupList(cfg, cmdArgs)
	case "compact":
		return cmdCompact(cfg, cmdArgs)
	case "flush":
		return cmdFlush(cfg, cmdArgs)
	case "levels":
		return cmdLevels(cfg, cmdArgs)
	case "sstables":
		return cmdSSTables(cfg, cmdArgs)
	case "import-csv":
		return cmdImportCSV(cfg, cmdArgs)
	case "import-json":
		return cmdImportJSON(cfg, cmdArgs)
	case "export-csv":
		return cmdExportCSV(cfg, cmdArgs)
	case "export-json":
		return cmdExportJSON(cfg, cmdArgs)

	default:
		return fmt.Errorf("unknown command: %s", cmd)
	}
}

// ============================================================================
// Memcached Protocol Commands
// ============================================================================

func cmdGet(cfg *Config, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: get <key>")
	}

	key := args[0]
	conn, err := dialMemcached(cfg)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	if _, err = fmt.Fprintf(conn, "get %s\r\n", key); err != nil {
		return fmt.Errorf("write error: %w", err)
	}

	reader := bufio.NewReader(conn)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("read error: %w", err)
		}
		line = strings.TrimSpace(line)

		if line == "END" {
			fmt.Println("(not found)")
			return nil
		}

		if strings.HasPrefix(line, "VALUE") {
			parts := strings.Fields(line)
			if len(parts) < 4 {
				return fmt.Errorf("invalid response: %s", line)
			}

			size, _ := strconv.Atoi(parts[3])

			value := make([]byte, size)
			_, err := io.ReadFull(reader, value)
			if err != nil {
				return fmt.Errorf("read value error: %w", err)
			}

			_, _ = reader.ReadString('\n')
			fmt.Println(string(value))
			_, _ = reader.ReadString('\n')
			return nil
		}
	}
}

func cmdSet(cfg *Config, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: set <key> <value> [ttl]")
	}

	key := args[0]
	value := args[1]
	ttl := 0

	if len(args) >= 3 {
		var err error
		ttl, err = strconv.Atoi(args[2])
		if err != nil {
			return fmt.Errorf("invalid TTL: %s", args[2])
		}
	}

	conn, err := dialMemcached(cfg)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	if _, err = fmt.Fprintf(conn, "set %s 0 %d %d\r\n%s\r\n", key, ttl, len(value), value); err != nil {
		return fmt.Errorf("write error: %w", err)
	}

	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("read error: %w", err)
	}
	line = strings.TrimSpace(line)

	if line == "STORED" {
		fmt.Println("OK")
		return nil
	}

	return fmt.Errorf("server response: %s", line)
}

func cmdDelete(cfg *Config, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: delete <key>")
	}

	key := args[0]
	conn, err := dialMemcached(cfg)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	if _, err = fmt.Fprintf(conn, "delete %s\r\n", key); err != nil {
		return fmt.Errorf("write error: %w", err)
	}

	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("read error: %w", err)
	}
	line = strings.TrimSpace(line)

	if line == "DELETED" {
		fmt.Println("OK")
		return nil
	}
	if line == "NOT_FOUND" {
		fmt.Println("(not found)")
		return nil
	}

	return fmt.Errorf("server response: %s", line)
}

func cmdScan(cfg *Config, args []string) error {
	prefix := ""
	limit := 100

	for _, arg := range args {
		if strings.HasPrefix(arg, "--prefix=") {
			prefix = strings.TrimPrefix(arg, "--prefix=")
		} else if strings.HasPrefix(arg, "--limit=") {
			limit, _ = strconv.Atoi(strings.TrimPrefix(arg, "--limit="))
		} else if strings.HasPrefix(arg, "-n") {
			limit, _ = strconv.Atoi(strings.TrimPrefix(arg, "-n"))
		}
	}

	fmt.Printf("Scanning keys")
	if prefix != "" {
		fmt.Printf(" with prefix '%s'", prefix)
	}
	fmt.Printf(" (limit %d)...\n\n", limit)

	resp, err := adminGet(cfg, "/admin/stats")
	if err != nil {
		return fmt.Errorf("scan requires admin API: %w", err)
	}

	if data, ok := resp["data"].(map[string]interface{}); ok {
		if keys, ok := data["total_keys"].(float64); ok {
			fmt.Printf("Total keys in database: %.0f\n", keys)
		}
	}

	fmt.Println("\nNote: Full scan requires direct database access.")
	fmt.Println("Use export-csv or export-json for bulk key listing.")
	return nil
}

func cmdStats(cfg *Config, _ []string) error {
	conn, err := dialMemcached(cfg)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	if _, err = fmt.Fprintf(conn, "stats\r\n"); err != nil {
		return fmt.Errorf("write error: %w", err)
	}

	reader := bufio.NewReader(conn)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("read error: %w", err)
		}
		line = strings.TrimSpace(line)

		if line == "END" {
			break
		}

		if strings.HasPrefix(line, "STAT ") {
			parts := strings.SplitN(line, " ", 3)
			if len(parts) >= 3 {
				fmt.Printf("%-25s %s\n", parts[1]+":", parts[2])
			}
		}
	}

	return nil
}

func cmdPing(cfg *Config, _ []string) error {
	start := time.Now()

	conn, err := dialMemcached(cfg)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	if _, err = fmt.Fprintf(conn, "version\r\n"); err != nil {
		return fmt.Errorf("write error: %w", err)
	}

	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("read error: %w", err)
	}
	line = strings.TrimSpace(line)

	elapsed := time.Since(start)

	if strings.HasPrefix(line, "VERSION") {
		version := strings.TrimPrefix(line, "VERSION ")
		fmt.Printf("PONG from %s:%d (%s) - %v\n", cfg.Host, cfg.Port, version, elapsed)
		return nil
	}

	fmt.Printf("PONG from %s:%d - %v\n", cfg.Host, cfg.Port, elapsed)
	return nil
}

// ============================================================================
// Admin API Commands
// ============================================================================

func cmdInfo(cfg *Config, _ []string) error {
	resp, err := adminGet(cfg, "/admin/properties")
	if err != nil {
		return err
	}
	printJSON(resp)
	return nil
}

func cmdBackup(cfg *Config, args []string) error {
	backupType := "full"
	for _, arg := range args {
		if strings.HasPrefix(arg, "--type=") {
			backupType = strings.TrimPrefix(arg, "--type=")
		}
	}

	body := fmt.Sprintf(`{"type":"%s"}`, backupType)
	resp, err := adminPost(cfg, "/admin/backup", body)
	if err != nil {
		return err
	}
	printJSON(resp)
	return nil
}

func cmdRestore(cfg *Config, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: restore <backup-id> <target-dir>")
	}

	body := fmt.Sprintf(`{"backup_id":"%s","target_dir":"%s","verify":true}`, args[0], args[1])
	resp, err := adminPost(cfg, "/admin/backup/restore", body)
	if err != nil {
		return err
	}
	printJSON(resp)
	return nil
}

func cmdBackupList(cfg *Config, _ []string) error {
	resp, err := adminGet(cfg, "/admin/backup/list")
	if err != nil {
		return err
	}
	printJSON(resp)
	return nil
}

func cmdCompact(cfg *Config, _ []string) error {
	resp, err := adminPost(cfg, "/admin/compact", "{}")
	if err != nil {
		return err
	}
	printJSON(resp)
	return nil
}

func cmdFlush(cfg *Config, _ []string) error {
	resp, err := adminPost(cfg, "/admin/flush", "{}")
	if err != nil {
		return err
	}
	printJSON(resp)
	return nil
}

func cmdLevels(cfg *Config, _ []string) error {
	resp, err := adminGet(cfg, "/admin/levels")
	if err != nil {
		return err
	}
	printJSON(resp)
	return nil
}

func cmdSSTables(cfg *Config, _ []string) error {
	resp, err := adminGet(cfg, "/admin/sstables")
	if err != nil {
		return err
	}
	printJSON(resp)
	return nil
}

func cmdImportCSV(cfg *Config, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: import-csv <file> [--prefix=PREFIX]")
	}

	path := args[0]
	prefix := ""
	hasHeader := false

	for _, arg := range args[1:] {
		if strings.HasPrefix(arg, "--prefix=") {
			prefix = strings.TrimPrefix(arg, "--prefix=")
		}
		if arg == "--header" {
			hasHeader = true
		}
	}

	body := fmt.Sprintf(`{"path":"%s","key_prefix":"%s","has_header":%v,"skip_errors":true}`,
		path, prefix, hasHeader)
	resp, err := adminPost(cfg, "/admin/import/csv", body)
	if err != nil {
		return err
	}
	printJSON(resp)
	return nil
}

func cmdImportJSON(cfg *Config, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: import-json <file> [--prefix=PREFIX]")
	}

	path := args[0]
	prefix := ""

	for _, arg := range args[1:] {
		if strings.HasPrefix(arg, "--prefix=") {
			prefix = strings.TrimPrefix(arg, "--prefix=")
		}
	}

	body := fmt.Sprintf(`{"path":"%s","key_prefix":"%s","skip_errors":true}`, path, prefix)
	resp, err := adminPost(cfg, "/admin/import/json", body)
	if err != nil {
		return err
	}
	printJSON(resp)
	return nil
}

func cmdExportCSV(cfg *Config, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: export-csv <file> [--prefix=PREFIX] [--limit=N]")
	}

	path := args[0]
	prefix := ""
	limit := int64(0)

	for _, arg := range args[1:] {
		if strings.HasPrefix(arg, "--prefix=") {
			prefix = strings.TrimPrefix(arg, "--prefix=")
		}
		if strings.HasPrefix(arg, "--limit=") {
			limit, _ = strconv.ParseInt(strings.TrimPrefix(arg, "--limit="), 10, 64)
		}
	}

	body := fmt.Sprintf(`{"path":"%s","key_prefix":"%s","limit":%d,"include_header":true}`,
		path, prefix, limit)
	resp, err := adminPost(cfg, "/admin/export/csv", body)
	if err != nil {
		return err
	}
	printJSON(resp)
	return nil
}

func cmdExportJSON(cfg *Config, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: export-json <file> [--prefix=PREFIX] [--limit=N]")
	}

	path := args[0]
	prefix := ""
	limit := int64(0)

	for _, arg := range args[1:] {
		if strings.HasPrefix(arg, "--prefix=") {
			prefix = strings.TrimPrefix(arg, "--prefix=")
		}
		if strings.HasPrefix(arg, "--limit=") {
			limit, _ = strconv.ParseInt(strings.TrimPrefix(arg, "--limit="), 10, 64)
		}
	}

	body := fmt.Sprintf(`{"path":"%s","key_prefix":"%s","limit":%d}`, path, prefix, limit)
	resp, err := adminPost(cfg, "/admin/export/json", body)
	if err != nil {
		return err
	}
	printJSON(resp)
	return nil
}

// ============================================================================
// Interactive Mode
// ============================================================================

func runInteractive(cfg *Config) {
	fmt.Println("RapidoDB CLI - Interactive Mode")
	fmt.Println("Type 'help' for commands, 'exit' to quit")
	fmt.Println()

	reader := bufio.NewReader(os.Stdin)
	var connected bool

	for {
		prompt := fmt.Sprintf("rapidodb [%s:%d]> ", cfg.Host, cfg.Port)
		if !connected {
			prompt = "rapidodb (disconnected)> "
		}
		fmt.Print(prompt)

		input, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				fmt.Println()
				return
			}
			continue
		}

		input = strings.TrimSpace(input)
		if input == "" {
			continue
		}

		parts := strings.Fields(input)
		cmd := strings.ToLower(parts[0])

		switch cmd {
		case "exit", "quit", "q":
			fmt.Println("Bye!")
			return

		case "help", "?":
			printInteractiveHelp()

		case "connect":
			if len(parts) >= 2 {
				hostPort := strings.Split(parts[1], ":")
				cfg.Host = hostPort[0]
				if len(hostPort) > 1 {
					cfg.Port, _ = strconv.Atoi(hostPort[1])
				}
			}
			if err := cmdPing(cfg, nil); err != nil {
				fmt.Printf("Connection failed: %v\n", err)
				connected = false
			} else {
				connected = true
			}

		case "disconnect":
			connected = false
			fmt.Println("Disconnected")

		case "clear", "cls":
			fmt.Print("\033[H\033[2J")

		default:
			if !connected {
				conn, err := dialMemcached(cfg)
				if err != nil {
					fmt.Printf("Not connected. Use 'connect [host:port]' first.\n")
					continue
				}
				_ = conn.Close()
				connected = true
			}

			if err := runCommand(cfg, parts); err != nil {
				fmt.Printf("Error: %v\n", err)
			}
		}
	}
}

func printInteractiveHelp() {
	fmt.Print(`
Interactive Commands:
  connect [host:port]   Connect to server
  disconnect            Disconnect
  clear                 Clear screen
  help                  Show this help
  exit, quit            Exit CLI

Data Commands:
  get <key>             Get value
  set <key> <value>     Set value
  delete <key>          Delete key
  scan [--prefix=]      Scan keys

Admin Commands:
  stats                 Show stats
  ping                  Ping server
  info                  Server info
  flush                 Flush memtable
  compact               Trigger compaction
  levels                Show levels
  sstables              List SSTables
  backup                Create backup
  backups               List backups
`)
}

// ============================================================================
// Helpers
// ============================================================================

func dialMemcached(cfg *Config) (net.Conn, error) {
	addr := net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
	conn, err := (&net.Dialer{Timeout: cfg.Timeout}).DialContext(context.Background(), "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", addr, err)
	}
	if err := conn.SetDeadline(time.Now().Add(cfg.Timeout)); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("set deadline error: %w", err)
	}
	return conn, nil
}

func adminGet(cfg *Config, path string) (map[string]interface{}, error) {
	return adminRequest(cfg, "GET", path, "")
}

func adminPost(cfg *Config, path, body string) (map[string]interface{}, error) {
	return adminRequest(cfg, "POST", path, body)
}

func adminRequest(cfg *Config, method, path, body string) (map[string]interface{}, error) {
	addr := net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.AdminPort))
	conn, err := (&net.Dialer{Timeout: cfg.Timeout}).DialContext(context.Background(), "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to admin API at %s: %w", addr, err)
	}
	defer func() { _ = conn.Close() }()

	if err := conn.SetDeadline(time.Now().Add(cfg.Timeout * 2)); err != nil {
		return nil, fmt.Errorf("set deadline error: %w", err)
	}

	var req string
	if body != "" {
		req = fmt.Sprintf("%s %s HTTP/1.1\r\nHost: %s\r\nContent-Type: application/json\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s",
			method, path, cfg.Host, len(body), body)
	} else {
		req = fmt.Sprintf("%s %s HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n",
			method, path, cfg.Host)
	}

	_, err = conn.Write([]byte(req))
	if err != nil {
		return nil, fmt.Errorf("write error: %w", err)
	}

	reader := bufio.NewReader(conn)

	statusLine, err := reader.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("read status error: %w", err)
	}

	if !strings.Contains(statusLine, "200") {
		return nil, fmt.Errorf("HTTP error: %s", strings.TrimSpace(statusLine))
	}

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("read header error: %w", err)
		}
		if line == "\r\n" {
			break
		}
	}

	respBody, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("read body error: %w", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("JSON parse error: %w (body: %s)", err, string(respBody))
	}

	return result, nil
}

func printJSON(data map[string]interface{}) {
	output, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		fmt.Printf("%v\n", data)
		return
	}
	fmt.Println(string(output))
}
