// RapidoDB Server
// High-Performance LSM-Tree Key-Value Store with Memcached Protocol
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/rapidodb/rapidodb/pkg/config"
	"github.com/rapidodb/rapidodb/pkg/lsm"
	"github.com/rapidodb/rapidodb/pkg/server"
)

// Build-time variables (set by ldflags)
var (
	Version   = "dev"
	BuildTime = "unknown"
	Commit    = "unknown"
)

func main() {
	// Command-line flags
	configPath := flag.String("config", "", "Path to configuration file (YAML)")
	showVersion := flag.Bool("version", false, "Show version information")
	dataDir := flag.String("data-dir", "", "Data directory (overrides config)")
	host := flag.String("host", "", "Server host (overrides config)")
	port := flag.Int("port", 0, "Server port (overrides config)")
	flag.Parse()

	// Show version and exit
	if *showVersion {
		fmt.Printf("RapidoDB %s\n", Version)
		fmt.Printf("Build Time: %s\n", BuildTime)
		fmt.Printf("Commit: %s\n", Commit)
		os.Exit(0)
	}

	// Load configuration
	var cfg *config.Config
	var err error

	if *configPath != "" {
		cfg, err = config.LoadFromFile(*configPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
			os.Exit(1)
		}
	} else {
		cfg = config.DefaultConfig()
	}

	// Override with command-line flags
	if *dataDir != "" {
		cfg.DataDir = *dataDir
	}
	if *host != "" {
		cfg.Server.Host = *host
	}
	if *port != 0 {
		cfg.Server.Port = *port
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "Invalid configuration: %v\n", err)
		os.Exit(1)
	}

	// Print startup banner
	printBanner()

	// Initialize storage engine
	fmt.Printf("Opening database at: %s\n", cfg.DataDir)

	engineOpts := lsm.DefaultOptions(cfg.DataDir)
	engineOpts.MemTableSize = cfg.MemTable.MaxSize
	engineOpts.MaxMemTables = cfg.MemTable.MaxMemTables
	engineOpts.WALSyncOnWrite = cfg.WAL.SyncOnWrite
	engineOpts.WALMaxFileSize = cfg.WAL.MaxSize
	engineOpts.BlockSize = cfg.SSTable.BlockSize
	engineOpts.BloomBitsPerKey = cfg.BloomFilter.BitsPerKey
	engineOpts.MaxBackgroundCompactions = cfg.Compaction.MaxBackgroundCompactions
	engineOpts.L0CompactionTrigger = cfg.Compaction.Leveled.L0CompactionTrigger

	// Convert config strategy to lsm strategy
	switch cfg.Compaction.Strategy {
	case config.LeveledCompaction:
		engineOpts.CompactionStrategy = lsm.CompactionLeveled
	case config.TieredCompaction:
		engineOpts.CompactionStrategy = lsm.CompactionTiered
	case config.FIFOCompaction:
		engineOpts.CompactionStrategy = lsm.CompactionFIFO
	default:
		engineOpts.CompactionStrategy = lsm.CompactionLeveled
	}

	engine, err := lsm.Open(engineOpts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open database: %v\n", err)
		os.Exit(1)
	}

	// Create server
	serverOpts := server.DefaultOptions()
	serverOpts.Host = cfg.Server.Host
	serverOpts.Port = cfg.Server.Port
	serverOpts.ReadTimeout = cfg.Server.ReadTimeout
	serverOpts.WriteTimeout = cfg.Server.WriteTimeout
	serverOpts.MaxConnections = cfg.Server.MaxConnections
	serverOpts.Version = fmt.Sprintf("RapidoDB/%s", Version)

	srv := server.New(engine, serverOpts)

	// Start server
	if err := srv.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start server: %v\n", err)
		_ = engine.Close()
		os.Exit(1)
	}

	fmt.Printf("Server listening on %s\n", srv.Addr())
	fmt.Printf("Compaction strategy: %s\n", cfg.Compaction.Strategy)
	fmt.Println("\nReady to accept connections. Press Ctrl+C to stop.")

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\nShutting down...")

	// Graceful shutdown
	if err := srv.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "Error closing server: %v\n", err)
	}

	if err := engine.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "Error closing engine: %v\n", err)
	}

	fmt.Println("Goodbye!")
}

func printBanner() {
	banner := `
╦═╗┌─┐┌─┐┬┌┬┐┌─┐╔╦╗╔╗ 
╠╦╝├─┤├─┘│ │││ │ ║║╠╩╗
╩╚═┴ ┴┴  ┴─┴┘└─┘═╩╝╚═╝
High-Performance LSM-Tree Key-Value Store
`
	fmt.Println(banner)
	fmt.Printf("Version: %s | Commit: %s\n\n", Version, Commit)
}
