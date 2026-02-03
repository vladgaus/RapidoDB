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

	// TODO: Initialize storage engine (Step 7)
	// TODO: Start TCP server (Step 14)

	fmt.Printf("RapidoDB server starting on %s:%d\n", cfg.Server.Host, cfg.Server.Port)
	fmt.Printf("Data directory: %s\n", cfg.DataDir)
	fmt.Printf("Compaction strategy: %s\n", cfg.Compaction.Strategy)

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("\nServer is running. Press Ctrl+C to stop.")
	<-sigChan

	fmt.Println("\nShutting down...")
	// TODO: Graceful shutdown
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
