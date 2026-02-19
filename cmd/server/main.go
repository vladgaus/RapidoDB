// RapidoDB Server
// High-Performance LSM-Tree Key-Value Store with Memcached Protocol
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/vladgaus/RapidoDB/pkg/admin"
	"github.com/vladgaus/RapidoDB/pkg/config"
	"github.com/vladgaus/RapidoDB/pkg/health"
	"github.com/vladgaus/RapidoDB/pkg/logging"
	"github.com/vladgaus/RapidoDB/pkg/lsm"
	"github.com/vladgaus/RapidoDB/pkg/metrics"
	"github.com/vladgaus/RapidoDB/pkg/server"
	"github.com/vladgaus/RapidoDB/pkg/shutdown"
	"github.com/vladgaus/RapidoDB/pkg/tracing"
)

// Build-time variables (set by ldflags)
var (
	Version   = "dev"
	BuildTime = "unknown"
	Commit    = "unknown"
)

func main() {
	// Command-line flags
	configPath := flag.String("config", "", "Path to configuration file (JSON)")
	showVersion := flag.Bool("version", false, "Show version information")
	dataDir := flag.String("data-dir", "", "Data directory (overrides config)")
	host := flag.String("host", "", "Server host (overrides config)")
	port := flag.Int("port", 0, "Server port (overrides config)")
	healthPort := flag.Int("health-port", 0, "Health server port (0 to disable, overrides config)")
	noHealth := flag.Bool("no-health", false, "Disable health HTTP server")
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
	if *healthPort != 0 {
		cfg.Health.Port = *healthPort
	}
	if *noHealth {
		cfg.Health.Enabled = false
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "Invalid configuration: %v\n", err)
		os.Exit(1)
	}

	// Setup logging
	logger, logCloser := setupLogging(cfg)
	defer logCloser()

	logger.Info("starting RapidoDB",
		"version", Version,
		"commit", Commit,
		"build_time", BuildTime,
	)

	// Print startup banner
	printBanner()

	// Initialize storage engine
	fmt.Printf("Opening database at: %s\n", cfg.DataDir)
	logger.Info("opening database", "path", cfg.DataDir)

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
		logger.Error("failed to open database", "error", err)
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
	srv.SetLogger(logger)

	// Setup metrics early (before server start) so command handlers can use them
	var metricsServer *metrics.Server
	var rapidoDBMetrics *metrics.RapiDoDBMetrics
	if cfg.Metrics.Enabled {
		rapidoDBMetrics = metrics.NewRapiDoDBMetrics()
		rapidoDBMetrics.SetVersion(Version)
		srv.SetMetrics(rapidoDBMetrics)

		// Start background stats collector (updates storage metrics every 5 seconds)
		rapidoDBMetrics.StartStatsCollector(engine, 5*time.Second)
	}

	// Setup tracing
	var tracer *tracing.Tracer
	if cfg.Tracing.Enabled {
		tracer = setupTracing(cfg, logger)
		if tracer != nil {
			srv.SetTracer(tracer)
			logger.Info("tracing enabled",
				"exporter", cfg.Tracing.Exporter,
				"sample_rate", cfg.Tracing.SampleRate,
			)
		}
	}

	// Start server
	if err := srv.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start server: %v\n", err)
		logger.Error("failed to start server", "error", err)
		_ = engine.Close()
		os.Exit(1)
	}

	logger.Info("server started",
		"addr", srv.Addr(),
		"compaction_strategy", cfg.Compaction.Strategy,
	)
	fmt.Printf("Server listening on %s\n", srv.Addr())
	fmt.Printf("Compaction strategy: %s\n", cfg.Compaction.Strategy)

	// Setup health checks
	var healthServer *health.HTTPServer
	var healthChecker *health.HealthChecker
	if cfg.Health.Enabled {
		healthChecker = setupHealthChecks(cfg, engine, srv)

		healthOpts := health.DefaultHTTPServerOptions()
		healthOpts.Host = cfg.Health.Host
		healthOpts.Port = cfg.Health.Port

		healthServer = health.NewHTTPServer(healthChecker, healthOpts)

		if err := healthServer.Start(); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to start health server: %v\n", err)
			logger.Error("failed to start health server", "error", err)
			_ = srv.Close()
			_ = engine.Close()
			os.Exit(1)
		}

		logger.Info("health server started", "addr", healthServer.Addr())
		fmt.Printf("Health server listening on %s\n", healthServer.Addr())
		fmt.Println("  GET /health       - Full health status")
		fmt.Println("  GET /health/live  - Kubernetes liveness probe")
		fmt.Println("  GET /health/ready - Kubernetes readiness probe")

		// Mark as ready now that everything is started
		healthChecker.SetReady(true)
	}

	// Start metrics HTTP server
	if cfg.Metrics.Enabled && rapidoDBMetrics != nil {
		metricsServer = metrics.NewServer(metrics.ServerOptions{
			Host:    cfg.Metrics.Host,
			Port:    cfg.Metrics.Port,
			Metrics: rapidoDBMetrics,
		})

		if err := metricsServer.Start(); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to start metrics server: %v\n", err)
			logger.Error("failed to start metrics server", "error", err)
			if healthServer != nil {
				_ = healthServer.Close()
			}
			_ = srv.Close()
			_ = engine.Close()
			os.Exit(1)
		}

		logger.Info("metrics server started", "addr", metricsServer.Addr())
		fmt.Printf("Metrics server listening on %s\n", metricsServer.Addr())
	}

	// Setup Admin API server
	var adminServer *admin.Server
	if cfg.Admin.Enabled {
		adminServer = admin.NewServer(admin.Options{
			Host:      cfg.Admin.Host,
			Port:      cfg.Admin.Port,
			Engine:    engine,
			Logger:    logger,
			Version:   Version,
			AuthToken: cfg.Admin.AuthToken,
		})

		if err := adminServer.Start(); err != nil {
			logger.Error("failed to start admin server", "error", err)
			fmt.Printf("Warning: Admin server failed to start: %v\n", err)
		} else {
			logger.Info("admin server started", "addr", adminServer.Addr())
			fmt.Printf("Admin API listening on %s\n", adminServer.Addr())
		}
	}

	// Setup graceful shutdown coordinator
	shutdownCoordinator := shutdown.NewCoordinator(shutdown.Options{
		Timeout:      cfg.Shutdown.Timeout,
		DrainTimeout: cfg.Shutdown.DrainTimeout,
	})

	// Register shutdown hooks in order of execution

	// 1. Mark as not ready (stop health readiness probe)
	if healthChecker != nil {
		shutdownCoordinator.RegisterHook("mark-not-ready", func(ctx context.Context) error {
			healthChecker.SetReady(false)
			fmt.Println("  → Marked as not ready")
			return nil
		}, shutdown.PriorityFirst)
	}

	// 2. Close health server (stop accepting health requests)
	if healthServer != nil {
		shutdownCoordinator.RegisterHook("health-server", func(ctx context.Context) error {
			err := healthServer.Close()
			if err != nil {
				fmt.Printf("  → Health server closed with error: %v\n", err)
			} else {
				fmt.Println("  → Health server closed")
			}
			return err
		}, shutdown.PriorityEarly)
	}

	// 2b. Stop metrics stats collector
	if rapidoDBMetrics != nil {
		shutdownCoordinator.RegisterHook("metrics-collector", func(ctx context.Context) error {
			rapidoDBMetrics.StopStatsCollector()
			fmt.Println("  → Metrics collector stopped")
			return nil
		}, shutdown.PriorityEarly)
	}

	// 2c. Close metrics server
	if metricsServer != nil {
		shutdownCoordinator.RegisterHook("metrics-server", func(ctx context.Context) error {
			err := metricsServer.Close()
			if err != nil {
				fmt.Printf("  → Metrics server closed with error: %v\n", err)
			} else {
				fmt.Println("  → Metrics server closed")
			}
			return err
		}, shutdown.PriorityEarly)
	}

	// 2d. Shutdown tracer (flush pending spans)
	if tracer != nil {
		shutdownCoordinator.RegisterHook("tracer", func(ctx context.Context) error {
			err := tracer.Shutdown()
			if err != nil {
				fmt.Printf("  → Tracer shutdown with error: %v\n", err)
			} else {
				fmt.Println("  → Tracer shutdown")
			}
			return err
		}, shutdown.PriorityEarly)
	}

	// 2e. Close admin server
	if adminServer != nil {
		shutdownCoordinator.RegisterHook("admin-server", func(ctx context.Context) error {
			err := adminServer.Close()
			if err != nil {
				fmt.Printf("  → Admin server closed with error: %v\n", err)
			} else {
				fmt.Println("  → Admin server closed")
			}
			return err
		}, shutdown.PriorityEarly)
	}

	// 3. Drain TCP connections (finish in-flight requests)
	shutdownCoordinator.RegisterHook("tcp-server", func(ctx context.Context) error {
		fmt.Printf("  → Draining %d active connections...\n", srv.ActiveConnections())

		// Create drain context with timeout
		drainCtx, cancel := context.WithTimeout(ctx, cfg.Shutdown.DrainTimeout)
		defer cancel()

		err := srv.GracefulClose(drainCtx)
		if err != nil {
			fmt.Printf("  → TCP server closed with error: %v\n", err)
		} else {
			fmt.Println("  → TCP server closed")
		}
		return err
	}, shutdown.PriorityNormal)

	// 4. Flush and close storage engine
	shutdownCoordinator.RegisterHook("storage-engine", func(ctx context.Context) error {
		fmt.Println("  → Flushing MemTable and syncing WAL...")
		logger.Info("flushing MemTable and syncing WAL")

		err := engine.GracefulClose(ctx)
		if err != nil {
			fmt.Printf("  → Storage engine closed with error: %v\n", err)
			logger.Error("storage engine closed with error", "error", err)
		} else {
			fmt.Println("  → Storage engine closed")
			logger.Info("storage engine closed")
		}
		return err
	}, shutdown.PriorityLast)

	logger.Info("ready to accept connections",
		"shutdown_timeout", cfg.Shutdown.Timeout,
		"drain_timeout", cfg.Shutdown.DrainTimeout,
	)
	fmt.Printf("\nGraceful shutdown: timeout=%v, drain=%v\n",
		cfg.Shutdown.Timeout, cfg.Shutdown.DrainTimeout)
	fmt.Println("Ready to accept connections. Press Ctrl+C to stop.")

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigChan

	logger.Info("received shutdown signal", "signal", sig.String())
	fmt.Printf("\nReceived signal: %v\n", sig)
	fmt.Println("Starting graceful shutdown...")
	startTime := time.Now()

	// Trigger graceful shutdown
	shutdownCoordinator.Shutdown(fmt.Sprintf("received signal: %v", sig))

	// Wait for shutdown to complete
	<-shutdownCoordinator.Done()

	// Report shutdown summary
	elapsed := time.Since(startTime)
	errors := shutdownCoordinator.Errors()

	if len(errors) > 0 {
		logger.Warn("shutdown completed with errors",
			"error_count", len(errors),
			"duration_ms", elapsed.Milliseconds(),
		)
		fmt.Printf("\nShutdown completed with %d errors in %v:\n", len(errors), elapsed.Round(time.Millisecond))
		for _, err := range errors {
			fmt.Printf("  - %v\n", err)
		}
	} else {
		logger.Info("shutdown completed",
			"duration_ms", elapsed.Milliseconds(),
		)
		fmt.Printf("\nShutdown completed successfully in %v\n", elapsed.Round(time.Millisecond))
	}

	fmt.Println("Goodbye!")
}

// setupHealthChecks creates and configures the health checker with all checks.
func setupHealthChecks(cfg *config.Config, engine *lsm.Engine, srv *server.Server) *health.HealthChecker {
	hc := health.NewHealthChecker(health.Options{
		Version: fmt.Sprintf("RapidoDB/%s", Version),
	})

	// Memory checker
	memChecker := health.NewMemoryChecker()
	if cfg.Health.MemoryMaxHeapMB > 0 {
		memChecker.MaxHeapBytes = uint64(cfg.Health.MemoryMaxHeapMB) * 1024 * 1024
	}
	hc.RegisterChecker(memChecker)

	// Disk checker
	diskPath := cfg.Health.DiskPath
	if diskPath == "" {
		diskPath = cfg.DataDir
	}
	diskChecker := health.NewDiskChecker(diskPath)
	diskChecker.WarningThresholdPercent = cfg.Health.DiskWarningPercent
	diskChecker.CriticalThresholdPercent = cfg.Health.DiskCriticalPercent
	hc.RegisterChecker(diskChecker)

	// Engine checker
	engineChecker := health.NewEngineChecker(engine)
	hc.RegisterChecker(engineChecker)

	// Server checker
	serverChecker := health.NewServerChecker(srv, cfg.Server.MaxConnections)
	hc.RegisterChecker(serverChecker)

	return hc
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

// setupLogging creates and configures the structured logger.
func setupLogging(cfg *config.Config) (*logging.Logger, func()) {
	// Determine output writer
	var output io.Writer
	var closer func()

	switch cfg.Logging.Output {
	case "stdout", "":
		output = os.Stdout
		closer = func() {}
	case "stderr":
		output = os.Stderr
		closer = func() {}
	default:
		// File output with rotation
		rf, err := logging.NewRotatingFile(logging.RotatingFileOptions{
			Path:       cfg.Logging.Output,
			MaxSize:    cfg.Logging.File.MaxSize,
			MaxBackups: cfg.Logging.File.MaxBackups,
			MaxAge:     cfg.Logging.File.MaxAge,
			Compress:   cfg.Logging.File.Compress,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create log file, falling back to stdout: %v\n", err)
			output = os.Stdout
			closer = func() {}
		} else {
			output = rf
			closer = func() { _ = rf.Close() }
		}
	}

	// Create logger
	logger := logging.New(logging.Options{
		Level:     logging.ParseLevel(cfg.Logging.Level),
		Format:    logging.ParseFormat(cfg.Logging.Format),
		Output:    output,
		AddSource: cfg.Logging.AddSource,
		Component: "rapidodb",
	})

	// Set as default logger
	logging.SetDefault(logger)

	return logger, closer
}

// setupTracing creates and configures the distributed tracer.
func setupTracing(cfg *config.Config, logger *logging.Logger) *tracing.Tracer {
	// Create exporter based on config
	var exporter tracing.Exporter
	switch cfg.Tracing.Exporter {
	case "json":
		exporter = tracing.NewJSONExporter(os.Stdout)
	case "jaeger":
		endpoint := cfg.Tracing.Endpoint
		if endpoint == "" {
			endpoint = "http://localhost:14268/api/traces"
		}
		exporter = tracing.NewJaegerExporter(tracing.JaegerExporterOptions{
			Endpoint: endpoint,
		})
		logger.Info("jaeger exporter configured", "endpoint", endpoint)
	case "zipkin":
		endpoint := cfg.Tracing.Endpoint
		if endpoint == "" {
			endpoint = "http://localhost:9411/api/v2/spans"
		}
		exporter = tracing.NewZipkinExporter(tracing.ZipkinExporterOptions{
			Endpoint: endpoint,
		})
		logger.Info("zipkin exporter configured", "endpoint", endpoint)
	case "none", "":
		exporter = tracing.NoopExporter()
	default:
		logger.Warn("unknown tracing exporter, using noop", "exporter", cfg.Tracing.Exporter)
		exporter = tracing.NoopExporter()
	}

	// Create sampler based on sample rate
	var sampler tracing.Sampler
	if cfg.Tracing.SampleRate <= 0 {
		sampler = tracing.NeverSample()
	} else if cfg.Tracing.SampleRate >= 1.0 {
		sampler = tracing.AlwaysSample()
	} else {
		sampler = tracing.RatioSampler(cfg.Tracing.SampleRate)
	}

	// Get service name
	serviceName := cfg.Tracing.ServiceName
	if serviceName == "" {
		serviceName = "rapidodb"
	}

	// Create tracer
	tracer := tracing.NewTracer(tracing.TracerOptions{
		ServiceName: serviceName,
		Exporter:    exporter,
		Sampler:     sampler,
		Enabled:     true,
		Resource: []tracing.Attribute{
			tracing.String("service.version", Version),
		},
	})

	// Set as global tracer
	tracing.SetGlobalTracer(tracer)

	return tracer
}
