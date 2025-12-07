package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"trading-system/src/config"
	"trading-system/src/grpc_control"
	"trading-system/src/ingestor"
	"trading-system/src/logger"
	"trading-system/src/rest"
)

func main() {
	// Parse command line flags
	// configPath := flag.String("config", "../../config/default.yaml", "path to config file")
	configPath := flag.String("config", "../../config/default.yaml", "path to config file")
	apiPort := 8080
	flag.Parse()

	// Load config from YAML file
	config, err := config.NewConfig(*configPath)
	if err != nil {
		fmt.Printf("Error loading config: %v\n", err)
		os.Exit(1)
	}

	// Setup logger
	appLogger := logger.NewLogger(config, config.Name)

	// Create ingestor from config
	ingestorService := ingestor.NewIngestor(config, appLogger)
	defer ingestorService.Stop()

	// Create control service
	controlService, err := grpc_control.NewGRPCService(config, appLogger, ingestorService)
	if err != nil {
		appLogger.Critical("failed to create control service: %v", err)
		os.Exit(1)
	}
	defer controlService.Stop(context.Background())

	// Start REST API server
	go func() {
		appLogger.Info("starting REST API server on :%d", apiPort)
		if err := startAPIServer(config, appLogger, apiPort); err != nil {
			appLogger.Error("REST API server error: %v", err)
			os.Exit(1)
		}
	}()

	// Start gRPC control server
	go func() {
		appLogger.Info("starting gRPC control service on %s:%d", config.GRPC_Host, config.GRPC_Port)
		if err := controlService.Start(); err != nil {
			appLogger.Error("control server error: %v", err)
			os.Exit(1)
		}
	}()

	// Start ingestor
	if err := ingestorService.Start(); err != nil {
		appLogger.Critical("failed to start ingestor: %v", err)
		os.Exit(1)
	}

	appLogger.Info("trading system running. REST API: :%d, gRPC: %s:%d",
		apiPort, config.GRPC_Host, config.GRPC_Port)
	appLogger.Info("Press Ctrl+C to stop.")

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	appLogger.Info("shutting down...")
}

// startAPIServer starts the HTTP REST API server
func startAPIServer(config *config.Config, logger *logger.Logger, port int) error {
	// Create pure gRPC client API handler
	apiHandler, err := rest.NewAPIHandler(config, logger)
	if err != nil {
		return fmt.Errorf("failed to create API handler: %w", err)
	}
	defer apiHandler.Close()

	mux := http.NewServeMux()

	// Register pure gRPC client endpoints
	mux.HandleFunc("/rest/health", apiHandler.HealthCheck)
	mux.HandleFunc("/rest/datasource/add", apiHandler.AddDataSource)
	mux.HandleFunc("/rest/datasource/remove", apiHandler.RemoveDataSource)
	mux.HandleFunc("/rest/brokers/start", apiHandler.StartBroker)
	mux.HandleFunc("/rest/brokers/stop", apiHandler.StopBroker)
	mux.HandleFunc("/rest/brokers/status", apiHandler.GetBrokerStatus)
	mux.HandleFunc("/rest/brokers/list", apiHandler.ListBrokers)
	mux.HandleFunc("/rest/brokers/symbols/add", apiHandler.AddSymbols)
	mux.HandleFunc("/rest/brokers/symbols/add/all", apiHandler.AddSymbolsAll)
	mux.HandleFunc("/rest/brokers/symbols/remove", apiHandler.RemoveSymbols)
	mux.HandleFunc("/rest/brokers/symbols/remove/all", apiHandler.RemoveSymbolsAll)

	// Serve static files
	mux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("static"))))
	mux.HandleFunc("/", serveTestPage)

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	logger.Info("REST API server started on :%d (pure gRPC client mode)", port)
	return server.ListenAndServe()
}

// serveTestPage serves the HTML test interface
func serveTestPage(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	// Read the HTML file from the same directory as main.go
	testPageHTML, err := os.ReadFile("testpage.html")
	if err != nil {
		http.Error(w, "Test page not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte(testPageHTML))
}
