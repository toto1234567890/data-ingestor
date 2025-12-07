package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"data-ingestor/src/config"
	"data-ingestor/src/grpc_control"
	"data-ingestor/src/ingestor"
	"data-ingestor/src/logger"
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

	// Start gRPC control server
	go func() {
		appLogger.Info("starting gRPC control service on %s:%d", config.GRPC_Host, config.GRPC_Port)
		if err := controlService.Start(); err != nil {
			appLogger.Critical("control server error: %v", err)
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
