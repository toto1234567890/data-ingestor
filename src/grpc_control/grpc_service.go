package grpc_control

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"trading-system/src/config"
	"trading-system/src/ingestor"
	"trading-system/src/logger"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

// -----------------------------------------------------------------------------
// GRPCService handles gRPC server lifecycle
// -----------------------------------------------------------------------------

type GRPCService struct {
	server   *grpc.Server
	listener net.Listener
	config   *config.Config
	logger   *logger.Logger
	ingestor *ingestor.Ingestor
	running  bool
}

// -----------------------------------------------------------------------------

// NewGRPCService creates a new GRPCService instance
func NewGRPCService(config *config.Config, logger *logger.Logger, ingestor *ingestor.Ingestor) (*GRPCService, error) {
	// Create listener
	address := fmt.Sprintf("%s:%d", config.GRPC_Host, config.GRPC_Port)

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", address, err)
	}

	// Create gRPC server with options
	serverOptions := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(10 * 1024 * 1024), // 10MB
		grpc.MaxSendMsgSize(10 * 1024 * 1024), // 10MB
	}

	server := grpc.NewServer(serverOptions...)

	return &GRPCService{
		server:   server,
		listener: listener,
		config:   config,
		logger:   logger,
		ingestor: ingestor,
		running:  false,
	}, nil
}

// -----------------------------------------------------------------------------

// Start starts the gRPC server
func (g *GRPCService) Start() error {
	g.logger.Info("Starting gRPC service on %s", g.listener.Addr().String())

	// Register services
	controlService := NewControlService(g.config, g.logger, g.ingestor)
	RegisterControlServiceServer(g.server, controlService)

	// Register health service
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(g.server, healthServer)
	healthServer.SetServingStatus("grpc_control.ControlService", grpc_health_v1.HealthCheckResponse_SERVING)

	// Set up graceful shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	// Start server in goroutine
	go func() {
		g.running = true
		if err := g.server.Serve(g.listener); err != nil && err != grpc.ErrServerStopped {
			g.logger.Error("gRPC server failed: %v", err)
		}
		g.running = false
	}()

	g.logger.Info("gRPC service started successfully on %s", g.listener.Addr().String())

	// Wait for shutdown signal
	<-stop
	g.logger.Info("Received shutdown signal, stopping gRPC service...")

	// Graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	g.Stop(ctx)

	return nil
}

// -----------------------------------------------------------------------------

// Stop gracefully stops the gRPC server
func (g *GRPCService) Stop(ctx context.Context) error {
	g.logger.Info("Stopping gRPC service...")

	if g.server != nil {
		// Graceful stop
		done := make(chan struct{})
		go func() {
			g.server.GracefulStop()
			close(done)
		}()

		select {
		case <-ctx.Done():
			g.logger.Warning("gRPC graceful shutdown timeout, forcing stop...")
			g.server.Stop()
		case <-done:
			g.logger.Info("gRPC service stopped gracefully")
		}
	}

	if g.listener != nil {
		g.listener.Close()
	}

	g.running = false
	g.logger.Info("gRPC service stopped")
	return nil
}

// -----------------------------------------------------------------------------

// IsRunning returns whether the gRPC server is running
func (g *GRPCService) IsRunning() bool {
	return g.running
}

// -----------------------------------------------------------------------------
// Helper functions for running the service
// -----------------------------------------------------------------------------

// StartGRPCServer creates and runs the gRPC server (simplified entry point)
func StartGRPCServer(config *config.Config, logger *logger.Logger, ingestor *ingestor.Ingestor) error {
	grpcService, err := NewGRPCService(config, logger, ingestor)
	if err != nil {
		return fmt.Errorf("failed to create gRPC service: %w", err)
	}

	return grpcService.Start()
}
