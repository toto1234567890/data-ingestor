// src/grpc_control/control_service.go
package grpc_control

import (
	context "context"
	"fmt"
	"time"

	"trading-system/src/config"
	"trading-system/src/ingestor"
	"trading-system/src/logger"
	"trading-system/src/models"
	"trading-system/src/utils"
)

// -----------------------------------------------------------------------------
// ControlService Implementation
// -----------------------------------------------------------------------------

type ControlServiceImpl struct {
	UnimplementedControlServiceServer
	Name     string
	ingestor *ingestor.Ingestor
	config   *config.Config
	logger   *logger.Logger
}

// -----------------------------------------------------------------------------

// NewControlService creates a new ControlServiceImpl instance
func NewControlService(config *config.Config, logger *logger.Logger, ingestor *ingestor.Ingestor) *ControlServiceImpl {
	return &ControlServiceImpl{
		Name:     "GRPCControlService",
		ingestor: ingestor,
		config:   config,
		logger:   logger,
	}
}

// -----------------------------------------------------------------------------
// Data Source Management
// -----------------------------------------------------------------------------

// AddDataSource implements the gRPC AddDataSource method
func (s *ControlServiceImpl) AddDataSource(ctx context.Context, req *AddDataSourceRequest) (*ControlResponse, error) {
	s.logger.Info("%s : received AddDataSource request for %s", s.Name, req.SourceName)

	// Convert request to internal model
	dataSource := models.MDataSourceConfig{
		Name:     req.SourceName,
		Type:     req.SourceType,
		Symbols:  req.Symbols,
		APIKey:   req.ApiKey,
		Endpoint: req.Endpoint,
	}

	// Add to ingestor
	if err := s.ingestor.AddDataSource(dataSource.Name); err != nil {
		s.logger.Error("%s : failed to add data source %s: %v", s.Name, req.SourceName, err)
		return &ControlResponse{
			Success:   false,
			Message:   fmt.Sprintf("Failed to add data source: %v", err),
			Timestamp: time.Now().Unix(),
			ErrorCode: "ADD_SOURCE_FAILED",
		}, nil
	}

	return &ControlResponse{
		Success:   true,
		Message:   fmt.Sprintf("Data source '%s' added successfully", req.SourceName),
		Timestamp: time.Now().Unix(),
	}, nil
}

// -----------------------------------------------------------------------------

// RemoveDataSource implements the gRPC RemoveDataSource method
func (s *ControlServiceImpl) RemoveDataSource(ctx context.Context, req *RemoveDataSourceRequest) (*ControlResponse, error) {
	s.logger.Info("%s : received RemoveDataSource request for %s", s.Name, req.SourceName)

	if err := s.ingestor.RemoveDataSource(req.SourceName); err != nil {
		s.logger.Error("%s : failed to remove data source %s: %v", s.Name, req.SourceName, err)
		return &ControlResponse{
			Success:   false,
			Message:   fmt.Sprintf("Failed to remove data source: %v", err),
			Timestamp: time.Now().Unix(),
			ErrorCode: "REMOVE_SOURCE_FAILED",
		}, nil
	}

	return &ControlResponse{
		Success:   true,
		Message:   fmt.Sprintf("Data source '%s' removed successfully", req.SourceName),
		Timestamp: time.Now().Unix(),
	}, nil
}

// -----------------------------------------------------------------------------
// Broker Management
// -----------------------------------------------------------------------------

// StartBroker implements the gRPC StartBroker method
func (s *ControlServiceImpl) StartBroker(ctx context.Context, req *StartBrokerRequest) (*ControlResponse, error) {
	s.logger.Info("%s : received StartBroker request for %s (type: %s)", s.Name, req.BrokerName, req.BrokerType)

	// Validate source name
	if req.BrokerName == "" {
		s.logger.Error("%s : StartBroker called with empty broker_name", s.Name)
		return &ControlResponse{
			Success:   false,
			Message:   "Source name cannot be empty",
			Timestamp: time.Now().Unix(),
			ErrorCode: "INVALID_REQUEST",
		}, nil
	}

	// Use source name as broker ID (simpler and allows restart)
	brokerID := req.BrokerName

	// Check if broker is already running by querying the ingestor directly
	if status, err := s.ingestor.GetDataSourceStatus(brokerID); err == nil {
		if status.Running {
			return &ControlResponse{
				Success:   false,
				Message:   fmt.Sprintf("Broker '%s' is already running", brokerID),
				Timestamp: time.Now().Unix(),
				ErrorCode: "BROKER_ALREADY_RUNNING",
			}, nil
		}
	}

	// Check if data source exists in ingestor, if not add it
	if _, err := s.ingestor.GetDataSourceStatus(brokerID); err != nil {
		// Data source doesn't exist, try to add it
		s.logger.Info("%s : data source '%s' not found, attempting to add it", s.Name, brokerID)
		if err := s.ingestor.AddDataSource(brokerID); err != nil {
			s.logger.Error("%s : failed to add data source %s: %v", s.Name, brokerID, err)
			return &ControlResponse{
				Success:   false,
				Message:   fmt.Sprintf("Failed to add data source: %v", err),
				Timestamp: time.Now().Unix(),
				ErrorCode: "ADD_SOURCE_FAILED",
			}, nil
		}
	}

	// Start the broker in the ingestor
	if err := s.ingestor.StartDataSource(brokerID); err != nil {
		s.logger.Error("%s : failed to start broker %s: %v", s.Name, brokerID, err)
		return &ControlResponse{
			Success:   false,
			Message:   fmt.Sprintf("Failed to start broker: %v", err),
			Timestamp: time.Now().Unix(),
			ErrorCode: "START_BROKER_FAILED",
		}, nil
	}

	s.logger.Info("%s : broker '%s' started successfully", s.Name, brokerID)
	return &ControlResponse{
		Success:   true,
		Message:   fmt.Sprintf("Broker '%s' started successfully", req.BrokerName),
		Timestamp: time.Now().Unix(),
	}, nil
}

// -----------------------------------------------------------------------------

// StopBroker implements the gRPC StopBroker method
func (s *ControlServiceImpl) StopBroker(ctx context.Context, req *StopBrokerRequest) (*ControlResponse, error) {
	s.logger.Info("%s : received StopBroker request for %s", s.Name, req.BrokerName)

	// Check if broker exists and is running
	if status, err := s.ingestor.GetDataSourceStatus(req.BrokerName); err != nil {
		s.logger.Error("%s : broker %s not found: %v", s.Name, req.BrokerName, err)
		return &ControlResponse{
			Success:   false,
			Message:   fmt.Sprintf("Broker '%s' not found", req.BrokerName),
			Timestamp: time.Now().Unix(),
			ErrorCode: "BROKER_NOT_FOUND",
		}, nil
	} else if !status.Running {
		// Broker exists but is already stopped
		s.logger.Info("%s : broker %s is already stopped", s.Name, req.BrokerName)
		return &ControlResponse{
			Success:   false,
			Message:   fmt.Sprintf("Broker '%s' is already stopped", req.BrokerName),
			Timestamp: time.Now().Unix(),
			ErrorCode: "BROKER_ALREADY_STOPPED",
		}, nil
	}

	// Stop the broker in the ingestor
	if err := s.ingestor.StopDataSource(req.BrokerName); err != nil {
		s.logger.Error("%s : failed to stop broker %s: %v", s.Name, req.BrokerName, err)
		return &ControlResponse{
			Success:   false,
			Message:   fmt.Sprintf("Failed to stop broker: %v", err),
			Timestamp: time.Now().Unix(),
			ErrorCode: "STOP_BROKER_FAILED",
		}, nil
	}

	s.logger.Info("%s : broker '%s' stopped successfully", s.Name, req.BrokerName)
	return &ControlResponse{
		Success:   true,
		Message:   fmt.Sprintf("Broker '%s' stopped successfully", req.BrokerName),
		Timestamp: time.Now().Unix(),
	}, nil
}

// -----------------------------------------------------------------------------

// ListBrokers implements the gRPC ListBrokers method
func (s *ControlServiceImpl) ListBrokers(ctx context.Context, req *ListBrokersRequest) (*ListBrokersResponse, error) {
	s.logger.Debug("%s : received ListBrokers request", s.Name)

	runningBrokers := make([]*BrokerInfo, 0)
	availableBrokers := make([]*BrokerInfo, 0)

	// Get running brokers
	for _, broker := range s.ingestor.Sources {
		if broker.GetStatus().Running {
			runningBrokers = append(runningBrokers, &BrokerInfo{
				BrokerName: broker.GetStatus().SourceName,
				BrokerType: broker.GetStatus().Type,
				Endpoint:   utils.MaskAPIKey(broker.GetStatus().Endpoint),
				Running:    broker.GetStatus().Running,
				Symbols:    broker.GetStatus().Symbols,
				Status:     "Running", // Or get detailed status if available
			})
		}
	}

	// Get available brokers if requested
	if req.IncludeAvailable {
		for _, broker := range s.ingestor.Sources {
			// Avoid duplicates if it's already in running list (though logic below adds all)
			// Actually, "Available" usually implies "Configured but maybe not running" or "All known".
			// The proto definition says "running_brokers" and "available_brokers".
			// Let's assume "available" means "all configured sources" or "stopped sources".
			// Given the context of "IncludeAvailable", it likely means "All configured sources".
			// Let's add them all to available for now as per original intent, or filter?
			// The original code was just iterating s.ingestor.Sources.

			availableBrokers = append(availableBrokers, &BrokerInfo{
				BrokerName: broker.GetStatus().SourceName,
				BrokerType: broker.GetStatus().Type,
				Endpoint:   utils.MaskAPIKey(broker.GetStatus().Endpoint),
				Running:    broker.GetStatus().Running,
				Symbols:    broker.GetStatus().Symbols,
				Status: func() string {
					if broker.GetStatus().Running {
						return "Running"
					}
					return "Stopped"
				}(),
			})
		}
	}

	return &ListBrokersResponse{
		RunningBrokers:   runningBrokers,
		AvailableBrokers: availableBrokers,
		TotalRunning:     int32(len(runningBrokers)),
		TotalAvailable:   int32(len(availableBrokers)),
		Timestamp:        time.Now().Unix(),
	}, nil
}

// -----------------------------------------------------------------------------
// Symbol Management
// -----------------------------------------------------------------------------

// AddSymbols implements the gRPC AddSymbols method
func (s *ControlServiceImpl) AddSymbols(ctx context.Context, req *AddSymbolsRequest) (*ControlResponse, error) {
	s.logger.Info("%s : received AddSymbols request for broker %s: %v", s.Name, req.BrokerName, req.Symbols)

	// Validate broker exists and is running
	if status, err := s.ingestor.GetDataSourceStatus(req.BrokerName); err != nil {
		s.logger.Error("%s : broker %s not found: %v", s.Name, req.BrokerName, err)
		return &ControlResponse{
			Success:   false,
			Message:   fmt.Sprintf("Broker '%s' not found. Please start the broker first.", req.BrokerName),
			Timestamp: time.Now().Unix(),
			ErrorCode: "BROKER_NOT_FOUND",
		}, nil
	} else if !status.Running {
		s.logger.Warning("%s : broker %s is not running", s.Name, req.BrokerName)
		return &ControlResponse{
			Success:   false,
			Message:   fmt.Sprintf("Broker '%s' is not running. Please start the broker first.", req.BrokerName),
			Timestamp: time.Now().Unix(),
			ErrorCode: "BROKER_NOT_RUNNING",
		}, nil
	}

	if err := s.ingestor.SubscribeSource(req.BrokerName, req.Symbols); err != nil {
		s.logger.Error("%s : failed to add symbols to broker %s: %v", s.Name, req.BrokerName, err)
		return &ControlResponse{
			Success:   false,
			Message:   fmt.Sprintf("Failed to add symbols: %v", err),
			Timestamp: time.Now().Unix(),
			ErrorCode: "ADD_SYMBOLS_FAILED",
		}, nil
	}

	return &ControlResponse{
		Success:   true,
		Message:   fmt.Sprintf("Successfully added %d symbol(s) to broker '%s'", len(req.Symbols), req.BrokerName),
		Timestamp: time.Now().Unix(),
	}, nil
}

// -----------------------------------------------------------------------------

// AddSymbolsAll implements the gRPC AddSymbolsAll method
func (s *ControlServiceImpl) AddSymbolsAll(ctx context.Context, req *AddSymbolsAllRequest) (*ControlResponse, error) {
	s.logger.Info("%s : received AddSymbolsAll request: %v", s.Name, req.Symbols)

	if err := s.ingestor.SubscribeAllSources(req.Symbols); err != nil {
		s.logger.Error("%s : failed to add symbols to all brokers: %v", s.Name, err)
		return &ControlResponse{
			Success:   false,
			Message:   fmt.Sprintf("Failed to add symbols to all brokers: %v", err),
			Timestamp: time.Now().Unix(),
			ErrorCode: "ADD_SYMBOLS_ALL_FAILED",
		}, nil
	}

	return &ControlResponse{
		Success:   true,
		Message:   fmt.Sprintf("Added %d symbols to all running brokers", len(req.Symbols)),
		Timestamp: time.Now().Unix(),
	}, nil
}

// -----------------------------------------------------------------------------

// RemoveSymbols implements the gRPC RemoveSymbols method
func (s *ControlServiceImpl) RemoveSymbols(ctx context.Context, req *RemoveSymbolsRequest) (*ControlResponse, error) {
	s.logger.Info("%s : received RemoveSymbols request for broker %s: %v", s.Name, req.BrokerName, req.Symbols)

	// Validate broker exists and is running
	if status, err := s.ingestor.GetDataSourceStatus(req.BrokerName); err != nil {
		s.logger.Error("%s : broker %s not found: %v", s.Name, req.BrokerName, err)
		return &ControlResponse{
			Success:   false,
			Message:   fmt.Sprintf("Broker '%s' not found", req.BrokerName),
			Timestamp: time.Now().Unix(),
			ErrorCode: "BROKER_NOT_FOUND",
		}, nil
	} else if !status.Running {
		s.logger.Warning("%s : broker %s is not running", s.Name, req.BrokerName)
		return &ControlResponse{
			Success:   false,
			Message:   fmt.Sprintf("Broker '%s' is not running. Cannot remove symbols from stopped broker.", req.BrokerName),
			Timestamp: time.Now().Unix(),
			ErrorCode: "BROKER_NOT_RUNNING",
		}, nil
	}

	if err := s.ingestor.UnSubscribeSource(req.BrokerName, req.Symbols); err != nil {
		s.logger.Error("%s : failed to remove symbols from broker %s: %v", s.Name, req.BrokerName, err)
		return &ControlResponse{
			Success:   false,
			Message:   fmt.Sprintf("Failed to remove symbols: %v", err),
			Timestamp: time.Now().Unix(),
			ErrorCode: "REMOVE_SYMBOLS_FAILED",
		}, nil
	}

	return &ControlResponse{
		Success:   true,
		Message:   fmt.Sprintf("Successfully removed %d symbol(s) from broker '%s'", len(req.Symbols), req.BrokerName),
		Timestamp: time.Now().Unix(),
	}, nil
}

// -----------------------------------------------------------------------------

// RemoveSymbolsAll implements the gRPC RemoveSymbolsAll method
func (s *ControlServiceImpl) RemoveSymbolsAll(ctx context.Context, req *RemoveSymbolsAllRequest) (*ControlResponse, error) {
	s.logger.Info("%s : received RemoveSymbolsAll request: %v", s.Name, req.Symbols)

	if err := s.ingestor.UnSubscribeAllSources(req.Symbols); err != nil {
		s.logger.Error("%s : failed to remove symbols from all brokers: %v", s.Name, err)
		return &ControlResponse{
			Success:   false,
			Message:   fmt.Sprintf("Failed to remove symbols from all brokers: %v", err),
			Timestamp: time.Now().Unix(),
			ErrorCode: "REMOVE_SYMBOLS_ALL_FAILED",
		}, nil
	}

	return &ControlResponse{
		Success:   true,
		Message:   fmt.Sprintf("Removed %d symbols from all running brokers", len(req.Symbols)),
		Timestamp: time.Now().Unix(),
	}, nil
}

// -----------------------------------------------------------------------------
// Health Check
// -----------------------------------------------------------------------------

// GetBrokerStatus implements the gRPC GetBrokerStatus method
func (s *ControlServiceImpl) GetBrokerStatus(ctx context.Context, req *GetBrokerStatusRequest) (*BrokerStatusResponse, error) {
	s.logger.Debug("%s : received GetBrokerStatus request for %s", s.Name, req.BrokerName)

	// Get status directly from ingestor
	status, err := s.ingestor.GetDataSourceStatus(req.BrokerName)
	if err != nil {
		s.logger.Error("%s : broker %s not found: %v", s.Name, req.BrokerName, err)
		return &BrokerStatusResponse{
			BrokerName:    req.BrokerName,
			Running:       false,
			StatusMessage: "Not found",
		}, nil
	}

	// Convert ingestor status to gRPC response
	return &BrokerStatusResponse{
		BrokerName: req.BrokerName,
		Running:    status.Running,
		BrokerType: status.Type,
		StatusMessage: func() string {
			if status.Running {
				return "Running"
			}
			return "Stopped"
		}(),
	}, nil
}

// -----------------------------------------------------------------------------

// HealthCheck implements the gRPC HealthCheck method
func (s *ControlServiceImpl) HealthCheck(ctx context.Context, req *HealthCheckRequest) (*HealthCheckResponse, error) {
	s.logger.Debug("%s : received HealthCheck request for service %s", s.Name, req.ServiceName)

	// Check ingestor health
	isHealthy := true
	statusMessage := "All systems operational"
	details := make(map[string]string)

	// Get ingestor status
	details["ingestor"] = "Healthy"
	details["ingestor_name"] = s.ingestor.Name
	for _, source_object := range s.ingestor.Sources {
		if source_object.GetStatus().Running {
			details["running_sources"] = details["running_sources"] + source_object.GetName()
		}
		details["total_sources"] = fmt.Sprintf("%d", len(s.ingestor.Sources))
	}

	return &HealthCheckResponse{
		Healthy:   isHealthy,
		Status:    statusMessage,
		Timestamp: time.Now().Unix(),
		Details:   details,
	}, nil
}
