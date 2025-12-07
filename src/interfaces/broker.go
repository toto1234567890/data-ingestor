package interfaces

import (
	"data-ingestor/src/config"
	"data-ingestor/src/logger"
	"data-ingestor/src/models"
)

// -----------------------------------------------------------------------------

// BrokerConstructor defines the function signature for creating a new IBroker instance.
type IBrokerConstructor func(config *config.Config, logger *logger.Logger, name string) (IBroker, error)

// -----------------------------------------------------------------------------

// Broker defines the core interface for all exchange broker implementations
type IBroker interface {
	// GetName return the broker name
	GetName() string

	// GetType return the asset type (equity, crypto, stock...)
	GetType() string

	// GetEndPoint return the API endpoint of the broker (for display/logging)
	GetEndPoint() string

	// GetConnectionEndpoint returns the full endpoint with credentials for WebSocket connection
	GetEndpointWithCredentials() string

	// GetSymbols return the subscriber symbols list
	GetSymbols() []string

	// AddSubscription creates subscription message for symbols
	AddSubscription(symbols []string) ([]byte, error)

	// RemoveSubscription removes subscription for symbols
	RemoveSubscription(symbols []string) ([]byte, error)

	// ParseMessage processes incoming messages into MarketData
	ParseMessage(message []byte) (*models.MMarketData, error)
}
