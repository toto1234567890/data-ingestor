package interfaces

import "data-ingestor/src/models"

// -----------------------------------------------------------------------------

// IPublisher defines the interface for publishing market data
type IPublisher interface {
	// OnMarketData processes and publishes market data
	OnMarketData(data *models.MMarketData)

	// Connect establishes connection to the message broker
	Connect() error

	// Disconnect closes the connection to the message broker
	Disconnect() error

	// IsConnected returns the current connection status
	IsConnected() bool

	// OnOtherDataType...

	// OnOtherEvent...
}
