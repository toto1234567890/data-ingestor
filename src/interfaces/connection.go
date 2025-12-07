package interfaces

import (
	"context"
)

// -----------------------------------------------------------------------------

// WebSocketClient defines the interface for WebSocket connections
type IConnectionClient interface {
	// The callback is expected to be passed during client initialization (NewWebSocketClient).
	Connect(ctx context.Context) error

	// Disconnect closes the connection
	Disconnect() error

	// IsRunning returns the connection status
	IsRunning() bool

	// GetName returns the client name
	GetName() string

	// GetType returns the transport type
	GetType() string

	// Send a message regarding protocol and transport
	SendMessage([]byte) error

	// Receive a message regarding protocol
	ReceiveMessage(ctx context.Context)
}
