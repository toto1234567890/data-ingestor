package transports

import (
	"context"
	"fmt"
	"sync"
	"time"

	"data-ingestor/src/logger"
	"data-ingestor/src/models"
	"data-ingestor/src/utils"

	"github.com/gorilla/websocket"
)

// -----------------------------------------------------------------------------

// WebSocketClient implements IConnectionClient using Gorilla WebSocket
type WebSocketClient struct {
	conn         *websocket.Conn
	name         string
	config       *models.MDataSourceConfig
	logger       *logger.Logger
	isRunning    bool
	mu           sync.RWMutex
	recvMsgChann chan []byte
	errChann     chan error
	done         chan struct{}
	onRawData    func([]byte)
}

// -----------------------------------------------------------------------------

// NewWebSocketClient creates a new WebSocket client
func NewWebSocketClient(config *models.MDataSourceConfig, logger *logger.Logger, name string, onRawData func([]byte)) *WebSocketClient {
	return &WebSocketClient{
		name:         name,
		config:       config,
		logger:       logger,
		isRunning:    false,
		recvMsgChann: make(chan []byte, 1000), // FIXME take message buffer from config
		errChann:     make(chan error, 10),
		done:         make(chan struct{}),
		onRawData:    onRawData,
	}
}

// -----------------------------------------------------------------------------

// Connect establishes WebSocket connection and starts processing
func (w *WebSocketClient) Connect(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	conn, _, err := dialer.Dial(w.config.Endpoint, nil)
	if err != nil {
		w.logger.Error("%s : failed to connect to %s: %v", w.name, w.config.Endpoint, err)
		return fmt.Errorf("failed to connect to %s: %w", w.config.Endpoint, err)
	}

	// Recreate channels for new connection
	w.recvMsgChann = make(chan []byte, 1000)
	w.errChann = make(chan error, 10)
	w.done = make(chan struct{})

	w.conn = conn
	w.isRunning = true

	w.logger.Info("%s : WebSocket connected to %s", w.name, utils.MaskAPIKey(w.config.Endpoint))

	// Start message processing
	go w.ReceiveMessage(ctx)
	go w.ProcessIncomingMessage(ctx)
	go w.processErrors(ctx)

	return nil
}

// -----------------------------------------------------------------------------

// Disconnect closes the connection
func (w *WebSocketClient) Disconnect() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.isRunning {
		return nil
	}

	w.isRunning = false
	close(w.done)
	close(w.recvMsgChann)

	if w.conn != nil {
		err := w.conn.Close()
		w.conn = nil
		if err != nil {
			return fmt.Errorf("failed to close connection: %s: %w", w.config.Endpoint, err)
		}
	}

	w.logger.Info("%s : WebSocket disconnected from %s", w.name, w.config.Endpoint)
	return nil
}

// -----------------------------------------------------------------------------

// GetName returns the client name
func (w *WebSocketClient) GetName() string {
	return w.name
}

// -----------------------------------------------------------------------------

// GetType returns the transport type
func (w *WebSocketClient) GetType() string {
	return "websocket"
}

// -----------------------------------------------------------------------------

// IsRunning returns the connection status
func (w *WebSocketClient) IsRunning() bool {
	return w.isRunning
}

// -----------------------------------------------------------------------------

// SendMessage sends a message to the WebSocket
func (w *WebSocketClient) SendMessage(data []byte) error {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.conn == nil {
		return fmt.Errorf("connection is nil")
	}

	err := w.conn.WriteMessage(websocket.TextMessage, data)
	if err != nil {
		return fmt.Errorf("failed to send byte message: %w", err)
	}
	return nil
}

// -----------------------------------------------------------------------------

// ReceiveMessage receives messages
func (w *WebSocketClient) ReceiveMessage(ctx context.Context) {
	reconnectAttempts := 0

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.done:
			return
		default:
			if !w.IsRunning() {
				return
			}

			messageType, message, err := w.conn.ReadMessage()
			if err != nil {
				// Check if we are shutting down
				select {
				case <-w.done:
					return
				default:
				}

				w.errChann <- fmt.Errorf("read message error: %w", err)

				if reconnectAttempts < w.config.ConnectionConfig.ReconnectAttempts {
					reconnectAttempts++
					w.logger.Info("%s : attempting to reconnect (attempt %d/%d)", w.name, reconnectAttempts, w.config.ConnectionConfig.ReconnectAttempts)
					w.attemptReconnect(ctx)
					continue
				}
				return
			}

			if messageType == websocket.TextMessage {
				select {
				case w.recvMsgChann <- message:
				case <-ctx.Done():
					return
				case <-w.done:
					return
				}
			}

			// Reset reconnect attempts on successful read
			reconnectAttempts = 0
		}
	}
}

// -----------------------------------------------------------------------------

// processMessages processes incoming messages from the channel
func (w *WebSocketClient) ProcessIncomingMessage(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-w.done:
			return
		case byteMessage, ok := <-w.recvMsgChann:
			if !ok {
				return
			}
			w.onRawData(byteMessage)
		}
	}
}

// -----------------------------------------------------------------------------

// processMessages processes incoming errors from the channel
func (w *WebSocketClient) processErrors(ctx context.Context) {
	var err error
	for {
		select {
		case <-ctx.Done():
			return
		case <-w.done:
			return
		default:
			if !w.IsRunning() {
				return
			}
			err = <-w.errChann
			w.logger.Error("%s : websocket error: %v", w.name, err)
		}
	}
}

// -----------------------------------------------------------------------------

// attemptReconnect attempts to reconnect to the WebSocket
func (w *WebSocketClient) attemptReconnect(ctx context.Context) {
	w.mu.Lock()
	defer w.mu.Unlock()

	select {
	case <-ctx.Done():
		return
	case <-w.done:
		return
	default:
		if !w.IsRunning() {
			return
		}
	}

	if w.conn != nil {
		w.conn.Close()
		w.conn = nil
	}

	// Wait before reconnecting
	time.Sleep(1 * time.Second)

	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	conn, _, err := dialer.Dial(w.config.Endpoint, nil)
	if err != nil {
		w.logger.Error("%s : reconnection failed: %v", w.name, err)
		return
	}

	w.conn = conn
	w.logger.Info("%s : successfully reconnected to %s", w.name, w.config.Endpoint)
}
