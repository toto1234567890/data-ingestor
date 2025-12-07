package publishers

import (
	"fmt"
	"sync"
	"time"

	"trading-system/src/interfaces"
	"trading-system/src/logger"
	"trading-system/src/models"

	"github.com/nats-io/nats.go"
)

// -----------------------------------------------------------------------------
// NATSPublisher implements interfaces.INATSClient and adds publishing logic
// -----------------------------------------------------------------------------

// NATSClient implements the interfaces.INATSClient interface
type NATSPublisher struct {
	name   string
	config *models.MNATSConfig
	logger *logger.Logger

	useJetStream bool

	mu sync.RWMutex

	nc         *nats.Conn             // NATS core connection
	js         nats.JetStreamContext  // JetStream context (if enabled)
	serializer interfaces.ISerializer // serialize message before sending

	connected bool
}

// -----------------------------------------------------------------------------

// NewNATSClient creates a new NATS client instance
func NewNATSPublisher(config *models.MNATSConfig, logger *logger.Logger, serializer interfaces.ISerializer) interfaces.IPublisher {
	return &NATSPublisher{
		name:   config.ClientID,
		config: config,
		logger: logger,

		serializer: serializer,
	}
}

// -----------------------------------------------------------------------------

// OnMarketData is the central callback where all parsed MMarketData lands.
func (np *NATSPublisher) OnMarketData(data *models.MMarketData) {
	//// 1. Log or perform basic health checks (optional)
	//// m.Logger.Debug("Received Data: Type=%s, Symbol=%s, Price=%f", data.DataType, data.Symbol, data.Price)
	if data.DataType == models.DataTypeTrade {
		np.logger.Info("%s : TRADE: %s @ %.2f (Volume: %.2f)", np.name, data.Symbol, data.Price, data.Volume)
	}
	if data.DataType == models.DataTypeQuote {
		np.logger.Info("%s : QUOTE: %s | Bid: %.2f / Ask: %.2f", np.name, data.Symbol, data.BidPrice, data.AskPrice)
	}
	if data.DataType == models.DataTypeOrderBook {
		np.logger.Info("%s : ORDERBOOK: %s | Bid: %.2f / Ask: %.2f", np.name, data.Symbol, data.BidPrice, data.AskPrice)
	}

	// handle aggregation, if supportaggregation is false and has timeframe....

	// 2. DISTRIBUTION LOGIC GOES HERE:
	//    - Send data to a channel for consumers (e.g., strategy runner, database writer)
	//    - Simple example: just log the trade price
	subject := fmt.Sprintf("marketdata.%s.%s", data.DataType, data.Symbol)
	// PublishObject marshals the *models.MMarketData struct and sends it
	// over NATS Core (fire-and-forget delivery).

	dataSerialized, err := np.serializer.Marshal(data)
	if err != nil {
		// CRITICAL: Log a high-severity error if publishing fails,
		// as it indicates a failure in data distribution.
		np.logger.Error("%s : failed to serialize data for %s to NATS subject... %v", np.name, subject, err)
		return
	}

	if np.useJetStream {
		err = np.PublishJetStream(subject, dataSerialized)
	} else {
		err = np.Publish(subject, dataSerialized)
	}

	if err != nil {
		// CRITICAL: Log a high-severity error if publishing fails,
		// as it indicates a failure in data distribution.
		np.logger.Error("%s : failed to publish %s data for %s to NATS subject %s: %v",
			np.name, data.DataType, data.Symbol, subject, err)
		return
	}
}

// -----------------------------------------------------------------------------

// Publish sends raw data to a NATS core subject.
func (np *NATSPublisher) Publish(subject string, data []byte) error {
	if !np.IsConnected() {
		return fmt.Errorf("nats client not connected")
	}
	// Prefix the subject for consistency, if configured
	fullSubject := np.getSubject(subject)

	// This is fire-and-forget; use PublishJetStream for persistence
	return np.nc.Publish(fullSubject, data)
}

// -----------------------------------------------------------------------------

// PublishJetStream sends raw data using JetStream.
func (np *NATSPublisher) PublishJetStream(subject string, data []byte) error {
	if !np.IsConnected() {
		return fmt.Errorf("nats client not connected")
	}
	if np.js == nil {
		return fmt.Errorf("jetstream is not initialized or enabled")
	}

	fullSubject := np.getSubject(subject)

	// Publish with persistence and guaranteed delivery acknowledgement
	// The PubAck (Publish Acknowledgment) is implicitly handled here.
	_, err := np.js.Publish(fullSubject, data)
	if err != nil {
		np.logger.Error("%s : jetstream publish failed for %s: %v", np.name, fullSubject, err)
		return err
	}
	return nil
}

// -----------------------------------------------------------------------------

// Connect establishes connection to NATS server and sets up JetStream context if configured.
func (np *NATSPublisher) Connect() error {
	np.mu.Lock()
	defer np.mu.Unlock()

	if np.nc != nil && np.nc.IsConnected() {
		return nil
	}

	opts := []nats.Option{
		nats.Name(np.config.ClientID),
		nats.Timeout(np.config.ConnectTimeout),
		nats.ReconnectWait(np.config.ReconnectWait),
		nats.MaxReconnects(np.config.MaxReconnects),
		nats.FlusherTimeout(np.config.FlushTimeout),

		// Connection Event Handlers
		nats.RetryOnFailedConnect(true),
		nats.ClosedHandler(func(nc *nats.Conn) {
			np.logger.Error("%s : NATS connection closed unexpectedly", np.name)
			np.setConnected(false)
		}),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			np.logger.Warning("%s : NATS disconnected, attempting reconnect: %v", np.name, err)
			np.setConnected(false)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			np.logger.Info("%s : NATS successfully reconnected to %s", np.name, nc.ConnectedUrl())
			np.setConnected(true)
		}),
	}

	var err error
	np.nc, err = nats.Connect(np.config.Servers[0], opts...)
	if err != nil {
		return fmt.Errorf("nats connection failed: %w", err)
	}

	np.setConnected(true)
	np.logger.Info("%s : successfully connected to NATS at %s", np.name, np.nc.ConnectedUrl())

	// Set the publish function based on configuration
	if np.config.JetStream != nil && np.config.JetStream.Enabled {
		np.useJetStream = true
		np.logger.Info("%s : publisher using NATS JetStream for persistent data publishing", np.name)

		np.js, err = np.nc.JetStream()
		if err != nil {
			np.logger.Error("%s : failed to create JetStream context: %v", np.name, err)
			return fmt.Errorf("jetstream context creation failed: %w", err)
		}
		np.logger.Info("%s : JetStream context initialized", np.name)

		// Automatically create or update the stream based on configuration
		if err := np.ensureStreamExists(); err != nil {
			np.logger.Warning("%s : failed to ensure stream exists: %v (continuing anyway)", np.name, err)
			// Don't return error - allow publishing to fail later if stream really doesn't exist
		}

	} else {

		np.useJetStream = false
		np.logger.Warning("%s : publisher using NATS Core (fire-and-forget), JetStream is disabled in config", np.name)
	}

	return nil
}

// -----------------------------------------------------------------------------

// ensureStreamExists creates or updates the JetStream stream based on configuration.
// This is called automatically during Connect() to ensure the stream is ready for publishing.
func (np *NATSPublisher) ensureStreamExists() error {
	if np.js == nil || np.config.JetStream == nil {
		return fmt.Errorf("jetstream not initialized")
	}

	streamName := np.config.JetStream.StreamName
	if streamName == "" {
		return fmt.Errorf("stream name not configured")
	}

	// Check if stream already exists
	stream, err := np.js.StreamInfo(streamName)
	if err == nil {
		// Stream exists - log and return
		np.logger.Info("%s : JetStream stream '%s' already exists with %d subjects",
			np.name, streamName, len(stream.Config.Subjects))
		return nil
	}

	// Stream doesn't exist - create it
	np.logger.Info("%s : creating JetStream stream '%s'", np.name, streamName)

	// Use max_age from config (already a time.Duration)
	maxAge := np.config.JetStream.MaxAge
	if maxAge == 0 {
		maxAge = 72 * time.Hour // Default to 72 hours
	}

	// Build stream configuration from config
	streamConfig := &nats.StreamConfig{
		Name:       streamName,
		Subjects:   np.config.JetStream.Subjects,
		Retention:  nats.LimitsPolicy, // Always use limits for trading data
		Storage:    nats.FileStorage,  // Always use file storage
		Replicas:   int(np.config.JetStream.Replicas),
		MaxAge:     maxAge,
		MaxMsgs:    np.config.JetStream.MaxMsgs,
		MaxBytes:   np.config.JetStream.MaxBytes,
		MaxMsgSize: int32(np.config.JetStream.MaxMsgSize),
		Discard:    nats.DiscardOld, // Discard old messages when limits are reached
	}

	// Create the stream
	_, err = np.js.AddStream(streamConfig)
	if err != nil {
		return fmt.Errorf("failed to create stream '%s': %w", streamName, err)
	}

	np.logger.Info("%s : successfully created JetStream stream '%s' with subjects: %v",
		np.name, streamName, np.config.JetStream.Subjects)
	return nil
}

// -----------------------------------------------------------------------------

// Close closes the NATS connection
func (np *NATSPublisher) Disconnect() error {
	np.mu.Lock()
	defer np.mu.Unlock()

	if np.nc == nil || np.nc.IsClosed() {
		return nil
	}

	np.nc.Close()
	np.setConnected(false)
	np.logger.Info("%s : NATS connection closed successfully", np.name)
	return nil
}

// -----------------------------------------------------------------------------

// IsConnected returns connection status
func (np *NATSPublisher) IsConnected() bool {
	np.mu.RLock()
	defer np.mu.RUnlock()
	return np.connected
}

// -----------------------------------------------------------------------------

// GetName returns client identifier
func (np *NATSPublisher) GetName() string {
	return np.name
}

// -----------------------------------------------------------------------------

// Flush waits for all published messages to be acknowledged by the server (for core NATS).
func (np *NATSPublisher) Flush() error {
	if !np.IsConnected() {
		return fmt.Errorf("cannot flush: nats client not connected")
	}
	return np.nc.Flush()
}

// -----------------------------------------------------------------------------

// GetConfig returns the underlying NATS configuration
func (np *NATSPublisher) GetConfig() *models.MNATSConfig {
	return np.config
}

// -----------------------------------------------------------------------------

// setConnected updates the connection status safely.
// This method is called from NATS connection event handlers (different goroutines),
// so it must be thread-safe.
func (np *NATSPublisher) setConnected(status bool) {
	np.connected = status
}

// -----------------------------------------------------------------------------

// getSubject prepends the configured subject prefix if it exists.
func (np *NATSPublisher) getSubject(subject string) string {
	if np.config.SubjectPrefix != "" {
		return fmt.Sprintf("%s.%s", np.config.SubjectPrefix, subject)
	}
	return subject
}
