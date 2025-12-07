package ingestor

import (
	"context"
	"fmt"

	"data-ingestor/src/interfaces"
	"data-ingestor/src/logger"
	"data-ingestor/src/models"
)

// -----------------------------------------------------------------------------
// Core Application and Configuration Structs
// -----------------------------------------------------------------------------

// BrokerConnectionSource holds the broker and connection client for a single data source flow
type MarketDataSource struct {
	Name   string
	Logger *logger.Logger
	Broker interfaces.IBroker
	Client interfaces.IConnectionClient
}

// -----------------------------------------------------------------------------

// Start initiates the connection client and sends the initial subscriptions.
func (s *MarketDataSource) GetName() string {
	return s.Name
}

// -----------------------------------------------------------------------------

// Start initiates the connection client and sends the initial subscriptions.
func (s *MarketDataSource) Start() error {
	s.Logger.Info("%s : starting connection client for broker", s.Name)
	if err := s.Client.Connect(context.Background()); err != nil {
		return fmt.Errorf("failed to start client %s: %w", s.Name, err)
	}

	s.Logger.Info("%s : connection client started", s.Name)
	return nil
}

// -----------------------------------------------------------------------------

// Stop closes the connection client.
func (s *MarketDataSource) Stop() error {
	s.Logger.Info("%s : stopping connection client for broker", s.Name)
	return s.Client.Disconnect()
}

// -----------------------------------------------------------------------------
// Subscription Methods
// -----------------------------------------------------------------------------

// Subscribe creates a subscription message for the given symbols and sends it to the broker.
func (s *MarketDataSource) Subscribe(symbols []string) error {
	if len(symbols) == 0 {
		return nil // Nothing to subscribe to
	}

	// Create the subscription message using the broker's logic
	subscriptionMsg, err := s.Broker.AddSubscription(symbols)
	if err != nil {
		return fmt.Errorf("failed to marshal subscription message for %s: %w", s.Name, err)
	}

	// Send the subscription message over the established connection
	if err := s.Client.SendMessage(subscriptionMsg); err != nil {
		return fmt.Errorf("failed to send subscription message for %s: %w", s.Name, err)
	}

	s.Logger.Info("%s : successfully sent subscription message for %d symbols", s.Name, len(symbols))
	return nil
}

// -----------------------------------------------------------------------------

// Unsubscribe creates an unsubscription message for the given symbols and sends it to the broker.
func (s *MarketDataSource) UnSubscribe(symbols []string) error {
	if len(symbols) == 0 {
		return nil // Nothing to unsubscribe from
	}

	// Create the unsubscription message using the broker's logic
	unsubscriptionMsg, err := s.Broker.RemoveSubscription(symbols)
	if err != nil {
		return fmt.Errorf("failed to marshal unsubscription message for %s: %w", s.Name, err)
	}

	// Send the unsubscription message over the established connection
	if err := s.Client.SendMessage(unsubscriptionMsg); err != nil {
		return fmt.Errorf("failed to send unsubscription message for %s: %w", s.Name, err)
	}

	s.Logger.Info("%s : successfully sent unsubscription message for %d symbols", s.Name, len(symbols))
	return nil
}

// -----------------------------------------------------------------------------

func (s *MarketDataSource) GetStatus() *models.MDataSourceStatus {
	return &models.MDataSourceStatus{
		SourceName:    s.Broker.GetName(),
		Running:       s.Client.IsRunning(),   // <-- From IConnectionClient
		Type:          s.Broker.GetType(),     // <-- From IBroker
		TransportType: s.Client.GetType(),     // <-- From IConnectionClient
		Endpoint:      s.Broker.GetEndPoint(), // <-- From IBroker
		Symbols:       s.Broker.GetSymbols(),  // <-- From IBroker
	}
}

