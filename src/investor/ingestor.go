package ingestor

import (
	"context"
	"fmt"
	"sync"

	"trading-system/src/config"
	"trading-system/src/factories"
	"trading-system/src/interfaces"
	"trading-system/src/logger"
	"trading-system/src/models"
	"trading-system/src/publishers"
	"trading-system/src/serializers"
)

// -----------------------------------------------------------------------------
// Core Application and Configuration Structs
// -----------------------------------------------------------------------------

// Ingestor handles real-time market data ingestion from multiple sources
type Ingestor struct {
	Name   string
	Config *config.Config
	Logger *logger.Logger
	// NATSClient is used to publish market data to the message bus
	// NATSClient interfaces.INATSClient
	Publisher *interfaces.IPublisher
	// Factory dependency to create Broker and Connection clients
	Factory *factories.MarketDataSourceFactory
	// List of running data source instances now uses the interface.
	Sources map[string]interfaces.IDataSource
	mu      sync.RWMutex
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
}

// -----------------------------------------------------------------------------

// NewIngestor creates a new Ingestor instance
func NewIngestor(config *config.Config, logger *logger.Logger) *Ingestor {
	ctx, cancel := context.WithCancel(context.Background())

	// create new nats client and serializer
	NATSSerilizer := serializers.NewJSONSerializer()

	// create the nats publisher that handle and push messages
	Publisher := publishers.NewNATSPublisher(&config.NATS, logger, NATSSerilizer)

	ingestor := &Ingestor{
		Name:   "MarketDataIngestor",
		Config: config,
		Logger: logger,

		// Publisher, route and send message to the publisher (NATS, KAFKA...)
		Publisher: &Publisher,

		// Update the factory to use the ingestor's, publisher distribution method
		Factory: factories.NewBrokerFactory(config, logger, Publisher.OnMarketData), // Placeholder: Factory needs update

		Sources: make(map[string]interfaces.IDataSource),
		ctx:     ctx,
		cancel:  cancel,
	}

	return ingestor
}

// -----------------------------------------------------------------------------
// Public Lifecycle Methods (All Sources)
// -----------------------------------------------------------------------------

// Start begins ingesting market data from all configured sources
func (mdi *Ingestor) Start() error {
	mdi.Logger.Info("%s : starting market data ingestor", mdi.Name)

	// 1. Connect to publisher first - fail fast if publisher unavailable
	mdi.Logger.Info("%s : connecting to publisher", mdi.Name)
	if err := (*mdi.Publisher).Connect(); err != nil {
		return fmt.Errorf("failed to connect to publisher: %w", err)
	}
	mdi.Logger.Info("%s : publisher connected successfully", mdi.Name)

	// 2. Create all sources using the factory
	if err := mdi.createAllSources(); err != nil {
		return fmt.Errorf("failed to create all data sources: %w", err)
	}

	// 3. Start all connections concurrently
	mdi.startAllDataSources()

	mdi.Logger.Info("ingestor started successfully, monitoring %d connections.", len(mdi.Sources))
	return nil
}

// -----------------------------------------------------------------------------

// StopAllDataSources gracefully shuts down the ingestor and all data sources (RENAMED FROM Stop)
func (mdi *Ingestor) Stop() error {
	mdi.Logger.Info("%s : stopping ingestors", mdi.Name)

	// Call stop on all sources
	mdi.mu.RLock()
	for _, source := range mdi.Sources {
		source.Stop()
	}
	mdi.mu.RUnlock()

	// Signal goroutines to exit (if they check the context)
	mdi.cancel()

	// Wait for all connection goroutines to finish
	mdi.wg.Wait()

	// Disconnect publisher after all data sources have stopped
	mdi.Logger.Info("%s : disconnecting publisher", mdi.Name)
	if err := (*mdi.Publisher).Disconnect(); err != nil {
		mdi.Logger.Error("%s : failed to disconnect publisher: %v", mdi.Name, err)
	}

	mdi.Logger.Info("%s : ingestor stopped", mdi.Name)
	return nil
}

// -----------------------------------------------------------------------------
// Dynamic Data Source Management Methods
// -----------------------------------------------------------------------------

// StartDataSource starts a single, named data source synchronously. (NEW)
func (mdi *Ingestor) StartDataSource(sourceName string) error {
	mdi.mu.RLock()
	source, ok := mdi.Sources[sourceName]
	mdi.mu.RUnlock()

	if !ok {
		return fmt.Errorf("data source '%s' not found", sourceName)
	}

	mdi.Logger.Info("%s : starting MarketDataSource for %s", mdi.Name, sourceName)
	if err := source.Start(); err != nil {
		mdi.Logger.Error("%s : data source %s startup error: %v", mdi.Name, sourceName, err)
		return err
	}

	mdi.Logger.Info("%s : data source '%s' started successfully", mdi.Name, sourceName)
	return nil
}

// -----------------------------------------------------------------------------

// StopDataSource stops a single, named data source. (NEW)
func (mdi *Ingestor) StopDataSource(sourceName string) error {
	mdi.mu.RLock()
	source, ok := mdi.Sources[sourceName]
	mdi.mu.RUnlock()

	if !ok {
		return fmt.Errorf("%s : data source '%s' not found", mdi.Name, sourceName)
	}

	mdi.Logger.Info("%s : stopping interfaces.IDataSource for %s", mdi.Name, sourceName)
	return source.Stop()
}

// -----------------------------------------------------------------------------

// AddDataSource creates a new interfaces.IDataSource instance based on the configuration name, and stores it.
func (mdi *Ingestor) AddDataSource(sourceName string) error {
	mdi.Logger.Info("%s : attempting to add new data source: %s", mdi.Name, sourceName)

	mdi.mu.RLock()
	_, exists := mdi.Sources[sourceName]
	mdi.mu.RUnlock()

	if exists {
		return fmt.Errorf("data source '%s' is already registered", sourceName)
	}

	broker, connection, err := mdi.Factory.CreateBrokerWithConnection(sourceName)
	if err != nil {
		return fmt.Errorf("failed to create broker/connection for %s: %w", sourceName, err)
	}

	mdi.mu.Lock()
	mdi.Sources[sourceName] = &MarketDataSource{
		Name:   sourceName,
		Logger: mdi.Logger,
		Broker: broker,
		Client: connection,
	}
	mdi.mu.Unlock()

	mdi.Logger.Info("%s : data source '%s' successfully added, ready to be started", mdi.Name, sourceName)
	return nil
}

// -----------------------------------------------------------------------------

// DeleteDataSource removes a interfaces.IDataSource instance from the map.
func (mdi *Ingestor) RemoveDataSource(sourceName string) error {
	mdi.mu.RLock()
	source, exists := mdi.Sources[sourceName]
	mdi.mu.RUnlock()

	if !exists {
		return fmt.Errorf("data source '%s' not found for deletion", sourceName)
	}

	if source.GetStatus().Running == true {
		source.Stop()
	}

	mdi.mu.Lock()
	delete(mdi.Sources, sourceName)
	mdi.mu.Unlock()
	mdi.Logger.Info("%s : data source '%s' successfully deleted from management map", mdi.Name, sourceName)
	return nil
}

// -----------------------------------------------------------------------------

func (mdi *Ingestor) ListDatasources() []string {
	var names []string

	mdi.mu.RLock()
	defer mdi.mu.RUnlock()

	for name := range mdi.Sources {
		names = append(names, name)
	}
	return names
}

// -----------------------------------------------------------------------------

// SubscribeAllSources concurrently subscribes all initialized data sources to the given symbols.
func (mdi *Ingestor) SubscribeAllSources(symbols []string) error {
	if len(symbols) == 0 {
		return fmt.Errorf("subscription failed: attempted to subscribe all sources with an empty symbol list")
	}

	mdi.Logger.Info("%s : subscribing all %d data sources to symbols: %v", mdi.Name, len(mdi.Sources), symbols)

	var wg sync.WaitGroup

	mdi.mu.RLock()
	// Create a copy or iterate under lock. Iterating under lock is fine if the operations are non-blocking or we launch goroutines.
	// We are launching goroutines, so we can iterate under lock.
	sources := make(map[string]interfaces.IDataSource, len(mdi.Sources))
	for k, v := range mdi.Sources {
		sources[k] = v
	}
	mdi.mu.RUnlock()

	errCh := make(chan error, len(sources))

	for name, source := range sources {
		wg.Add(1)
		go func(n string, s interfaces.IDataSource) {
			defer wg.Done()
			if err := s.Subscribe(symbols); err != nil {
				wrappedErr := fmt.Errorf("failed to subscribe source %s to symbols %v: %w", n, symbols, err)
				errCh <- wrappedErr
			}
		}(name, source)
	}

	wg.Wait()
	close(errCh)

	var allErrors error
	for err := range errCh {
		if allErrors == nil {
			allErrors = err
		} else {
			allErrors = fmt.Errorf("%s; %w", allErrors.Error(), err)
		}
	}

	if allErrors != nil {
		mdi.Logger.Error("%s : subscription request failed for one or more sources", mdi.Name)
		return fmt.Errorf("market data subscription failed: %w", allErrors)
	}

	mdi.Logger.Info("%s : subscription request sent successfully to all data sources", mdi.Name)
	return nil
}

// -----------------------------------------------------------------------------

// UnsubscribeAllSources concurrently unsubscribes all initialized data sources from the given symbols.
func (mdi *Ingestor) UnSubscribeAllSources(symbols []string) error {
	if len(symbols) == 0 {
		return fmt.Errorf("unsubscription failed: attempted to unsubscribe all sources with an empty symbol list")
	}

	mdi.Logger.Info("%s : unsubscribing all %d data sources from symbols: %v", mdi.Name, len(mdi.Sources), symbols)

	var wg sync.WaitGroup

	mdi.mu.RLock()
	sources := make(map[string]interfaces.IDataSource, len(mdi.Sources))
	for k, v := range mdi.Sources {
		sources[k] = v
	}
	mdi.mu.RUnlock()

	errCh := make(chan error, len(sources))

	for name, source := range sources {
		wg.Add(1)
		go func(n string, s interfaces.IDataSource) {
			defer wg.Done()
			if err := s.UnSubscribe(symbols); err != nil {
				wrappedErr := fmt.Errorf("failed to unsubscribe source %s from symbols %v: %w", n, symbols, err)
				errCh <- wrappedErr
			}
		}(name, source)
	}

	wg.Wait()
	close(errCh)

	var allErrors error
	for err := range errCh {
		if allErrors == nil {
			allErrors = err
		} else {
			allErrors = fmt.Errorf("%s; %w", allErrors.Error(), err)
		}
	}

	if allErrors != nil {
		mdi.Logger.Error("%s : unsubscription request failed for one or more sources", mdi.Name)
		return fmt.Errorf("market data unsubscription failed: %w", allErrors)
	}

	mdi.Logger.Info("%s : unsubscription request sent successfully to all data sources", mdi.Name)
	return nil
}

// -----------------------------------------------------------------------------
// Status Methods
// -----------------------------------------------------------------------------

// GetDataSourceStatus returns the current status information for a single data source.
func (mdi *Ingestor) GetDataSourceStatus(sourceName string) (*models.MDataSourceStatus, error) {
	mdi.mu.RLock()
	source, ok := mdi.Sources[sourceName]
	mdi.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("data source '%s' not found in ingestor map", sourceName)
	}

	status := source.GetStatus()
	return status, nil
}

// -----------------------------------------------------------------------------
// Subscription Management Methods
// -----------------------------------------------------------------------------

// SubscribeSource subscribes a single, named broker connection to the given symbols.
func (mdi *Ingestor) SubscribeSource(sourceName string, symbols []string) error {
	mdi.mu.RLock()
	source, ok := mdi.Sources[sourceName]
	mdi.mu.RUnlock()

	if !ok {
		return fmt.Errorf("data source '%s' not found", sourceName)
	}

	mdi.Logger.Info("%s : sending subscription for symbols %v to single source: %s", mdi.Name, symbols, sourceName)
	return source.Subscribe(symbols)
}

// -----------------------------------------------------------------------------

// UnSubscribeSource unsubscribes a single, named broker connection to the given symbols.
func (mdi *Ingestor) UnSubscribeSource(sourceName string, symbols []string) error {
	mdi.mu.RLock()
	source, ok := mdi.Sources[sourceName]
	mdi.mu.RUnlock()

	if !ok {
		return fmt.Errorf("data source '%s' not found", sourceName)
	}

	mdi.Logger.Info("%s : sending unsubscription for symbols %v to single source: %s", mdi.Name, symbols, sourceName)
	return source.UnSubscribe(symbols)
}

// -----------------------------------------------------------------------------
// Private/Helper Methods
// -----------------------------------------------------------------------------

// createAllSources uses the DataSourceFactory to initialize all necessary
// broker and connection clients based on the config.
func (mdi *Ingestor) createAllSources() error {
	mdi.Sources = make(map[string]interfaces.IDataSource)

	for _, dataSourceConfig := range mdi.Config.DataSources {
		brokerName := dataSourceConfig.Name
		broker, connection, err := mdi.Factory.CreateBrokerWithConnection(brokerName)
		if err != nil {
			mdi.Logger.Error("%s : skipping data source %s: failed to create broker/connection: %v", mdi.Name, brokerName, err)
			continue
		}

		mdi.Sources[brokerName] = &MarketDataSource{
			Name:   brokerName,
			Logger: mdi.Logger,
			Broker: broker,
			Client: connection,
		}
	}

	if len(mdi.Sources) == 0 {
		return fmt.Errorf("no valid data sources were initialized from configuration")
	}

	return nil
}

// -----------------------------------------------------------------------------

// StartAllDataSources starts all registered data sources concurrently (RENAMED FROM startDataSources)
func (mdi *Ingestor) startAllDataSources() {
	mdi.mu.RLock()
	defer mdi.mu.RUnlock()
	for name, source := range mdi.Sources {
		mdi.wg.Add(1)
		go func(n string, s interfaces.IDataSource) {
			defer mdi.wg.Done()
			mdi.Logger.Info("%s : starting MarketDataSource for %s", mdi.Name, n)
			if err := s.Start(); err != nil {
				mdi.Logger.Error("%s : data source %s startup error: %v", mdi.Name, n, err)
			}
			// Note: The Start() call here blocks until the connection client is running.
			// The actual data flow (receiving/parsing) happens internally via the IConnectionClient.
		}(name, source)
	}
}
