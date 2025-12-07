package factories

import (
	"fmt"

	"data-ingestor/src/brokers"
	"data-ingestor/src/config"
	"data-ingestor/src/interfaces"
	"data-ingestor/src/logger"
	"data-ingestor/src/models"
	"data-ingestor/src/transports"
)

// -----------------------------------------------------------------------------

// Factory creates broker instances based on configuration
type MarketDataSourceFactory struct {
	Name   string
	Config *config.Config
	Logger *logger.Logger
	// The final callback function for distributing parsed market data
	OnDataCallback func(*models.MMarketData)
}

// -----------------------------------------------------------------------------

// NewBrokerFactory creates a new MarketDataSourceFactory instance
func NewBrokerFactory(config *config.Config, logger *logger.Logger, onData func(*models.MMarketData)) *MarketDataSourceFactory {
	return &MarketDataSourceFactory{
		Name:           "MarketDataSourceFactory",
		Config:         config,
		Logger:         logger,
		OnDataCallback: onData,
	}
}

// -----------------------------------------------------------------------------

// CreateBroker creates a broker instance by name using the dynamic registry.
func (mdsf *MarketDataSourceFactory) CreateBroker(brokerName string) (interfaces.IBroker, error) {
	// Dynamically fetch the constructor from the broker package registry
	constructor, err := brokers.GetConstructor(brokerName)
	if err != nil {
		return nil, err // Returns "unknown broker type: ..." error
	}

	newBroker, err := constructor(mdsf.Config, mdsf.Logger, brokerName)
	if err != nil {
		return nil, fmt.Errorf("failed to create broker %s: %w", brokerName, err)
	}

	mdsf.Logger.Info("%s : successfully created broker %s of type %s",
		mdsf.Name,
		newBroker.GetName(),
		newBroker.GetType(),
	)

	return newBroker, nil
}

// -----------------------------------------------------------------------------

// CreateConnectionClient creates a connection client for a broker
func (mdsf *MarketDataSourceFactory) CreateConnectionClient(brokerName string) (interfaces.IConnectionClient, error) {
	dataSource := mdsf.Config.GetDataSourceByName(brokerName)
	if dataSource == nil {
		return nil, fmt.Errorf("data source %s not found in config", brokerName)
	}

	// 1. Get the IBroker instance to access the ParseMessage method
	brokerInstance, err := mdsf.CreateBroker(brokerName)
	if err != nil {
		// This should not happen if the brokerName is in the config, but we check anyway.
		return nil, fmt.Errorf("failed to get broker %s for connection client: %w", brokerName, err)
	}

	// 2. Override the endpoint with the broker's connection endpoint (includes credentials)
	dataSource.Endpoint = brokerInstance.GetEndpointWithCredentials()

	// 3. Define the onRawData callback closure for the transport client.
	onRawData := func(message []byte) {
		// The Ingestor layer will handle the client-side aggregation logic,
		// but the transport layer needs to push the raw data up.

		// For simplicity at the transport layer, we still use the broker's ParseMessage
		// only for broker-side aggregated data. If client-aggregation is required,
		// a dedicated Ingestor method (like ProcessIncomingData from a previous turn)
		// would handle the raw message, not the ParseMessage method here.

		// Since this factory only handles parsing before distribution, we stick to ParseMessage
		// for now, recognizing that a dedicated Ingestor is usually inserted here.

		marketData, err := brokerInstance.ParseMessage(message)
		if err != nil {
			mdsf.Logger.Error("%s : failed to parse message for %s: %v (raw: %s)", mdsf.Name, brokerName, err, string(message))
			return
		}

		// Call the final data distribution callback if parsing was successful and data exists
		if marketData != nil && mdsf.OnDataCallback != nil {
			mdsf.OnDataCallback(marketData)
		}
	}

	// 4. Create the appropriate connection client based on transport type
	switch dataSource.Type {
	case "websocket":
		return transports.NewWebSocketClient(
			dataSource,
			mdsf.Logger,
			brokerName,
			onRawData, // Pass the closure that handles parsing and distribution
		), nil
	// FIXME implement transports if needed...
	// case "tcp":
	//     return tcp.NewTCPClient(connConfig, mdsf.Logger, brokerName), nil
	// case "http":
	//     return http.NewHTTPClient(connConfig, mdsf.Logger, brokerName), nil
	default:
		// Raise an error if the connection type is not explicitly supported.
		return nil, fmt.Errorf("unsupported connection type '%s' for data source %s", dataSource.Type, brokerName)
	}
}

// -----------------------------------------------------------------------------

// CreateBrokerWithConnection creates both broker and connection client
func (mdsf *MarketDataSourceFactory) CreateBrokerWithConnection(brokerName string) (interfaces.IBroker, interfaces.IConnectionClient, error) {
	broker, err := mdsf.CreateBroker(brokerName)
	if err != nil {
		return nil, nil, err
	}

	connection, err := mdsf.CreateConnectionClient(brokerName)
	if err != nil {
		return nil, nil, err
	}

	return broker, connection, nil
}

// -----------------------------------------------------------------------------

// CreateAllBrokers creates all brokers from configuration
func (mdsf *MarketDataSourceFactory) CreateAllBrokers() (map[string]interfaces.IBroker, error) {
	brokers := make(map[string]interfaces.IBroker)

	for _, dataSource := range mdsf.Config.DataSources {
		broker, err := mdsf.CreateBroker(dataSource.Name)
		if err != nil {
			mdsf.Logger.Error("%s : failed to create broker %s: %v", mdsf.Name, dataSource.Name, err)
			continue
		}
		brokers[dataSource.Name] = broker
	}

	return brokers, nil
}

// -----------------------------------------------------------------------------

// CreateAllConnections creates all connection clients from configuration
func (mdsf *MarketDataSourceFactory) CreateAllConnections() (map[string]interfaces.IConnectionClient, error) {
	connections := make(map[string]interfaces.IConnectionClient)

	for _, dataSource := range mdsf.Config.DataSources {
		connection, err := mdsf.CreateConnectionClient(dataSource.Name)
		if err != nil {
			mdsf.Logger.Error("%s : failed to create connection for %s: %v", mdsf.Name, dataSource.Name, err)
			continue
		}
		connections[dataSource.Name] = connection
	}

	return connections, nil
}

// -----------------------------------------------------------------------------

// CreateAllBrokersWithConnections creates all brokers with their connections
func (mdsf *MarketDataSourceFactory) CreateAllBrokersWithConnections() (map[string]interfaces.IBroker, map[string]interfaces.IConnectionClient, error) {
	brokers := make(map[string]interfaces.IBroker)
	connections := make(map[string]interfaces.IConnectionClient)

	for _, dataSource := range mdsf.Config.DataSources {
		broker, connection, err := mdsf.CreateBrokerWithConnection(dataSource.Name)
		if err != nil {
			mdsf.Logger.Error("%s : failed to create broker with connection for %s: %v", mdsf.Name, dataSource.Name, err)
			continue
		}
		brokers[dataSource.Name] = broker
		connections[dataSource.Name] = connection
	}

	return brokers, connections, nil
}
