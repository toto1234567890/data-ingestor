package models

// -----------------------------------------------------------------------------

// DataSourceStatus represents the runtime status and technical metadata of a data stream.
// It aggregates information from the underlying broker and connection client.

type MDataSourceStatus struct {
	SourceName    string   // The name of the data source
	Running       bool     // From IConnectionClient.IsRunning()
	Type          string   // e.g., "Equity", "Crypto" (from IBroker.GetType())
	TransportType string   // e.g., "websocket", "REST" (from IConnectionClient.GetType())
	Endpoint      string   // e.g., "wss://stream.binance.com:9443/ws", "REST" (from IBroker.EndPoint)
	Symbols       []string // List of subscribed symbols
}
