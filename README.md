# 🚀 Market Data Ingestor

A production-grade, high-performance market data ingestion system written in Go. Connects to multiple financial exchanges simultaneously, normalizes real-time market data, and distributes it via NATS/JetStream (can be change easely) with comprehensive control through gRPC and REST APIs.

## ✨ Key Features

### 🔌 **Multi-Exchange Connectivity**
- **Binance Support**: Real-time WebSocket streams for crypto markets
- **Extensible Architecture**: Easily add new exchanges via the `IBroker` interface
- **Concurrent Connections**: Connect to multiple data sources simultaneously

### 📊 **Comprehensive Data Coverage**
- **Trade Executions**: Real-time trade data with price and volume
- **Best Bid/Ask Quotes**: Level 1 market data for top-of-book
- **Order Book Updates**: Level 2 market depth with configurable update frequency
- **Normalized Format**: All data converted to unified `MMarketData` structure

### 🏗️ **Modern Architecture**
- **Clean Interface Design**: Extensible via well-defined Go interfaces
- **Factory Pattern**: Dynamic broker and connection creation
- **Dependency Injection**: Loose coupling between components
- **Thread-Safe Operations**: Concurrent-safe data structures and operations

### 🚀 **High-Performance Processing**
- **Zero-Copy Parsing**: Efficient message processing with minimal allocations
- **Non-Blocking I/O**: Gorilla WebSocket for high-throughput connections
- **Buffered Channels**: Efficient message passing between goroutines
- **Configurable Buffer Sizes**: Optimize for your specific throughput needs

### 🔄 **Reliable Message Distribution**
- **NATS JetStream**: Persistent, fault-tolerant message streaming
- **Configurable Retention**: Control data retention policies (time, size, messages)
- **Guaranteed Delivery**: At-least-once message delivery semantics
- **Stream Replication**: Data durability with configurable replica counts

### 🎮 **Dual Control Interface**
- **gRPC Control Service**: High-performance internal control plane (port 50051)
- **REST API Gateway**: HTTP/JSON API for external integration (port 8080)
- **Web Control Panel**: Modern HTML/JS interface for real-time monitoring
- **Dynamic Management**: Add/remove brokers and symbols at runtime

### 🛡️ **Production-Ready Features**
- **Automatic Reconnection**: Configurable reconnection logic with backoff
- **Health Monitoring**: Comprehensive system health checks
- **Structured Logging**: Contextual logging with multiple severity levels
- **Configuration Validation**: YAML configuration with built-in validation
- **Graceful Shutdown**: Clean shutdown of all connections and goroutines

### 📈 **Real-Time Monitoring**
- **Live Status Dashboard**: Web interface showing all active connections
- **Performance Metrics**: Connection stats and message throughput
- **Error Tracking**: Real-time error reporting and alerting
- **Resource Monitoring**: Memory and goroutine usage tracking

## 🏗️ Architecture Overview
text
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ Data Sources    │───▶│ Ingestor        │───▶│ NATS/JetStream  │
│ (Binance, etc)  │    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
        │                      │
┌───────▼────────┐     ┌───────▼────────┐
│ gRPC Control   │     │ Consumers      │
│                │     │ (Strategies,   │
│                │     │ Databases)     │
└───────┬────────┘     └────────────────┘
        │
┌───────▼──────────┐
│ REST Gateway     │
│ + Web UI         │
└──────────────────┘
```


📦 Components
-------------

### Core Modules

*   **ingestor/**: Main ingestion engine managing data sources
    
*   **brokers/**: Exchange-specific implementations (Binance)
    
*   **transports/**: Connection handlers (WebSocket, planned: TCP, HTTP)
    
*   **publishers/**: NATS/JetStream integration for data distribution
    
*   **factories/**: Dynamic broker and connection creation
    
*   **interfaces/**: Core abstractions for extensibility

### Control & API

*   **grpc\_control/**: gRPC service for internal control
    
*   **rest/**: REST API gateway with web interface
    
*   **config/**: YAML configuration management
    
*   **models/**: Data structures and types
    
*   **serializers/**: JSON and binary serialization
    

### Utilities

*   **logger/**: Structured logging
    
*   **utils/**: Helper functions
    

🚀 Quick Start
--------------

### Prerequisites

*   Go 1.21+
    
*   NATS Server with JetStream enabled
    
*   Binance API access (for crypto data)
    

### Installation

1.  **Clone and build:**
    

bash

```   
git clone
cd data-ingestor
go mod download
go build -o data-ingestor ./src/main.go
```

1.  **Configure NATS:**
    

bash
```  
# Start NATS server with JetStream  
nats-server -js   
```

1.  **Edit configuration:
    **Update config/default.yaml with your settings:
    

yaml

```  
nats:    
  servers: ["nats://127.0.0.1:4222"]    
  jetstream:      
  enabled: true  
  data_sources:    
    - name: "binance"      
    type: "websocket"      
    endpoint: "wss://stream.binance.com:9443/ws"    
    symbols: ["btcusdt", "ethusdt"]   
```

1.  **Run the system:**
    

bash

```   
./data-ingestor --config config/default.yaml
```

The system will start:

*   Ingestion service on port 8080 (REST API + Web UI)
    
*   gRPC control service on port 50051
    
*   Web interface at [http://127.0.0.1:8080](http://127.0.0.1:8080/)

📖 Configuration
----------------

### Key Configuration Sections

yaml

```   
# Application
name: "MarketDataIngestor"
port: 8080
grpc_port: 50051

# NATS/JetStream
nats:
  servers: ["nats://127.0.0.1:4222"]
  jetstream:
      enabled: true
      stream_name: "MARKET_DATA"
      # 3-day retention
      max_age: "72h"
      storage: "file"

# Data Sources
data_sources:
  - name: "binance"
  type: "websocket"
  endpoint: "wss://stream.binance.com:9443/ws"
  symbols: ["btcusdt", "ethusdt", "solusdt"]
  connection_config:
    reconnect_attempts: 10
    reconnect_delay: "5s"
```

### Data Source Types

Currently supported:

*   **Binance**: WebSocket streams for trades, quotes, and order books
    
*   Extensible via IBroker interface


    

🔧 API Reference
----------------

### REST API Endpoints
text 
``` 
Endpoint                      Method     Description

/rest/health                  GET        System health check       
/rest/brokers/list            GET        List all brokers          
/rest/brokers/start           POST       Start a broker            
/rest/brokers/stop            POST       Stop a broker             
/rest/brokers/status          POST       Get broker status         
/rest/brokers/symbols/add     POST       Add symbols to broker     
/rest/brokers/symbols/remove  POST       Remove symbols from broker
/rest/datasource/add          POST       Add new data source        
/rest/datasource/remove       POST       Remove data source
``` 

### gRPC Service

Protocol buffers define the control service in grpc\_control/grpc\_control.proto:

*   ControlService: Broker and symbol management
    
*   Health checking and monitoring
    

### Web Interface

Access the control panel at http://127.0.0.1:8080:

*   Real-time broker status
    
*   Symbol management
    
*   System health monitoring
    
*   Interactive API testing
    

🧩 Extending the System
-----------------------

### Adding a New Broker

1.  Implement the IBroker interface:
    

go

```   
type MyBroker struct {
      // Broker implementation
}
func (b *MyBroker) ParseMessage(message []byte) (*models.MMarketData, error) {
  // Parse exchange-specific messages
}

func (b *MyBroker) AddSubscription(symbols []string) ([]byte, error) {
// Create subscription message
}
```

1.  Register the broker in brokers/ package:
    

go

```   
func init() {
  Register("mybroker", NewMyBroker)
}  
```

1.  Add to configuration (need borker implementation):
    

yaml

```   
data_sources:
  - name: "mybroker"
  type: "websocket"  # or other transport
  endpoint: "wss://myexchange.com/ws"
  symbols: ["SYMBOL1", "SYMBOL2"]
```

### Adding a New Transport

1.  Implement IConnectionClient interface in transports/ package
    
2.  Update factory to support new transport type
    

📊 Data Model
-------------

### Market Data Types

go

```   
type MMarketData struct {
  Symbol    string
  Timestamp time.Time
  Exchange  string
  Source    string
  DataType  MDataType
  // TRADE, QUOTE, or ORDERBOOK

  // Trade data
  Price  float64
  Volume float64

  // Quote data
  BidPrice float64
  AskPrice float64
  BidSize  float64
  AskSize  float64

  // Order book data
  OrderBook *MOrderBook
}
```

### NATS Subject Structure

text

```   
marketdata.{data_type}.{symbol}

Examples:
  marketdata.TRADE.BTCUSDT
  marketdata.QUOTE.ETHUSDT
   marketdata.ORDERBOOK.ADAUSDT
```

🛠️ Development
---------------

### Building from Source

bash

```   
# Install dependencies  go mod download
# Run tests
go test ./...
# Build
go build -o data-ingestor ./src/main.go
# Run with development config
./data-ingestor --config config/default.yaml   
```

### Project Structure

text

```   
data-ingestor/
├── src/
│
├── main.go                    # Entry point
│   ├── brokers/               # Exchange implementations
│   ├── config/                # Configuration management
│   ├── factories/             # Object factories
│   ├── grpc_control/          # gRPC control service
│   ├── ingestor/              # Core ingestion engine
│   ├── interfaces/            # Core abstractions
│   ├── logger/                # Logging utilities
│   ├── models/                # Data structures
│   ├── publishers/            # NATS publishing
│   ├── rest/                  # REST API
│   ├── serializers/           # Serialization
│   ├── transports/            # Transport implementations
│   └── utils/                 # Helper functions
├── config/
│   └── default.yaml          # Example configuration
├── static/                   # Web assets
└── testpage.html             # Web interface
```

🔍 Monitoring & Debugging
-------------------------

### Logging Levels

*   DEBUG: Detailed debugging information
    
*   INFO: Operational messages
    
*   WARNING: Non-critical issues
    
*   ERROR: Recoverable errors
    
*   CRITICAL: Unrecoverable errors
    

### Health Checks

bash

```   
# System health
curl http://127.0.0.1:8080/rest/health
# List running brokers
curl http://127.0.0.1:8080/rest/brokers/list
# Check specific broker
curl -X POST http://127.0.0.1:8080/rest/brokers/status -H "Content-Type: application/json" -d '{"broker_id": "binance"}'
```

### NATS Monitoring

bash

```   
# Check JetStream streams
  nats stream ls
# View stream info
  nats stream info MARKET_DATA
# Monitor messages
  nats consumer sub MARKET_DATA DATA_INGEST_CONSUMER
```
    

### Code Style

*   Follow Go standard formatting (gofmt)
    
*   Use interfaces for extensibility
    
*   Write unit tests for new functionality
    
*   Document public APIs and types
    
    

🚀 Roadmap
----------

*   Additional exchange implementations (Coinbase, Kraken, etc.)
    
*   More transport protocols (FIX, TCP, HTTP/2)
    
*   Data aggregation and OHLCV generation

*   Plug multiple realtime data analysis service to nats-server
    
*   Database integration (TimescaleDB, ClickHouse)
    
*   Prometheus metrics exporter + grafana
    

    
    

**Note**: This is a production-grade system for real-time market data processing. Ensure proper testing before deploying in trading environments.
