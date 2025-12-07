package brokers

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"data-ingestor/src/config"
	"data-ingestor/src/interfaces"
	"data-ingestor/src/logger"
	"data-ingestor/src/models"
	"data-ingestor/src/serializers"
	"data-ingestor/src/utils"
)

// -----------------------------------------------------------------------------
// STRUCT DEFINITION
// -----------------------------------------------------------------------------

// Binance implements interfaces.IBroker for Binance exchange
type Binance struct {
	Name       string
	Logger     *logger.Logger
	Config     *models.MDataSourceConfig
	Symbols    map[string]bool
	Serializer interfaces.ISerializer
}

// -----------------------------------------------------------------------------
// CONSTRUCTOR AND REGISTRATION
// -----------------------------------------------------------------------------

func init() {
	// Register the broker with the name "binance" for dynamic creation
	if err := Register("binance", NewBinance); err != nil {
		fmt.Printf("Error registering Binance broker: %v\n", err)
	}
}

// -----------------------------------------------------------------------------

// NewBinance creates a new Binance broker instance.
// Matches the interfaces.IBrokerConstructor signature: (config, logger, name) -> (IBroker, error)
func NewBinance(config *config.Config, logger *logger.Logger, name string) (interfaces.IBroker, error) {
	brokerName := name
	binanceDataSource := config.GetDataSourceByName(brokerName)

	if binanceDataSource == nil {
		logger.Warning("%s : Binance config not found; returning error", brokerName)
		return nil, fmt.Errorf("data source config '%s' not found", brokerName)
	}

	return &Binance{
		Name:       brokerName,
		Logger:     logger,
		Config:     binanceDataSource,
		Symbols:    make(map[string]bool),
		Serializer: serializers.NewJSONSerializer(),
	}, nil
}

// -----------------------------------------------------------------------------
// IBroker IMPLEMENTATION
// -----------------------------------------------------------------------------

// GetName returns the broker name
func (b *Binance) GetName() string {
	return b.Name
}

// -----------------------------------------------------------------------------

// GetType returns the asset type (e.g., "crypto")
func (b *Binance) GetType() string {
	return b.Config.Type
}

// -----------------------------------------------------------------------------

// GetEndPoint returns the WebSocket endpoint URL
func (b *Binance) GetEndPoint() string {
	return b.Config.Endpoint
}

// -----------------------------------------------------------------------------

// GetEndPoint returns the WebSocket endpoint URL
// Binance public streams don't require authentication, so it's the same as GetEndPoint
func (b *Binance) GetEndpointWithCredentials() string {
	// no credentials required for binance
	return b.Config.Endpoint
}

// -----------------------------------------------------------------------------

// GetSymbols returns the list of configured trading symbols
func (b *Binance) GetSymbols() []string {
	return b.Config.Symbols
}

// -----------------------------------------------------------------------------

// AddSubscription creates the subscription message for multiple Binance WebSocket streams:
// - Trade stream: real-time trade executions
// - Book ticker stream: best bid/ask updates
// - Depth stream: order book delta updates at 100ms intervals
func (b *Binance) AddSubscription(symbols []string) ([]byte, error) {
	streams := make([]string, 0, len(symbols)*3)
	for _, symbol := range symbols {
		lowerSymbol := strings.ToLower(symbol)
		// 1. Trade Stream: individual trade executions
		streams = append(streams, lowerSymbol+"@trade")
		// 2. Book Ticker Stream: top-of-book bid/ask updates
		streams = append(streams, lowerSymbol+"@bookTicker")
		// 3. Depth Stream: order book differential updates (100ms frequency)
		streams = append(streams, lowerSymbol+"@depth@100ms")
	}

	// Mark symbols as subscribed
	for _, symbol := range symbols {
		b.Symbols[symbol] = true
	}

	// Serialize subscription message
	subMsg, err := b.Serializer.Marshal(map[string]interface{}{
		"method": "SUBSCRIBE",
		"params": streams,
	})
	if err != nil {
		b.Logger.Error("%s : failed to serialize subscription message for symbols %v: %v", b.Name, symbols, err)
		return nil, fmt.Errorf("failed to serialize subscription message: %w", err)
	}

	return subMsg, nil
}

// -----------------------------------------------------------------------------

// RemoveSubscription creates unsubscribe message for Binance WebSocket streams
func (b *Binance) RemoveSubscription(symbols []string) ([]byte, error) {
	streams := make([]string, 0, len(symbols)*3)
	for _, symbol := range symbols {
		lowerSymbol := strings.ToLower(symbol)
		// Unsubscribe from all three stream types
		streams = append(streams, lowerSymbol+"@trade")
		streams = append(streams, lowerSymbol+"@bookTicker")
		streams = append(streams, lowerSymbol+"@depth@100ms")
	}

	// Mark symbols as unsubscribed
	for _, symbol := range symbols {
		b.Symbols[symbol] = false
	}

	// Serialize unsubscription message
	unsubMsg, err := b.Serializer.Marshal(map[string]interface{}{
		"method": "UNSUBSCRIBE",
		"params": streams,
	})
	if err != nil {
		b.Logger.Error("%s : failed to serialize unsubscription message for symbols %v: %v", b.Name, symbols, err)
		return nil, fmt.Errorf("failed to serialize unsubscription message: %w", err)
	}

	return unsubMsg, nil
}

// -----------------------------------------------------------------------------

// ParseMessage processes incoming WebSocket messages from Binance, routing based on event type.
func (b *Binance) ParseMessage(message []byte) (*models.MMarketData, error) {
	var data map[string]interface{}
	if err := json.Unmarshal(message, &data); err != nil {
		b.Logger.Error("%s : failed to unmarshal message: %v (raw: %s)", b.Name, err, string(message))
		return nil, fmt.Errorf("failed to unmarshal message: %w", err)
	}

	// Skip subscription confirmations/pongs (messages with "result")
	if _, ok := data["result"]; ok {
		return nil, nil
	}

	// Check for event type "e"
	eventType, ok := data["e"].(string)
	if !ok {
		// If "e" is missing, check if it looks like a bookTicker (has "b" and "a" fields)
		if _, hasBid := data["b"]; hasBid {
			if _, hasAsk := data["a"]; hasAsk {
				return b.parseQuoteEvent(data)
			}
		}
		// Event type not found and doesn't look like bookTicker, ignore message
		return nil, nil
	}

	switch eventType {
	case "trade":
		return b.parseTradeEvent(data)
	case "bookTicker": // Handles single best bid/ask updates (Quote)
		return b.parseQuoteEvent(data)
	case "depthUpdate": // Handles level 2 order book updates
		return b.parseOrderBookEvent(data)
	default:
		// Ignore unknown event types
		return nil, nil
	}
}

// -----------------------------------------------------------------------------

// ValidateConfiguration validates Binance broker configuration
func (b *Binance) ValidateConfiguration() error {
	// Check if essential fields are set
	if b.Config.Endpoint == "" {
		return fmt.Errorf("binance endpoint cannot be empty")
	}

	// Binance-specific validation: enforce secure websocket protocol
	if !strings.HasPrefix(b.Config.Endpoint, "wss://") {
		return fmt.Errorf("binance endpoint must use wss:// protocol")
	}

	return nil
}

// -----------------------------------------------------------------------------
// PRIVATE METHODS
// -----------------------------------------------------------------------------

// parseTradeEvent extracts market data from Binance trade events.
// Trade events contain: symbol, price, quantity, and timestamp.
func (b *Binance) parseTradeEvent(data map[string]interface{}) (*models.MMarketData, error) {
	// getString safely extracts a string field from the event data
	getString := func(key string) (string, error) {
		val, ok := data[key].(string)
		if !ok {
			return "", fmt.Errorf("invalid or missing string field '%s'", key)
		}
		return val, nil
	}

	// getFloat safely extracts a float64 field from the event data (used for timestamps)
	getFloat := func(key string) (float64, error) {
		val, ok := data[key].(float64)
		if !ok {
			return 0, fmt.Errorf("invalid or missing float field '%s'", key)
		}
		return val, nil
	}

	symbol, err := getString("s")
	if err != nil {
		return nil, fmt.Errorf("parse trade event error (symbol): %w", err)
	}

	priceStr, err := getString("p")
	if err != nil {
		return nil, fmt.Errorf("parse trade event error (price): %w", err)
	}

	quantityStr, err := getString("q")
	if err != nil {
		return nil, fmt.Errorf("parse trade event error (quantity): %w", err)
	}

	// Timestamp 'T' is milliseconds, comes as float64 in unmarshalled JSON
	timestampFloat, err := getFloat("T")
	if err != nil {
		return nil, fmt.Errorf("parse trade event error (timestamp): %w", err)
	}

	// Convert millisecond timestamp (float64) to time.Time
	timestamp := time.Unix(0, int64(timestampFloat)*int64(time.Millisecond))

	return &models.MMarketData{
		Symbol:    strings.ToUpper(symbol),
		Timestamp: timestamp,
		Price:     utils.ParseFloat(priceStr),
		Volume:    utils.ParseFloat(quantityStr),
		Exchange:  b.Name,
		Source:    b.Name,
		DataType:  models.DataTypeTrade,
	}, nil
}

// -----------------------------------------------------------------------------

// parseQuoteEvent extracts market data from Binance book ticker events (best bid/ask).
// Book ticker events contain: symbol, best bid price/size, best ask price/size, and event time.
func (b *Binance) parseQuoteEvent(data map[string]interface{}) (*models.MMarketData, error) {
	// getString safely extracts a string field from the event data
	getString := func(key string) (string, error) {
		val, ok := data[key].(string)
		if !ok {
			return "", fmt.Errorf("invalid or missing string field '%s'", key)
		}
		return val, nil
	}

	// getFloat safely extracts a float64 field from the event data (used for event time)
	getFloat := func(key string) (float64, error) {
		val, ok := data[key].(float64)
		if !ok {
			return 0, fmt.Errorf("invalid or missing float field '%s'", key)
		}
		return val, nil
	}

	symbol, err := getString("s")
	if err != nil {
		return nil, fmt.Errorf("parse quote event error (symbol): %w", err)
	}

	bidPriceStr, err := getString("b")
	if err != nil {
		return nil, fmt.Errorf("parse quote event error (bid price): %w", err)
	}

	askPriceStr, err := getString("a")
	if err != nil {
		return nil, fmt.Errorf("parse quote event error (ask price): %w", err)
	}

	bidQtyStr, err := getString("B")
	if err != nil {
		return nil, fmt.Errorf("parse quote event error (bid size): %w", err)
	}

	askQtyStr, err := getString("A")
	if err != nil {
		return nil, fmt.Errorf("parse quote event error (ask size): %w", err)
	}

	// Event time 'E' is optional in bookTicker (sometimes missing)
	var timestamp time.Time
	if eventTimeFloat, err := getFloat("E"); err == nil {
		// Convert millisecond timestamp (float64) to time.Time
		timestamp = time.Unix(0, int64(eventTimeFloat)*int64(time.Millisecond))
	} else {
		// Fallback to current time if 'E' is missing
		timestamp = time.Now()
	}

	return &models.MMarketData{
		Symbol:    strings.ToUpper(symbol),
		Timestamp: timestamp,
		Exchange:  b.Name,
		Source:    b.Name,
		DataType:  models.DataTypeQuote,

		BidPrice: utils.ParseFloat(bidPriceStr),
		AskPrice: utils.ParseFloat(askPriceStr),
		BidSize:  utils.ParseFloat(bidQtyStr),
		AskSize:  utils.ParseFloat(askQtyStr),
	}, nil
}

// -----------------------------------------------------------------------------

// parseOrderBookEvent extracts market data from Binance depth update events.
// Depth updates contain: symbol, bid levels (price/quantity pairs), ask levels, and event time.
// These are differential updates, not full snapshots.
func (b *Binance) parseOrderBookEvent(data map[string]interface{}) (*models.MMarketData, error) {
	// getString safely extracts a string field from the event data
	getString := func(key string) (string, error) {
		val, ok := data[key].(string)
		if !ok {
			return "", fmt.Errorf("invalid or missing string field '%s'", key)
		}
		return val, nil
	}

	// getFloat safely extracts a float64 field from the event data (used for event time)
	getFloat := func(key string) (float64, error) {
		val, ok := data[key].(float64)
		if !ok {
			return 0, fmt.Errorf("invalid or missing float field '%s'", key)
		}
		return val, nil
	}

	// getLevels extracts and parses bid or ask price levels from the event data.
	// Each level is a [price, quantity] pair where both are strings in the JSON.
	getLevels := func(key string) ([][2]float64, error) {
		rawLevels, ok := data[key].([]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid or missing levels array field '%s'", key)
		}

		levels := make([][2]float64, len(rawLevels))
		for i, rawLevel := range rawLevels {
			level, ok := rawLevel.([]interface{})
			if !ok || len(level) < 2 {
				return nil, fmt.Errorf("invalid level format in array for '%s'", key)
			}

			// Extract price and quantity (both are strings in Binance JSON)
			priceStr, ok := level[0].(string)
			if !ok {
				return nil, fmt.Errorf("price is not string in level array for '%s'", key)
			}
			qtyStr, ok := level[1].(string)
			if !ok {
				return nil, fmt.Errorf("quantity is not string in level array for '%s'", key)
			}

			// Convert strings to float64
			levels[i][0] = utils.ParseFloat(priceStr)
			levels[i][1] = utils.ParseFloat(qtyStr)
		}
		return levels, nil
	}

	symbol, err := getString("s")
	if err != nil {
		return nil, fmt.Errorf("parse order book event error (symbol): %w", err)
	}

	bids, err := getLevels("b") // 'b' for bids
	if err != nil {
		return nil, fmt.Errorf("parse order book event error (bids): %w", err)
	}

	asks, err := getLevels("a") // 'a' for asks
	if err != nil {
		return nil, fmt.Errorf("parse order book event error (asks): %w", err)
	}

	// Event time 'E' is milliseconds
	eventTimeFloat, err := getFloat("E")
	if err != nil {
		return nil, fmt.Errorf("parse order book event error (event time): %w", err)
	}

	// Convert millisecond timestamp (float64) to time.Time
	timestamp := time.Unix(0, int64(eventTimeFloat)*int64(time.Millisecond))

	return &models.MMarketData{
		Symbol:    strings.ToUpper(symbol),
		Timestamp: timestamp,
		Exchange:  b.Name,
		Source:    b.Name,
		DataType:  models.DataTypeOrderBook,

		OrderBook: &models.MOrderBook{
			Bids: bids,
			Asks: asks,
		},
	}, nil
}
