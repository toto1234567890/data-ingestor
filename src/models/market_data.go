package models

import (
	"time"
)

// -----------------------------------------------------------------------------

// MMarketData represents standardized market data from any source.
// This single struct is designed to handle multiple data types (Trade, Quote, OrderBook)
// by utilizing omitempty on type-specific fields.
type MMarketData struct {
	// Common Fields
	Symbol    string    `json:"symbol"`
	Timestamp time.Time `json:"timestamp"`
	Exchange  string    `json:"exchange"`
	Source    string    `json:"source"`
	DataType  MDataType `json:"data_type"`

	// Trade Fields (Used when DataType == DataTypeTrade)
	Price  float64 `json:"price,omitempty"`  // Price of the trade
	Volume float64 `json:"volume,omitempty"` // Volume of the trade

	// Quote Fields (Used when DataType == DataTypeQuote - for Bid/Ask)
	BidPrice float64 `json:"bid_price,omitempty"`
	AskPrice float64 `json:"ask_price,omitempty"`
	BidSize  float64 `json:"bid_size,omitempty"`
	AskSize  float64 `json:"ask_size,omitempty"`

	// Order Book Snapshot/Update (Used when DataType == DataTypeOrderBook)
	// Points to a separate struct for complex L2/L3 data
	OrderBook *MOrderBook `json:"order_book,omitempty"`
}

// -----------------------------------------------------------------------------

// MOrderBook represents a simplified snapshot or update of the order book (L2/L3 data)
type MOrderBook struct {
	// Bids and Asks are defined as arrays of [Price, Quantity] pairs for simplicity in serialization
	// e.g., [[100.50, 10], [100.49, 5]]
	Bids [][2]float64 `json:"bids"`
	Asks [][2]float64 `json:"asks"`
	// Additional fields like UpdateID or SequenceNumber might be added here for tracking
}

// -----------------------------------------------------------------------------

// DataType defines the type of market data
type MDataType string

const (
	DataTypeTrade     MDataType = "TRADE"
	DataTypeQuote     MDataType = "QUOTE"     // For single best bid/ask (Top of Book)
	DataTypeOrderBook MDataType = "ORDERBOOK" // For full Level 2 or Level 3 data
)

// -----------------------------------------------------------------------------

// SubscriptionMessage represents a subscription request
type MSubscriptionMessage struct {
	Symbols  []string `json:"symbols"`
	Channels []string `json:"channels"`
	Interval string   `json:"interval,omitempty"`
}
