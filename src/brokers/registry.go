package brokers

import (
	"fmt"
	"sync"

	"data-ingestor/src/interfaces"
)

// The global registry map. Key is the broker name (e.g., "binance"), value is the constructor function.
var (
	registry = make(map[string]interfaces.IBrokerConstructor)
	mu       sync.RWMutex // Use a mutex for concurrent map access
)

// Register is called by each broker's init() function to add itself to the map.
func Register(name string, constructor interfaces.IBrokerConstructor) error {
	mu.Lock()
	defer mu.Unlock()
	if _, exists := registry[name]; exists {
		return fmt.Errorf("broker constructor already registered for name: %s", name)
	}
	registry[name] = constructor
	return nil
}

// GetConstructor is used by the BrokerFactory to retrieve the constructor.
func GetConstructor(name string) (interfaces.IBrokerConstructor, error) {
	mu.RLock()
	defer mu.RUnlock()
	constructor, exists := registry[name]
	if !exists {
		return nil, fmt.Errorf("unknown broker type: %s", name)
	}
	return constructor, nil
}
