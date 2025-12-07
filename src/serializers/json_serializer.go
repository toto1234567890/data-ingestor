package serializers

import (
	"encoding/json"
	"fmt"

	"trading-system/src/interfaces"
)

// -----------------------------------------------------------------------------

// JSONSerializer implements interfaces.ISerializer using Go's built-in JSON encoder.
type JSONSerializer struct{}

// -----------------------------------------------------------------------------

// NewJSONSerializer creates a new instance of the JSON serializer.
func NewJSONSerializer() interfaces.ISerializer {
	return &JSONSerializer{}
}

// -----------------------------------------------------------------------------

// Marshal converts the object to a JSON byte array.
func (j *JSONSerializer) Marshal(obj any) ([]byte, error) {
	data, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("json marshal error: %w", err)
	}
	return data, nil
}

// -----------------------------------------------------------------------------

// Unmarshal converts a JSON byte array back into the target object.
func (j *JSONSerializer) Unmarshal(data []byte, obj any) error {
	if err := json.Unmarshal(data, obj); err != nil {
		return fmt.Errorf("json unmarshal error: %w", err)
	}
	return nil
}
