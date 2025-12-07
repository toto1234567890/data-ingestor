package interfaces

// -----------------------------------------------------------------------------

// ISerializer defines the contract for marshaling and unmarshaling data.
// This interface allows the NATS client to be agnostic about the actual format (JSON, Capnp, etc.).
type ISerializer interface {
	// Marshal converts a Go object (struct) into a byte slice.
	Marshal(obj interface{}) ([]byte, error)

	// Unmarshal converts a byte slice back into a Go object.
	Unmarshal(data []byte, obj interface{}) error
}
