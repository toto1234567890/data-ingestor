package interfaces

import "trading-system/src/models"

// -----------------------------------------------------------------------------

// DataSource defines the interface for managing a single data stream
type IDataSource interface {
	GetName() string
	Start() error
	Stop() error
	Subscribe(symbols []string) error
	UnSubscribe(symbols []string) error
	GetStatus() *models.MDataSourceStatus
}
