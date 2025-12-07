package config

import (
	"fmt"
	"os"

	"data-ingestor/src/models"

	"gopkg.in/yaml.v3"
)

// -----------------------------------------------------------------------------

// Config wraps models.Config and provides business logic methods
type Config struct {
	*models.MConfig
}

// -----------------------------------------------------------------------------

// NewConfig creates a new Config instance from YAML file
func NewConfig(configPath string) (*Config, error) {
	// 1. Read the YAML file content
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file '%s': %w", configPath, err)
	}

	// 2. Unmarshal data into the models struct
	var modelConfig models.MConfig
	if err := yaml.Unmarshal(data, &modelConfig); err != nil {
		return nil, fmt.Errorf("failed to parse config from YAML: %w", err)
	}

	config := &Config{MConfig: &modelConfig}

	// 3. Validate the loaded configuration
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return config, nil
}

// -----------------------------------------------------------------------------

// Validate performs basic configuration validation and checks NATS/DataSources sub-configs.
func (c *Config) Validate() error {
	if c.Name == "" {
		return fmt.Errorf("config name cannot be empty")
	}

	// Validate Application Ports (using c.Port directly due to embedding)
	if c.Port <= 1024 || c.Port > 65535 {
		return fmt.Errorf("invalid application port number: %d (must be between 1025 and 65535)", c.Port)
	}
	if c.GRPC_Port <= 1024 || c.GRPC_Port > 65535 {
		return fmt.Errorf("invalid gRPC port number: %d (must be between 1025 and 65535)", c.GRPC_Port)
	}

	// Validate Data Sources
	if len(c.DataSources) == 0 {
		return fmt.Errorf("at least one data source must be configured")
	}
	for i, source := range c.DataSources {
		if source.Name == "" {
			return fmt.Errorf("data source %d: name cannot be empty", i)
		}
		if source.Endpoint == "" {
			return fmt.Errorf("data source '%s': endpoint cannot be empty", source.Name)
		}
		if len(source.Symbols) == 0 {
			return fmt.Errorf("data source '%s': symbols list cannot be empty", source.Name)
		}
	}

	// Validation of NATS config (minimal check)
	if len(c.NATS.Servers) == 0 {
		return fmt.Errorf("NATS servers list cannot be empty")
	}

	return nil
}

// -----------------------------------------------------------------------------

// GetDataSourceByName returns a single data source by name
func (c *Config) GetDataSourceByName(name string) *models.MDataSourceConfig {
	for _, source := range c.DataSources {
		if source.Name == name {
			return source
		}
	}
	return nil
}

// -----------------------------------------------------------------------------

// GetDataSourceByType returns data source configurations by type
func (c *Config) GetDataSourceByType(sourceType string) []models.MDataSourceConfig {
	var result []models.MDataSourceConfig
	for _, source := range c.DataSources {
		if source.Type == sourceType {
			result = append(result, *source)
		}
	}
	return result
}
