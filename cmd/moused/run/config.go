package run

import (
	"mousedb/pkg/logger"
	"mousedb/service/storage"
)

const (
	// DefaultBindAddress is the default address for various server.
	DefaultBindAddress = "127.0.0.1:8062"
)

// Config represents the configuration format for the moused binary.
type Config struct {
	BindAddress string `toml:"bind-address" json:"bind_address,omitempty"`

	Logging logger.Config `toml:"logging" json:"logging"`

	Storage storage.Config `toml:"storage"`
}

func (c *Config) Validate() error {

	if err := c.Storage.Validate(); err != nil {
		return err
	}
	return nil
}

func NewConfig() *Config {

	c := &Config{}
	c.BindAddress = DefaultBindAddress
	c.Logging = logger.NewConfig()

	return c
}

func NewDefaultConfig() *Config {
	return NewConfig()
}
