package run

import logger "mouse/pkg/logger"

const (
	// DefaultBindAddress is the default address for various server.
	DefaultBindAddress = "127.0.0.1:2080"
)

// Config represents the configuration format for the moused binary.
type Config struct {
	BindAddress string        `toml:"bind-address"`
	Logging     logger.Config `toml:"logging"`
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
