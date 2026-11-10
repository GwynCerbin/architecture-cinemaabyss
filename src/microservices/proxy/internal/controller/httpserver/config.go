package httpserver

import "time"

const fallbackShutdownTimeout = 5 * time.Second

type Config struct {
	Address         string        `env:"SERVER_ADDRESS" yaml:"address"`
	ShutdownTimeout time.Duration `env:"SHUTDOWN_TIMEOUT" yaml:"shutdown_timeout"`
}
