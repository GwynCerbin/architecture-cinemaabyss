package httpserver

import "time"

const fallbackShutdownTimeout = 5 * time.Second

type Config struct {
	ShutdownTimeout time.Duration `env:"SHUTDOWN_TIMEOUT"`
	Port            int           `env:"PORT"`
}
