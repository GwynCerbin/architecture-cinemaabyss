package kafkatopics

import (
	"time"

	"github.com/cenkalti/backoff/v4"
)

type Config struct {
	Workers                    int           `env:"WORKERS"`
	DefaultBufferSize          int           `env:"DEFAULT_BUFFER_SIZE"`
	MaxMessageBytes            int64         `env:"MAX_MESSAGE_BYTES"`
	BackoffInitialInterval     time.Duration `env:"BACKOFF_INITIAL_INTERVAL"`
	BackoffMaxInterval         time.Duration `env:"BACKOFF_MAX_INTERVAL"`
	BackoffMultiplier          float64       `env:"BACKOFF_MULTIPLIER"`
	BackoffRandomizationFactor float64       `env:"BACKOFF_RANDOMIZATION_FACTOR"`
}

func WithConfig(config Config) Option {
	return func(c *listenerConfig) {
		c.workers = config.Workers
		c.defaultBufferSize = config.DefaultBufferSize
		c.maxPayloadBytes = config.MaxMessageBytes
		c.backoff = backoff.ExponentialBackOff{
			MaxElapsedTime:      0, // Retry until the delivery context is canceled.
			Clock:               backoff.SystemClock,
			InitialInterval:     config.BackoffInitialInterval,
			MaxInterval:         config.BackoffMaxInterval,
			Multiplier:          config.BackoffMultiplier,
			RandomizationFactor: config.BackoffRandomizationFactor,
		}
	}
}

func DefaultConfig() Config {
	return Config{
		Workers:                    128,
		DefaultBufferSize:          1 << 10,
		MaxMessageBytes:            12 << 10,
		BackoffInitialInterval:     backoff.DefaultInitialInterval,
		BackoffMaxInterval:         backoff.DefaultMaxInterval,
		BackoffMultiplier:          backoff.DefaultMultiplier,
		BackoffRandomizationFactor: backoff.DefaultRandomizationFactor,
	}
}
