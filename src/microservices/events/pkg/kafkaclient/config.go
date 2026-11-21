package kafkaclient

import "time"

type ClientConfig struct {
	Brokers  []string `env:"BROKERS,required"`
	ClientID string   `env:"CLIENT_ID"`
}

type ConsumerConfig struct {
	ClientConfig
	Group           string        `env:"GROUP,required"`
	Topics          []string      `env:"TOPICS,required"`
	PartitionBuffer int           `env:"PARTITION_BUFFER" envDefault:"1024"`
	CommitInterval  time.Duration `env:"COMMIT_INTERVAL" envDefault:"100ms"`
	CommitTimeout   time.Duration `env:"COMMIT_TIMEOUT" envDefault:"5s"`
}

func DefaultConsumerConfig() ConsumerConfig {
	return ConsumerConfig{PartitionBuffer: 1024, CommitInterval: 100 * time.Millisecond, CommitTimeout: 5 * time.Second}
}
