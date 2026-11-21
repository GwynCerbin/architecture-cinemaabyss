package main

import (
	"events/internal/controller/httpserver"
	"events/internal/controller/listener"
	"events/internal/infra/eventProvider"
	"events/pkg/kafkaclient"
	kafkatopics "events/pkg/kafkalistener"
	"time"
)

type Config struct {
	HTTPServer httpserver.Config        `envPrefix:"HTTP_SERVER_"`
	Kafka      kafkaclient.ClientConfig `envPrefix:"KAFKA_"`
	Consumer   ConsumerConfig           `envPrefix:"KAFKA_CONSUMER_"`
	Publisher  eventProvider.Config     `envPrefix:"EVENTS_"`
	Listener   listener.Config          `envPrefix:"KAFKA_LISTENER_"`
	Worker     kafkatopics.Config       `envPrefix:"KAFKA_WORKER_"`
}

type ConsumerConfig struct {
	Group           string        `env:"GROUP" envDefault:"events-worker"`
	PartitionBuffer int           `env:"PARTITION_BUFFER" envDefault:"1024"`
	CommitInterval  time.Duration `env:"COMMIT_INTERVAL" envDefault:"100ms"`
	CommitTimeout   time.Duration `env:"COMMIT_TIMEOUT" envDefault:"5s"`
}

func (c Config) consumerConfig() kafkaclient.ConsumerConfig {
	return kafkaclient.ConsumerConfig{
		ClientConfig:    c.Kafka,
		Group:           c.Consumer.Group,
		Topics:          []string{c.Publisher.MovieTopic, c.Publisher.UserTopic, c.Publisher.PaymentTopic},
		PartitionBuffer: c.Consumer.PartitionBuffer,
		CommitInterval:  c.Consumer.CommitInterval,
		CommitTimeout:   c.Consumer.CommitTimeout,
	}
}
