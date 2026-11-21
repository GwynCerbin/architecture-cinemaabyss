// Package kafkaclient provides a Kafka client, publisher, and ACK-tracking consumer.
package kafkaclient

import (
	"errors"
	"github.com/twmb/franz-go/pkg/kgo"
	"strings"
)

// Client owns the Kafka transport, without consumer-specific state.
type Client struct {
	kafka *kgo.Client
}

func NewClient(config ClientConfig) (*Client, error) {
	return newClient(config)
}

func newClient(config ClientConfig, options ...kgo.Opt) (*Client, error) {
	if len(config.Brokers) == 0 {
		return nil, errors.New("Kafka brokers are required")
	}
	for _, broker := range config.Brokers {
		if strings.TrimSpace(broker) == "" {
			return nil, errors.New("empty Kafka broker")
		}
	}
	opts := append([]kgo.Opt(nil), options...)
	opts = append(opts, kgo.SeedBrokers(config.Brokers...))
	if config.ClientID != "" {
		opts = append(opts, kgo.ClientID(config.ClientID))
	}

	kafka, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &Client{kafka: kafka}, nil
}

// Close closes a standalone client after all publishers have finished.
// For a consumer-owned client, call Consumer.Close instead.
func (c *Client) Close() error {
	c.kafka.Close()
	return nil
}
