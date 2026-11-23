package kafkaclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Publisher borrows a Client. Its owner is responsible for closing that client.
// It has no consumer state and starts no additional goroutines.
type Publisher struct {
	client *Client
}

func NewPublisher(client *Client) (*Publisher, error) {
	if client == nil {
		return nil, errors.New("Kafka client is nil")
	}
	return &Publisher{client: client}, nil
}

// Publish serializes one message as JSON and waits for delivery to the supplied topic.
func (p *Publisher) Publish(ctx context.Context, topic string, message any) error {
	if strings.TrimSpace(topic) == "" {
		return errors.New("publisher topic is required")
	}
	data, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("marshal kafka message: %w", err)
	}

	return p.client.kafka.ProduceSync(ctx, &kgo.Record{
		Topic: topic,
		Value: data,
	}).FirstErr()
}
