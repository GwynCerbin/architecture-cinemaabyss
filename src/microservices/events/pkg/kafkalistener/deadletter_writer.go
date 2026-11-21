package kafkatopics

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaDeadLetterWriter uses a caller-owned producer. Configure RequireAllISRAcks
// (franz-go's default). Source commit and DLQ publish are not atomic.
type KafkaDeadLetterWriter struct {
	client *kgo.Client
	topic  string
}

func NewKafkaDeadLetterWriter(client *kgo.Client, topic string) (*KafkaDeadLetterWriter, error) {
	if client == nil || topic == "" {
		return nil, errors.New("DLQ client and topic are required")
	}
	return &KafkaDeadLetterWriter{client: client, topic: topic}, nil
}

func (w *KafkaDeadLetterWriter) Write(ctx context.Context, record DeadLetter) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	key, err := json.Marshal([]any{record.Topic, record.PartitionID, record.Offset})
	if err != nil {
		return err
	}
	return w.client.ProduceSync(ctx, &kgo.Record{Topic: w.topic, Key: key, Value: data}).FirstErr()
}

// Close does not close the shared producer.
func (w *KafkaDeadLetterWriter) Close(context.Context) error { return nil }
