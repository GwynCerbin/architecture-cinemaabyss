package kafkatopics_test

import (
	"context"
	"testing"
	"time"

	kc "events/pkg/kafkaclient"
	kt "events/pkg/kafkalistener"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestKafkaInjectedClientRoutesAndCommitsAcrossTopics(t *testing.T) {
	fake, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "a", "b"), kfake.GroupMinSessionTimeout(time.Second))
	require.NoError(t, err)
	defer fake.Close()
	producer, err := kgo.NewClient(kgo.SeedBrokers(fake.ListenAddrs()...))
	require.NoError(t, err)
	defer producer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	require.NoError(t, producer.ProduceSync(ctx, &kgo.Record{Topic: "a", Value: []byte("a")}, &kgo.Record{Topic: "b", Value: []byte("b")}).FirstErr())
	config := kc.DefaultConsumerConfig()
	config.Brokers, config.Group, config.Topics = fake.ListenAddrs(), "listener", []string{"a", "b"}
	config.CommitInterval = 20 * time.Millisecond
	client, err := kc.NewConsumer(config)
	require.NoError(t, err)
	defer client.Close()
	seen := make(chan string, 2)
	router := kt.NewRouter()
	for _, topic := range config.Topics {
		router.Add(topic, func(delivery *kt.Context) error {
			require.Equal(t, topic, delivery.Topic())
			require.Equal(t, topic, string(delivery.Bytes()))
			seen <- topic
			delivery.Ack()
			return nil
		})
	}
	l, err := kt.NewListener(ctx, client, router, kt.WithWorkers(2))
	require.NoError(t, err)
	result := make(chan error, 1)
	go func() { result <- l.ListenAndServe() }()
	for range 2 {
		select {
		case <-seen:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	require.NoError(t, l.Shutdown(ctx))
	require.ErrorIs(t, <-result, kt.ErrListenerClosed)
	// Listener does not close the injected client: it can still flush and read.
	require.NoError(t, client.Flush(ctx))
	require.NoError(t, producer.ProduceSync(ctx, &kgo.Record{Topic: "b", Value: []byte("next")}).FirstErr())
	next, err := client.ReadMessage(ctx)
	require.NoError(t, err)
	require.Equal(t, "b", next.Record.Topic)
	require.EqualValues(t, 1, next.Record.Offset)
	require.NoError(t, next.Commit(ctx))
}
