package kafkatopics_test

import (
	"context"
	"testing"
	"time"

	kc "events/pkg/kafkaclient"
	kt "events/pkg/kafkalistener"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestKafkaPartitionLifecycleCancelsRunningHandlers(t *testing.T) {
	for _, event := range []string{"revoked", "lost"} {
		t.Run(event, func(t *testing.T) {
			fake, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(2, "a"), kfake.GroupMinSessionTimeout(time.Second))
			require.NoError(t, err)
			defer fake.Close()
			producer, err := kgo.NewClient(kgo.SeedBrokers(fake.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
			require.NoError(t, err)
			defer producer.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			for id, key := range []string{"zero", "one"} {
				require.NoError(t, producer.ProduceSync(ctx, &kgo.Record{Topic: "a", Partition: int32(id), Value: []byte(key)}).FirstErr())
			}
			cfg := kc.DefaultConsumerConfig()
			cfg.Brokers, cfg.Group, cfg.Topics = fake.ListenAddrs(), "worker-"+event, []string{"a"}
			cfg.PartitionBuffer = 1
			opts := []kgo.Opt{kgo.Balancers(kgo.CooperativeStickyBalancer()), kgo.SessionTimeout(3 * time.Second), kgo.HeartbeatInterval(100 * time.Millisecond), kgo.RebalanceTimeout(3 * time.Second)}
			first, err := kc.NewConsumer(cfg, opts...)
			require.NoError(t, err)
			defer first.Close()
			type invocation struct {
				key string
				ctx context.Context
			}
			started, exited := make(chan invocation, 8), make(chan invocation, 8)
			router := kt.NewRouter()
			router.Add("a", func(delivery *kt.Context) error {
				call := invocation{key: string(delivery.Bytes()), ctx: delivery.Context}
				started <- call
				defer func() { exited <- call }()
				<-delivery.Done()
				return delivery.Err()
			})
			listener, err := kt.NewListener(ctx, first, router, kt.WithWorkers(4))
			require.NoError(t, err)
			done := make(chan error, 1)
			stopped := false
			go func() { done <- listener.ListenAndServe() }()
			defer func() {
				// Force cancellation of deliberately blocked handlers during cleanup.
				shutdownCtx, stop := context.WithCancel(context.Background())
				stop()
				shutdownErr := listener.Shutdown(shutdownCtx)
				require.ErrorIs(t, shutdownErr, context.Canceled)
				if stopped {
					return
				}
				select {
				case err := <-done:
					if event == "lost" {
						require.ErrorIs(t, err, kerr.UnknownMemberID)
					} else {
						require.ErrorIs(t, err, kt.ErrListenerClosed)
					}
				case <-time.After(3 * time.Second):
					t.Error("listener did not stop")
				}
			}()
			next := func(ch <-chan invocation) invocation {
				t.Helper()
				select {
				case call := <-ch:
					return call
				case <-ctx.Done():
					t.Fatal(ctx.Err())
					return invocation{}
				}
			}
			original := make(map[string]context.Context)
			for range 2 {
				call := next(started)
				original[call.key] = call.ctx
			}
			require.Len(t, original, 2)
			canceled := make(map[string]context.Context)
			if event == "revoked" {
				second, err := kc.NewConsumer(cfg, opts...)
				require.NoError(t, err)
				defer second.Close()
				message, err := second.ReadMessage(ctx)
				require.NoError(t, err)
				key := string(message.Record.Value)
				call := next(exited)
				require.Equal(t, key, call.key)
				require.Same(t, original[key], call.ctx)
				require.ErrorIs(t, call.ctx.Err(), context.Canceled)
				canceled[key] = call.ctx
				for other, old := range original {
					if other != key {
						require.NoError(t, old.Err(), "retained partition's worker must continue")
					}
				}
				// Return the partition without ACK: the same listener must process its replay.
				require.NoError(t, second.Close())
			} else {
				fake.ControlKey(kmsg.NewPtrHeartbeatRequest().Key(), func(req kmsg.Request) (kmsg.Response, error, bool) {
					res := req.ResponseKind().(*kmsg.HeartbeatResponse)
					res.ErrorCode = 25
					return res, nil, true
				})
				for range 2 {
					call := next(exited)
					require.Same(t, original[call.key], call.ctx)
					require.ErrorIs(t, call.ctx.Err(), context.Canceled)
					canceled[call.key] = call.ctx
				}
			}
			// Fatal fetch errors terminate this listener by its existing contract.
			// Worker cancellation is still required; automatic restart is not implemented.
			if event == "lost" {
				select {
				case err := <-done:
					stopped = true
					require.ErrorIs(t, err, kerr.UnknownMemberID)
				case <-ctx.Done():
					t.Fatal("listener did not report fatal session error")
				}
				return
			}
			for range len(canceled) {
				call := next(started)
				require.Contains(t, canceled, call.key)
				require.NotSame(t, canceled[call.key], call.ctx)
				require.NoError(t, call.ctx.Err(), "new assignment must supply a live worker context")
			}
			for key, old := range original {
				if _, ok := canceled[key]; !ok {
					require.NoError(t, old.Err(), "retained worker must survive reassignment too")
				}
			}
		})
	}
}
