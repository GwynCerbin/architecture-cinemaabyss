package kafkatopics_test

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	kc "events/pkg/kafkaclient"
	kt "events/pkg/kafkalistener"
)

type source struct {
	messages   chan *kc.Message
	reads      atomic.Int32
	reading    atomic.Bool
	concurrent atomic.Bool
}

func (s *source) ReadMessage(ctx context.Context) (*kc.Message, error) {
	if !s.reading.CompareAndSwap(false, true) {
		s.concurrent.Store(true)
	}
	defer s.reading.Store(false)
	s.reads.Add(1)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case m, ok := <-s.messages:
		if !ok {
			return nil, io.EOF
		}
		return m, nil
	}
}
func message(ctx context.Context, topic string, offset int64, acked *atomic.Int32) *kc.Message {
	return &kc.Message{Record: &kgo.Record{Topic: topic, Offset: offset, Value: []byte("payload")}, Context: ctx, Commit: func(ctx context.Context) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		acked.Add(1)
		return nil
	}}
}
func listener(t *testing.T, s *source, r kt.Router, options ...kt.Option) *kt.Listener {
	t.Helper()
	cfg := kt.DefaultConfig()
	cfg.Workers = 2
	cfg.BackoffInitialInterval = time.Millisecond
	cfg.BackoffMaxInterval = 2 * time.Millisecond
	opts := append([]kt.Option{kt.WithConfig(cfg)}, options...)
	l, err := kt.NewListener(context.Background(), s, r, opts...)
	require.NoError(t, err)
	return l
}

func TestListenerSingleReaderRoutesTopicsAndBoundsWorkers(t *testing.T) {
	s := &source{messages: make(chan *kc.Message, 6)}
	var acked, active, maxActive, a, b atomic.Int32
	for i := range 6 {
		topic := "a"
		if i%2 == 1 {
			topic = "b"
		}
		s.messages <- message(context.Background(), topic, int64(i), &acked)
	}
	close(s.messages)
	gate := make(chan struct{})
	r := kt.NewRouter()
	handler := func(count *atomic.Int32) kt.ProcessEvent {
		return func(ctx *kt.Context) error {
			count.Add(1)
			n := active.Add(1)
			defer active.Add(-1)
			for {
				old := maxActive.Load()
				if old >= n || maxActive.CompareAndSwap(old, n) {
					break
				}
			}
			<-gate
			ctx.Ack()
			return nil
		}
	}
	r.Add("a", handler(&a))
	r.Add("b", handler(&b))
	l := listener(t, s, r)
	done := make(chan error, 1)
	go func() { done <- l.ListenAndServe() }()
	require.Eventually(t, func() bool { return active.Load() == 2 }, time.Second, time.Millisecond)
	require.EqualValues(t, 2, s.reads.Load(), "reserve workers before reading")
	close(gate)
	require.ErrorIs(t, <-done, io.EOF)
	require.NoError(t, l.Shutdown(context.Background()))
	require.False(t, s.concurrent.Load())
	require.EqualValues(t, 2, maxActive.Load())
	require.EqualValues(t, 3, a.Load())
	require.EqualValues(t, 3, b.Load())
	require.EqualValues(t, 6, acked.Load())
}

type dlq func(context.Context, kt.DeadLetter) error

func (f dlq) Write(ctx context.Context, d kt.DeadLetter) error { return f(ctx, d) }

func TestListenerRetryAndDLQPrecedeLocalACK(t *testing.T) {
	s := &source{messages: make(chan *kc.Message, 1)}
	var acked, attempts, writes atomic.Int32
	s.messages <- message(context.Background(), "a", 0, &acked)
	close(s.messages)
	r := kt.NewRouter()
	r.Add("a", func(ctx *kt.Context) error {
		attempts.Add(1)
		require.Equal(t, "payload", string(ctx.Bytes()))
		ctx.Reject()
		return errors.New("poison")
	}, kt.WithDeadLetter())
	writer := dlq(func(ctx context.Context, d kt.DeadLetter) error {
		require.Zero(t, acked.Load())
		if writes.Add(1) == 1 {
			return errors.New("DLQ temporarily unavailable")
		}
		require.Equal(t, "a", d.Topic)
		return nil
	})
	l := listener(t, s, r, kt.WithDeadLetterWriter(writer))
	require.ErrorIs(t, l.ListenAndServe(), io.EOF)
	require.NoError(t, l.Shutdown(context.Background()))
	require.EqualValues(t, 2, attempts.Load())
	require.EqualValues(t, 2, writes.Load())
	require.EqualValues(t, 1, acked.Load())
}

func TestListenerAssignmentCancellationStopsRetryWithoutACK(t *testing.T) {
	s := &source{messages: make(chan *kc.Message, 1)}
	session, cancel := context.WithCancel(context.Background())
	defer cancel()
	var acked, attempts atomic.Int32
	s.messages <- message(session, "a", 0, &acked)
	close(s.messages)
	r := kt.NewRouter()
	r.Add("a", func(ctx *kt.Context) error {
		attempts.Add(1)
		cancel()
		<-ctx.Done()
		ctx.Nack()
		return ctx.Err()
	})
	l := listener(t, s, r)
	require.ErrorIs(t, l.ListenAndServe(), io.EOF)
	require.NoError(t, l.Shutdown(context.Background()))
	require.EqualValues(t, 1, attempts.Load())
	require.Zero(t, acked.Load())
}

func TestListenerShutdownCancelsHandlerAndUnblocksRead(t *testing.T) {
	s := &source{messages: make(chan *kc.Message, 1)}
	var acked atomic.Int32
	s.messages <- message(context.Background(), "a", 0, &acked)
	started := make(chan struct{})
	r := kt.NewRouter()
	r.Add("a", func(ctx *kt.Context) error { close(started); <-ctx.Done(); return ctx.Err() })
	l := listener(t, s, r)
	done := make(chan error, 1)
	go func() { done <- l.ListenAndServe() }()
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, l.Shutdown(ctx), context.DeadlineExceeded)
	require.ErrorIs(t, <-done, kt.ErrListenerClosed)
	require.Zero(t, acked.Load())
}
