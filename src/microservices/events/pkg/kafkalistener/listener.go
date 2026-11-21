package kafkatopics

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"sync"
	"sync/atomic"

	"events/pkg/kafkaclient"
	"events/pkg/kafkalistener/workerpool"
)

// ProcessEvent runs synchronously. Its error is saved in Context without stopping
// subsequent handlers. An explicit decision overrides business errors. Processing
// and offset commits are not atomic; handlers must implement durable idempotency.
type ProcessEvent func(*Context) error

// MessageReader is injected by the caller. One listener calls ReadMessage
// sequentially for all routed topics. The caller closes the client AFTER Shutdown.
// A message must carry its assignment context and gap-safe local commit callback.
type MessageReader interface {
	ReadMessage(context.Context) (*kafkaclient.Message, error)
}

type Listener struct {
	source         MessageReader
	handlers       map[string]*topicHandler
	maxPayload     int64
	payloadPool    *sync.Pool
	releasePayload func([]byte)
	readCtx        context.Context
	stopReads      context.CancelFunc
	workCtx        context.Context
	cancelWork     context.CancelFunc
	slots          chan struct{}
	tasks          chan func()
	pool           *workerpool.Pool
	started        atomic.Bool
	err            atomic.Pointer[error]
}

// NewListener builds handlers around an injected client, without opening Kafka
// connections. It uses one read loop regardless of the number of routes.
func NewListener(ctx context.Context, client MessageReader, router Router, opts ...Option) (*Listener, error) {
	if client == nil {
		return nil, errors.New("Kafka client is nil")
	}

	if len(router) == 0 {
		return nil, errors.New("listener router is empty")
	}

	config := listenerConfig{}

	WithConfig(DefaultConfig())(&config)

	for _, opt := range opts {
		opt(&config)
	}

	if config.workers <= 0 {
		return nil, errors.New("worker limit must be positive")
	}

	if config.maxPayloadBytes <= 0 {
		return nil, errors.New("maximum message size must be positive")
	}

	if config.defaultBufferSize <= 0 {
		return nil, errors.New("default buffer size must be positive")
	}

	if config.backoff.InitialInterval <= 0 || config.backoff.MaxInterval < config.backoff.InitialInterval ||
		math.IsNaN(config.backoff.Multiplier) || math.IsInf(config.backoff.Multiplier, 0) || config.backoff.Multiplier < 1 ||
		math.IsNaN(config.backoff.RandomizationFactor) || config.backoff.RandomizationFactor < 0 || config.backoff.RandomizationFactor > 1 {
		return nil, errors.New("invalid listener backoff")
	}

	names := make([]string, 0, len(router))

	for name, chain := range router {
		if name == "" {
			return nil, errors.New("listener topic path is empty")
		}

		if len(chain.handlers) == 0 {
			return nil, fmt.Errorf("handler chain for topic %q is empty", name)
		}

		if chain.deadLetter {
			local := config
			for _, opt := range chain.options {
				opt(&local)
			}

			if local.deadLetterWriter == nil {
				return nil, fmt.Errorf("topic %q: %w", name, ErrDeadLetterWriterMissing)
			}
		}

		for _, set := range chain.handlers {
			if set.handler == nil {
				return nil, fmt.Errorf("handler for topic %q is nil", name)
			}

			if set.timeout < 0 {
				return nil, fmt.Errorf("handler timeout for topic %q is negative", name)
			}
		}

		names = append(names, name)
	}

	sort.Strings(names)

	jobCtx := context.WithoutCancel(ctx)
	readCtx, stopReads := context.WithCancel(jobCtx)
	workCtx, cancelWork := context.WithCancel(jobCtx)
	slots := make(chan struct{}, config.workers)
	listener := &Listener{
		readCtx: readCtx, stopReads: stopReads, workCtx: workCtx, cancelWork: cancelWork,
		slots: slots, tasks: make(chan func(), 1), pool: workerpool.NewPool(slots),
		source: client, handlers: make(map[string]*topicHandler), maxPayload: config.maxPayloadBytes,
	}
	listener.payloadPool, listener.releasePayload = newPayloadBufferPool(config.defaultBufferSize)
	for _, name := range names {
		listener.handlers[name] = newTopicHandler(workCtx, name, router[name], config)
	}

	return listener, nil
}

// ListenAndServe starts the prepared pipeline once. The caller must call Shutdown
// after serving, including when a fatal error returns before accepted work drains.
func (l *Listener) ListenAndServe() error {
	if !l.started.CompareAndSwap(false, true) {
		return errors.New("listener already started")
	}

	done := make(chan struct{})

	go func() {
		l.serve()
		close(done)
	}()

	select {
	case <-l.readCtx.Done():
		if err := l.error(); err != nil {
			return err
		}

		// Ordinary Shutdown still waits for accepted work to finish.
		<-done
	case <-done:
	}

	if err := l.error(); err != nil {
		return err
	}

	if l.readCtx.Err() != nil {
		return ErrListenerClosed
	}

	return io.EOF
}

var ErrListenerClosed = errors.New("listener is closed")

// Readers and workers record failures before releasing their reserved capacity.
func (l *Listener) fail(err error) {
	if err == nil {
		return
	}

	for {
		previous := l.err.Load()
		next := err

		if previous != nil {
			next = errors.Join(*previous, err)
		}

		if l.err.CompareAndSwap(previous, &next) {
			break
		}
	}

	l.stopReads()
}

func (l *Listener) error() error {
	if err := l.err.Load(); err != nil {
		return *err
	}

	return nil
}

// Shutdown stops the reader and drains accepted work. It does not close the client. On expiry it
// cancels the handlers' context, but still waits for all tasks and cleanup.
// Call Shutdown once; an unstarted listener only needs its resources closed.
func (l *Listener) Shutdown(ctx context.Context) error {
	l.stopReads()

	if l.started.Load() {
		done := l.pool.Wait()
		select {
		case <-done:
		case <-ctx.Done():
			l.cancelWork()
		}
		<-done
		// No reserved deliveries remain; wait for the last reader to close tasks.
		for range l.tasks {
		}
	}

	l.cancelWork()

	return errors.Join(ctx.Err(), l.error())
}
