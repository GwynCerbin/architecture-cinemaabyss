package kafkatopics

import (
	"context"
	"errors"

	"github.com/cenkalti/backoff/v4"
)

type listenerConfig struct {
	workers           int
	defaultBufferSize int
	maxPayloadBytes   int64
	backoff           backoff.ExponentialBackOff
	deadLetterWriter  DeadLetterWriter
}

// Option configures resources allocated by NewListener.
type Option func(*listenerConfig)

// WithDeadLetterWriter supplies the writer without enabling DLQ on any route.
// The caller owns the writer and closes it after Shutdown.
func WithDeadLetterWriter(writer DeadLetterWriter) Option {
	return func(c *listenerConfig) { c.deadLetterWriter = writer }
}

func WithWorkers(limit int) Option { return func(c *listenerConfig) { c.workers = limit } }
func WithMaxMessageBytes(limit int64) Option {
	return func(c *listenerConfig) { c.maxPayloadBytes = limit }
}

func (l *Listener) serve() {
	go func() {
		l.read()
		close(l.tasks)
	}()
	for task := range l.tasks {
		l.pool.Start(task)
	}
	<-l.pool.Wait()
}

// A worker owns the entire chain and its retries, including during shutdown's
// grace period. Session loss cancels both the running chain and its retries.
// It does not stop the listener; the SDK supplies new sessions on subsequent reads.
func processTask(ctx context.Context, source *Context, chain renderedChain, schedule backoff.ExponentialBackOff) handlerResult {
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	stopSession := context.AfterFunc(source.Context, cancel)
	defer stopSession()

	if source.Err() != nil {
		cancel()
	}

	source.Context = workerCtx
	retryNack := errors.New("retry nacked delivery")
	preprocessingErr := source.processingErr
	attempt := 0

	var result handlerResult

	_ = backoff.Retry(func() error {
		attempt++

		source.Context = workerCtx
		source.processingErr = preprocessingErr

		result = chain(source, attempt)
		if result.err != nil || result.disposition != Nack || source.Err() != nil {
			return nil
		}

		return retryNack
	}, backoff.WithContext(&schedule, workerCtx))

	return result
}
