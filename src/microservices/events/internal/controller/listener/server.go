package listener

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"
)

var ErrServerFailed = errors.New("Kafka listener failed")

// TopicListener owns topic readers and drains accepted work during Shutdown.
type TopicListener interface {
	ListenAndServe() error
	Shutdown(context.Context) error
}

// Server adapts a prepared topic listener to the application's context lifecycle.
// A Server is run once; the Kafka client remains owned by the caller.
type Server struct {
	listener        TopicListener
	shutdownTimeout time.Duration
}

func NewServer(listener TopicListener, config Config) *Server {
	var shutdownTimeout = config.ShutdownTimeout

	if shutdownTimeout == 0 {
		shutdownTimeout = 20 * time.Second
	}

	return &Server{listener: listener, shutdownTimeout: shutdownTimeout}
}

func (s *Server) ListenAndServe(ctx context.Context) error {
	log.Print("Starting Kafka listener")

	var childCtx, cancelCause = context.WithCancelCause(ctx)

	defer cancelCause(nil)

	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := s.listener.ListenAndServe(); err != nil {
			cancelCause(fmt.Errorf("%w: %w", ErrServerFailed, err))
		}
	}()

	<-childCtx.Done()

	var err error

	if errContext := context.Cause(childCtx); errors.Is(errContext, ErrServerFailed) {
		err = errContext
	}

	ctxTimeout, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.shutdownTimeout)

	defer cancel()

	shutdownErr := s.listener.Shutdown(ctxTimeout)
	<-done
	switch {
	case shutdownErr == nil:
		return err
	case errors.Is(shutdownErr, context.DeadlineExceeded):
		return fmt.Errorf("Kafka listener shutdown exceeded %s: %w", s.shutdownTimeout, errors.Join(err, shutdownErr))
	default:
		return fmt.Errorf("Kafka listener shutdown failed: %w", errors.Join(err, shutdownErr))
	}
}
