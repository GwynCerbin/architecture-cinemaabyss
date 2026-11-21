package httpserver

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"
)

type Server struct {
	server          http.Server
	shutdownTimeout time.Duration
}

func NewHTTPServer(router http.Handler, config Config) *Server {
	port := config.Port
	if port == 0 {
		port = 80
	}

	var shutdownTimeout = config.ShutdownTimeout
	if shutdownTimeout <= 0 {
		shutdownTimeout = fallbackShutdownTimeout
	}

	return &Server{
		server: http.Server{
			Addr:              fmt.Sprintf(":%d", port),
			ReadHeaderTimeout: 3 * time.Minute,
			Handler:           router,
		},

		shutdownTimeout: shutdownTimeout,
	}
}

func (s *Server) ListenAndServe(ctx context.Context) error {
	if ctx.Err() != nil {
		return nil
	}

	var childCtx, cancelCause = context.WithCancelCause(ctx)

	defer cancelCause(nil)

	go func() {
		if err := s.server.ListenAndServe(); err != nil {
			cancelCause(fmt.Errorf("%w: %w", ErrServerFailed, err))
		}
	}()

	<-childCtx.Done()

	var err error

	if errContext := context.Cause(childCtx); errors.Is(errContext, ErrServerFailed) {
		err = errContext
	}

	ctxTimeout, cancel := context.WithTimeout(context.Background(), s.shutdownTimeout)

	defer cancel()

	switch shutdownErr := s.server.Shutdown(ctxTimeout); {
	case shutdownErr == nil:
		return err
	case errors.Is(shutdownErr, context.DeadlineExceeded):
		err = fmt.Errorf("HTTP shutdown exceeded %s: %w", s.shutdownTimeout, errors.Join(err, shutdownErr))
	default:
		err = fmt.Errorf("HTTP shutdown failed: %w", errors.Join(err, shutdownErr))
	}

	if closeErr := s.server.Close(); closeErr != nil {
		err = fmt.Errorf("HTTP close failed: %w", errors.Join(err, closeErr))
	}

	return err
}
