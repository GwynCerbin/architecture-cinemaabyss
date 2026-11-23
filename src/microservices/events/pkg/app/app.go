package app

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
)

// Controller optionally prepares resources before any controllers start.
// Prepare may register cleanup through AddCleanup and must return on cancellation.
// Run must not return while handlers or child goroutines still use shared infrastructure.
type Controller struct {
	Prepare func(context.Context) error
	Run     func(context.Context) error
}

// Cleanup releases a resource and must respect the supplied deadline.
// Resources are registered immediately after creation, in creation order.
type Cleanup func(context.Context) error

var (
	ErrStarted        = errors.New("application is already running or closed")
	ErrRunning        = errors.New("cannot close resources while controllers are running")
	ErrUnexpectedExit = errors.New("controller exited before cancellation")
)

type controller struct {
	name string
	Controller
}

type resource struct {
	name  string
	close Cleanup
}

const (
	building uint32 = iota
	preparing
	running
	stopped
	closing
	closed
)

// Application is single-use. Controllers are registered only before Run/Close.
// Cancellation stops controllers; resources are closed only after all Run calls
// return. A controller that ignores cancellation can block Run indefinitely:
// closing its resources concurrently would be unsafe.
// Registrations must be sequential, before Run/Close or, for AddCleanup only,
// synchronously inside Prepare. No concurrent registration is supported.
// Atomic state transitions do not synchronize registration slices.
// An Application must not be copied after first use.
type Application struct {
	ctx            context.Context
	state          atomic.Uint32
	controllers    []controller
	resources      []resource
	cleanupTimeout time.Duration
	closed         chan struct{}
	closeErr       error
}

// New sets the total cooperative cleanup budget. Nonpositive values use 10s.
func New(cleanupTimeout time.Duration, options ...Option) *Application {
	if cleanupTimeout <= 0 {
		cleanupTimeout = 10 * time.Second
	}

	a := &Application{ctx: context.Background(), cleanupTimeout: cleanupTimeout, closed: make(chan struct{})}
	for _, option := range options {
		option(a)
	}

	return a
}

type Option func(*Application)

// WithContext supplies the parent context and shared objects for controllers.
// Cleanup retains its values, but runs with an independent shutdown deadline.
func WithContext(ctx context.Context) Option {
	return func(a *Application) { a.ctx = ctx }
}

// Close releases resources once. It rejects calls while controllers are running.
// Concurrent callers wait for cleanup and receive the same result.
func (a *Application) Close() error {
	for {
		current := a.state.Load()
		switch current {
		case preparing, running:
			return ErrRunning
		case closing, closed:
			<-a.closed

			return a.closeErr
		case building, stopped:
			if !a.state.CompareAndSwap(current, closing) {
				continue
			}
		}

		break
	}

	ctx, cancel := context.WithTimeout(context.WithoutCancel(a.ctx), a.cleanupTimeout)

	defer cancel()

	var failures []error

	for i := len(a.resources) - 1; i >= 0; i-- {
		r := a.resources[i]
		// Attempt every cleanup, even when another failed or exhausted the budget.
		if err := r.close(ctx); err != nil {
			failures = append(failures, fmt.Errorf("close %q: %w", r.name, err))
		}
	}

	a.closeErr = errors.Join(failures...)
	a.state.Store(closed)
	close(a.closed)

	return a.closeErr
}

func (a *Application) AddController(name string, c Controller) error {
	if a.state.Load() != building {
		return ErrStarted
	}

	if name == "" || c.Run == nil {
		return errors.New("controller name and implementation are required")
	}

	for _, existing := range a.controllers {
		if existing.name == name {
			return fmt.Errorf("duplicate controller %q", name)
		}
	}

	a.controllers = append(a.controllers, controller{name: name, Controller: c})

	return nil
}

func (a *Application) AddCleanup(name string, cleanup Cleanup) error {
	if state := a.state.Load(); state != building && state != preparing {
		return ErrStarted
	}

	if name == "" || cleanup == nil {
		return errors.New("resource name and cleanup are required")
	}

	for _, existing := range a.resources {
		if existing.name == name {
			return fmt.Errorf("duplicate resource %q", name)
		}
	}

	a.resources = append(a.resources, resource{name: name, close: cleanup})

	return nil
}

// Run prepares controllers in registration order, then starts them concurrently.
// Preparation failure prevents every Run callback from starting. OS signals cancel
// both phases. Call Close after Run returns, including on preparation failure.
func (a *Application) Run() error {
	if !a.state.CompareAndSwap(building, preparing) {
		return ErrStarted
	}
	defer a.state.Store(stopped)

	var (
		notifyCtx, cancel = signal.NotifyContext(a.ctx, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
		group, runCtx     = errgroup.WithContext(notifyCtx)
	)

	defer cancel()

	for _, c := range a.controllers {
		if runCtx.Err() != nil {
			return nil
		}

		if c.Prepare == nil {
			continue
		}

		if err := c.Prepare(runCtx); err != nil {
			if runCtx.Err() != nil && errors.Is(err, runCtx.Err()) {
				return nil
			}

			return fmt.Errorf("prepare controller %q: %w", c.name, err)
		}
	}

	a.state.Store(running)

	for _, c := range a.controllers {
		group.Go(func() error {
			if runCtx.Err() != nil {
				return nil
			}

			err := c.Run(runCtx)
			if err != nil {
				if runCtx.Err() != nil && errors.Is(err, runCtx.Err()) {
					return nil
				}

				return fmt.Errorf("controller %q: %w", c.name, err)
			}

			if runCtx.Err() == nil {
				return fmt.Errorf("controller %q: %w", c.name, ErrUnexpectedExit)
			}

			return nil
		})
	}

	return group.Wait()
}
