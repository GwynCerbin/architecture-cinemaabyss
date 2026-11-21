package workerpool

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()

	select {
	case value := <-ch:
		return value
	case <-time.After(time.Second):
		t.Fatal("worker did not produce an event")
	}

	var zero T

	return zero
}

func TestPoolReleasesSharedSlot(t *testing.T) {
	slots := make(chan struct{}, 1)
	pool := NewPool(slots)
	started, release := make(chan struct{}), make(chan struct{})

	slots <- struct{}{}

	pool.Start(func() { close(started); <-release })
	receive(t, started)

	select {
	case slots <- struct{}{}:
		t.Fatal("worker released its reservation before completion")
	default:
	}

	close(release)
	select {
	case slots <- struct{}{}:
		<-slots
	case <-time.After(time.Second):
		t.Fatal("worker did not release the shared reservation")
	}

	receive(t, pool.Wait())
}

func TestPoolWaitIncludesReservedDeliveryNotYetStarted(t *testing.T) {
	slots := make(chan struct{}, 1)
	pool := NewPool(slots)
	slots <- struct{}{}

	done := pool.Wait()

	select {
	case <-done:
		t.Fatal("Wait returned while the reader owned a reservation")
	case <-time.After(20 * time.Millisecond):
	}

	finished := make(chan struct{})

	pool.Start(func() { close(finished) })
	receive(t, finished)
	receive(t, done)
}

func TestPoolWaitIncludesReaderWithoutDelivery(t *testing.T) {
	slots := make(chan struct{}, 1)
	pool := NewPool(slots)
	slots <- struct{}{}

	done := pool.Wait()

	select {
	case <-done:
		t.Fatal("Wait returned while the reader owned a reservation")
	case <-time.After(20 * time.Millisecond):
	}

	// The reader releases its reservation itself on EOF or read failure.
	<-slots
	receive(t, done)
}

func TestPoolDoesNotSkipCanceledWork(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	slots := make(chan struct{}, 1)
	pool := NewPool(slots)
	called := make(chan error, 1)
	slots <- struct{}{}

	pool.Start(func() { called <- ctx.Err() })
	require.ErrorIs(t, receive(t, called), context.Canceled)
	receive(t, pool.Wait())
}

func TestIdlePoolWait(t *testing.T) {
	pool := NewPool(make(chan struct{}, 4096))
	done := pool.Wait()
	require.Equal(t, done, pool.Wait())
	receive(t, done)
}

func TestPoolWaitForEverySlot(t *testing.T) {
	slots := make(chan struct{}, 2)
	pool := NewPool(slots)
	first, second := make(chan struct{}), make(chan struct{})
	finished := make(chan struct{})

	t.Cleanup(func() {
		for _, release := range []chan struct{}{first, second} {
			select {
			case <-release:
			default:
				close(release)
			}
		}

		<-pool.Wait()
	})

	slots <- struct{}{}

	pool.Start(func() { <-first; close(finished) })
	slots <- struct{}{}

	pool.Start(func() { <-second })

	done := pool.Wait()

	close(first)
	receive(t, finished)
	select {
	case <-done:
		t.Fatal("Wait returned while a slot was still occupied")
	case <-time.After(20 * time.Millisecond):
	}

	close(second)
	receive(t, done)
}
