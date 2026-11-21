// Package workerpool owns bounded task execution for the listener.
package workerpool

import "sync"

// Pool starts tasks directly, without a dispatcher or an input queue.
// The caller reserves a shared slot before reading and passes it to Start.
// Jobs own their processing context and panic handling; the pool releases their
// reservations and waits for all shared slots to become free.
type Pool struct {
	slots    chan struct{}
	done     chan struct{}
	waitOnce sync.Once
}

func NewPool(slots chan struct{}) *Pool {
	return &Pool{slots: slots, done: make(chan struct{})}
}

// Start runs a job with a slot already acquired by the caller.
func (p *Pool) Start(job func()) {
	go func() {
		defer func() { <-p.slots }()
		job()
	}()
}

// Wait closes its channel after all reader and worker reservations are released.
// Stop new reads before waiting. Already reserved deliveries may still be started.
func (p *Pool) Wait() <-chan struct{} {
	p.waitOnce.Do(func() {
		go func() {
			for range cap(p.slots) {
				p.slots <- struct{}{}
			}

			close(p.done)
		}()
	})

	return p.done
}
