package kafkatopics

import (
	"context"
	"io"
)

type Disposition uint32

const (
	Pending Disposition = iota
	Ack
	Nack
	Reject
)

// Context carries raw data and shared values through one sequential handler chain.
// Payload bytes are read-only and must not be retained after processing. The first explicit
// decision wins; a handler timeout or DLQ write failure forces Nack.
// Handlers must not retain Context or use it asynchronously; pass Context.Context
// to operations that need an independently retained standard context.
type Context struct {
	context.Context
	topic         string
	partitionID   int64
	offset        int64
	attempt       int
	processingErr error

	disposition Disposition
	commit      func(context.Context) error
	payload     *payloadBuffer
	fatal       error
}

// NewContext wraps data without copying it. The caller must not mutate data
// while the context is in use.
func NewContext(ctx context.Context, data []byte) *Context {
	return &Context{Context: ctx, payload: &payloadBuffer{data: data}}
}

// Bytes returns the full payload without copying or changing the reader position.
// The result is read-only and must not be retained after processing finishes.
func (c *Context) Bytes() []byte {
	if c.payload == nil {
		return nil
	}

	return c.payload.data
}

// Topic and Offset identify the delivery without exposing mutable metadata.
func (c *Context) Topic() string { return c.topic }
func (c *Context) Offset() int64 { return c.offset }

// Attempt is one-based: the first processing attempt is not a retry.
func (c *Context) Attempt() int { return c.attempt }

// ProcessingError reports errors from preceding handlers in the current attempt.
// Err remains the embedded context's cancellation/deadline error.
func (c *Context) ProcessingError() error { return c.processingErr }

// Reader rewinds the delivery's reader. Do not retain it after the chain
// returns or another handler calls Reader. Handlers use it synchronously.
func (c *Context) Reader() io.Reader {
	if c.payload == nil {
		c.payload = new(payloadBuffer)
	}

	c.payload.Rewind()

	return c.payload
}

// release returns the payload buffer after all attempts and drops delivery state.
func (c *Context) release() {
	c.releaseAttempt()
	c.commit = nil

	if c.payload != nil {
		c.payload.Reset()

		if c.payload.release != nil {
			c.payload.release(c.payload.data)
		}

		c.payload = nil
	}
}

func (c *Context) releaseAttempt() {
	if c.payload != nil {
		c.payload.offset = len(c.payload.data)
	}
}

func (c *Context) Disposition() Disposition { return c.disposition }
func (c *Context) IsAcked() bool            { return c.Disposition() == Ack }
func (c *Context) Ack()                     { c.setDisposition(Ack) }
func (c *Context) Nack()                    { c.setDisposition(Nack) }
func (c *Context) Reject()                  { c.setDisposition(Reject) }
func (c *Context) setDisposition(next Disposition) {
	if c.disposition == Pending {
		c.disposition = next
	}
}
