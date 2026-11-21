package kafkatopics

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/cenkalti/backoff/v4"
)

type topicHandler struct {
	handler renderedChain
	backoff backoff.ExponentialBackOff
	workCtx context.Context
}

func newTopicHandler(ctx context.Context, name string, set HandlerSet, config listenerConfig) *topicHandler {
	for _, opt := range set.options {
		opt(&config)
	}

	return &topicHandler{
		handler: renderTopic(name, set, config),
		backoff: config.backoff,
		workCtx: ctx,
	}
}

// One shared read loop reserves worker capacity before reading, then routes by
// topic. Partition assignment and ACK ordering belong to the injected client.
func (l *Listener) read() {
	for {
		select {
		case <-l.readCtx.Done():
			return
		case l.slots <- struct{}{}:
		}
		message, err := l.source.ReadMessage(l.readCtx)
		if err != nil {
			if l.readCtx.Err() == nil && !errors.Is(err, io.EOF) {
				l.fail(err)
			}
			<-l.slots
			return
		}
		if message == nil || message.Record == nil || message.Context == nil || message.Commit == nil {
			l.fail(errors.New("invalid Kafka delivery"))
			<-l.slots
			return
		}
		record := message.Record
		route := l.handlers[record.Topic]
		if route == nil {
			l.fail(fmt.Errorf("no handler for Kafka topic %q", record.Topic))
			<-l.slots
			return
		}
		data := record.Value
		oversized := int64(len(data)) > l.maxPayload
		if oversized {
			data = data[:l.maxPayload+1]
		}
		buffer := l.payloadPool.Get().([]byte)
		buffer = append(buffer[:0], data...)
		delivery := &Context{Context: message.Context, topic: record.Topic, partitionID: int64(record.Partition), offset: record.Offset, payload: &payloadBuffer{data: buffer, release: l.releasePayload}}
		delivery.commit = message.Commit
		if oversized {
			delivery.processingErr = fmt.Errorf("topic message exceeds maximum message size: limit=%d", l.maxPayload)
		}
		l.tasks <- func() {
			defer delivery.release()
			l.fail(processTask(route.workCtx, delivery, route.handler, route.backoff).err)
		}
	}
}

type payloadBuffer struct {
	data    []byte
	offset  int
	release func([]byte)
}

func (b *payloadBuffer) Read(p []byte) (int, error) {
	if b.offset >= len(b.data) {
		return 0, io.EOF
	}

	n := copy(p, b.data[b.offset:])
	b.offset += n

	return n, nil
}

func (b *payloadBuffer) Rewind() { b.offset = 0 }

func (b *payloadBuffer) Reset() {
	b.data = b.data[:0]
	b.offset = 0
}

func newPayloadBufferPool(size int) (*sync.Pool, func([]byte)) {
	pool := &sync.Pool{}
	release := func(data []byte) {
		if cap(data) <= size {
			pool.Put(data[:0])
		}
	}
	pool.New = func() any {
		return make([]byte, 0, size)
	}

	return pool, release
}
