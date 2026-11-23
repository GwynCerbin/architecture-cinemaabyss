package kafkaclient

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

var ErrClosed = errors.New("Kafka client closed")

// Message is owned by the caller after ReadMessage. Context is canceled on
// assignment loss or client closure. Commit acknowledges locally, not on Kafka.
// Keep the record immutable; duplicate local commits are harmless.
type Message struct {
	Record  *kgo.Record
	Context context.Context
	Commit  func(context.Context) error
}

// entry is a node in either the unacknowledged list or the free list.
// Indices stay within the partition's preallocated array.
type entry struct {
	offset     int64
	epoch      int32
	prev, next int
	generation uint64
}

type partition struct {
	ctx              context.Context
	cancel           context.CancelFunc
	slots            chan struct{}
	mu               sync.Mutex
	entries          []entry
	head, tail, free int
	last, next       kgo.EpochOffset
	dirty            bool
}

func newPartition(ctx context.Context, capacity int) *partition {
	ctx, cancel := context.WithCancel(ctx)
	p := &partition{ctx: ctx, cancel: cancel, slots: make(chan struct{}, capacity), entries: make([]entry, capacity), head: -1, tail: -1}
	for i := range p.entries {
		p.entries[i].next = i + 1
	}
	p.entries[capacity-1].next = -1
	return p
}

// acknowledge removes a known node in O(1), without walking an ACKed prefix.
// Generation invalidates duplicate ACKs even after the array slot is reused.
func (p *partition) acknowledge(ctx context.Context, index int, generation uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := p.ctx.Err(); err != nil {
		return err
	}
	e := &p.entries[index]
	if e.generation != generation {
		return nil
	}
	if e.prev == -1 {
		p.head = e.next
		p.next = p.last
		if p.head != -1 {
			head := &p.entries[p.head]
			p.next = kgo.EpochOffset{Epoch: head.epoch, Offset: head.offset}
		}
		p.dirty = true
	} else {
		p.entries[e.prev].next = e.next
	}
	if e.next == -1 {
		p.tail = e.prev
	} else {
		p.entries[e.next].prev = e.prev
	}
	e.generation++
	e.next = p.free
	p.free = index
	<-p.slots
	return nil
}

// partitionEvent describes an operation for the single preprocessor loop.
// bindMessage snapshots an assignment while Kafka rebalance is blocked.
type eventKind uint8

const (
	assignPartitions eventKind = iota
	revokePartitions
	losePartitions
	bindMessage
	flushOffsets
	stopProcessor
)

type partitionEvent struct {
	kind       eventKind
	partitions map[string][]int32
	request    *pendingRecord  // Only bindMessage.
	ctx        context.Context // Only flushOffsets.
	reply      chan error
}

// pendingRecord retains one polled record across canceled semaphore waits.
// Only the sequential ReadMessage loop uses it after assignment binding.
type pendingRecord struct {
	record    *kgo.Record
	partition *partition
}

// Consumer owns its Client and one preprocessor goroutine. The loop alone owns the map
// and sends commits. Partition mutexes protect lists and commit boundaries.
type Consumer struct {
	*Client
	config     ConsumerConfig
	ctx        context.Context
	cancel     context.CancelFunc
	pending    *pendingRecord
	events     chan partitionEvent
	done       chan struct{}
	partitions map[string]map[int32]*partition
	closeOnce  sync.Once
	closeErr   error
}

// NewConsumer accepts Kafka options for TLS, SASL, fetch limits, etc. Consumer group,
// topics, callbacks, disabled autocommit and BlockRebalanceOnPoll are owned by the
// wrapper and set last. Do not bypass ReadMessage with direct polling/commits.
func NewConsumer(config ConsumerConfig, options ...kgo.Opt) (*Consumer, error) {
	if len(config.Brokers) == 0 || config.Group == "" || len(config.Topics) == 0 {
		return nil, errors.New("brokers, group and topics are required")
	}
	if config.PartitionBuffer <= 0 || config.CommitInterval <= 0 || config.CommitTimeout <= 0 {
		return nil, errors.New("buffer size and commit durations must be positive")
	}
	for _, topic := range config.Topics {
		if topic == "" {
			return nil, errors.New("empty topic")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &Consumer{config: config, ctx: ctx, cancel: cancel, events: make(chan partitionEvent), done: make(chan struct{}), partitions: make(map[string]map[int32]*partition)}
	opts := []kgo.Opt{kgo.FetchMaxBytes(16 << 20), kgo.FetchMaxPartitionBytes(1 << 20), kgo.MaxConcurrentFetches(1)}
	opts = append(opts, options...)
	opts = append(opts, kgo.ConsumerGroup(config.Group), kgo.ConsumeTopics(config.Topics...), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(), kgo.OnPartitionsAssigned(c.assigned), kgo.OnPartitionsRevoked(c.revoked), kgo.OnPartitionsLost(c.lost))
	var err error
	c.Client, err = newClient(config.ClientConfig, opts...)
	if err != nil {
		cancel()
		return nil, err
	}
	go c.run()
	return c, nil
}

func (c *Consumer) run() {
	defer close(c.done)
	timer := time.NewTicker(c.config.CommitInterval)
	defer timer.Stop()
	for {
		select {
		case event := <-c.events:
			switch event.kind {
			case assignPartitions:
				for topic, ids := range event.partitions {
					if c.partitions[topic] == nil {
						c.partitions[topic] = make(map[int32]*partition)
					}
					for _, id := range ids {
						// Retained assignments keep their existing context and outstanding entries.
						if c.partitions[topic][id] != nil {
							continue
						}
						c.partitions[topic][id] = newPartition(c.ctx, c.config.PartitionBuffer)
					}
				}
			case revokePartitions, losePartitions:
				c.remove(event.partitions, event.kind == revokePartitions)
			case bindMessage:
				r := event.request
				r.partition = c.partitions[r.record.Topic][r.record.Partition]
			case flushOffsets:
				event.reply <- c.flush(event.ctx, c.partitions)
				continue
			case stopProcessor:
				for _, parts := range c.partitions {
					for _, p := range parts {
						p.cancel()
					}
				}
				event.reply <- nil
				return
			}
			event.reply <- nil
		case <-timer.C:
			ctx, cancel := context.WithTimeout(context.Background(), c.config.CommitTimeout)
			// A failure retains dirty boundaries for the next tick. Flush exposes errors.
			_ = c.flush(ctx, c.partitions)
			cancel()
		}
	}
}

func (c *Consumer) sendEvent(ctx context.Context, event partitionEvent) error {
	event.reply = make(chan error, 1)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return ErrClosed
	case c.events <- event:
	}
	select {
	case err := <-event.reply:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Consumer) assigned(_ context.Context, _ *kgo.Client, partitions map[string][]int32) {
	_ = c.sendEvent(context.Background(), partitionEvent{kind: assignPartitions, partitions: partitions})
}
func (c *Consumer) revoked(_ context.Context, _ *kgo.Client, partitions map[string][]int32) {
	_ = c.sendEvent(context.Background(), partitionEvent{kind: revokePartitions, partitions: partitions})
}
func (c *Consumer) lost(_ context.Context, _ *kgo.Client, partitions map[string][]int32) {
	_ = c.sendEvent(context.Background(), partitionEvent{kind: losePartitions, partitions: partitions})
}
func (c *Consumer) remove(assignments map[string][]int32, commit bool) {
	selected := make(map[string]map[int32]*partition)
	for topic, ids := range assignments {
		for _, id := range ids {
			p := c.partitions[topic][id]
			if p == nil {
				continue
			}
			p.cancel()
			if selected[topic] == nil {
				selected[topic] = make(map[int32]*partition)
			}
			selected[topic][id] = p
		}
	}
	if commit {
		ctx, cancel := context.WithTimeout(context.Background(), c.config.CommitTimeout)
		// Failure leaves the next owner to replay. Never commit on outright loss.
		_ = c.flush(ctx, selected)
		cancel()
	}
	for topic, parts := range selected {
		for id := range parts {
			delete(c.partitions[topic], id)
		}
		if len(c.partitions[topic]) == 0 {
			delete(c.partitions, topic)
		}
	}
}

// ReadMessage polls one record, snapshots its assignment while rebalance is
// blocked, then releases rebalance BEFORE waiting for partition capacity or handlers.
// Calls must not overlap. One record can wait outside its partition list. This deliberately allows head-of-line
// blocking across topics; no fairness scheduler is hidden in this wrapper.
func (c *Consumer) ReadMessage(ctx context.Context) (*Message, error) {
	readCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.ctx, cancel)
	defer stop()
	defer cancel()
	for {
		if c.ctx.Err() != nil {
			return nil, ErrClosed
		}
		if err := readCtx.Err(); err != nil {
			return nil, err
		}
		if c.pending == nil {
			fetches := c.kafka.PollRecords(readCtx, 1)
			if errs := fetches.Errors(); len(errs) > 0 {
				c.kafka.AllowRebalance()
				return nil, fmt.Errorf("fetch %s/%d: %w", errs[0].Topic, errs[0].Partition, errs[0].Err)
			}
			records := fetches.Records()
			if len(records) == 0 {
				c.kafka.AllowRebalance()
				continue
			}
			record := records[0]
			r := &pendingRecord{record: record}
			// Resolve binding before AllowRebalance, even if the caller cancels.
			err := c.sendEvent(context.Background(), partitionEvent{kind: bindMessage, request: r})
			c.kafka.AllowRebalance()
			if err != nil {
				return nil, err
			}
			c.pending = r
		}
		r := c.pending
		if r.partition == nil || r.partition.ctx.Err() != nil {
			c.pending = nil
			continue
		}
		p := r.partition
		select {
		case <-readCtx.Done():
			return nil, readCtx.Err()
		case <-p.ctx.Done():
			c.pending = nil
			continue
		case p.slots <- struct{}{}:
		}
		// A ready slot can win select concurrently with cancellation.
		// Keep a canceled read's record, but discard a revoked assignment's record.
		p.mu.Lock()
		if err := readCtx.Err(); err != nil {
			p.mu.Unlock()
			<-p.slots
			return nil, err
		}
		if p.ctx.Err() != nil {
			p.mu.Unlock()
			<-p.slots
			c.pending = nil
			continue
		}
		index := p.free
		e := &p.entries[index]
		p.free = e.next
		e.offset, e.epoch = r.record.Offset, r.record.LeaderEpoch
		e.prev, e.next = p.tail, -1
		e.generation++
		generation := e.generation
		if p.tail == -1 {
			p.head = index
		} else {
			p.entries[p.tail].next = index
		}
		p.tail = index
		p.last = kgo.EpochOffset{Epoch: e.epoch, Offset: e.offset + 1}
		p.mu.Unlock()
		c.pending = nil
		return &Message{Record: r.record, Context: p.ctx, Commit: func(ctx context.Context) error {
			return p.acknowledge(ctx, index, generation)
		}}, nil
	}
}

// Flush synchronously commits completed prefixes only. It can run while handlers
// finish; ACKs after the boundary snapshot remain dirty for the next flush.
func (c *Consumer) Flush(ctx context.Context) error {
	return c.sendEvent(ctx, partitionEvent{kind: flushOffsets, ctx: ctx})
}

func (c *Consumer) flush(ctx context.Context, states map[string]map[int32]*partition) error {
	offsets := make(map[string]map[int32]kgo.EpochOffset)
	for topic, parts := range states {
		for id, p := range parts {
			p.mu.Lock()
			if p.dirty {
				if offsets[topic] == nil {
					offsets[topic] = make(map[int32]kgo.EpochOffset)
				}
				offsets[topic][id] = p.next
			}
			p.mu.Unlock()
		}
	}
	if len(offsets) == 0 {
		return nil
	}
	var result error
	c.kafka.CommitOffsetsSync(ctx, offsets, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, response *kmsg.OffsetCommitResponse, err error) {
		result = err
		if err != nil {
			return
		}
		for _, topic := range response.Topics {
			for _, part := range topic.Partitions {
				if err := kerr.ErrorForCode(part.ErrorCode); err != nil {
					result = errors.Join(result, fmt.Errorf("commit %s/%d: %w", topic.Topic, part.Partition, err))
				}
			}
		}
	})
	if result == nil {
		for topic, parts := range offsets {
			for id, committed := range parts {
				p := states[topic][id]
				p.mu.Lock()
				// An ACK may have advanced the boundary while Kafka was committing.
				if p.next == committed {
					p.dirty = false
				}
				p.mu.Unlock()
			}
		}
	}
	return result
}

// Close follows listener Shutdown: all accepted handlers should be drained first.
// It cancels outstanding reads, flushes completed prefixes, closes Kafka, and
// finally stops the preprocessor. The underlying client is owned by this wrapper.
func (c *Consumer) Close() error {
	c.closeOnce.Do(func() {
		c.cancel()
		ctx, cancel := context.WithTimeout(context.Background(), c.config.CommitTimeout)
		c.closeErr = c.Flush(ctx)
		cancel()
		c.kafka.CloseAllowingRebalance()
		_ = c.sendEvent(context.Background(), partitionEvent{kind: stopProcessor})
		<-c.done
	})
	return c.closeErr
}
