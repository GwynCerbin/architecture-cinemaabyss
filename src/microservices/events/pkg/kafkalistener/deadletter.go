package kafkatopics

import (
	"context"
	"errors"
)

var ErrDeadLetterWriterMissing = errors.New("reject requires a dead letter writer")

// DeadLetter preserves the source position for deduplication and manual replay.
// DLQ writing and source commit are not atomic, so writes may be retried.
type DeadLetter struct {
	Data        []byte `json:"data"`
	Topic       string `json:"topic"`
	PartitionID int64  `json:"partition_id"`
	Offset      int64  `json:"offset"`
	Attempt     int    `json:"attempt"`
	Reason      string `json:"reason"`
}

// DeadLetterWriter must return nil only after durable acceptance of the record.
// Implementations must support concurrent Write calls, honor cancellation,
// and must not retain or mutate Data.
// Ownership remains with the caller; close the writer after the listener stops.
type DeadLetterWriter interface {
	Write(context.Context, DeadLetter) error
}
