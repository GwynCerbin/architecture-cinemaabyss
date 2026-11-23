package kafkatopics

import (
	"context"
	"fmt"
)

// DeadLetterHandler writes rejected deliveries before final acknowledgement.
type DeadLetterHandler struct{ writer DeadLetterWriter }

func NewDeadLetterHandler(writer DeadLetterWriter) *DeadLetterHandler {
	return &DeadLetterHandler{writer: writer}
}

func (h *DeadLetterHandler) Handle(delivery *Context) error {
	if delivery.Disposition() != Reject {
		return nil
	}

	var err error

	if h.writer == nil {
		err = ErrDeadLetterWriterMissing
	} else {
		reason := ""
		if delivery.processingErr != nil {
			reason = delivery.processingErr.Error()
		}

		err = h.writer.Write(delivery, DeadLetter{Data: delivery.Bytes(), Topic: delivery.topic, PartitionID: delivery.partitionID, Offset: delivery.offset, Attempt: delivery.attempt, Reason: reason})
	}

	if err != nil {
		delivery.disposition = Nack

		return fmt.Errorf("write rejected delivery: %w", err)
	}

	return nil
}

func finalHandler() ProcessEvent {
	return func(delivery *Context) error {
		defer delivery.releaseAttempt()

		if delivery.Err() != nil {
			delivery.disposition = Nack
		}

		if delivery.Disposition() == Pending {
			if delivery.processingErr == nil {
				delivery.Ack()
			} else {
				delivery.Nack()
			}
		}

		if delivery.fatal != nil {
			return delivery.fatal
		}

		if delivery.Err() != nil || delivery.Disposition() == Nack {
			return nil
		}

		if err := delivery.commit(delivery); err != nil {
			delivery.disposition = Nack

			return fmt.Errorf("commit topic delivery: %w", err)
		}

		return nil
	}
}

func renderHandler(set Handler) ProcessEvent {
	return func(delivery *Context) (err error) {
		if delivery.Err() != nil {
			delivery.disposition = Nack

			return nil
		}

		base := delivery.Context
		ctx := base

		if set.timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, set.timeout)

			defer cancel()
		}

		delivery.Context = ctx

		defer func() {
			if value := recover(); value != nil {
				err = fmt.Errorf("topic handler panic: %v", value)
				delivery.fatal = err
				delivery.disposition = Nack
			} else if contextErr := ctx.Err(); contextErr != nil {
				err = contextErr

				delivery.disposition = Nack
			}

			// A handler's deadline never cancels the remaining chain elements.
			delivery.Context = base
		}()

		return set.handler(delivery)
	}
}
