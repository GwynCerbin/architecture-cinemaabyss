package event

import (
	"encoding/json"
	"events/internal/controller/listener/handlers/event/adapter"
	kafkatopics "events/pkg/kafkalistener"
	"fmt"
	"log"
)

type Config struct {
	MovieTopic   string
	UserTopic    string
	PaymentTopic string
}

func NewHandlerSet(router kafkatopics.Router, config Config) {
	router.Add(config.MovieTopic, func(ctx *kafkatopics.Context) error {
		var event adapter.CreateMovieRequest
		if err := json.Unmarshal(ctx.Bytes(), &event); err != nil {
			log.Printf("invalid movie event: topic=%s offset=%d error=%v", ctx.Topic(), ctx.Offset(), err)
			ctx.Reject()
			return fmt.Errorf("decode movie event: %w", err)
		}
		log.Printf("movie event processed: topic=%s offset=%d event=%+v", ctx.Topic(), ctx.Offset(), event.ToEntity())
		ctx.Ack()
		return nil
	})
	router.Add(config.UserTopic, func(ctx *kafkatopics.Context) error {
		var event adapter.CreateUserRequest
		if err := json.Unmarshal(ctx.Bytes(), &event); err != nil {
			log.Printf("invalid user event: topic=%s offset=%d error=%v", ctx.Topic(), ctx.Offset(), err)
			ctx.Reject()
			return fmt.Errorf("decode user event: %w", err)
		}
		log.Printf("user event processed: topic=%s offset=%d event=%+v", ctx.Topic(), ctx.Offset(), event.ToEntity())
		ctx.Ack()
		return nil
	})
	router.Add(config.PaymentTopic, func(ctx *kafkatopics.Context) error {
		var event adapter.CreatePaymentRequest
		if err := json.Unmarshal(ctx.Bytes(), &event); err != nil {
			log.Printf("invalid payment event: topic=%s offset=%d error=%v", ctx.Topic(), ctx.Offset(), err)
			ctx.Reject()
			return fmt.Errorf("decode payment event: %w", err)
		}
		log.Printf("payment event processed: topic=%s offset=%d event=%+v", ctx.Topic(), ctx.Offset(), event.ToEntity())
		ctx.Ack()
		return nil
	})
}
