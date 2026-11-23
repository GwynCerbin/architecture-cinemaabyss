package eventProvider

import (
	"context"
	"errors"
	"events/internal/domain/entities"
	"events/internal/infra/eventProvider/adapters"
	"events/pkg/kafkaclient"
	"strings"
)

type Publisher struct {
	client *kafkaclient.Publisher
	config Config
}

// NewPublisher borrows a shared client; the caller owns its lifetime.
func NewPublisher(client *kafkaclient.Client, config Config) (*Publisher, error) {
	if strings.TrimSpace(config.MovieTopic) == "" || strings.TrimSpace(config.UserTopic) == "" || strings.TrimSpace(config.PaymentTopic) == "" {
		return nil, errors.New("movie, user and payment topics are required")
	}
	publisher, err := kafkaclient.NewPublisher(client)
	if err != nil {
		return nil, err
	}
	return &Publisher{client: publisher, config: config}, nil
}

func (p *Publisher) PublishMovieEvent(ctx context.Context, movie *entities.Movie) error {
	return p.client.Publish(ctx, p.config.MovieTopic, adapters.FromMovie(movie))
}

func (p *Publisher) PublishPaymentEvent(ctx context.Context, payment *entities.Payment) error {
	return p.client.Publish(ctx, p.config.PaymentTopic, adapters.FromPayment(payment))
}

func (p *Publisher) PublishUserEvent(ctx context.Context, user *entities.User) error {
	return p.client.Publish(ctx, p.config.UserTopic, adapters.FromUser(user))
}
