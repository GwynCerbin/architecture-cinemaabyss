package usecase

import (
	"context"

	"events/internal/domain/entities"
)

type MessagePublisher interface {
	PublishMovieEvent(context.Context, *entities.Movie) error
	PublishPaymentEvent(context.Context, *entities.Payment) error
	PublishUserEvent(context.Context, *entities.User) error
}
type UseCase struct {
	publisher MessagePublisher
}

func NewUseCase(publisher MessagePublisher) UseCase {
	return UseCase{
		publisher: publisher,
	}
}

func (uc *UseCase) SendCreateMovieEvent(ctx context.Context, movie *entities.Movie) error {
	return uc.publisher.PublishMovieEvent(ctx, movie)
}

func (uc *UseCase) SendCreatePaymentEvent(ctx context.Context, payment *entities.Payment) error {
	return uc.publisher.PublishPaymentEvent(ctx, payment)
}

func (uc *UseCase) SendCreateUserEvent(ctx context.Context, user *entities.User) error {
	return uc.publisher.PublishUserEvent(ctx, user)
}
