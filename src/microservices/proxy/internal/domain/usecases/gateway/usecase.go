package gateway

import (
	"api-gateway/internal/domain/entities"
)

type provider interface {
	Provide(ctx *entities.Context)
}

type UseCase struct {
	provider provider
}

func New(provider provider) UseCase {
	return UseCase{
		provider: provider,
	}
}

func (uc UseCase) Proxy(ctx *entities.Context) {
	uc.provider.Provide(ctx)
}
