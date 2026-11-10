package gateway

import (
	"api-gateway/internal/domain/entities"
	"api-gateway/internal/domain/usecases/gateway"
	"api-gateway/internal/domain/usecases/middleware"
	"api-gateway/pkg/config"

	"github.com/gin-gonic/gin"
)

type handlerSet struct {
	uc gateway.UseCase
}

func NewHandlerSet(uc gateway.UseCase) *handlerSet {
	return &handlerSet{
		uc: uc,
	}
}

func NewHandlerRoute(router *gin.RouterGroup, cfg config.DumbServices, responder SessionVerifier, useCase gateway.UseCase, middlewareUseCase middleware.UseCase) {
	router.Any("/gateway/*proxying",
		NewMiddlewareSet(cfg, responder, middlewareUseCase).Init,
		NewHandlerSet(useCase).MultiTasker,
	)
}

type SessionVerifier interface {
	GetAccessName() string
	CheckAccessToken(string) (*entities.UserInfo, error)
}
