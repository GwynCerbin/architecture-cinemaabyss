package gateway

import (
	adapter "api-gateway/internal/controller/httpserver/handlers/v1/gateway/adapters"
	"api-gateway/internal/domain/entities"
	"api-gateway/internal/domain/usecases/gateway"
	"api-gateway/internal/domain/usecases/middleware"

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

func NewHandlerRoute(router *gin.RouterGroup, routes map[string]adapter.Routes, responder SessionVerifier, useCase gateway.UseCase, middlewareUseCase middleware.UseCase) {
	router.Any("/api/*proxying",
		NewMiddlewareSet(routes, responder, middlewareUseCase).Init,
		NewHandlerSet(useCase).MultiTasker,
	)
}

type SessionVerifier interface {
	GetAccessName() string
	CheckAccessToken(string) (*entities.UserInfo, error)
}
