package gateway

import (
	"fmt"

	adapter "api-gateway/internal/controller/httpserver/handlers/v1/gateway/adapters"
	"api-gateway/internal/domain/entities"
	"api-gateway/internal/domain/usecases/middleware"
	"api-gateway/pkg/ginbinder"

	"github.com/gin-gonic/gin"
)

type middlewareSet struct {
	responder SessionVerifier
	router    map[string]adapter.Routes
	mwUC      middleware.UseCase
}

func NewMiddlewareSet(routes map[string]adapter.Routes, responder SessionVerifier, useCase middleware.UseCase) *middlewareSet {
	return &middlewareSet{
		responder: responder,
		router:    routes,
		mwUC:      useCase,
	}
}

func (ms *middlewareSet) Init(c *gin.Context) {
	req := c.Request

	routes, ok := ms.router[adapter.SetKey(req.Method, req.URL.Path)]
	if !ok {
		ginbinder.NoRoute(c)

		c.Abort()

		return
	}

	cfg, ok := routes.SelectRoute()
	if !ok {
		c.AbortWithStatusJSON(503, ginbinder.Response{Message: "no available route"})
		return
	}

	responder := ms.responder

	if cfg.SkipCookieCheck {
		c.Set(ginbinder.Ctx, entities.ContextBuilder{
			Service:       cfg.Service,
			Request:       req,
			Response:      c.Writer,
			CheckResponse: cfg.CheckResp,
		}.Build())

		c.Next()

		return
	}

	cookie, err := c.Cookie(responder.GetAccessName())
	if err != nil {
		c.JSON(adapter.NewResponse(nil, fmt.Errorf("unauthorized: %w", ginbinder.UnauthorizedError{})))

		c.Abort()

		return
	}

	user, err := responder.CheckAccessToken(cookie)
	if err != nil {
		c.JSON(adapter.NewResponse(nil, fmt.Errorf("check access token: %w: %w", ginbinder.UnauthorizedError{}, err)))

		c.Abort()

		return
	}

	ctx := entities.ContextBuilder{
		Service:       cfg.Service,
		Request:       req,
		Response:      c.Writer,
		GwAction:      cfg.Description,
		CheckResponse: cfg.CheckResp,
		Rules:         cfg.CheckPowers,
	}.Build()

	if err = ms.mwUC.CheckForbidden(ctx, user); err != nil {
		c.JSON(adapter.NewResponse(nil, err))

		c.Abort()

		return
	}

	ctx.SetHeaders(adapter.UserHeaders(user))

	c.Set(ginbinder.Ctx, ctx)

	c.Next()
}
