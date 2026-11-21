package gateway

import (
	"errors"
	"fmt"

	adapter "api-gateway/internal/controller/httpserver/handlers/v1/gateway/adapters"
	"api-gateway/internal/domain/entities"
	"api-gateway/pkg/ginbinder"

	"github.com/gin-gonic/gin"
)

func (hs *handlerSet) MultiTasker(c *gin.Context) {
	ctxRaw, ok := c.Get(ginbinder.Ctx)
	if !ok {
		c.JSON(adapter.NewResponse(nil, fmt.Errorf("get context from middleware: %w", ginbinder.UnauthorizedError{})))

		return
	}

	ctx, ok := ctxRaw.(*entities.Context)
	if !ok {
		c.JSON(adapter.NewResponse(nil, errors.New("context not provided")))

		return
	}

	hs.uc.Proxy(ctx)
}
