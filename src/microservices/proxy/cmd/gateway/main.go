package main

import (
	"context"
	"log"
	"net/http"

	"api-gateway/internal/controller/httpserver"
	"api-gateway/internal/controller/httpserver/handlers/v1/auth"
	"api-gateway/internal/controller/httpserver/handlers/v1/gateway"
	authUC "api-gateway/internal/domain/usecases/auth"
	gatewayUC "api-gateway/internal/domain/usecases/gateway"
	"api-gateway/internal/domain/usecases/middleware"
	authInfra "api-gateway/internal/infra/auth"
	"api-gateway/internal/infra/gateway/proxy"
	"api-gateway/pkg/app"
	"api-gateway/pkg/config"
	"api-gateway/pkg/ginbinder"

	"github.com/gin-gonic/gin"
)

func main() {
	cfg, err := config.Init()
	if err != nil {
		log.Printf("config init: %v", err)
		return
	}

	gatewayUseCase := gatewayUC.New(proxy.New())
	middlewareUseCase := middleware.New()
	verifier := authInfra.NewVerifier(cfg.Auth)

	var authUseCase authUC.UseCase

	var authClient *authInfra.Client
	if cfg.Auth.Enabled {
		authClient, err = authInfra.NewClient(cfg.Auth)
		if err != nil {
			log.Printf("auth client init: %v", err)
			return
		}

		defer authClient.Close()
		authUseCase = authUC.New(authClient, verifier)
	}

	application := app.New(0)
	defer func() {
		if err := application.Close(); err != nil {
			log.Printf("application close: %v", err)
		}
	}()

	if err := application.AddController("http listener", func(ctx context.Context) error {
		if !cfg.Debug {
			gin.SetMode(gin.ReleaseMode)
		}
		router := gin.New()
		router.Use(gin.Recovery())

		if cfg.Debug {
			router.Use(httpserver.CORSMiddleware())
		}

		router.NoRoute(ginbinder.NoRoute)
		router.GET("/ping", func(c *gin.Context) { c.String(http.StatusOK, "alive") })

		routes := router.Group("/v1", gin.Logger())
		if cfg.Auth.Enabled {
			auth.NewHandlerRoute(routes, cfg.Auth, authUseCase)
		}

		gateway.NewHandlerRoute(routes, cfg.RestServer.DumbServices, verifier, gatewayUseCase, middlewareUseCase)

		return httpserver.NewHTTPServer(router, cfg.RestServer.Config).ListenAndServe(ctx)
	}); err != nil {
		log.Printf("failed to add controller: %v", err)
		return
	}

	if err := application.Run(); err != nil {
		log.Printf("application runtime error: %v", err)
	}
}
