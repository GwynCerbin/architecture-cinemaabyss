package httpserver

import (
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

func CORSMiddleware() gin.HandlerFunc {
	config := cors.DefaultConfig()
	config.AllowOrigins = []string{"*"} // Add this line
	config.AllowHeaders = []string{
		"Accept",
		"Content-Type",
		"Content-Length",
		"Accept-Encoding",
		"X-CSRF-Token",
		"Authorization",
		"Upgrade",
		"Connection",
		"Access-Control-Allow-Origin",
	}
	config.AllowMethods = []string{"GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"} // Add this line
	config.AllowWebSockets = true
	config.AllowCredentials = true

	return cors.New(config)
}
