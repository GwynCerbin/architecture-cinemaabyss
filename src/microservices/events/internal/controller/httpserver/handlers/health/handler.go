package health

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func NewHealthHandler(r *gin.RouterGroup) {
	r.GET("/health", func(context *gin.Context) {
		context.JSON(http.StatusOK, gin.H{
			"status": true,
		})
	})
}
