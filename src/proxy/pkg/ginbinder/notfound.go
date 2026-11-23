package ginbinder

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func NoRoute(c *gin.Context) {
	c.JSON(http.StatusNotFound, Response{
		Success: false,
		Message: "Not Found",
		Result:  nil,
	})
}
