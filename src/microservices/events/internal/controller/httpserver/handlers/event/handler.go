package event

import (
	"log"
	"net/http"

	"events/internal/controller/httpserver/handlers/event/adapter"
	"events/internal/domain/usecase"
	"events/pkg/ginbinder"

	"github.com/gin-gonic/gin"
)

func NewHealthHandlerSet(r *gin.RouterGroup, publisher usecase.UseCase) {
	r.POST("/payment", func(ctx *gin.Context) {
		var createPayment adapter.CreatePaymentRequest
		if err := ctx.ShouldBindJSON(&createPayment); err != nil {
			ctx.JSON(ginbinder.HandleBinding(err))

			return
		}

		if err := publisher.SendCreatePaymentEvent(ctx, createPayment.ToEntity()); err != nil {
			log.Printf("internal error on  %s: %v", ctx.Request.RequestURI, err)

			ctx.JSON(http.StatusInternalServerError, ginbinder.Response{
				Status:  "fail",
				Message: "internal server error",
			})

			return
		}

		ctx.JSON(http.StatusCreated, ginbinder.Response{
			Status:  "success",
			Message: "success",
		})
	})

	r.POST("/user", func(ctx *gin.Context) {
		var createUser adapter.CreateUserRequest
		if err := ctx.ShouldBindJSON(&createUser); err != nil {
			ctx.JSON(ginbinder.HandleBinding(err))

			return
		}

		if err := publisher.SendCreateUserEvent(ctx, createUser.ToEntity()); err != nil {
			log.Printf("internal error on  %s: %v", ctx.Request.RequestURI, err)

			ctx.JSON(http.StatusInternalServerError, ginbinder.Response{
				Status:  "fail",
				Message: "internal server error",
			})

			return
		}

		ctx.JSON(http.StatusCreated, ginbinder.Response{
			Status:  "success",
			Message: "success",
		})
	})

	r.POST("/movie", func(ctx *gin.Context) {
		var createMovie adapter.CreateMovieRequest
		if err := ctx.ShouldBindJSON(&createMovie); err != nil {
			ctx.JSON(ginbinder.HandleBinding(err))

			return
		}

		if err := publisher.SendCreateMovieEvent(ctx, createMovie.ToEntity()); err != nil {
			log.Printf("internal error on  %s: %v", ctx.Request.RequestURI, err)

			ctx.JSON(http.StatusInternalServerError, ginbinder.Response{
				Status:  "fail",
				Message: "internal server error",
			})

			return
		}

		ctx.JSON(http.StatusCreated, ginbinder.Response{
			Status:  "success",
			Message: "success",
		})
	})
}
