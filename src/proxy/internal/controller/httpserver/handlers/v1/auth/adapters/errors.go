package adapters

import (
	authUC "api-gateway/internal/domain/usecases/auth"
	"api-gateway/pkg/ginbinder"
	"errors"
	"net/http"
)

func ErrorResponse(err error) (int, ginbinder.Response) {
	switch {
	case errors.Is(err, authUC.ErrInvalidInput):
		return http.StatusBadRequest, ginbinder.Response{Message: "invalid authentication input"}
	case errors.Is(err, authUC.ErrInvalidCredentials), errors.Is(err, authUC.ErrInvalidRefreshToken):
		return http.StatusUnauthorized, ginbinder.Response{Message: "unauthorized"}
	case errors.Is(err, authUC.ErrAccessDenied):
		return http.StatusForbidden, ginbinder.Response{Message: "access denied"}
	case errors.Is(err, authUC.ErrRateLimited):
		return http.StatusTooManyRequests, ginbinder.Response{Message: "too many requests"}
	default:
		return http.StatusBadGateway, ginbinder.Response{Message: "authentication service failed"}
	}
}
