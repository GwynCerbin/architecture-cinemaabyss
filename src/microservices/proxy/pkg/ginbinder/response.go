package ginbinder

import (
	"errors"
	"log"
	"net/http"
)

type Response struct {
	Success bool        `json:"success"`
	Message string      `json:"message"`
	Result  interface{} `json:"result"`
}

func NewResponse(data interface{}, err error) (int, *Response) {
	if err == nil {
		return http.StatusOK, &Response{Success: true, Message: "success", Result: data}
	}

	log.Printf("HTTP request failed: %v", err)

	switch {
	case errors.Is(err, BadRequestError{}):
		return http.StatusBadRequest, &Response{Message: err.Error()}
	case errors.Is(err, UnauthorizedError{}):
		return http.StatusUnauthorized, &Response{Message: "Сессия истекла"}
	case errors.Is(err, ForbiddenError{}):
		return http.StatusForbidden, &Response{Message: "Доступ запрещен"}
	default:
		return http.StatusInternalServerError, &Response{Message: "internal server error"}
	}
}
