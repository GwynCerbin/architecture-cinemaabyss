package ginbinder

import "api-gateway/internal/domain/entities"

type BadRequestError struct {
}

func (BadRequestError) Error() string {
	return "wrong query parameter or body"
}

type UnauthorizedError struct {
}

func (UnauthorizedError) Error() string {
	return "session is expired or incorrect credentials"
}

type ForbiddenError = entities.ForbiddenError
