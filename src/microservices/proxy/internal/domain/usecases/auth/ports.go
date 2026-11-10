package auth

import (
	"api-gateway/internal/domain/entities"
	"context"
)

type Service interface {
	Login(context.Context, entities.Credentials) (entities.Tokens, error)
	Refresh(context.Context, entities.RefreshToken) (entities.Tokens, error)
	Logout(context.Context, entities.RefreshToken) error
}

type TokenVerifier interface {
	CheckAccessToken(string) (*entities.UserInfo, error)
}
