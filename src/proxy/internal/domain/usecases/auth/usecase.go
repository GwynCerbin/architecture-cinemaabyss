package auth

import (
	"api-gateway/internal/domain/entities"
	"context"
	"fmt"
	"strings"
)

type UseCase struct {
	service  Service
	verifier TokenVerifier
}

func New(service Service, verifier TokenVerifier) UseCase {
	return UseCase{service: service, verifier: verifier}
}

func (u UseCase) Login(ctx context.Context, credentials entities.Credentials) (*entities.Session, error) {
	if strings.TrimSpace(credentials.Login) == "" || credentials.Password == "" {
		return nil, ErrInvalidInput
	}

	tokens, err := u.service.Login(ctx, credentials)
	if err != nil {
		return nil, err
	}

	return u.acceptTokens(tokens)
}

func (u UseCase) Refresh(ctx context.Context, token entities.RefreshToken) (*entities.Session, error) {
	if token == "" {
		return nil, ErrInvalidRefreshToken
	}

	tokens, err := u.service.Refresh(ctx, token)
	if err != nil {
		return nil, err
	}

	return u.acceptTokens(tokens)
}

func (u UseCase) Logout(ctx context.Context, token entities.RefreshToken) error {
	if token == "" {
		return ErrInvalidRefreshToken
	}

	return u.service.Logout(ctx, token)
}

func (u UseCase) Authenticate(accessToken string) (*entities.UserInfo, error) {
	if accessToken == "" {
		return nil, ErrInvalidCredentials
	}

	return u.verifier.CheckAccessToken(accessToken)
}

func (u UseCase) acceptTokens(tokens entities.Tokens) (*entities.Session, error) {
	user, err := u.Authenticate(tokens.AccessToken)
	if err != nil {
		return nil, fmt.Errorf("%w: access token verification failed", ErrInvalidResponse)
	}

	return &entities.Session{Tokens: tokens, User: user}, nil
}
