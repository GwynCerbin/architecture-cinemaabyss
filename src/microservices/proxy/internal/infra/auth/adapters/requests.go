package adapters

import "api-gateway/internal/domain/entities"

type LoginRequest struct {
	Login    string `json:"login"`
	Password string `json:"password"`
}

func FromCredentials(value entities.Credentials) LoginRequest {
	return LoginRequest{Login: value.Login, Password: value.Password}
}

type RefreshRequest struct {
	RefreshToken string `json:"refresh_token"`
}

func FromRefreshToken(token entities.RefreshToken) RefreshRequest {
	return RefreshRequest{RefreshToken: string(token)}
}
