package adapters

import "api-gateway/internal/domain/entities"

type Tokens struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token,omitempty"`
}

func FromTokens(tokens entities.Tokens) Tokens {
	return Tokens{AccessToken: tokens.AccessToken, RefreshToken: tokens.RefreshToken}
}
