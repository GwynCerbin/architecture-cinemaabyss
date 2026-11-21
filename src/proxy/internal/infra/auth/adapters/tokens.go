package adapters

import "api-gateway/internal/domain/entities"

type TokensResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
}

func (t TokensResponse) ToEntity() entities.Tokens {
	return entities.Tokens{AccessToken: t.AccessToken, RefreshToken: t.RefreshToken}
}
