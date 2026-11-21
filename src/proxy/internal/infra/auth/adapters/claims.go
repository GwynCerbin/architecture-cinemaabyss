package adapters

import (
	"api-gateway/internal/domain/entities"
	"github.com/golang-jwt/jwt/v5"
)

type Claims struct {
	UserId    string   `json:"id"`
	BasePower []string `json:"base_power"`
	jwt.RegisteredClaims
}

func (val *Claims) ToEntity() *entities.UserInfo {
	id := val.UserId
	if id == "" {
		id = val.Subject
	}

	return entities.UserInfoBuilder{
		ID:        id,
		BasePower: val.BasePower,
		ExpiresAt: val.ExpiresAt.Time.UnixMilli(),
	}.Build()
}
