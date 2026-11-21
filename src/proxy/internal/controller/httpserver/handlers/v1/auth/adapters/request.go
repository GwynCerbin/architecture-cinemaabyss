package adapters

import "api-gateway/internal/domain/entities"

type LoginRequest struct {
	Login    string `json:"login" binding:"required"`
	Password string `json:"password" binding:"required"`
}

func (r LoginRequest) ToEntity() entities.Credentials {
	return entities.Credentials{Login: r.Login, Password: r.Password}
}
