package adapters

import "api-gateway/internal/domain/entities"

type SessionInfo struct {
	ID        string   `json:"id"`
	BasePower []string `json:"base_power"`
	ExpiresAt int64    `json:"expires_at"`
}

func SwitchEntity(info *entities.UserInfo) SessionInfo {
	return SessionInfo{ID: info.ID(), BasePower: info.BasePower(), ExpiresAt: info.ExpiresAt()}
}
