package adapters

import "api-gateway/internal/domain/entities"

func UserHeaders(user *entities.UserInfo) map[string]string {
	return map[string]string{"User-Info": user.ID()}
}
