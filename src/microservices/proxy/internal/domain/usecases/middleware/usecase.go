package middleware

import "api-gateway/internal/domain/entities"

type UseCase struct{}

func New() UseCase { return UseCase{} }
func (u UseCase) CheckForbidden(ctx *entities.Context, user *entities.UserInfo) error {
	for _, power := range ctx.GetRules() {
		if !user.HasPower(power) {
			return entities.ForbiddenError{}
		}
	}

	return nil
}
