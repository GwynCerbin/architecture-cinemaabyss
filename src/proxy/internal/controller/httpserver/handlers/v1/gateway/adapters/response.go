package adapters

import "api-gateway/pkg/ginbinder"

func NewResponse(data interface{}, err error) (int, *ginbinder.Response) {
	return ginbinder.NewResponse(data, err)
}
