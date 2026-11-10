package adapters

import (
	"net/url"

	"api-gateway/pkg/config"
)

type RouteConfig struct {
	Service   *url.URL
	CheckResp bool

	SkipCookieCheck bool
	Description     string
	CheckPowers     []string
}

func SetKey(httpMethod, fullPath string) string {
	return httpMethod + "#" + fullPath
}

func GenerateRouter(cfg config.DumbServices) map[string]RouteConfig {
	var routes = make(map[string]RouteConfig, cfg.Count)

	for _, service := range cfg.Services {
		for _, handlers := range service.Handlers {
			routes[SetKey(string(handlers.Method), handlers.GWPath)] = RouteConfig{
				Service:         service.Host.SetPath(handlers.Path),
				CheckResp:       !service.NoCheck,
				SkipCookieCheck: !handlers.UseCookie,
				Description:     handlers.Description,
				CheckPowers:     handlers.CheckPowers,
			}
		}
	}

	clear(cfg.Services)

	return routes
}
