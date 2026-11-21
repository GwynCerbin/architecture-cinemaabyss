package adapters

import (
	"math/rand/v2"
	"net/url"
)

type RouteConfig struct {
	Share     int
	Service   *url.URL
	CheckResp bool

	SkipCookieCheck bool
	Description     string
	CheckPowers     []string
}

type Routes []RouteConfig

func SetKey(httpMethod, fullPath string) string {
	return httpMethod + "#" + fullPath
}

// SelectRoute selects one target from a validated 100-percent distribution.
func (routes Routes) SelectRoute() (RouteConfig, bool) {
	ticket := rand.IntN(100)
	for _, route := range routes {
		if ticket < route.Share {
			return route, true
		}

		ticket -= route.Share
	}

	return RouteConfig{}, false
}
