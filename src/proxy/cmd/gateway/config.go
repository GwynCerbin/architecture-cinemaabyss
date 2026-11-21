package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"slices"
	"strings"

	"api-gateway/internal/controller/httpserver"
	gatewayAdapters "api-gateway/internal/controller/httpserver/handlers/v1/gateway/adapters"
	authInfra "api-gateway/internal/infra/auth"
	"api-gateway/pkg/types"

	"github.com/caarlos0/env/v10"
	"github.com/go-playground/validator/v10"
	"github.com/joho/godotenv"
	"gopkg.in/yaml.v3"
)

type Config struct {
	MoviesMigrationPercent *int              `env:"MOVIES_MIGRATION_PERCENT" yaml:"-"`
	HTTPServer             httpserver.Config `yaml:"-"`
	Auth                   authInfra.Config  `envPrefix:"AUTH_" yaml:"auth"`
	Path                   string            `env:"CONFIG_PATH" envDefault:"config.yaml"`
	RestServer             *Router           `yaml:"rest_server"`
	Debug                  bool              `env:"DEBUG" yaml:"debug"`
}

func configParseYaml(cfg *Config) error {
	file, err := os.Open(cfg.Path)
	if err != nil {
		return err
	}

	defer func() {
		if err = file.Close(); err != nil {
			log.Printf("close config file: %v", err)
		}
	}()

	decoder := yaml.NewDecoder(file)
	decoder.KnownFields(true)

	return decoder.Decode(cfg)
}

func initConfig() (cfg *Config, err error) {
	cfg = new(Config)

	if err = godotenv.Load(); err != nil {
		log.Printf("load .env file: %v", err)
	}

	cfg.Path = os.Getenv("CONFIG_PATH")
	if cfg.Path == "" {
		cfg.Path = "config.yaml"
	}
	{
		if err = configParseYaml(cfg); err != nil {
			return nil, fmt.Errorf("config reading: %w", err)
		}
	}

	if err = env.Parse(cfg); err != nil {
		return nil, fmt.Errorf("environment parsing: %w", err)
	}

	if err = cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (cfg *Config) Validate() error {
	if cfg.RestServer == nil {
		return errors.New("empty rest config")
	}

	if err := cfg.applyMoviesMigration(); err != nil {
		return err
	}

	if err := cfg.RestServer.Validate(); err != nil {
		return fmt.Errorf("rest server config: %w", err)
	}

	if err := cfg.Auth.Validate(); err != nil {
		return err
	}

	if !cfg.Auth.Enabled {
		for _, service := range cfg.RestServer.Gateway.Services {
			for _, handler := range service.Handlers {
				if handler.UseCookie {
					return fmt.Errorf("auth is disabled, but route %s has use_cookie=true", handler.GWPath)
				}
			}
		}
	}

	return nil
}

// applyMoviesMigration adapts the assignment's env setting to the configured route shares.
func (cfg *Config) applyMoviesMigration() error {
	if cfg.MoviesMigrationPercent == nil {
		return nil
	}

	percent := *cfg.MoviesMigrationPercent
	if percent < 0 || percent > 100 {
		return errors.New("MOVIES_MIGRATION_PERCENT must be between 0 and 100")
	}

	targets := make(map[string]map[string]*Handler)

	for _, service := range cfg.RestServer.Gateway.Services {
		for i := range service.Handlers {
			handler := &service.Handlers[i]
			if strings.Trim(handler.GWPath, "/") != "api/movies" {
				continue
			}

			if service.Name != "Monolith" && service.Name != "Movies" {
				return fmt.Errorf("MOVIES_MIGRATION_PERCENT: unexpected movies target %q", service.Name)
			}

			method := string(handler.Method)
			if targets[method] == nil {
				targets[method] = make(map[string]*Handler)
			}

			if targets[method][service.Name] != nil {
				return fmt.Errorf("MOVIES_MIGRATION_PERCENT: duplicate %s target for %s /api/movies", service.Name, method)
			}

			targets[method][service.Name] = handler
		}
	}

	if len(targets) == 0 {
		return errors.New("MOVIES_MIGRATION_PERCENT requires /api/movies routes")
	}

	for method, services := range targets {
		if services["Monolith"] == nil || services["Movies"] == nil {
			return fmt.Errorf("MOVIES_MIGRATION_PERCENT requires Monolith and Movies targets for %s /api/movies", method)
		}
	}

	for _, services := range targets {
		monolithShare, moviesShare := 100-percent, percent
		services["Monolith"].Share = &monolithShare
		services["Movies"].Share = &moviesShare
	}

	return nil
}

type Router struct {
	User    string  `env:"USER" yaml:"-"` // no usage, created for basic auth
	Pass    string  `env:"PASS" yaml:"-"` // no usage, created for basic auth
	Gateway Gateway `env:"GATEWAY" yaml:"gateway"`
}

type Gateway struct {
	Services []Service `envPrefix:"SERVICES" yaml:"services" validate:"dive"`
	Count    int       `yaml:"-"`
}

type Service struct {
	Name        string      `yaml:"name"`
	Description string      `yaml:"description"`
	Host        types.MyUrl `yaml:"host"`
	NoCheck     bool        `yaml:"no_check"`
	Handlers    []Handler   `yaml:"handlers" validate:"dive"`
}

type Handler struct {
	Share       *int             `yaml:"share,omitempty" validate:"omitempty,gte=0,lte=100"`
	GWPath      string           `yaml:"gw_path"`
	Path        string           `yaml:"path"`
	Method      types.HttpMethod `yaml:"method"`
	Description string           `yaml:"description"`

	UseCookie   bool     `yaml:"use_cookie" validate:"required_with=CheckPowers"`
	CheckPowers []string `yaml:"check_power" validate:"unique,dive,required"`
}

func (b *Router) Validate() error {
	if err := validator.New().Struct(b); err != nil {
		return fmt.Errorf("router validation: %w", err)
	}

	type routePolicy struct {
		share     int
		explicit  int
		handlers  []*Handler
		useCookie bool
		powers    []string
	}

	policies := make(map[string]routePolicy)

	var counter int

	for _, services := range b.Gateway.Services {
		for i, v := range services.Handlers {
			counter++

			path, err := validateGwPath(services.Name, v.GWPath)
			if err != nil {
				return err
			}

			services.Handlers[i].GWPath = path
			key := string(v.Method) + "#" + path
			powers := slices.Clone(v.CheckPowers)
			slices.Sort(powers)

			policy, exists := policies[key]
			if exists && (policy.useCookie != v.UseCookie || !slices.Equal(policy.powers, powers)) {
				return fmt.Errorf("route %s has conflicting authentication requirements", key)
			}

			if v.Share != nil {
				policy.share += *v.Share
				policy.explicit++
			}

			policy.handlers = append(policy.handlers, &services.Handlers[i])
			policy.useCookie = v.UseCookie
			policy.powers = powers
			policies[key] = policy
		}
	}

	for key, policy := range policies {
		if policy.explicit == 0 {
			continue
		}

		if policy.explicit != len(policy.handlers) {
			return fmt.Errorf("route %s must specify share for all targets or none", key)
		}

		if policy.share != 100 {
			return fmt.Errorf("route %s shares must sum to 100, got %d", key, policy.share)
		}
	}

	for _, policy := range policies {
		if policy.explicit != 0 {
			continue
		}

		share, remainder := 100/len(policy.handlers), 100%len(policy.handlers)
		for i, handler := range policy.handlers {
			value := share
			if i < remainder {
				value++
			}

			handler.Share = &value
		}
	}

	b.Gateway.Count = counter

	return nil
}

func validateGwPath(service, gwPath string) (string, error) {
	const gwPathCountCheck = 2

	switch ss := strings.Split(strings.Trim(gwPath, "/"), "/"); {
	case len(ss) < gwPathCountCheck:
		return "", fmt.Errorf("invalid path, not valid count of args: %s", service)
	case ss[0] != "api":
		return "", fmt.Errorf("gateway path must start with /api/: %s", service)
	default:
		return "/" + strings.Join(ss, "/"), nil
	}
}

func (cfg Gateway) Routes() map[string]gatewayAdapters.Routes {
	var routes = make(map[string]gatewayAdapters.Routes, cfg.Count)

	for _, service := range cfg.Services {
		for _, handlers := range service.Handlers {
			key := gatewayAdapters.SetKey(string(handlers.Method), handlers.GWPath)
			routes[key] = append(routes[key], gatewayAdapters.RouteConfig{
				Share:           *handlers.Share,
				Service:         service.Host.SetPath(handlers.Path),
				CheckResp:       !service.NoCheck,
				SkipCookieCheck: !handlers.UseCookie,
				Description:     handlers.Description,
				CheckPowers:     handlers.CheckPowers,
			})
		}
	}

	return routes
}
