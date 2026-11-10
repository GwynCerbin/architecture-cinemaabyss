package config

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"api-gateway/internal/controller/httpserver"
	authInfra "api-gateway/internal/infra/auth"
	"api-gateway/pkg/types"

	"github.com/caarlos0/env/v10"
	"github.com/go-playground/validator/v10"
	"github.com/joho/godotenv"
	"gopkg.in/yaml.v3"
)

//go:generate mockgen -source=config.go -destination=mocks.go -package=config

type Config struct {
	Auth       authInfra.Config `envPrefix:"AUTH_" yaml:"auth"`
	Path       string           `env:"CONFIG_PATH" envDefault:"config.yaml"`
	RestServer *Router          `envPrefix:"REST_SERVER_" yaml:"rest_server"`
	Debug      bool             `env:"DEBUG" yaml:"debug"`
}

type Router struct {
	User              string `env:"USER" yaml:"-"` // no usage, created for basic auth
	Pass              string `env:"PASS" yaml:"-"` // no usage, created for basic auth
	httpserver.Config `yaml:",inline"`
	DumbServices      DumbServices `env:"DUMB_GATEWAY" yaml:"dumb_gateway"`
}

type DumbServices struct {
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

	var counter int

	for _, services := range b.DumbServices.Services {
		for i, v := range services.Handlers {
			counter++

			path, err := validateGwPath(services.Name, v.GWPath)
			if err != nil {
				return err
			}

			services.Handlers[i].GWPath = path
		}
	}

	b.DumbServices.Count = counter

	return nil
}

func validateGwPath(service, gwPath string) (string, error) {
	const gwPathCountCheck = 3

	switch ss := strings.Split(strings.Trim(gwPath, "/"), "/"); {
	case len(ss) < gwPathCountCheck:
		return "", fmt.Errorf("invalid path, not valid count of args: %s", service)
	case !strings.HasPrefix(ss[0], "v"):
		return "", fmt.Errorf("invalid path should start with version: %s", service)
	case !strings.HasSuffix(ss[1], "gateway"):
		return "", fmt.Errorf("invalid path should have the second word gateway: %s", service)
	default:
		return "/" + strings.Join(ss, "/"), nil
	}
}

type Client struct {
	Url      types.MyUrl   `env:"HOST" yaml:"host"`
	Timeout  time.Duration `env:"TIMEOUT" yaml:"timeout"`
	Username string        `env:"USERNAME" yaml:"-"` // no usage, created for basic auth
	Password string        `env:"PASSWORD" yaml:"-"` // no usage, created for basic auth
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

func Init() (cfg *Config, err error) {
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

	if err := cfg.RestServer.Validate(); err != nil {
		return fmt.Errorf("rest server config: %w", err)
	}

	if err := cfg.Auth.Validate(); err != nil {
		return err
	}

	if !cfg.Auth.Enabled {
		for _, service := range cfg.RestServer.DumbServices.Services {
			for _, handler := range service.Handlers {
				if handler.UseCookie {
					return fmt.Errorf("auth is disabled, but route %s has use_cookie=true", handler.GWPath)
				}
			}
		}
	}

	return nil
}
