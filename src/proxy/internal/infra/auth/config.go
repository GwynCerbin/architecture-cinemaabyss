package auth

import (
	"crypto/ecdsa"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/go-playground/validator/v10"
)

type Endpoint struct {
	Method string `yaml:"method" validate:"required,oneof=POST PATCH PUT DELETE GET"`
	Path   string `yaml:"path" validate:"required,startswith=/"`
}

type Config struct {
	Enabled         bool                `env:"ENABLED" yaml:"enabled"`
	URL             string              `env:"URL" yaml:"url"`
	Timeout         time.Duration       `env:"TIMEOUT" yaml:"timeout"`
	PublicKey       string              `env:"PUBLIC_KEY" yaml:"public_key"`
	Algorithm       string              `env:"ALGORITHM" yaml:"algorithm"`
	Issuer          string              `env:"ISSUER" yaml:"issuer"`
	Audience        string              `env:"AUDIENCE" yaml:"audience"`
	AccessCookie    string              `env:"ACCESS_COOKIE" yaml:"access_cookie"`
	RefreshCookie   string              `env:"REFRESH_COOKIE" yaml:"refresh_cookie"`
	SecureCookies   bool                `env:"SECURE_COOKIES" yaml:"secure_cookies"`
	RefreshTTL      time.Duration       `env:"REFRESH_TTL" yaml:"refresh_ttl"`
	Endpoints       map[string]Endpoint `yaml:"endpoints" validate:"dive,keys,oneof=login logout refresh,endkeys"`
	VerificationKey any                 `env:"-" yaml:"-"`
}

func (c *Config) Validate() error {
	if !c.Enabled {
		return nil
	}

	if c.Timeout == 0 {
		c.Timeout = 5 * time.Second
	}

	if c.RefreshTTL == 0 {
		c.RefreshTTL = 30 * 24 * time.Hour
	}

	if c.Algorithm == "" {
		c.Algorithm = "RS256"
	}

	if c.AccessCookie == "" {
		c.AccessCookie = "access"
	}

	if c.RefreshCookie == "" {
		c.RefreshCookie = "refresh"
	}

	for _, name := range []string{c.AccessCookie, c.RefreshCookie} {
		if err := (&http.Cookie{Name: name, Value: "token"}).Valid(); err != nil {
			return fmt.Errorf("auth cookie: %w", err)
		}
	}

	if c.AccessCookie == c.RefreshCookie {
		return errors.New("auth cookie names must differ")
	}

	if c.Timeout <= 0 || c.RefreshTTL <= 0 {
		return errors.New("auth timeouts must be positive")
	}

	u, err := url.Parse(c.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("auth.url must be an HTTP(S) URL without credentials, query or fragment")
	}

	if err := validator.New().Struct(c); err != nil {
		return fmt.Errorf("auth config: %w", err)
	}

	for _, name := range []string{"login", "logout", "refresh"} {
		if _, ok := c.Endpoints[name]; !ok {
			return fmt.Errorf("auth endpoint %q is required", name)
		}
	}

	for name, ep := range c.Endpoints {
		path, err := url.Parse(ep.Path)
		if err != nil || path.IsAbs() || path.Host != "" || path.RawQuery != "" || path.Fragment != "" || strings.HasPrefix(ep.Path, "//") {
			return fmt.Errorf("auth endpoint %q must be a relative path", name)
		}
	}

	block, rest := pem.Decode([]byte(strings.ReplaceAll(c.PublicKey, `\n`, "\n")))
	if block == nil || strings.TrimSpace(string(rest)) != "" || block.Type != "PUBLIC KEY" {
		return errors.New("auth.public_key must contain one PEM PUBLIC KEY (PKIX)")
	}

	key, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return fmt.Errorf("auth public key: %w", err)
	}

	switch c.Algorithm {
	case "RS256":
		if _, ok := key.(*rsa.PublicKey); !ok {
			return errors.New("RS256 requires an RSA public key")
		}
	case "ES256":
		k, ok := key.(*ecdsa.PublicKey)
		if !ok || k.Curve.Params().BitSize != 256 {
			return errors.New("ES256 requires a P-256 public key")
		}
	default:
		return errors.New("auth.algorithm must be RS256 or ES256")
	}

	c.VerificationKey = key

	return nil
}
