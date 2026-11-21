package auth

import (
	"api-gateway/internal/domain/entities"
	"api-gateway/internal/infra/auth/adapters"
	"errors"
	"github.com/golang-jwt/jwt/v5"
)

type Verifier struct{ cfg Config }

func NewVerifier(cfg Config) *Verifier    { return &Verifier{cfg: cfg} }
func (v *Verifier) GetAccessName() string { return v.cfg.AccessCookie }
func (v *Verifier) CheckAccessToken(raw string) (*entities.UserInfo, error) {
	if v == nil || !v.cfg.Enabled || v.cfg.VerificationKey == nil {
		return nil, errors.New("authentication disabled or unconfigured")
	}

	options := []jwt.ParserOption{jwt.WithValidMethods([]string{v.cfg.Algorithm}), jwt.WithExpirationRequired(), jwt.WithIssuedAt()}
	if v.cfg.Issuer != "" {
		options = append(options, jwt.WithIssuer(v.cfg.Issuer))
	}

	if v.cfg.Audience != "" {
		options = append(options, jwt.WithAudience(v.cfg.Audience))
	}

	claims := new(adapters.Claims)

	token, err := jwt.ParseWithClaims(raw, claims, func(*jwt.Token) (any, error) { return v.cfg.VerificationKey, nil }, options...)
	if err != nil {
		return nil, err
	}

	if !token.Valid || (claims.UserId == "" && claims.Subject == "") {
		return nil, errors.New("invalid token or missing subject")
	}

	return claims.ToEntity(), nil
}
