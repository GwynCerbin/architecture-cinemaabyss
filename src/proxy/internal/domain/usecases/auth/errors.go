package auth

import "errors"

var (
	ErrInvalidInput        = errors.New("invalid authentication input")
	ErrInvalidCredentials  = errors.New("invalid credentials")
	ErrInvalidRefreshToken = errors.New("invalid refresh token")
	ErrAccessDenied        = errors.New("authentication denied")
	ErrRateLimited         = errors.New("authentication rate limit exceeded")
	ErrUnavailable         = errors.New("authentication service unavailable")
	ErrInvalidResponse     = errors.New("invalid authentication service response")
)
