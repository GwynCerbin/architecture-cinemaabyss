package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"api-gateway/internal/domain/entities"
	authUC "api-gateway/internal/domain/usecases/auth"
	"api-gateway/internal/infra/auth/adapters"
)

type Client struct {
	client    *http.Client
	base      *url.URL
	endpoints map[string]Endpoint
}

func NewClient(cfg Config) (*Client, error) {
	base, err := url.Parse(cfg.URL)
	if err != nil {
		return nil, err
	}

	return &Client{
		client: &http.Client{
			Timeout:       cfg.Timeout,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		base:      base,
		endpoints: cfg.Endpoints,
	}, nil
}

func (c *Client) Close() { c.client.CloseIdleConnections() }

func (c *Client) Login(ctx context.Context, credentials entities.Credentials) (entities.Tokens, error) {
	var response adapters.TokensResponse
	if err := c.do(ctx, "login", adapters.FromCredentials(credentials), &response); err != nil {
		return entities.Tokens{}, err
	}

	return response.ToEntity(), nil
}

func (c *Client) Refresh(ctx context.Context, token entities.RefreshToken) (entities.Tokens, error) {
	var response adapters.TokensResponse
	if err := c.do(ctx, "refresh", adapters.FromRefreshToken(token), &response); err != nil {
		return entities.Tokens{}, err
	}

	return response.ToEntity(), nil
}

func (c *Client) Logout(ctx context.Context, token entities.RefreshToken) error {
	return c.do(ctx, "logout", adapters.FromRefreshToken(token), nil)
}

func (c *Client) do(ctx context.Context, operation string, input, output any) error {
	endpoint, ok := c.endpoints[operation]
	if !ok {
		return fmt.Errorf("%w: endpoint %q missing", authUC.ErrUnavailable, operation)
	}

	body, err := json.Marshal(input)
	if err != nil {
		return fmt.Errorf("%w: encode request", authUC.ErrInvalidInput)
	}

	target := c.base.JoinPath(endpoint.Path)

	req, err := http.NewRequestWithContext(ctx, endpoint.Method, target.String(), bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("%w: create request", authUC.ErrUnavailable)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %w", authUC.ErrUnavailable, err)
	}

	defer resp.Body.Close()

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		if output == nil {
			_, err = io.Copy(io.Discard, resp.Body)
			if err != nil {
				return fmt.Errorf("%w: read response", authUC.ErrInvalidResponse)
			}

			return nil
		}

		decoder := json.NewDecoder(resp.Body)
		if err = decoder.Decode(output); err != nil {
			return fmt.Errorf("%w: decode tokens", authUC.ErrInvalidResponse)
		}

		var extra any
		if err = decoder.Decode(&extra); err != io.EOF {
			return fmt.Errorf("%w: unexpected trailing response data", authUC.ErrInvalidResponse)
		}

		return nil
	case resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusUnauthorized:
		if operation == "login" {
			return authUC.ErrInvalidCredentials
		}

		return authUC.ErrInvalidRefreshToken
	case resp.StatusCode == http.StatusForbidden:
		return authUC.ErrAccessDenied
	case resp.StatusCode == http.StatusTooManyRequests:
		return authUC.ErrRateLimited
	default:
		return fmt.Errorf("%w: upstream status %d", authUC.ErrUnavailable, resp.StatusCode)
	}
}
