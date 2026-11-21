package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"time"
)

type Client struct {
	client *http.Client
	uri    *url.URL
}

func NewClient(uri *url.URL, timeout time.Duration) *Client {
	return &Client{
		client: &http.Client{
			Transport: http.DefaultTransport,
			Timeout:   timeout,
		},
		uri: uri,
	}
}

func (c *Client) Ping(path string) error {
	const pingTimeout = 5 * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.uri.JoinPath(path).String(), http.NoBody)
	if err != nil {
		return fmt.Errorf("create ping request: %w", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}

	if err = resp.Body.Close(); err != nil {
		log.Printf("close response body: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ping client status: %w", fmt.Errorf("unexpected response code %d", resp.StatusCode))
	}

	return nil
}

func (c *Client) Get(ctx context.Context, path string, headers ...map[string]string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.uri.JoinPath(path).String(), http.NoBody)
	if err != nil {
		return nil, err
	}

	setUpHeaders(req, headers)

	return c.client.Do(req)
}

func (c *Client) Post(ctx context.Context, path string, data interface{}, headers ...map[string]string) (*http.Response, error) {
	body, err := jsonMarshaller(data)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.uri.JoinPath(path).String(), body)
	if err != nil {
		return nil, err
	}

	setUpHeaders(req, headers)

	return c.client.Do(req)
}

func (c *Client) Put(ctx context.Context, path string, data interface{}, headers ...map[string]string) (*http.Response, error) {
	body, err := jsonMarshaller(data)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.uri.JoinPath(path).String(), body)
	if err != nil {
		return nil, err
	}

	setUpHeaders(req, headers)

	return c.client.Do(req)
}

func (c *Client) Delete(ctx context.Context, path string, headers ...map[string]string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.uri.JoinPath(path).String(), http.NoBody)
	if err != nil {
		return nil, err
	}

	setUpHeaders(req, headers)

	return c.client.Do(req)
}

func (c *Client) Patch(ctx context.Context, path string, data interface{}, headers ...map[string]string) (*http.Response, error) {
	body, err := jsonMarshaller(data)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, c.uri.JoinPath(path).String(), body)
	if err != nil {
		return nil, err
	}

	setUpHeaders(req, headers)

	return c.client.Do(req)
}

func (c *Client) Close() {
	c.client.CloseIdleConnections()
}

func jsonMarshaller(meta interface{}) (io.Reader, error) {
	data, err := json.Marshal(meta)
	if err != nil {
		return nil, err
	}

	return bytes.NewReader(data), nil
}

func setUpHeaders(req *http.Request, headers []map[string]string) {
	if len(headers) > 0 {
		for header, value := range headers[0] {
			req.Header.Set(header, value)
		}
	}

	req.Header.Set("Content-Type", "application/json")
}
