package types

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

type HttpMethod string

func (m *HttpMethod) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return fmt.Errorf("unmarshal http method: %w", err)
	}

	switch s = strings.ToUpper(strings.TrimSpace(s)); s {
	case http.MethodGet, http.MethodHead, http.MethodPut, http.MethodPatch, http.MethodPost, http.MethodDelete:
	default:
		return fmt.Errorf("invalid service method: %+v", s)
	}

	*m = HttpMethod(s)

	return nil
}

type MyUrl struct {
	*url.URL
}

func (m *MyUrl) SetPath(path string) *url.URL {
	path = "/" + strings.Trim(path, "/")
	res := *m.URL
	res.Path = path

	return &res
}

func (m *MyUrl) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return fmt.Errorf("unmarshal string MyUrl: %w", err)
	}

	if err := m.marshaller(s); err != nil {
		return fmt.Errorf("failed to validate: %w", err)
	}

	return nil
}

func (m *MyUrl) UnmarshalText(v []byte) error {
	var s = string(v)
	if err := m.marshaller(s); err != nil {
		return fmt.Errorf("failed to validate: %w", err)
	}

	return nil
}

func (m *MyUrl) marshaller(s string) error {
	if !strings.HasPrefix(s, "http") {
		return fmt.Errorf("invalid service url, only http/https schema should be http|https://url: %s", s)
	}

	parsedURL, err := url.Parse(s)
	if err != nil {
		return fmt.Errorf("unmarshal URL, incorrect host:%s, %w", s, err)
	}

	*m = MyUrl{parsedURL}

	return nil
}
