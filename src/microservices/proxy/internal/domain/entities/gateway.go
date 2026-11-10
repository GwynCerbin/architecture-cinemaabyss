package entities

import (
	"context"
	"net/http"
	"net/url"
)

type Context struct {
	response http.ResponseWriter
	request  *http.Request
	context.Context
	gwAction   string
	serviceUri *url.URL
	checkResp  bool
	rules      []string
}

func (c *Context) GetAction() string {
	return c.gwAction
}

func (c *Context) GetRespWriter() http.ResponseWriter {
	return c.response
}

func (c *Context) CheckResponse() bool {
	return c.checkResp
}

func (c *Context) GetContext() context.Context {
	return c.request.Context()
}

func (c *Context) GetRequest() *http.Request {
	return c.request
}

func (c *Context) GetDestination() *url.URL {
	return c.serviceUri
}

func (c *Context) GetRules() []string {
	return c.rules
}

func (c *Context) SetHeaders(headers map[string]string) {
	for k, v := range headers {
		c.request.Header.Set(k, v)
	}
}

type ContextBuilder struct {
	Response      http.ResponseWriter
	Request       *http.Request
	GwAction      string
	Service       *url.URL
	CheckResponse bool
	Rules         []string
}

func (cb ContextBuilder) Build() *Context {
	return &Context{
		response:   cb.Response,
		request:    cb.Request,
		serviceUri: cb.Service,
		gwAction:   cb.GwAction,
		Context:    cb.Request.Context(),
		rules:      cb.Rules,
		checkResp:  cb.CheckResponse,
	}
}
