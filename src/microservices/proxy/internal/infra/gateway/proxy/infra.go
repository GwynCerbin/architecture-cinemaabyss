package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"strconv"

	"api-gateway/internal/domain/entities"
	"api-gateway/pkg/ginbinder"
)

type ketCtx string

const respModifier ketCtx = "response_modifier"

type Infra struct {
	rp *httputil.ReverseProxy
}

func New() *Infra {
	return &Infra{
		rp: &httputil.ReverseProxy{
			ErrorHandler: errorHandler,
			Director: func(req *http.Request) {
				req.Header.Del("Cookie") // deletes header cookie
			},
			ModifyResponse: func(resp *http.Response) error {

				raw := resp.Request.Context().Value(respModifier)

				if raw == nil || !raw.(bool) {
					return nil
				}

				if resp.StatusCode < 200 || resp.StatusCode >= 400 {
					if err := modifyResponse(resp); err != nil {
						return err
					}
				}

				return nil
			},
		},
	}
}

func (rp *Infra) Provide(ctx *entities.Context) {
	req := ctx.GetRequest()
	req.Header.Set("X-Forwarded-Host", req.Host)

	{
		uri := ctx.GetDestination()
		req.URL.Host = uri.Host
		req.URL.Scheme = uri.Scheme
		req.URL.Path = uri.Path
		req.Host = uri.Host
	}

	req = req.WithContext(context.WithValue(req.Context(), respModifier, ctx.CheckResponse()))

	rp.rp.ServeHTTP(ctx.GetRespWriter(), req)
}

func errorHandler(writer http.ResponseWriter, _ *http.Request, err error) {
	log.Printf("error handler proxy: %v", err)
	writer.WriteHeader(http.StatusInternalServerError)

	if err = json.NewEncoder(writer).Encode(ginbinder.Response{
		Success: false,
		Message: "internal server error",
	}); err != nil {
		log.Printf("encode error: encode error handler: %v", err)
	}
}

func modifyResponse(resp *http.Response) error {
	resp.Header.Set("Content-Type", "application/json")

	data, err := io.ReadAll(resp.Body)
	if err := resp.Body.Close(); err != nil {
		log.Printf("close response body on unsuccess proxy body: %v", err)
	}

	if err != nil {
		return fmt.Errorf("read response body on unsuccess proxy body: %w", err)
	}

	data, err = json.Marshal(ginbinder.Response{
		Success: false,
		Message: string(data),
	})
	if err != nil {
		return fmt.Errorf("marshal response body on unsuccess proxy body: %w", err)
	}
	{
		ln := int64(len(data))
		resp.ContentLength = ln
		resp.Header.Set("Content-Length", strconv.FormatInt(ln, 10))
	}

	resp.Body = io.NopCloser(bytes.NewReader(data))

	return nil
}
