package script

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strings"
	"time"

	"github.com/metacubex/mihomo/component/ca"
	mihomoHTTP "github.com/metacubex/mihomo/component/http"

	"github.com/grafana/sobek"
	"github.com/metacubex/http"
	"github.com/metacubex/tls"
)

type httpClientRequest struct {
	url          string
	headers      http.Header
	host         *string
	body         []byte
	timeout      time.Duration
	insecure     bool
	autoCookie   bool
	autoRedirect bool
	binaryMode   bool
	policy       string
}

type httpClientResult struct {
	err        error
	statusCode int
	headers    http.Header
	body       []byte
}

func (h *runtimeHost) httpClientObject() *sobek.Object {
	object := h.vm.NewObject()
	for _, method := range []string{
		http.MethodGet,
		http.MethodPost,
		http.MethodPut,
		http.MethodDelete,
		http.MethodHead,
		http.MethodOptions,
		http.MethodPatch,
	} {
		methodCopy := method
		_ = object.Set(strings.ToLower(methodCopy), func(call sobek.FunctionCall) sobek.Value {
			h.scheduleHTTPRequest(methodCopy, call)
			return sobek.Undefined()
		})
	}
	return object
}

func (h *runtimeHost) scheduleHTTPRequest(method string, call sobek.FunctionCall) {
	if len(call.Arguments) < 2 {
		panic(h.vm.NewTypeError("$httpClient.%s requires options and a callback", strings.ToLower(method)))
	}
	callback, ok := sobek.AssertFunction(call.Argument(1))
	if !ok {
		panic(h.vm.NewTypeError("$httpClient.%s callback must be a function", strings.ToLower(method)))
	}
	request, err := parseHTTPClientRequest(call.Argument(0))
	if err != nil {
		panic(h.vm.NewTypeError("$httpClient.%s: %s", strings.ToLower(method), err.Error()))
	}
	go func() {
		result := h.performHTTPRequestSafely(method, request)
		job := func(host *runtimeHost) error {
			return host.invokeHTTPCallback(callback, request.binaryMode, result)
		}
		select {
		case h.jobs <- job:
		case <-h.ctx.Done():
		}
	}()
}

func (h *runtimeHost) performHTTPRequestSafely(method string, request httpClientRequest) (result httpClientResult) {
	defer func() {
		if recovered := recover(); recovered != nil {
			if recoveredErr, ok := recovered.(error); ok {
				result = httpClientResult{err: fmt.Errorf("http client panic: %w", recoveredErr)}
			} else {
				result = httpClientResult{err: fmt.Errorf("http client panic: %v", recovered)}
			}
		}
	}()
	return h.performHTTPRequest(method, request)
}

func parseHTTPClientRequest(value sobek.Value) (httpClientRequest, error) {
	request := httpClientRequest{
		timeout:      5 * time.Second,
		autoCookie:   true,
		autoRedirect: true,
	}
	exported := value.Export()
	if text, ok := exported.(string); ok {
		request.url = text
		return request, nil
	}
	options, ok := exported.(map[string]any)
	if !ok {
		return request, fmt.Errorf("options must be a URL string or object")
	}
	urlValue, ok := options["url"].(string)
	if !ok || urlValue == "" {
		return request, fmt.Errorf("options.url is required")
	}
	request.url = urlValue
	if value, found := options["headers"]; found {
		headers, host, err := parseHeaders(value, true)
		if err != nil {
			return request, fmt.Errorf("options.headers: %w", err)
		}
		request.headers = headers
		request.host = host
	}
	if value, found := options["body"]; found {
		body, contentType, err := httpClientBody(value)
		if err != nil {
			return request, fmt.Errorf("options.body: %w", err)
		}
		request.body = body
		if contentType != "" {
			if request.headers == nil {
				request.headers = make(http.Header)
			}
			if request.headers.Get("Content-Type") == "" {
				request.headers.Set("Content-Type", contentType)
			}
		}
	}
	if value, found := options["timeout"]; found {
		seconds, err := numericOption(value)
		if err != nil || seconds <= 0 || math.IsNaN(seconds) || math.IsInf(seconds, 0) || seconds > float64(math.MaxInt64)/float64(time.Second) {
			return request, fmt.Errorf("options.timeout must be a positive number")
		}
		request.timeout = time.Duration(seconds * float64(time.Second))
	}
	if value, found := options["insecure"]; found {
		request.insecure, ok = value.(bool)
		if !ok {
			return request, fmt.Errorf("options.insecure must be a boolean")
		}
	}
	if value, found := options["auto-cookie"]; found {
		request.autoCookie, ok = value.(bool)
		if !ok {
			return request, fmt.Errorf("options.auto-cookie must be a boolean")
		}
	}
	if value, found := options["auto-redirect"]; found {
		request.autoRedirect, ok = value.(bool)
		if !ok {
			return request, fmt.Errorf("options.auto-redirect must be a boolean")
		}
	}
	if value, found := options["binary-mode"]; found {
		request.binaryMode, ok = value.(bool)
		if !ok {
			return request, fmt.Errorf("options.binary-mode must be a boolean")
		}
	}
	if value, found := options["policy"]; found {
		request.policy, ok = value.(string)
		if !ok {
			return request, fmt.Errorf("options.policy must be a string")
		}
	}
	return request, nil
}

func httpClientBody(value any) ([]byte, string, error) {
	switch typed := value.(type) {
	case string:
		return []byte(typed), "", nil
	case []byte:
		return append([]byte(nil), typed...), "", nil
	case nil:
		return nil, "", nil
	default:
		content, err := json.Marshal(typed)
		if err != nil {
			return nil, "", err
		}
		return content, "application/json", nil
	}
}

func numericOption(value any) (float64, error) {
	switch typed := value.(type) {
	case int64:
		return float64(typed), nil
	case int:
		return float64(typed), nil
	case float64:
		return typed, nil
	default:
		return 0, fmt.Errorf("must be a number")
	}
}

func (h *runtimeHost) performHTTPRequest(method string, request httpClientRequest) httpClientResult {
	ctx, cancel := context.WithTimeout(h.ctx, request.timeout)
	defer cancel()
	var body io.Reader
	if request.body != nil {
		body = bytes.NewReader(request.body)
	}
	options := []mihomoHTTP.Option{
		mihomoHTTP.WithSpecialProxy(request.policy),
		mihomoHTTP.WithAutoRedirect(request.autoRedirect),
	}
	if request.host != nil {
		options = append(options, mihomoHTTP.WithHost(*request.host))
	}
	if request.autoCookie {
		options = append(options, mihomoHTTP.WithCookieJar(h.jar))
	}
	if request.insecure {
		options = append(options, mihomoHTTP.WithCAOption(ca.Option{
			TLSConfig: &tls.Config{InsecureSkipVerify: true},
		}))
	}
	response, err := mihomoHTTP.HttpRequest(ctx, request.url, method, request.headers, body, options...)
	if err != nil {
		return httpClientResult{err: err}
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		return httpClientResult{err: err}
	}
	return httpClientResult{
		statusCode: response.StatusCode,
		headers:    response.Header.Clone(),
		body:       responseBody,
	}
}

func (h *runtimeHost) invokeHTTPCallback(callback sobek.Callable, binaryMode bool, result httpClientResult) error {
	if result.err != nil {
		_, err := callback(
			sobek.Undefined(),
			h.vm.ToValue(result.err.Error()),
			sobek.Null(),
			sobek.Null(),
		)
		return err
	}
	response := h.vm.NewObject()
	_ = response.Set("status", result.statusCode)
	_ = response.Set("statusCode", result.statusCode)
	_ = response.Set("headers", h.headersObject(result.headers, ""))
	var data sobek.Value
	if binaryMode {
		content := append([]byte(nil), result.body...)
		buffer := h.vm.NewArrayBuffer(content)
		array, err := h.vm.New(h.vm.Get("Uint8Array"), h.vm.ToValue(buffer))
		if err != nil {
			return err
		}
		data = array
	} else {
		data = h.vm.ToValue(string(result.body))
	}
	_, err := callback(sobek.Undefined(), sobek.Null(), response, data)
	return err
}
