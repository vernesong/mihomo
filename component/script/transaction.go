package script

import (
	"bytes"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"

	R "github.com/metacubex/mihomo/component/rewrite"
	"github.com/metacubex/mihomo/log"

	"github.com/metacubex/http"
	"golang.org/x/net/http/httpguts"
)

func (m *Manager) ProcessRequest(request *http.Request, requestID string) (*http.Response, bool) {
	if m == nil || request == nil || request.URL == nil {
		return nil, false
	}
	requestURL := absoluteRequestURL(request)
	matched := matchingEntries(m.requestEntries, requestURL)
	for _, scriptEntry := range matched {
		response, abort, skipped, err := m.processRequest(scriptEntry, request, requestID)
		if skipped {
			continue
		}
		if err != nil {
			log.Errorln("[Script] %s request execution error: %s", scriptEntry.name, err.Error())
			continue
		}
		if abort || response != nil {
			return response, abort
		}
	}
	return nil, false
}

func (m *Manager) ProcessResponse(request *http.Request, response *http.Response, requestID string) bool {
	if m == nil || request == nil || request.URL == nil || response == nil {
		return false
	}
	requestURL := absoluteRequestURL(request)
	matched := matchingEntries(m.responseEntries, requestURL)
	for _, scriptEntry := range matched {
		abort, skipped, err := m.processResponse(scriptEntry, request, response, requestID)
		if skipped {
			continue
		}
		if err != nil {
			log.Errorln("[Script] %s response execution error: %s", scriptEntry.name, err.Error())
			continue
		}
		if abort {
			return true
		}
	}
	return false
}

func matchingEntries(entries []*entry, requestURL string) []*entry {
	matched := make([]*entry, 0, len(entries))
	for _, scriptEntry := range entries {
		if scriptEntry.enable && scriptEntry.match.MatchString(requestURL) {
			matched = append(matched, scriptEntry)
		}
	}
	return matched
}

func (m *Manager) processRequest(scriptEntry *entry, request *http.Request, requestID string) (*http.Response, bool, bool, error) {
	body, bodyPresent, tooLarge, err := prepareBody(
		&request.Body,
		request.ContentLength,
		scriptEntry.requiresBody,
		scriptEntry.maxBodySize,
	)
	if err != nil {
		return nil, false, false, err
	}
	if tooLarge {
		if scriptEntry.debug {
			log.Infoln("[Script] %s skipped: request body exceeds max-body-size", scriptEntry.name)
		}
		return nil, false, true, nil
	}
	body, bodyEncoding, tooLarge, err := decodeScriptBody(body, bodyPresent, request.Header, scriptEntry.maxBodySize)
	if err != nil {
		return nil, false, false, err
	}
	if tooLarge {
		if scriptEntry.debug {
			log.Infoln("[Script] %s skipped: decoded request body exceeds max-body-size", scriptEntry.name)
		}
		return nil, false, true, nil
	}

	patch, err := m.evaluate(scriptEntry, evaluationInput{
		request:     request,
		requestID:   requestID,
		body:        body,
		bodyPresent: bodyPresent,
	})
	if err != nil {
		return nil, false, false, err
	}
	if patch == nil {
		return nil, false, false, nil
	}
	mutation, err := parseRequestMutation(patch, scriptEntry.requiresBody, request)
	if err != nil {
		return nil, false, false, err
	}
	if mutation.setBody {
		targetHeaders := request.Header
		if mutation.headers != nil {
			targetHeaders = mutation.headers
		}
		mutation.body, err = encodeScriptBody(mutation.body, bodyEncoding, targetHeaders)
		if err != nil {
			return nil, false, false, fmt.Errorf("encode $done.body: %w", err)
		}
	}
	mutation.apply(request)
	return mutation.response, mutation.abort, false, nil
}

func (m *Manager) processResponse(scriptEntry *entry, request *http.Request, response *http.Response, requestID string) (bool, bool, error) {
	requiresBody := scriptEntry.requiresBody && request.Method != http.MethodHead && statusAllowsBody(response.StatusCode)
	body, bodyPresent, tooLarge, err := prepareBody(
		&response.Body,
		response.ContentLength,
		requiresBody,
		scriptEntry.maxBodySize,
	)
	if err != nil {
		return false, false, err
	}
	if tooLarge {
		if scriptEntry.debug {
			log.Infoln("[Script] %s skipped: response body exceeds max-body-size", scriptEntry.name)
		}
		return false, true, nil
	}
	body, bodyEncoding, tooLarge, err := decodeScriptBody(body, bodyPresent, response.Header, scriptEntry.maxBodySize)
	if err != nil {
		return false, false, err
	}
	if tooLarge {
		if scriptEntry.debug {
			log.Infoln("[Script] %s skipped: decoded response body exceeds max-body-size", scriptEntry.name)
		}
		return false, true, nil
	}

	patch, err := m.evaluate(scriptEntry, evaluationInput{
		request:     request,
		response:    response,
		requestID:   requestID,
		body:        body,
		bodyPresent: bodyPresent,
	})
	if err != nil {
		return false, false, err
	}
	if patch == nil {
		return false, false, nil
	}
	mutation, err := parseResponseMutation(patch, requiresBody)
	if err != nil {
		return false, false, err
	}
	if mutation.setBody {
		targetHeaders := response.Header
		if mutation.headers != nil {
			targetHeaders = mutation.headers
		}
		mutation.body, err = encodeScriptBody(mutation.body, bodyEncoding, targetHeaders)
		if err != nil {
			return false, false, fmt.Errorf("encode $done.body: %w", err)
		}
	}
	mutation.apply(response)
	return mutation.abort, false, nil
}

type requestMutation struct {
	url      *url.URL
	headers  http.Header
	host     *string
	body     []byte
	setBody  bool
	response *http.Response
	abort    bool
}

func parseRequestMutation(patch map[string]any, allowBody bool, request *http.Request) (*requestMutation, error) {
	mutation := &requestMutation{}
	if value, found := patch["url"]; found {
		text, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("$done.url must be a string")
		}
		parsed, err := url.Parse(text)
		if err != nil || parsed.Scheme != "http" && parsed.Scheme != "https" || parsed.Host == "" {
			return nil, fmt.Errorf("$done.url is not a valid absolute HTTP URL")
		}
		mutation.url = parsed
	}
	if value, found := patch["headers"]; found {
		headers, host, err := parseHeaders(value, true)
		if err != nil {
			return nil, fmt.Errorf("$done.headers: %w", err)
		}
		mutation.headers = headers
		mutation.host = host
	}
	if allowBody {
		if value, found := patch["body"]; found {
			body, err := resultBody(value)
			if err != nil {
				return nil, fmt.Errorf("$done.body: %w", err)
			}
			mutation.body = body
			mutation.setBody = true
		}
	}
	if value, found := patch["response"]; found {
		response, err := parseMockResponse(value, request)
		if err != nil {
			return nil, fmt.Errorf("$done.response: %w", err)
		}
		mutation.response = response
	}
	if value, found := patch["abort"]; found {
		abort, ok := value.(bool)
		if !ok {
			return nil, fmt.Errorf("$done.abort must be a boolean")
		}
		mutation.abort = abort
	}
	return mutation, nil
}

func (m *requestMutation) apply(request *http.Request) {
	if m.url != nil {
		request.URL = m.url
	}
	if m.headers != nil {
		request.Header = m.headers
		if m.host != nil {
			request.Host = *m.host
		}
	}
	if m.setBody {
		setRequestBody(request, m.body)
	}
}

type responseMutation struct {
	status  int
	headers http.Header
	body    []byte
	setBody bool
	abort   bool
}

func parseResponseMutation(patch map[string]any, allowBody bool) (*responseMutation, error) {
	mutation := &responseMutation{}
	if value, found := patch["status"]; found {
		status, err := resultStatus(value)
		if err != nil {
			return nil, fmt.Errorf("$done.status: %w", err)
		}
		mutation.status = status
	}
	if value, found := patch["headers"]; found {
		headers, _, err := parseHeaders(value, false)
		if err != nil {
			return nil, fmt.Errorf("$done.headers: %w", err)
		}
		mutation.headers = headers
	}
	if allowBody {
		if value, found := patch["body"]; found {
			body, err := resultBody(value)
			if err != nil {
				return nil, fmt.Errorf("$done.body: %w", err)
			}
			mutation.body = body
			mutation.setBody = true
		}
	}
	if value, found := patch["abort"]; found {
		abort, ok := value.(bool)
		if !ok {
			return nil, fmt.Errorf("$done.abort must be a boolean")
		}
		mutation.abort = abort
	}
	return mutation, nil
}

func (m *responseMutation) apply(response *http.Response) {
	if m.status != 0 {
		response.StatusCode = m.status
		response.Status = fmt.Sprintf("%d %s", m.status, http.StatusText(m.status))
	}
	if m.headers != nil {
		response.Header = m.headers
	}
	if m.setBody {
		setResponseBody(response, m.body)
	}
	if !statusAllowsBody(response.StatusCode) {
		setResponseBody(response, nil)
	}
}

func parseMockResponse(value any, request *http.Request) (*http.Response, error) {
	object, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("must be an object")
	}
	status := http.StatusOK
	if rawStatus, found := firstValue(object, "status", "$status"); found {
		parsed, err := resultStatus(rawStatus)
		if err != nil {
			return nil, fmt.Errorf("status: %w", err)
		}
		status = parsed
	}
	response := &http.Response{
		StatusCode:    status,
		Status:        fmt.Sprintf("%d %s", status, http.StatusText(status)),
		Proto:         request.Proto,
		ProtoMajor:    request.ProtoMajor,
		ProtoMinor:    request.ProtoMinor,
		Header:        make(http.Header),
		Body:          http.NoBody,
		ContentLength: 0,
		Request:       request,
	}
	if rawHeaders, found := firstValue(object, "headers", "$headers"); found {
		headers, _, err := parseHeaders(rawHeaders, false)
		if err != nil {
			return nil, fmt.Errorf("headers: %w", err)
		}
		response.Header = headers
	}
	if rawBody, found := firstValue(object, "body", "$body"); found {
		body, err := resultBody(rawBody)
		if err != nil {
			return nil, fmt.Errorf("body: %w", err)
		}
		setResponseBody(response, body)
	}
	if request.Method == http.MethodHead || !statusAllowsBody(status) {
		setResponseBody(response, nil)
	}
	return response, nil
}

func firstValue(object map[string]any, keys ...string) (any, bool) {
	for _, key := range keys {
		if value, found := object[key]; found {
			return value, true
		}
	}
	return nil, false
}

func resultStatus(value any) (int, error) {
	var status int64
	switch typed := value.(type) {
	case int64:
		status = typed
	case int:
		status = int64(typed)
	case float64:
		status = int64(typed)
		if float64(status) != typed {
			return 0, fmt.Errorf("must be an integer")
		}
	default:
		return 0, fmt.Errorf("must be a number")
	}
	if status < 100 || status > 999 {
		return 0, fmt.Errorf("must be between 100 and 999")
	}
	return int(status), nil
}

func resultBody(value any) ([]byte, error) {
	switch body := value.(type) {
	case string:
		return []byte(body), nil
	case []byte:
		return append([]byte(nil), body...), nil
	default:
		return nil, fmt.Errorf("must be a string or Uint8Array")
	}
}

func parseHeaders(value any, requestHeaders bool) (http.Header, *string, error) {
	object, ok := value.(map[string]any)
	if !ok {
		return nil, nil, fmt.Errorf("must be an object")
	}
	headers := make(http.Header, len(object))
	var host *string
	for field, rawValue := range object {
		if !httpguts.ValidHeaderFieldName(field) {
			return nil, nil, fmt.Errorf("invalid field %q", field)
		}
		values, err := headerValues(rawValue)
		if err != nil {
			return nil, nil, fmt.Errorf("field %q: %w", field, err)
		}
		if requestHeaders && strings.EqualFold(field, "Host") {
			if len(values) > 1 {
				return nil, nil, fmt.Errorf("host must contain one value")
			}
			value := ""
			if len(values) == 1 {
				value = values[0]
			}
			if !httpguts.ValidHostHeader(value) {
				return nil, nil, fmt.Errorf("host contains an invalid value")
			}
			host = &value
			continue
		}
		if isManagedHeader(field) {
			continue
		}
		for _, headerValue := range values {
			if !httpguts.ValidHeaderFieldValue(headerValue) {
				return nil, nil, fmt.Errorf("field %q contains an invalid value", field)
			}
			headers.Add(field, headerValue)
		}
	}
	return headers, host, nil
}

func headerValues(value any) ([]string, error) {
	switch typed := value.(type) {
	case string:
		return []string{typed}, nil
	case []string:
		return append([]string(nil), typed...), nil
	case []any:
		values := make([]string, 0, len(typed))
		for _, item := range typed {
			text, ok := scalarString(item)
			if !ok {
				return nil, fmt.Errorf("values must be strings or scalar values")
			}
			values = append(values, text)
		}
		return values, nil
	default:
		if text, ok := scalarString(typed); ok {
			return []string{text}, nil
		}
		return nil, fmt.Errorf("value must be a string or array")
	}
}

func scalarString(value any) (string, bool) {
	switch typed := value.(type) {
	case string:
		return typed, true
	case bool:
		return strconv.FormatBool(typed), true
	case int64:
		return strconv.FormatInt(typed, 10), true
	case int:
		return strconv.Itoa(typed), true
	case float64:
		return strconv.FormatFloat(typed, 'g', -1, 64), true
	default:
		return "", false
	}
}

func isManagedHeader(field string) bool {
	return strings.EqualFold(field, "Content-Length") ||
		strings.EqualFold(field, "Transfer-Encoding") ||
		strings.EqualFold(field, "Trailer")
}

func decodeScriptBody(body []byte, present bool, headers http.Header, limit int64) ([]byte, R.BodyEncoding, bool, error) {
	headerValues := headers.Values("Content-Encoding")
	if !present {
		return body, R.ParseBodyEncoding(headerValues), false, nil
	}
	decoded, encoding, tooLarge, err := R.DecodeBodyContent(body, headerValues, limit)
	if err != nil {
		return nil, encoding, false, fmt.Errorf("decode body: %w", err)
	}
	if tooLarge {
		return nil, encoding, true, nil
	}
	return decoded, encoding, false, nil
}

func encodeScriptBody(body []byte, original R.BodyEncoding, targetHeaders http.Header) ([]byte, error) {
	headerValues := targetHeaders.Values("Content-Encoding")
	encoding := original
	if !original.Matches(headerValues) {
		encoding = R.ParseBodyEncoding(headerValues)
	}
	return encoding.Encode(body)
}

func prepareBody(body *io.ReadCloser, contentLength int64, required bool, limit int64) ([]byte, bool, bool, error) {
	if !required || body == nil || *body == nil {
		return nil, false, false, nil
	}
	if limit >= 0 && contentLength > limit {
		return nil, false, true, nil
	}
	original := *body
	var reader io.Reader = original
	if limit >= 0 {
		reader = io.LimitReader(original, limit+1)
	}
	content, err := io.ReadAll(reader)
	if err != nil {
		*body = &continuedBody{Reader: io.MultiReader(bytes.NewReader(content), original), closer: original}
		return nil, false, false, fmt.Errorf("read body: %w", err)
	}
	if limit >= 0 && int64(len(content)) > limit {
		*body = &continuedBody{Reader: io.MultiReader(bytes.NewReader(content), original), closer: original}
		return nil, false, true, nil
	}
	_ = original.Close()
	content = append([]byte(nil), content...)
	*body = io.NopCloser(bytes.NewReader(content))
	return content, len(content) != 0, false, nil
}

type continuedBody struct {
	io.Reader
	closer io.Closer
}

func (b *continuedBody) Close() error {
	return b.closer.Close()
}

func setRequestBody(request *http.Request, body []byte) {
	previousBody := request.Body
	request.ContentLength = int64(len(body))
	request.TransferEncoding = nil
	request.Trailer = nil
	request.Header.Del("Content-Length")
	request.Header.Del("Transfer-Encoding")
	request.Header.Del("Trailer")
	if len(body) == 0 {
		request.Body = http.NoBody
		request.GetBody = func() (io.ReadCloser, error) { return http.NoBody, nil }
	} else {
		request.Body = io.NopCloser(bytes.NewReader(body))
		request.GetBody = func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(body)), nil
		}
	}
	if previousBody != nil {
		_ = previousBody.Close()
	}
}

func setResponseBody(response *http.Response, body []byte) {
	previousBody := response.Body
	response.ContentLength = int64(len(body))
	response.TransferEncoding = nil
	response.Trailer = nil
	response.Header.Del("Content-Length")
	response.Header.Del("Transfer-Encoding")
	response.Header.Del("Trailer")
	if len(body) == 0 {
		response.Body = http.NoBody
	} else {
		response.Body = io.NopCloser(bytes.NewReader(body))
	}
	if previousBody != nil {
		_ = previousBody.Close()
	}
}

func statusAllowsBody(statusCode int) bool {
	return statusCode < 100 || statusCode >= 200 && statusCode != http.StatusNoContent && statusCode != http.StatusNotModified
}

func absoluteRequestURL(request *http.Request) string {
	requestURL := *request.URL
	if requestURL.Scheme == "" {
		requestURL.Scheme = "https"
	}
	if requestURL.Host == "" {
		requestURL.Host = request.Host
	}
	requestURL.Fragment = ""
	return requestURL.String()
}
