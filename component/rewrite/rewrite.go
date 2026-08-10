package rewrite

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/textproto"
	"net/url"
	"strconv"

	"github.com/metacubex/http"
)

var transparentGIF = []byte{
	0x47, 0x49, 0x46, 0x38, 0x39, 0x61, 0x01, 0x00,
	0x01, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00,
	0xff, 0xff, 0xff, 0x21, 0xf9, 0x04, 0x01, 0x00,
	0x00, 0x00, 0x00, 0x2c, 0x00, 0x00, 0x00, 0x00,
	0x01, 0x00, 0x01, 0x00, 0x00, 0x02, 0x02, 0x44,
	0x01, 0x00, 0x3b,
}

func (c *Config) RewriteRequest(request *http.Request) *http.Response {
	if c == nil || request == nil || request.URL == nil {
		return nil
	}

	requestURL := absoluteRequestURL(request)
	for _, rule := range c.urlRules {
		if !rule.match.MatchString(requestURL) {
			continue
		}
		replacement := rule.match.ReplaceAllString(requestURL, rule.value)
		switch rule.ruleType {
		case urlRuleTransparent:
			if rewrittenURL, err := url.Parse(replacement); err == nil && validUpstreamURL(rewrittenURL) {
				request.URL = rewrittenURL
				request.Host = rewrittenURL.Host
				request.RequestURI = ""
			}
		case urlRuleRedirect302:
			return redirectResponse(request, http.StatusFound, replacement)
		case urlRuleRedirect307:
			return redirectResponse(request, http.StatusTemporaryRedirect, replacement)
		case urlRuleReject:
			return rejectResponse(request, rule.rejectType)
		}
		break
	}

	requestURL = absoluteRequestURL(request)
	for _, rule := range c.mockRules {
		if rule.match.MatchString(requestURL) {
			return rule.response(request)
		}
	}

	c.rewriteHeaders(requestURL, c.requestHeaderRules, request.Header)
	c.rewriteRequestBody(requestURL, request)
	return nil
}

func (c *Config) RewriteResponse(request *http.Request, response *http.Response) {
	if c == nil || request == nil || response == nil {
		return
	}
	requestURL := absoluteRequestURL(request)
	c.rewriteHeaders(requestURL, c.responseHeaderRules, response.Header)
	c.rewriteResponseBody(requestURL, request, response)
}

func (c *Config) rewriteHeaders(requestURL string, rules []headerRule, header http.Header) {
	if header == nil {
		return
	}
	for _, rule := range rules {
		if !rule.match.MatchString(requestURL) {
			continue
		}
		switch rule.ruleType {
		case headerRuleAdd:
			header.Add(rule.field, rule.value)
		case headerRuleDelete:
			header.Del(rule.field)
		case headerRuleReplace:
			if headerValues(header, rule.field) != nil {
				header.Set(rule.field, rule.value)
			}
		case headerRuleReplaceRegex:
			values := headerValues(header, rule.field)
			if values == nil {
				continue
			}
			for index := range values {
				values[index] = rule.regex.ReplaceAllString(values[index], rule.value)
			}
			header[textproto.CanonicalMIMEHeaderKey(rule.field)] = values
		}
	}
}

func headerValues(header http.Header, field string) []string {
	values, found := header[textproto.CanonicalMIMEHeaderKey(field)]
	if !found {
		return nil
	}
	return append([]string(nil), values...)
}

func (c *Config) rewriteRequestBody(requestURL string, request *http.Request) {
	if request.Body == nil || request.Body == http.NoBody {
		return
	}
	rule := matchBodyRule(requestURL, c.requestBodyRules)
	if rule == nil {
		return
	}
	rewritten := rewriteBody(request.Body, request.Header, rule)
	setRequestBody(request, rewritten)
}

func (c *Config) rewriteResponseBody(requestURL string, request *http.Request, response *http.Response) {
	if response.Body == nil || response.Body == http.NoBody || request.Method == http.MethodHead || !statusAllowsBody(response.StatusCode) {
		return
	}
	rule := matchBodyRule(requestURL, c.responseBodyRules)
	if rule == nil {
		return
	}
	rewritten := rewriteBody(response.Body, response.Header, rule)
	setResponseBody(response, rewritten)
}

func matchBodyRule(requestURL string, rules []bodyRule) *bodyRule {
	for index := range rules {
		rule := &rules[index]
		if rule.match.MatchString(requestURL) {
			return rule
		}
	}
	return nil
}

func rewriteBody(body io.ReadCloser, header http.Header, rule *bodyRule) []byte {
	rawBody, err := io.ReadAll(body)
	_ = body.Close()
	if err != nil {
		return rawBody
	}
	decodedBody, codings, err := decodeBody(rawBody, header.Values("Content-Encoding"))
	if err != nil {
		return rawBody
	}

	rewrittenBody := decodedBody
	if rule.jqExpression != nil {
		rewrittenBody, err = applyJQ(rule, decodedBody)
		if err != nil {
			return rawBody
		}
	} else {
		for _, action := range rule.actions {
			rewrittenBody = action.regex.ReplaceAll(rewrittenBody, action.value)
		}
	}
	if bytes.Equal(rewrittenBody, decodedBody) {
		return rawBody
	}

	rewrittenBody, err = encodeBody(rewrittenBody, codings)
	if err != nil {
		return rawBody
	}
	return rewrittenBody
}

func applyJQ(rule *bodyRule, body []byte) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var input any
	if err := decoder.Decode(&input); err != nil {
		return nil, err
	}
	if err := ensureJSONEnd(decoder); err != nil {
		return nil, err
	}

	iterator := rule.jqExpression.Run(input)
	var output bytes.Buffer
	encoder := json.NewEncoder(&output)
	encoder.SetEscapeHTML(false)
	written := false
	for {
		value, found := iterator.Next()
		if !found {
			break
		}
		if runtimeError, isError := value.(error); isError {
			return nil, runtimeError
		}
		if err := encoder.Encode(value); err != nil {
			return nil, err
		}
		written = true
	}
	if !written {
		return []byte{}, nil
	}
	return bytes.TrimSuffix(output.Bytes(), []byte{'\n'}), nil
}

func ensureJSONEnd(decoder *json.Decoder) error {
	var extra any
	err := decoder.Decode(&extra)
	if err == io.EOF {
		return nil
	}
	if err == nil {
		return fmt.Errorf("multiple JSON values")
	}
	return err
}

func setRequestBody(request *http.Request, body []byte) {
	request.ContentLength = int64(len(body))
	request.TransferEncoding = nil
	request.Trailer = nil
	request.Header.Del("Content-Length")
	request.Header.Del("Trailer")
	request.Body = newBody(body)
	request.GetBody = func() (io.ReadCloser, error) {
		return newBody(body), nil
	}
}

func setResponseBody(response *http.Response, body []byte) {
	response.ContentLength = int64(len(body))
	response.TransferEncoding = nil
	response.Trailer = nil
	response.Header.Del("Content-Length")
	response.Header.Del("Trailer")
	response.Body = newBody(body)
}

func newBody(body []byte) io.ReadCloser {
	if len(body) == 0 {
		return http.NoBody
	}
	return io.NopCloser(bytes.NewReader(body))
}

func redirectResponse(request *http.Request, statusCode int, location string) *http.Response {
	response := newResponse(request, statusCode, nil)
	response.Header.Set("Location", location)
	return response
}

func rejectResponse(request *http.Request, responseType rejectResponseType) *http.Response {
	switch responseType {
	case rejectResponseOK:
		return newResponse(request, http.StatusOK, nil)
	case rejectResponseImage:
		response := newResponse(request, http.StatusOK, transparentGIF)
		response.Header.Set("Content-Type", "image/gif")
		return response
	case rejectResponseDict:
		response := newResponse(request, http.StatusOK, []byte("{}"))
		response.Header.Set("Content-Type", "application/json")
		return response
	case rejectResponseArray:
		response := newResponse(request, http.StatusOK, []byte("[]"))
		response.Header.Set("Content-Type", "application/json")
		return response
	default:
		return newResponse(request, http.StatusNotFound, nil)
	}
}

func (r mockRule) response(request *http.Request) *http.Response {
	response := newResponse(request, r.statusCode, r.body)
	for _, header := range r.headers {
		if textproto.CanonicalMIMEHeaderKey(header.Field) == "Content-Length" {
			continue
		}
		response.Header.Add(header.Field, header.Value)
	}
	return response
}

func newResponse(request *http.Request, statusCode int, body []byte) *http.Response {
	response := &http.Response{
		StatusCode:    statusCode,
		Status:        strconv.Itoa(statusCode) + " " + http.StatusText(statusCode),
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        make(http.Header),
		Body:          newBody(body),
		ContentLength: int64(len(body)),
		Request:       request,
	}
	if request != nil {
		response.Proto = request.Proto
		response.ProtoMajor = request.ProtoMajor
		response.ProtoMinor = request.ProtoMinor
		response.Close = request.Close
	}
	return response
}

func absoluteRequestURL(request *http.Request) string {
	if request == nil || request.URL == nil {
		return ""
	}
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

func validUpstreamURL(requestURL *url.URL) bool {
	return requestURL != nil && (requestURL.Scheme == "http" || requestURL.Scheme == "https") && requestURL.Host != ""
}

func statusAllowsBody(statusCode int) bool {
	return statusCode < 100 || statusCode >= 200 && statusCode != http.StatusNoContent && statusCode != http.StatusNotModified
}
