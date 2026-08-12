package rewrite

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/textproto"
	"net/url"
	"strconv"

	F "github.com/metacubex/mihomo/component/httpflow"

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
	return c.ProcessRequest(request).Response
}

func (c *Config) ProcessRequest(request *http.Request) F.Result {
	result := F.Result{Decision: F.DecisionContinue}
	if c == nil || request == nil || request.URL == nil {
		return result
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
				originalURL := requestURL
				originalHost := request.Host
				request.URL = rewrittenURL
				request.Host = rewrittenURL.Host
				request.RequestURI = ""
				var fields []string
				if originalURL != absoluteRequestURL(request) {
					fields = append(fields, "url")
				}
				if originalHost != request.Host {
					fields = append(fields, "host")
				}
				modified := len(fields) != 0
				result.Add(F.Action{
					Phase:    F.PhaseRequest,
					Source:   F.SourceRewrite,
					Kind:     F.KindURL,
					Outcome:  modificationOutcome(modified),
					Name:     "transparent",
					Rule:     rule.match.String(),
					Modified: modified,
					Target:   absoluteRequestURL(request),
					Fields:   fields,
				})
			} else {
				result.Add(F.Action{
					Phase:   F.PhaseRequest,
					Source:  F.SourceRewrite,
					Kind:    F.KindURL,
					Outcome: F.OutcomeFailed,
					Name:    "transparent",
					Rule:    rule.match.String(),
					Target:  replacement,
					Message: "replacement is not a valid absolute HTTP URL",
				})
			}
		case urlRuleRedirect302:
			result.Decision = F.DecisionRespond
			result.Response = redirectResponse(request, http.StatusFound, replacement)
			result.Add(localRewriteAction(F.KindRedirect, "redirect-302", rule.match.String(), http.StatusFound, replacement))
			return result
		case urlRuleRedirect307:
			result.Decision = F.DecisionRespond
			result.Response = redirectResponse(request, http.StatusTemporaryRedirect, replacement)
			result.Add(localRewriteAction(F.KindRedirect, "redirect-307", rule.match.String(), http.StatusTemporaryRedirect, replacement))
			return result
		case urlRuleReject:
			result.Decision = F.DecisionRespond
			result.Response = rejectResponse(request, rule.rejectType)
			result.Add(localRewriteAction(F.KindReject, rejectTypeName(rule.rejectType), rule.match.String(), result.Response.StatusCode, ""))
			return result
		}
		break
	}

	requestURL = absoluteRequestURL(request)
	for _, rule := range c.mockRules {
		if rule.match.MatchString(requestURL) {
			result.Decision = F.DecisionRespond
			result.Response = rule.response(request)
			result.Add(localRewriteAction(F.KindMock, "mock", rule.match.String(), result.Response.StatusCode, ""))
			return result
		}
	}

	result.Actions = append(result.Actions, c.rewriteHeaders(F.PhaseRequest, requestURL, c.requestHeaderRules, request.Header)...)
	if action := c.rewriteRequestBody(requestURL, request); action != nil {
		result.Add(*action)
	}
	return result
}

func (c *Config) RewriteResponse(request *http.Request, response *http.Response) {
	_ = c.ProcessResponse(request, response)
}

func (c *Config) ProcessResponse(request *http.Request, response *http.Response) F.Result {
	result := F.Result{Decision: F.DecisionContinue}
	if c == nil || request == nil || response == nil {
		return result
	}
	requestURL := absoluteRequestURL(request)
	result.Actions = append(result.Actions, c.rewriteHeaders(F.PhaseResponse, requestURL, c.responseHeaderRules, response.Header)...)
	if action := c.rewriteResponseBody(requestURL, request, response); action != nil {
		result.Add(*action)
	}
	return result
}

func (c *Config) rewriteHeaders(phase F.Phase, requestURL string, rules []headerRule, header http.Header) []F.Action {
	var actions []F.Action
	if header == nil {
		return actions
	}
	for _, rule := range rules {
		if !rule.match.MatchString(requestURL) {
			continue
		}
		modified := false
		switch rule.ruleType {
		case headerRuleAdd:
			header.Add(rule.field, rule.value)
			modified = true
		case headerRuleDelete:
			modified = headerValues(header, rule.field) != nil
			header.Del(rule.field)
		case headerRuleReplace:
			values := headerValues(header, rule.field)
			if values != nil {
				header.Set(rule.field, rule.value)
				modified = len(values) != 1 || values[0] != rule.value
			}
		case headerRuleReplaceRegex:
			values := headerValues(header, rule.field)
			if values != nil {
				for index := range values {
					replaced := rule.regex.ReplaceAllString(values[index], rule.value)
					modified = modified || replaced != values[index]
					values[index] = replaced
				}
				header[textproto.CanonicalMIMEHeaderKey(rule.field)] = values
			}
		}
		actions = append(actions, F.Action{
			Phase:    phase,
			Source:   F.SourceRewrite,
			Kind:     F.KindHeader,
			Outcome:  modificationOutcome(modified),
			Name:     headerRuleTypeName(rule.ruleType),
			Rule:     rule.match.String(),
			Modified: modified,
			Fields:   []string{textproto.CanonicalMIMEHeaderKey(rule.field)},
		})
	}
	return actions
}

func headerValues(header http.Header, field string) []string {
	values, found := header[textproto.CanonicalMIMEHeaderKey(field)]
	if !found {
		return nil
	}
	return append([]string(nil), values...)
}

func (c *Config) rewriteRequestBody(requestURL string, request *http.Request) *F.Action {
	if request.Body == nil || request.Body == http.NoBody {
		return nil
	}
	rule := matchBodyRule(requestURL, c.requestBodyRules)
	if rule == nil {
		return nil
	}
	rewritten, modified, err := rewriteBody(request.Body, request.Header, rule)
	setRequestBody(request, rewritten)
	return bodyRewriteAction(F.PhaseRequest, rule, modified, err)
}

func (c *Config) rewriteResponseBody(requestURL string, request *http.Request, response *http.Response) *F.Action {
	if response.Body == nil || response.Body == http.NoBody || request.Method == http.MethodHead || !statusAllowsBody(response.StatusCode) {
		return nil
	}
	rule := matchBodyRule(requestURL, c.responseBodyRules)
	if rule == nil {
		return nil
	}
	rewritten, modified, err := rewriteBody(response.Body, response.Header, rule)
	setResponseBody(response, rewritten)
	return bodyRewriteAction(F.PhaseResponse, rule, modified, err)
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

func rewriteBody(body io.ReadCloser, header http.Header, rule *bodyRule) ([]byte, bool, error) {
	rawBody, err := io.ReadAll(body)
	_ = body.Close()
	if err != nil {
		return rawBody, false, err
	}
	decodedBody, codings, err := decodeBody(rawBody, header.Values("Content-Encoding"))
	if err != nil {
		return rawBody, false, err
	}

	rewrittenBody := decodedBody
	if rule.jqExpression != nil {
		rewrittenBody, err = applyJQ(rule, decodedBody)
		if err != nil {
			return rawBody, false, err
		}
	} else {
		for _, action := range rule.actions {
			rewrittenBody = action.regex.ReplaceAll(rewrittenBody, action.value)
		}
	}
	if bytes.Equal(rewrittenBody, decodedBody) {
		return rawBody, false, nil
	}

	rewrittenBody, err = encodeBody(rewrittenBody, codings)
	if err != nil {
		return rawBody, false, err
	}
	return rewrittenBody, true, nil
}

func modificationOutcome(modified bool) F.Outcome {
	if modified {
		return F.OutcomeApplied
	}
	return F.OutcomeUnchanged
}

func localRewriteAction(kind F.Kind, name, rule string, statusCode int, target string) F.Action {
	return F.Action{
		Phase:      F.PhaseRequest,
		Source:     F.SourceRewrite,
		Kind:       kind,
		Outcome:    F.OutcomeResponded,
		Name:       name,
		Rule:       rule,
		Modified:   true,
		StatusCode: statusCode,
		Target:     target,
	}
}

func headerRuleTypeName(ruleType headerRuleType) string {
	switch ruleType {
	case headerRuleAdd:
		return "add"
	case headerRuleDelete:
		return "delete"
	case headerRuleReplace:
		return "replace"
	case headerRuleReplaceRegex:
		return "replace-regex"
	default:
		return "unknown"
	}
}

func rejectTypeName(responseType rejectResponseType) string {
	switch responseType {
	case rejectResponseOK:
		return "reject-200"
	case rejectResponseImage:
		return "reject-image"
	case rejectResponseDict:
		return "reject-dict"
	case rejectResponseArray:
		return "reject-array"
	default:
		return "reject"
	}
}

func bodyRewriteAction(phase F.Phase, rule *bodyRule, modified bool, err error) *F.Action {
	name := "regex"
	if rule.jqExpression != nil {
		name = "jq"
	}
	action := &F.Action{
		Phase:    phase,
		Source:   F.SourceRewrite,
		Kind:     F.KindBody,
		Outcome:  modificationOutcome(modified),
		Name:     name,
		Rule:     rule.match.String(),
		Modified: modified,
		Fields:   []string{"body"},
	}
	if err != nil {
		action.Outcome = F.OutcomeFailed
		action.Modified = false
		action.Message = err.Error()
	}
	return action
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
