package script

import (
	"bytes"
	"fmt"
	"io"
	"net/url"
	"reflect"
	"strconv"
	"strings"

	F "github.com/metacubex/mihomo/component/httpflow"
	R "github.com/metacubex/mihomo/component/rewrite"
	"github.com/metacubex/mihomo/log"

	"github.com/metacubex/http"
	"golang.org/x/net/http/httpguts"
)

func (m *Manager) ProcessRequest(request *http.Request, requestID string) (*http.Response, bool) {
	result := m.ProcessRequestFlow(request, requestID)
	logStandaloneScriptActions(result.Actions)
	return result.Response, result.Decision == F.DecisionAbort
}

func (m *Manager) ProcessRequestFlow(request *http.Request, requestID string) F.Result {
	result := F.Result{Decision: F.DecisionContinue}
	if m == nil || request == nil || request.URL == nil {
		return result
	}
	requestURL := absoluteRequestURL(request)
	matched := matchingEntries(m.requestEntries, requestURL)
	for _, scriptEntry := range matched {
		execution, err := m.processRequest(scriptEntry, request, requestID)
		action := scriptAction(F.PhaseRequest, scriptEntry)
		action.Fields = execution.fields
		action.Target = execution.target
		action.StatusCode = execution.statusCode
		switch {
		case err != nil:
			action.Outcome = F.OutcomeFailed
			action.Message = err.Error()
		case execution.skipped != "":
			action.Outcome = F.OutcomeSkipped
			action.Message = execution.skipped
		case execution.abort:
			action.Outcome = F.OutcomeAborted
			action.Modified = len(execution.fields) != 0
			action.Fields = execution.fields
			result.Decision = F.DecisionAbort
		case execution.response != nil:
			action.Outcome = F.OutcomeResponded
			action.Modified = true
			action.Fields = execution.fields
			action.StatusCode = execution.response.StatusCode
			result.Decision = F.DecisionRespond
			result.Response = execution.response
		case len(execution.fields) != 0:
			action.Outcome = F.OutcomeApplied
			action.Modified = true
			action.Fields = execution.fields
		default:
			action.Outcome = F.OutcomeUnchanged
		}
		result.Add(action)
		if result.Decision != F.DecisionContinue {
			return result
		}
	}
	return result
}

func (m *Manager) ProcessResponse(request *http.Request, response *http.Response, requestID string) bool {
	result := m.ProcessResponseFlow(request, response, requestID)
	logStandaloneScriptActions(result.Actions)
	return result.Decision == F.DecisionAbort
}

func (m *Manager) ProcessResponseFlow(request *http.Request, response *http.Response, requestID string) F.Result {
	result := F.Result{Decision: F.DecisionContinue}
	if m == nil || request == nil || request.URL == nil || response == nil {
		return result
	}
	requestURL := absoluteRequestURL(request)
	matched := matchingEntries(m.responseEntries, requestURL)
	for _, scriptEntry := range matched {
		execution, err := m.processResponse(scriptEntry, request, response, requestID)
		action := scriptAction(F.PhaseResponse, scriptEntry)
		action.Fields = execution.fields
		action.StatusCode = execution.statusCode
		switch {
		case err != nil:
			action.Outcome = F.OutcomeFailed
			action.Message = err.Error()
		case execution.skipped != "":
			action.Outcome = F.OutcomeSkipped
			action.Message = execution.skipped
		case execution.abort:
			action.Outcome = F.OutcomeAborted
			action.Modified = len(execution.fields) != 0
			action.Fields = execution.fields
			result.Decision = F.DecisionAbort
		case len(execution.fields) != 0:
			action.Outcome = F.OutcomeApplied
			action.Modified = true
			action.Fields = execution.fields
		default:
			action.Outcome = F.OutcomeUnchanged
		}
		result.Add(action)
		if result.Decision == F.DecisionAbort {
			return result
		}
	}
	return result
}

type scriptExecution struct {
	response   *http.Response
	abort      bool
	skipped    string
	fields     []string
	target     string
	statusCode int
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

func (m *Manager) processRequest(scriptEntry *entry, request *http.Request, requestID string) (scriptExecution, error) {
	body, bodyPresent, tooLarge, err := prepareBody(
		&request.Body,
		request.ContentLength,
		scriptEntry.requiresBody,
		scriptEntry.maxBodySize,
	)
	if err != nil {
		return scriptExecution{}, err
	}
	if tooLarge {
		return scriptExecution{skipped: "request body exceeds max-body-size"}, nil
	}
	body, bodyEncoding, tooLarge, err := decodeScriptBody(body, bodyPresent, request.Header, scriptEntry.maxBodySize)
	if err != nil {
		return scriptExecution{}, err
	}
	if tooLarge {
		return scriptExecution{skipped: "decoded request body exceeds max-body-size"}, nil
	}

	patch, err := m.evaluate(scriptEntry, evaluationInput{
		request:     request,
		requestID:   requestID,
		body:        body,
		bodyPresent: bodyPresent,
	})
	if err != nil {
		return scriptExecution{}, err
	}
	if patch == nil {
		return scriptExecution{}, nil
	}
	mutation, err := parseRequestMutation(patch, scriptEntry.requiresBody, request)
	if err != nil {
		return scriptExecution{}, err
	}
	fields := mutation.changedFields(request, body, bodyPresent)
	if mutation.setBody {
		targetHeaders := request.Header
		if mutation.headers != nil {
			targetHeaders = mutation.headers
		}
		mutation.body, err = encodeScriptBody(mutation.body, bodyEncoding, targetHeaders)
		if err != nil {
			return scriptExecution{}, fmt.Errorf("encode $done.body: %w", err)
		}
	}
	mutation.apply(request)
	target := ""
	if hasField(fields, "url") {
		target = absoluteRequestURL(request)
	}
	return scriptExecution{
		response:   mutation.response,
		abort:      mutation.abort,
		fields:     fields,
		target:     target,
		statusCode: responseStatusCode(mutation.response),
	}, nil
}

func (m *Manager) processResponse(scriptEntry *entry, request *http.Request, response *http.Response, requestID string) (scriptExecution, error) {
	requiresBody := scriptEntry.requiresBody && request.Method != http.MethodHead && statusAllowsBody(response.StatusCode)
	body, bodyPresent, tooLarge, err := prepareBody(
		&response.Body,
		response.ContentLength,
		requiresBody,
		scriptEntry.maxBodySize,
	)
	if err != nil {
		return scriptExecution{}, err
	}
	if tooLarge {
		return scriptExecution{skipped: "response body exceeds max-body-size"}, nil
	}
	body, bodyEncoding, tooLarge, err := decodeScriptBody(body, bodyPresent, response.Header, scriptEntry.maxBodySize)
	if err != nil {
		return scriptExecution{}, err
	}
	if tooLarge {
		return scriptExecution{skipped: "decoded response body exceeds max-body-size"}, nil
	}

	patch, err := m.evaluate(scriptEntry, evaluationInput{
		request:     request,
		response:    response,
		requestID:   requestID,
		body:        body,
		bodyPresent: bodyPresent,
	})
	if err != nil {
		return scriptExecution{}, err
	}
	if patch == nil {
		return scriptExecution{}, nil
	}
	mutation, err := parseResponseMutation(patch, requiresBody)
	if err != nil {
		return scriptExecution{}, err
	}
	fields := mutation.changedFields(response, body, bodyPresent)
	if mutation.setBody {
		targetHeaders := response.Header
		if mutation.headers != nil {
			targetHeaders = mutation.headers
		}
		mutation.body, err = encodeScriptBody(mutation.body, bodyEncoding, targetHeaders)
		if err != nil {
			return scriptExecution{}, fmt.Errorf("encode $done.body: %w", err)
		}
	}
	mutation.apply(response)
	statusCode := 0
	if hasField(fields, "status") {
		statusCode = response.StatusCode
	}
	return scriptExecution{abort: mutation.abort, fields: fields, statusCode: statusCode}, nil
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

func (m *requestMutation) changedFields(request *http.Request, body []byte, bodyPresent bool) []string {
	var fields []string
	if m.url != nil && (request.URL == nil || m.url.String() != request.URL.String()) {
		fields = append(fields, "url")
	}
	if m.headers != nil && !reflect.DeepEqual(m.headers, request.Header) {
		fields = append(fields, "headers")
	}
	if m.host != nil && *m.host != request.Host {
		fields = append(fields, "host")
	}
	if m.setBody && (!bodyPresent || !bytes.Equal(m.body, body)) {
		fields = append(fields, "body")
	}
	return fields
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

func (m *responseMutation) changedFields(response *http.Response, body []byte, bodyPresent bool) []string {
	var fields []string
	if m.status != 0 && m.status != response.StatusCode {
		fields = append(fields, "status")
	}
	if m.headers != nil && !reflect.DeepEqual(m.headers, response.Header) {
		fields = append(fields, "headers")
	}
	if m.setBody && (!bodyPresent || !bytes.Equal(m.body, body)) {
		fields = append(fields, "body")
	}
	targetStatus := response.StatusCode
	if m.status != 0 {
		targetStatus = m.status
	}
	if !statusAllowsBody(targetStatus) && !hasField(fields, "body") &&
		(response.Body != nil && response.Body != http.NoBody || response.ContentLength != 0) {
		fields = append(fields, "body")
	}
	return fields
}

func scriptAction(phase F.Phase, scriptEntry *entry) F.Action {
	return F.Action{
		Phase:  phase,
		Source: F.SourceScript,
		Kind:   F.KindScript,
		Name:   scriptEntry.name,
		Rule:   scriptEntry.match.String(),
	}
}

func hasField(fields []string, wanted string) bool {
	for _, field := range fields {
		if field == wanted {
			return true
		}
	}
	return false
}

func responseStatusCode(response *http.Response) int {
	if response == nil {
		return 0
	}
	return response.StatusCode
}

func logStandaloneScriptActions(actions []F.Action) {
	for _, action := range actions {
		switch action.Outcome {
		case F.OutcomeFailed:
			log.Errorln("[Script] %s %s execution error: %s", action.Name, action.Phase, action.Message)
		case F.OutcomeSkipped:
			log.Debugln("[Script] %s skipped: %s", action.Name, action.Message)
		}
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
