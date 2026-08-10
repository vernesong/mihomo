package mitm_test

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/andybalholm/brotli"
	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/component/mitm"
	"github.com/metacubex/mihomo/config"
	C "github.com/metacubex/mihomo/constant"

	"github.com/metacubex/http"
	"github.com/metacubex/http/httptest"
	"github.com/stretchr/testify/require"
)

func TestRewriteUserTransparentHeaderBodyAndCaptureAPI(t *testing.T) {
	const (
		originalHost = "rewrite.example.com"
		modifiedHost = "upstream.example.com"
	)
	parsedConfig := parseRewriteTestConfig(t, originalHost, `
rewrite:
  url:
    - match: '^http://rewrite\.example\.com(.*)$'
      type: transparent
      value: 'http://upstream.example.com$1'
    - match: '^http://rewrite\.example\.com(.*)$'
      type: transparent
      value: 'http://ignored.example.com$1'
  header:
    - match: '^http://upstream\.example\.com/transform'
      direction: request
      type: add
      field: X-Added
      value: one
    - match: '^http://upstream\.example\.com/transform'
      direction: request
      type: add
      field: X-Added
      value: two
    - match: '^http://upstream\.example\.com/transform'
      direction: request
      type: del
      field: X-Delete
    - match: '^http://upstream\.example\.com/transform'
      direction: request
      type: replace
      field: X-Replace
      value: rewritten
    - match: '^http://upstream\.example\.com/transform'
      direction: request
      type: replace
      field: X-Missing
      value: must-not-be-added
    - match: '^http://upstream\.example\.com/transform'
      direction: request
      type: replace-regex
      field: X-Regex
      regex: '^client-(.*)$'
      value: 'mihomo-$1'
    - match: '^http://upstream\.example\.com/transform'
      direction: response
      type: add
      field: X-Added
      value: one
    - match: '^http://upstream\.example\.com/transform'
      direction: response
      type: add
      field: X-Added
      value: two
    - match: '^http://upstream\.example\.com/transform'
      direction: response
      type: del
      field: X-Delete
    - match: '^http://upstream\.example\.com/transform'
      direction: response
      type: replace
      field: X-Replace
      value: rewritten
    - match: '^http://upstream\.example\.com/transform'
      direction: response
      type: replace-regex
      field: X-Regex
      regex: '^server-(.*)$'
      value: 'mihomo-$1'
  body:
    - match: '^http://upstream\.example\.com/transform'
      direction: request
      actions:
        - regex: '123'
          value: xray
        - regex: '153'
          value: mihomo
    - match: '^http://upstream\.example\.com/transform'
      direction: request
      actions:
        - regex: '.*'
          value: ignored
    - match: '^http://upstream\.example\.com/transform'
      direction: response
      jq-expression: '.source = "mihomo" | .rewritten = true'
    - match: '^http://upstream\.example\.com/invalid-json'
      direction: response
      jq-expression: '.rewritten = true'
    - match: '^http://upstream\.example\.com/jq-error'
      direction: response
      jq-expression: 'error("keep original")'
`)

	type upstreamRequest struct {
		Host    string
		Body    string
		Headers http.Header
		Err     error
	}
	upstreamRequests := make(chan upstreamRequest, 3)
	responseCodings := []string{"br", "gzip", "deflate"}
	invalidUpstreamBody := encodeRewriteTestBody(t, []byte("not-json"), responseCodings)
	jsonUpstreamBody := encodeRewriteTestBody(t, []byte(`{"source":"upstream"}`), responseCodings)
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			upstreamRequests <- upstreamRequest{Err: err}
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}
		decodedBody, err := decodeRewriteTestBodyRaw(body, splitRewriteTestCodings(request.Header.Values("Content-Encoding")))
		upstreamRequests <- upstreamRequest{Host: request.Host, Body: string(decodedBody), Headers: request.Header.Clone(), Err: err}
		if err != nil {
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}

		writer.Header().Set("Content-Encoding", strings.Join(responseCodings, ", "))
		if request.URL.Path == "/invalid-json" {
			writer.Header().Set("Content-Length", strconv.Itoa(len(invalidUpstreamBody)))
			_, _ = writer.Write(invalidUpstreamBody)
			return
		}
		writer.Header().Set("X-Delete", "remove-me")
		writer.Header().Set("X-Replace", "upstream")
		writer.Header().Set("X-Regex", "server-value")
		writer.Header().Set("Content-Length", strconv.Itoa(len(jsonUpstreamBody)))
		_, _ = writer.Write(jsonUpstreamBody)
	}))
	defer upstream.Close()

	client := newRewriteTestClient(t, parsedConfig, originalHost, upstream.Listener.Addr().String())
	requestCodings := []string{"gzip", "deflate", "br"}
	requestBody := encodeRewriteTestBody(t, []byte("123 153"), requestCodings)
	requestHeader := make(http.Header)
	requestHeader.Set("Content-Encoding", strings.Join(requestCodings, ", "))
	requestHeader.Set("X-Delete", "remove-me")
	requestHeader.Set("X-Replace", "client")
	requestHeader.Set("X-Regex", "client-value")
	response, rawResponseBody := client.do(t, http.MethodPost, "/transform?source=client", requestBody, requestHeader)

	seenRequest := receiveRewriteTestValue(t, upstreamRequests)
	require.NoError(t, seenRequest.Err)
	require.Equal(t, modifiedHost, seenRequest.Host)
	require.Equal(t, "xray mihomo", seenRequest.Body)
	require.Equal(t, []string{"one", "two"}, seenRequest.Headers.Values("X-Added"))
	require.Empty(t, seenRequest.Headers.Get("X-Delete"))
	require.Equal(t, "rewritten", seenRequest.Headers.Get("X-Replace"))
	require.Equal(t, "mihomo-value", seenRequest.Headers.Get("X-Regex"))
	require.Empty(t, seenRequest.Headers.Get("X-Missing"))

	dialed := receiveRewriteTestValue(t, client.dialed)
	require.Equal(t, modifiedHost+":80", dialed.Address)
	require.Equal(t, "http://"+modifiedHost+"/transform?source=client", dialed.URL)
	require.Equal(t, []string{"one", "two"}, response.Header.Values("X-Added"))
	require.Empty(t, response.Header.Get("X-Delete"))
	require.Equal(t, "rewritten", response.Header.Get("X-Replace"))
	require.Equal(t, "mihomo-value", response.Header.Get("X-Regex"))
	require.Equal(t, int64(len(rawResponseBody)), response.ContentLength)
	decodedResponseBody := decodeRewriteTestBody(t, rawResponseBody, responseCodings)
	var responseJSON map[string]any
	require.NoError(t, json.Unmarshal(decodedResponseBody, &responseJSON))
	require.Equal(t, "mihomo", responseJSON["source"])
	require.Equal(t, true, responseJSON["rewritten"])

	invalidResponse, invalidRawBody := client.do(t, http.MethodGet, "/invalid-json", nil, nil)
	require.Equal(t, http.StatusOK, invalidResponse.StatusCode)
	require.Equal(t, "not-json", string(decodeRewriteTestBody(t, invalidRawBody, responseCodings)))
	require.NoError(t, receiveRewriteTestValue(t, upstreamRequests).Err)
	receiveRewriteTestValue(t, client.dialed)
	runtimeErrorResponse, runtimeErrorRawBody := client.do(t, http.MethodGet, "/jq-error", nil, nil)
	require.Equal(t, http.StatusOK, runtimeErrorResponse.StatusCode)
	require.JSONEq(t, `{"source":"upstream"}`, string(decodeRewriteTestBody(t, runtimeErrorRawBody, responseCodings)))
	require.NoError(t, receiveRewriteTestValue(t, upstreamRequests).Err)
	receiveRewriteTestValue(t, client.dialed)

	snapshot := mitm.CapturedSessionsSnapshot()
	require.Len(t, snapshot.Sessions, 3)
	transformed := snapshot.Sessions[0]
	require.Equal(t, "http://"+originalHost+"/transform?source=client", transformed.Request.RawURL)
	require.Equal(t, "http://"+modifiedHost+"/transform?source=client", transformed.Request.URL)
	encodedSnapshot, err := json.Marshal(snapshot)
	require.NoError(t, err)
	var snapshotJSON map[string]any
	require.NoError(t, json.Unmarshal(encodedSnapshot, &snapshotJSON))
	sessions := snapshotJSON["sessions"].([]any)
	requestJSON := sessions[0].(map[string]any)["request"].(map[string]any)
	require.Equal(t, transformed.Request.RawURL, requestJSON["raw_url"])
	require.Equal(t, transformed.Request.URL, requestJSON["url"])
}

func TestRewriteUserRedirectRejectAndMockAPI(t *testing.T) {
	const originalHost = "rewrite-local.example.com"
	parsedConfig := parseRewriteTestConfig(t, originalHost, `
rewrite:
  url:
    - match: '^http://rewrite-local\.example\.com/redirect-302(.*)$'
      type: redirect-302
      value: 'https://example.com/found$1'
    - match: '^http://rewrite-local\.example\.com/redirect-307(.*)$'
      type: redirect-307
      value: 'https://example.com/temporary$1'
    - match: '^http://rewrite-local\.example\.com/reject-empty$'
      type: reject
    - match: '^http://rewrite-local\.example\.com/reject-200$'
      type: reject
      value: 200
    - match: '^http://rewrite-local\.example\.com/reject-img$'
      type: reject
      value: img
    - match: '^http://rewrite-local\.example\.com/reject-dict$'
      type: reject
      value: dict
    - match: '^http://rewrite-local\.example\.com/reject-array$'
      type: reject
      value: array
    - match: '^http://rewrite-local\.example\.com(/mock.*)$'
      type: transparent
      value: 'http://mock-target.example.com$1'
  header:
    - match: '^http://mock-target\.example\.com/'
      direction: response
      type: add
      field: X-Response-Rewrite
      value: applied
  mock:
    - match: '^http://mock-target\.example\.com/mock-map$'
      text: 'mocked'
      status-code: 201
      headers:
        Content-Type: text/plain
        X-Header-Form: map
        Content-Length: 999
    - match: '^http://mock-target\.example\.com/mock-list$'
      base64: 'AAE='
      headers:
        - Content-Type: application/octet-stream
        - X-Header-Form: first
        - X-Header-Form: second
`)
	unexpectedUpstream := make(chan struct{}, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		unexpectedUpstream <- struct{}{}
		writer.WriteHeader(http.StatusInternalServerError)
	}))
	defer upstream.Close()
	client := newRewriteTestClient(t, parsedConfig, originalHost, upstream.Listener.Addr().String())

	for _, testCase := range []struct {
		path        string
		statusCode  int
		location    string
		contentType string
		body        []byte
	}{
		{path: "/redirect-302?x=1", statusCode: http.StatusFound, location: "https://example.com/found?x=1"},
		{path: "/redirect-307?x=1", statusCode: http.StatusTemporaryRedirect, location: "https://example.com/temporary?x=1"},
		{path: "/reject-empty", statusCode: http.StatusNotFound},
		{path: "/reject-200", statusCode: http.StatusOK},
		{path: "/reject-img", statusCode: http.StatusOK, contentType: "image/gif"},
		{path: "/reject-dict", statusCode: http.StatusOK, contentType: "application/json", body: []byte("{}")},
		{path: "/reject-array", statusCode: http.StatusOK, contentType: "application/json", body: []byte("[]")},
		{path: "/mock-map", statusCode: http.StatusCreated, contentType: "text/plain", body: []byte("mocked")},
		{path: "/mock-list", statusCode: http.StatusOK, contentType: "application/octet-stream", body: []byte{0, 1}},
	} {
		response, body := client.do(t, http.MethodGet, testCase.path, nil, nil)
		require.Equal(t, testCase.statusCode, response.StatusCode, testCase.path)
		require.Equal(t, testCase.location, response.Header.Get("Location"), testCase.path)
		if testCase.contentType != "" {
			require.Equal(t, testCase.contentType, response.Header.Get("Content-Type"), testCase.path)
		}
		if testCase.path == "/reject-img" {
			require.NotEmpty(t, body)
		} else if len(testCase.body) == 0 {
			require.Empty(t, body, testCase.path)
		} else {
			require.Equal(t, testCase.body, body, testCase.path)
		}
		require.Equal(t, int64(len(body)), response.ContentLength, testCase.path)
		if strings.HasPrefix(testCase.path, "/mock") {
			require.Equal(t, "applied", response.Header.Get("X-Response-Rewrite"))
		}
		if testCase.path == "/mock-map" {
			require.Equal(t, []string{"map"}, response.Header.Values("X-Header-Form"))
		}
		if testCase.path == "/mock-list" {
			require.Equal(t, []string{"first", "second"}, response.Header.Values("X-Header-Form"))
		}
	}
	require.Zero(t, len(client.dialed))
	require.Zero(t, len(unexpectedUpstream))

	snapshot := mitm.CapturedSessionsSnapshot()
	require.Len(t, snapshot.Sessions, 9)
	mockCapture := snapshot.Sessions[7].Request
	require.Equal(t, "http://"+originalHost+"/mock-map", mockCapture.RawURL)
	require.Equal(t, "http://mock-target.example.com/mock-map", mockCapture.URL)
}

func TestRewriteUserConfigurationValidationAPI(t *testing.T) {
	for _, testCase := range []struct {
		name        string
		config      string
		errContains string
	}{
		{name: "invalid URL type", config: "rewrite:\n  url:\n    - match: '.*'\n      type: unknown\n", errContains: `rewrite.url[0]: invalid type "unknown"`},
		{name: "invalid reject value", config: "rewrite:\n  url:\n    - match: '.*'\n      type: reject\n      value: video\n", errContains: `invalid reject value "video"`},
		{name: "invalid direction", config: "rewrite:\n  header:\n    - match: '.*'\n      direction: request / response\n      type: del\n      field: X-Test\n", errContains: `invalid direction "request / response"`},
		{name: "invalid header regex", config: "rewrite:\n  header:\n    - match: '.*'\n      direction: request\n      type: replace-regex\n      field: X-Test\n      regex: '*.*'\n", errContains: "invalid regex expression"},
		{name: "body modes are exclusive", config: "rewrite:\n  body:\n    - match: '.*'\n      direction: request\n      jq-expression: '.'\n      actions:\n        - regex: x\n          value: y\n", errContains: "actions and jq-expression are mutually exclusive"},
		{name: "invalid jq", config: "rewrite:\n  body:\n    - match: '.*'\n      direction: response\n      jq-expression: '*.*'\n", errContains: "invalid jq-expression"},
		{name: "mock bodies are exclusive", config: "rewrite:\n  mock:\n    - match: '.*'\n      text: text\n      base64: dGV4dA==\n", errContains: "text and base64 are mutually exclusive"},
		{name: "invalid mock base64", config: "rewrite:\n  mock:\n    - match: '.*'\n      base64: '!invalid!'\n", errContains: "invalid base64 body"},
		{name: "invalid mock status", config: "rewrite:\n  mock:\n    - match: '.*'\n      status-code: 700\n", errContains: "invalid status-code 700"},
		{name: "invalid mock headers", config: "rewrite:\n  mock:\n    - match: '.*'\n      headers: value\n", errContains: "mock headers must be a mapping or list of mappings"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := config.Parse([]byte(testCase.config))
			require.ErrorContains(t, err, testCase.errContains)
		})
	}
}

type rewriteTestDial struct {
	Address string
	URL     string
}

type rewriteTestClient struct {
	connection net.Conn
	reader     *bufio.Reader
	host       string
	dialed     chan rewriteTestDial
}

func newRewriteTestClient(t *testing.T, parsedConfig *config.Config, hostname string, upstreamAddress string) *rewriteTestClient {
	t.Helper()
	mitm.SetCaptureEnabled(parsedConfig.Mitm.Capture)
	mitm.ClearCapturedSessions()
	clientConnection, serverConnection := net.Pipe()
	result := make(chan error, 1)
	dialed := make(chan rewriteTestDial, 16)
	interceptor := mitm.New(parsedConfig.Mitm, nil, parsedConfig.Rewrite)
	require.NotNil(t, interceptor)
	go func() {
		handled, err := interceptor.Handle(N.NewBufferedConn(serverConnection), &C.Metadata{
			NetWork: C.TCP,
			Type:    C.HTTP,
			SrcIP:   netip.MustParseAddr("127.0.0.1"),
			SrcPort: 54321,
			Host:    hostname,
			DstPort: 80,
		}, func(ctx context.Context, network, address string) (net.Conn, error) {
			dialed <- rewriteTestDial{Address: address, URL: mitm.RequestURLFromContext(ctx)}
			return (&net.Dialer{}).DialContext(ctx, network, upstreamAddress)
		})
		if !handled && err == nil {
			err = fmt.Errorf("connection was not handled")
		}
		result <- err
	}()

	t.Cleanup(func() {
		_ = clientConnection.Close()
		select {
		case err := <-result:
			require.NoError(t, err)
		case <-time.After(3 * time.Second):
			t.Error("rewrite HTTP handler did not stop after the client closed")
		}
		mitm.SetCaptureEnabled(false)
		mitm.ClearCapturedSessions()
	})
	return &rewriteTestClient{
		connection: clientConnection,
		reader:     bufio.NewReader(clientConnection),
		host:       hostname,
		dialed:     dialed,
	}
}

func (c *rewriteTestClient) do(t *testing.T, method string, path string, body []byte, header http.Header) (*http.Response, []byte) {
	t.Helper()
	request := &http.Request{
		Method: method,
		URL:    mustParseURL(t, "http://"+c.host+path),
		Host:   c.host,
		Header: make(http.Header),
	}
	for field, values := range header {
		request.Header[field] = append([]string(nil), values...)
	}
	if body != nil {
		request.Body = io.NopCloser(bytes.NewReader(body))
		request.ContentLength = int64(len(body))
	}
	require.NoError(t, request.Write(c.connection))
	response, err := http.ReadResponse(c.reader, request)
	require.NoError(t, err)
	responseBody, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	return response, responseBody
}

func parseRewriteTestConfig(t *testing.T, hostname string, rewriteConfig string) *config.Config {
	t.Helper()
	const passphrase = "password"
	_, _, caP12 := newTestAuthority(t, passphrase)
	rawConfig := fmt.Sprintf(`
mitm:
  capture: true
  hostname:
    - %s
  passphrase: %q
  ca-p12: %q
%s
`, hostname, passphrase, base64.StdEncoding.EncodeToString(caP12), rewriteConfig)
	parsedConfig, err := config.Parse([]byte(rawConfig))
	require.NoError(t, err)
	require.NotNil(t, parsedConfig.Mitm)
	require.NotNil(t, parsedConfig.Rewrite)
	return parsedConfig
}

func encodeRewriteTestBody(t *testing.T, body []byte, codings []string) []byte {
	t.Helper()
	encoded := append([]byte(nil), body...)
	for _, coding := range codings {
		var buffer bytes.Buffer
		switch coding {
		case "gzip":
			writer := gzip.NewWriter(&buffer)
			_, err := writer.Write(encoded)
			require.NoError(t, err)
			require.NoError(t, writer.Close())
		case "deflate":
			writer := zlib.NewWriter(&buffer)
			_, err := writer.Write(encoded)
			require.NoError(t, err)
			require.NoError(t, writer.Close())
		case "br":
			writer := brotli.NewWriter(&buffer)
			_, err := writer.Write(encoded)
			require.NoError(t, err)
			require.NoError(t, writer.Close())
		default:
			t.Fatalf("unsupported test content coding %q", coding)
		}
		encoded = buffer.Bytes()
	}
	return encoded
}

func decodeRewriteTestBody(t *testing.T, body []byte, codings []string) []byte {
	t.Helper()
	decoded, err := decodeRewriteTestBodyRaw(body, codings)
	require.NoError(t, err)
	return decoded
}

func decodeRewriteTestBodyRaw(body []byte, codings []string) ([]byte, error) {
	decoded := append([]byte(nil), body...)
	for index := len(codings) - 1; index >= 0; index-- {
		var reader io.ReadCloser
		var err error
		switch codings[index] {
		case "gzip":
			reader, err = gzip.NewReader(bytes.NewReader(decoded))
		case "deflate":
			reader, err = zlib.NewReader(bytes.NewReader(decoded))
		case "br":
			reader = io.NopCloser(brotli.NewReader(bytes.NewReader(decoded)))
		default:
			return nil, fmt.Errorf("unsupported test content coding %q", codings[index])
		}
		if err != nil {
			return nil, err
		}
		decoded, err = io.ReadAll(reader)
		closeErr := reader.Close()
		if err != nil {
			return nil, err
		}
		if closeErr != nil {
			return nil, closeErr
		}
	}
	return decoded, nil
}

func splitRewriteTestCodings(values []string) []string {
	var codings []string
	for _, value := range values {
		for _, coding := range strings.Split(value, ",") {
			if coding = strings.TrimSpace(coding); coding != "" {
				codings = append(codings, coding)
			}
		}
	}
	return codings
}

func receiveRewriteTestValue[T any](t *testing.T, values <-chan T) T {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(3 * time.Second):
		var zero T
		t.Fatal("timed out waiting for rewrite API event")
		return zero
	}
}
