package mitm_test

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/component/mitm"
	"github.com/metacubex/mihomo/component/profile/cachefile"
	"github.com/metacubex/mihomo/config"
	C "github.com/metacubex/mihomo/constant"

	"github.com/metacubex/http"
	"github.com/metacubex/http/httptest"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	originalHome := C.Path.HomeDir()
	cacheHome, err := os.MkdirTemp("", "mihomo-script-test-cache-")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "create script test cache directory: %v\n", err)
		os.Exit(1)
	}
	C.SetHomeDir(cacheHome)
	cache := cachefile.Cache()
	C.SetHomeDir(originalHome)

	exitCode := m.Run()
	if cache.DB != nil {
		if err = cache.Close(); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "close script test cache: %v\n", err)
			exitCode = 1
		}
	}
	if err = os.RemoveAll(cacheHome); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "remove script test cache directory: %v\n", err)
		exitCode = 1
	}
	os.Exit(exitCode)
}

func TestScriptUserRequestResponsePipelineAPI(t *testing.T) {
	const hostname = "script.example.com"
	responseCodings := []string{"gzip"}
	encodedUpstreamBody := encodeRewriteTestBody(t, []byte("upstream"), responseCodings)
	homeDir := useScriptTestHome(t)

	auxiliary := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Host != "http-client.example" {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		_, _ = writer.Write([]byte("auxiliary"))
	}))
	defer auxiliary.Close()

	writeScriptTestFile(t, homeDir, "request-first.js", `
const headers = $request.headers;
if (typeof $notification !== "undefined" || typeof $response !== "undefined") {
  throw new Error("unexpected request globals");
}
if ($script.name !== "request-first" || !$script.binaryBodyMode || !($script.startTime instanceof Date)) {
  throw new Error("unexpected script metadata");
}
headers["X-Order"] = "first";
headers["X-Script-Id"] = $request.id;
headers.Host = "script-updated.example.com";
if (!$persistentStore.write($argument, "script-shared-key")) {
  throw new Error("persistent write failed");
}
const body = $request.body;
body[0] = 9;
$done({
  url: $request.url.replace("script.example.com/pipeline", "script-updated.example.com/changed"),
  headers,
  body,
});
`)
	writeScriptTestFile(t, homeDir, "request-error.js", `
while (true) {}
`)
	writeScriptTestFile(t, homeDir, "request-second.js", fmt.Sprintf(`
$httpClient.get({url: %q, headers: {Host: "http-client.example"}}, function(error, response, data) {
  if (error || response.status !== 200) {
    throw new Error(error || "unexpected status");
  }
  const headers = $request.headers;
  headers["X-Order"] += ",second";
  headers["X-Persisted"] = $persistentStore.read("script-shared-key");
  headers["X-Auxiliary"] = data;
  $done({headers});
});
`, auxiliary.URL))
	writeScriptTestFile(t, homeDir, "response-first.js", `
if ($request.id !== $response.headers["X-Script-Id"]) {
  throw new Error("request id changed");
}
const headers = $response.headers;
headers["X-Response-Order"] = "first";
$done({status: 201, headers, body: $response.body.replace("upstream", "rewritten")});
`)
	writeScriptTestFile(t, homeDir, "response-error.js", `
throw new Error("the final response script must still run");
`)
	writeScriptTestFile(t, homeDir, "response-second.js", `
const headers = $response.headers;
headers["X-Response-Order"] += ",second";
$done({headers, body: $response.body + "-complete"});
`)

	parsedConfig := parseScriptTestConfig(t, hostname, `
scripts:
  request-first:
    enable: true
    type: http-request
    match: '^http://script\.example\.com/'
    path: ./scripts/request-first.js
    interval: 1
    options:
      requires-body: true
      binary-body-mode: true
    argument: persisted-value
  request-error:
    enable: true
    type: http-request
    match: '^http://script\.example\.com/'
    path: ./scripts/request-error.js
    options:
      timeout: 1
  request-second:
    enable: true
    type: http-request
    match: '^http://script\.example\.com/'
    path: ./scripts/request-second.js
  response-first:
    enable: true
    type: http-response
    match: '^http://script(?:-updated)?\.example\.com/'
    path: ./scripts/response-first.js
    options:
      requires-body: true
  response-error:
    enable: true
    type: http-response
    match: '^http://script(?:-updated)?\.example\.com/'
    path: ./scripts/response-error.js
  response-second:
    enable: true
    type: http-response
    match: '^http://script(?:-updated)?\.example\.com/'
    path: ./scripts/response-second.js
    options:
      requires-body: true
`)
	require.NotNil(t, parsedConfig.Scripts)
	t.Cleanup(func() { require.NoError(t, parsedConfig.Scripts.Close()) })

	type upstreamRequest struct {
		body    []byte
		headers http.Header
		host    string
		path    string
		err     error
	}
	upstreamRequests := make(chan upstreamRequest, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		rawBody, err := io.ReadAll(request.Body)
		body, decodeErr := decodeRewriteTestBodyRaw(rawBody, splitRewriteTestCodings(request.Header.Values("Content-Encoding")))
		if err == nil {
			err = decodeErr
		}
		upstreamRequests <- upstreamRequest{
			body:    body,
			headers: request.Header.Clone(),
			host:    request.Host,
			path:    request.URL.Path,
			err:     err,
		}
		writer.Header().Set("X-Script-Id", request.Header.Get("X-Script-Id"))
		writer.Header().Set("Content-Encoding", strings.Join(responseCodings, ", "))
		_, _ = writer.Write(encodedUpstreamBody)
	}))
	defer upstream.Close()

	client := newScriptTestClient(t, parsedConfig, hostname, upstream.Listener.Addr().String())
	requestCodings := []string{"gzip"}
	requestHeader := make(http.Header)
	requestHeader.Set("Accept-Encoding", "gzip")
	requestHeader.Set("Content-Encoding", strings.Join(requestCodings, ", "))
	response, rawResponseBody := client.do(
		t,
		http.MethodPost,
		"/pipeline",
		encodeRewriteTestBody(t, []byte{1, 2, 3}, requestCodings),
		requestHeader,
	)
	require.Equal(t, "gzip", response.Header.Get("Content-Encoding"))
	responseBody := decodeRewriteTestBody(t, rawResponseBody, splitRewriteTestCodings(response.Header.Values("Content-Encoding")))
	seen := receiveRewriteTestValue(t, upstreamRequests)
	require.NoError(t, seen.err)
	require.Equal(t, []byte{9, 2, 3}, seen.body)
	require.Equal(t, "script-updated.example.com", seen.host)
	require.Equal(t, "/changed", seen.path)
	require.Equal(t, "first,second", seen.headers.Get("X-Order"))
	require.Equal(t, "persisted-value", seen.headers.Get("X-Persisted"))
	require.Equal(t, "auxiliary", seen.headers.Get("X-Auxiliary"))
	require.NotEmpty(t, seen.headers.Get("X-Script-Id"))
	require.Equal(t, http.StatusCreated, response.StatusCode)
	require.Equal(t, "first,second", response.Header.Get("X-Response-Order"))
	require.Equal(t, "rewritten-complete", string(responseBody))
}

func TestScriptUserMockAndBodyLimitAPI(t *testing.T) {
	const hostname = "script-limit.example.com"
	homeDir := useScriptTestHome(t)
	writeScriptTestFile(t, homeDir, "body-limit.js", `
const headers = $request.headers;
headers["X-Body-Script-Ran"] = "true";
$done({headers, body: "changed"});
`)
	writeScriptTestFile(t, homeDir, "mock.js", `
if ($request.url.endsWith("/abort-request")) {
  $done({abort: true});
} else if ($request.url.endsWith("/mock")) {
  $done({response: {status: 202, headers: {"Content-Type": "text/plain"}, body: $argument}});
} else {
  $done({});
}
`)
	writeScriptTestFile(t, homeDir, "response-abort.js", `
$done({abort: $request.url.endsWith("/abort-response")});
`)
	parsedConfig := parseScriptTestConfig(t, hostname, `
scripts:
  body-limit:
    enable: true
    type: http-request
    match: '^http://script-limit\.example\.com/'
    path: ./scripts/body-limit.js
    options:
      requires-body: true
      max-body-size: 0
  mock:
    enable: true
    type: http-request
    match: '^http://script-limit\.example\.com/'
    path: ./scripts/mock.js
    argument: mocked-response
  response-abort:
    enable: true
    type: http-response
    match: '^http://script-limit\.example\.com/'
    path: ./scripts/response-abort.js
`)
	t.Cleanup(func() { require.NoError(t, parsedConfig.Scripts.Close()) })

	upstreamRequests := make(chan []byte, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, _ := io.ReadAll(request.Body)
		upstreamRequests <- body
		_, _ = writer.Write([]byte("upstream"))
	}))
	defer upstream.Close()
	client := newScriptTestClient(t, parsedConfig, hostname, upstream.Listener.Addr().String())

	response, body := client.do(t, http.MethodPost, "/passthrough", []byte("original"), nil)
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.Empty(t, response.Header.Get("X-Body-Script-Ran"))
	require.Equal(t, "upstream", string(body))
	require.Equal(t, []byte("original"), receiveRewriteTestValue(t, upstreamRequests))

	response, body = client.do(t, http.MethodGet, "/mock", nil, nil)
	require.Equal(t, http.StatusAccepted, response.StatusCode)
	require.Equal(t, "text/plain", response.Header.Get("Content-Type"))
	require.Equal(t, "mocked-response", string(body))
	select {
	case unexpected := <-upstreamRequests:
		t.Fatalf("mock request unexpectedly reached upstream: %q", unexpected)
	case <-time.After(100 * time.Millisecond):
	}

	requestAbortClient := newScriptTestClient(t, parsedConfig, hostname, upstream.Listener.Addr().String())
	expectScriptTestConnectionAbort(t, requestAbortClient, "/abort-request")
	responseAbortClient := newScriptTestClient(t, parsedConfig, hostname, upstream.Listener.Addr().String())
	expectScriptTestConnectionAbort(t, responseAbortClient, "/abort-response")
}

func TestScriptUserIndirectEvalAndTimerAPI(t *testing.T) {
	const hostname = "script-timer.example.com"
	homeDir := useScriptTestHome(t)
	writeScriptTestFile(t, homeDir, "timer.js", `
function evaluateLocalScope() {
  const localOnly = "local";
  return eval("typeof localOnly");
}

let canceledTimerRan = false;
const canceledTimer = setTimeout(function () {
  canceledTimerRan = true;
}, 0);
clearTimeout(canceledTimer);

const evalResult = evaluateLocalScope();
setTimeout(function (prefix, suffix) {
  if (canceledTimerRan) {
    throw new Error("clearTimeout did not cancel the callback");
  }
  $done({
    response: {
      status: 200,
      headers: {"Content-Type": "text/plain"},
      body: evalResult + "|" + prefix + suffix,
    },
  });
}, 10, "timer-", "ok");
`)
	parsedConfig := parseScriptTestConfig(t, hostname, `
scripts:
  timer:
    enable: true
    type: http-request
    match: '^http://script-timer\.example\.com/'
    path: ./scripts/timer.js
    options:
      indirect-eval: true
`)
	t.Cleanup(func() { require.NoError(t, parsedConfig.Scripts.Close()) })

	upstreamRequests := make(chan struct{}, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		upstreamRequests <- struct{}{}
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()
	client := newScriptTestClient(t, parsedConfig, hostname, upstream.Listener.Addr().String())

	response, body := client.do(t, http.MethodGet, "/run", nil, nil)
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.Equal(t, "text/plain", response.Header.Get("Content-Type"))
	require.Equal(t, "undefined|timer-ok", string(body))
	select {
	case <-upstreamRequests:
		t.Fatal("timer-generated response unexpectedly reached upstream")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestScriptUserRemoteUpdateAndCronAPI(t *testing.T) {
	homeDir := useScriptTestHome(t)
	ticks := make(chan string, 16)
	var sourceMutex sync.RWMutex
	var scriptSource string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/tick" {
			ticks <- request.Header.Get("X-Generation")
			writer.WriteHeader(http.StatusNoContent)
			return
		}
		sourceMutex.RLock()
		defer sourceMutex.RUnlock()
		_, _ = writer.Write([]byte(scriptSource))
	}))
	defer server.Close()

	makeSource := func(generation string) string {
		return fmt.Sprintf(`
if ($cronexp !== "* * * * * *") throw new Error("unexpected cron expression");
$httpClient.get({url: %q, headers: {"X-Generation": %q}}, function(error) {
  if (error) throw new Error(error);
  $done();
});
`, server.URL+"/tick", generation)
	}
	sourceMutex.Lock()
	scriptSource = makeSource("first")
	sourceMutex.Unlock()
	scriptURL := server.URL + "/script.js"
	parsedConfig, err := config.Parse([]byte(fmt.Sprintf(`
scripts:
  remote-cron:
    enable: true
    type: cron
    cron: '* * * * * *'
    url: %q
    interval: 1
  remote-custom-path:
    enable: true
    type: cron
    cron: '0 0 1 1 *'
    path: ./scripts/custom-cron.js
    url: %q
`, scriptURL, scriptURL)))
	require.NoError(t, err)
	require.NotNil(t, parsedConfig.Scripts)
	parsedConfig.Scripts.Start()
	t.Cleanup(func() { require.NoError(t, parsedConfig.Scripts.Close()) })

	cachePath := C.Path.GetPathByHash("scripts", scriptURL)
	require.Equal(t, filepath.Join(homeDir, "scripts", filepath.Base(cachePath)), cachePath)
	initialCache, err := os.ReadFile(cachePath)
	require.NoError(t, err)
	require.Equal(t, strings.TrimSpace(makeSource("first")), strings.TrimSpace(string(initialCache)))
	customCache, err := os.ReadFile(filepath.Join(homeDir, "scripts", "custom-cron.js"))
	require.NoError(t, err)
	require.Equal(t, strings.TrimSpace(makeSource("first")), strings.TrimSpace(string(customCache)))
	require.Equal(t, "first", receiveScriptTestValue(t, ticks, 3*time.Second))

	sourceMutex.Lock()
	scriptSource = makeSource("second")
	sourceMutex.Unlock()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case generation := <-ticks:
			if generation == "second" {
				updatedCache, readErr := os.ReadFile(cachePath)
				require.NoError(t, readErr)
				require.Equal(t, strings.TrimSpace(makeSource("second")), strings.TrimSpace(string(updatedCache)))
				return
			}
		case <-deadline:
			t.Fatal("remote cron script was not updated and executed")
		}
	}
}

func TestScriptUserConfigurationValidationAPI(t *testing.T) {
	useScriptTestHome(t)
	for _, testCase := range []struct {
		name        string
		config      string
		errContains string
	}{
		{name: "enable required", config: "scripts:\n  test:\n    type: http-request\n    match: '.*'\n    path: ./scripts/test.js\n", errContains: "scripts.test: enable is required"},
		{name: "type required", config: "scripts:\n  test:\n    enable: false\n    path: ./scripts/test.js\n", errContains: `invalid type ""`},
		{name: "HTTP match required", config: "scripts:\n  test:\n    enable: false\n    type: http-request\n    path: ./scripts/test.js\n", errContains: "match is required"},
		{name: "invalid regex", config: "scripts:\n  test:\n    enable: false\n    type: http-response\n    match: '*.*'\n    path: ./scripts/test.js\n", errContains: "invalid match expression"},
		{name: "cron required", config: "scripts:\n  test:\n    enable: false\n    type: cron\n    path: ./scripts/test.js\n", errContains: "cron is required"},
		{name: "invalid cron", config: "scripts:\n  test:\n    enable: false\n    type: cron\n    cron: '* * *'\n    path: ./scripts/test.js\n", errContains: "invalid cron expression"},
		{name: "cron descriptor unsupported", config: "scripts:\n  test:\n    enable: false\n    type: cron\n    cron: '@daily'\n    path: ./scripts/test.js\n", errContains: "must contain 5 or 6 fields"},
		{name: "cron timezone override unsupported", config: "scripts:\n  test:\n    enable: false\n    type: cron\n    cron: 'CRON_TZ=UTC 0 0 * * *'\n    path: ./scripts/test.js\n", errContains: "system timezone is used"},
		{name: "source required", config: "scripts:\n  test:\n    enable: false\n    type: cron\n    cron: '* * * * *'\n", errContains: "path or url is required"},
		{name: "duplicate path", config: "scripts:\n  first:\n    enable: false\n    type: cron\n    cron: '* * * * *'\n    path: ./scripts/same.js\n  second:\n    enable: false\n    type: cron\n    cron: '* * * * *'\n    path: ./scripts/same.js\n", errContains: "path duplicates script"},
		{name: "invalid timeout", config: "scripts:\n  test:\n    enable: false\n    type: cron\n    cron: '* * * * *'\n    path: ./scripts/test.js\n    options:\n      timeout: 0\n", errContains: "options.timeout must be greater than zero"},
		{name: "invalid body size", config: "scripts:\n  test:\n    enable: false\n    type: http-request\n    match: '.*'\n    path: ./scripts/test.js\n    options:\n      max-body-size: -2\n", errContains: "options.max-body-size must be -1 or greater"},
		{name: "invalid URL", config: "scripts:\n  test:\n    enable: false\n    type: cron\n    cron: '* * * * *'\n    url: file:///tmp/test.js\n", errContains: "invalid url"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := config.Parse([]byte(testCase.config))
			require.ErrorContains(t, err, testCase.errContains)
		})
	}
}

func parseScriptTestConfig(t *testing.T, hostname string, scripts string) *config.Config {
	t.Helper()
	const passphrase = "password"
	_, _, caP12 := newTestAuthority(t, passphrase)
	rawConfig := fmt.Sprintf(`
mitm:
  hostname:
    - %s
  passphrase: %q
  ca-p12: %q
%s
`, hostname, passphrase, base64.StdEncoding.EncodeToString(caP12), scripts)
	parsedConfig, err := config.Parse([]byte(rawConfig))
	require.NoError(t, err)
	require.NotNil(t, parsedConfig.Mitm)
	require.NotNil(t, parsedConfig.Scripts)
	return parsedConfig
}

func useScriptTestHome(t *testing.T) string {
	t.Helper()
	originalHome := C.Path.HomeDir()
	homeDir := t.TempDir()
	C.SetHomeDir(homeDir)
	t.Cleanup(func() { C.SetHomeDir(originalHome) })
	return homeDir
}

func writeScriptTestFile(t *testing.T, homeDir, name, content string) {
	t.Helper()
	directory := filepath.Join(homeDir, "scripts")
	require.NoError(t, os.MkdirAll(directory, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(directory, name), []byte(strings.TrimSpace(content)), 0o600))
}

func receiveScriptTestValue[T any](t *testing.T, values <-chan T, timeout time.Duration) T {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(timeout):
		var zero T
		t.Fatal("timed out waiting for script API event")
		return zero
	}
}

func expectScriptTestConnectionAbort(t *testing.T, client *rewriteTestClient, path string) {
	t.Helper()
	request := &http.Request{
		Method: http.MethodGet,
		URL:    mustParseURL(t, "http://"+client.host+path),
		Host:   client.host,
		Header: make(http.Header),
	}
	require.NoError(t, request.Write(client.connection))
	_, err := http.ReadResponse(client.reader, request)
	require.Error(t, err)
}

func newScriptTestClient(t *testing.T, parsedConfig *config.Config, hostname string, upstreamAddress string) *rewriteTestClient {
	t.Helper()
	clientConnection, serverConnection := net.Pipe()
	result := make(chan error, 1)
	dialed := make(chan rewriteTestDial, 16)
	interceptor := mitm.NewWithRewriteAndScripts(parsedConfig.Mitm, nil, parsedConfig.Rewrite, parsedConfig.Scripts)
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
			t.Error("script HTTP handler did not stop after the client closed")
		}
	})
	return &rewriteTestClient{
		connection: clientConnection,
		reader:     bufio.NewReader(clientConnection),
		host:       hostname,
		dialed:     dialed,
	}
}
