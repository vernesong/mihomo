package mitm_test

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/component/ca"
	"github.com/metacubex/mihomo/component/mitm"
	"github.com/metacubex/mihomo/config"
	C "github.com/metacubex/mihomo/constant"
	_ "github.com/metacubex/mihomo/hub/executor"

	"github.com/metacubex/http"
	"github.com/metacubex/http/http2"
	"github.com/metacubex/http/httptest"
	"github.com/metacubex/tls"
	"github.com/stretchr/testify/require"
	pkcs12 "software.sslmate.com/src/go-pkcs12"
)

func TestMitmUserConfigurationAndHTTPAPI(t *testing.T) {
	const (
		passphrase         = "password"
		hostname           = "api.example.com"
		firstConnectionID  = "11111111-1111-4111-8111-111111111111"
		secondConnectionID = "22222222-2222-4222-8222-222222222222"
	)

	caCertificate, caKey, caP12 := newTestAuthority(t, passphrase)
	mitm.SetCaptureEnabled(false)
	mitm.ClearCapturedSessions()
	t.Cleanup(func() {
		mitm.SetCaptureEnabled(false)
		mitm.ClearCapturedSessions()
	})
	ca.ResetCertificate()
	require.NoError(t, ca.AddCertificate(string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caCertificate.Raw}))))
	t.Cleanup(ca.ResetCertificate)

	originalHome := C.Path.HomeDir()
	temporaryHome := t.TempDir()
	C.SetHomeDir(temporaryHome)
	t.Cleanup(func() { C.SetHomeDir(originalHome) })
	caPath := filepath.Join(temporaryHome, "mitm-ca.p12")
	require.NoError(t, os.WriteFile(caPath, caP12, 0o600))

	for _, testCase := range []struct {
		name     string
		h2       bool
		caSource string
	}{
		{name: "base64 HTTP/1.1", caSource: base64.StdEncoding.EncodeToString(caP12)},
		{name: "path HTTP/2", h2: true, caSource: caPath},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			rawConfig := fmt.Sprintf(`
mitm:
  h2: %t
  capture: true
  hostname:
    - "*.example.com"
  hostname-exclude:
    - "*.pinned.example.com"
  client-source-address:
    - 127.0.0.0/8
  passphrase: %q
  ca-p12: %q
`, testCase.h2, passphrase, testCase.caSource)
			parsedConfig, err := config.Parse([]byte(rawConfig))
			require.NoError(t, err)
			require.NotNil(t, parsedConfig.Mitm)
			require.True(t, parsedConfig.Mitm.Capture)
			require.True(t, parsedConfig.Mitm.Match(hostname, netip.MustParseAddr("127.0.0.1")))
			require.False(t, parsedConfig.Mitm.Match("a.pinned.example.com", netip.MustParseAddr("127.0.0.1")))
			require.False(t, parsedConfig.Mitm.Match(hostname, netip.MustParseAddr("10.0.0.1")))

			upstreamCertificate := newTestServerCertificate(t, hostname, caCertificate, caKey)
			upstream := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				writer.Header().Set("X-Upstream-Response", "visible")
				_, _ = fmt.Fprintf(writer, "%s|%s", request.Header.Get("X-MITM-Request"), request.Proto)
			}))
			upstream.EnableHTTP2 = testCase.h2
			upstream.TLS = &tls.Config{Certificates: []tls.Certificate{upstreamCertificate}}
			upstream.StartTLS()
			defer upstream.Close()

			handler := &apiHandler{
				requestURLs:   make(chan string, 1),
				connectionIDs: make(chan string, 1),
			}
			mitm.ClearCapturedSessions()
			mitm.SetCaptureEnabled(parsedConfig.Mitm.Capture)
			interceptor := mitm.New(parsedConfig.Mitm, handler)
			require.NotNil(t, interceptor)

			clientConn, serverConn := net.Pipe()
			handleResult := make(chan error, 1)
			dialedURLs := make(chan string, 4)
			go func() {
				handled, err := interceptor.Handle(N.NewBufferedConn(serverConn), &C.Metadata{
					NetWork: C.TCP,
					Type:    C.TUN,
					SrcIP:   netip.MustParseAddr("127.0.0.1"),
					SrcPort: 54321,
					Host:    hostname,
					DstPort: 443,
				}, func(ctx context.Context, _, _ string) (net.Conn, error) {
					requestURL := mitm.RequestURLFromContext(ctx)
					dialedURLs <- requestURL
					connection, err := (&net.Dialer{}).DialContext(ctx, "tcp", upstream.Listener.Addr().String())
					if err != nil {
						return nil, err
					}
					connectionID := firstConnectionID
					if strings.Contains(requestURL, "/statistics?") {
						connectionID = secondConnectionID
					}
					return &apiTrackedConnection{Conn: connection, id: connectionID}, nil
				})
				if !handled && err == nil {
					err = fmt.Errorf("connection was not handled")
				}
				handleResult <- err
			}()

			rootPool := x509.NewCertPool()
			rootPool.AddCert(caCertificate)
			nextProtos := []string{"http/1.1"}
			if testCase.h2 {
				nextProtos = []string{"h2"}
			}
			tlsClient := tls.Client(clientConn, &tls.Config{
				RootCAs:    rootPool,
				ServerName: hostname,
				NextProtos: nextProtos,
			})
			require.NoError(t, tlsClient.Handshake())

			var h2Client *http2.ClientConn
			if testCase.h2 {
				h2Transport := &http2.Transport{}
				h2Client, err = h2Transport.NewClientConn(tlsClient)
				require.NoError(t, err)
			}
			responseReader := bufio.NewReader(tlsClient)
			requestCases := []struct {
				path         string
				rawQuery     string
				method       string
				body         string
				capture      bool
				local        bool
				dial         bool
				connectionID string
			}{
				{path: "/index.html", rawQuery: "source=mitm", method: http.MethodPost, body: "request-body", capture: true, dial: true, connectionID: firstConnectionID},
				{path: "/index.html", rawQuery: "source=mitm", method: http.MethodGet, capture: false, connectionID: firstConnectionID},
				{path: "/statistics", rawQuery: "event=finished", method: http.MethodGet, capture: false, dial: true, connectionID: secondConnectionID},
				{path: "/local", rawQuery: "reason=reject", method: http.MethodGet, capture: false, local: true},
			}
			for _, requestCase := range requestCases {
				mitm.SetCaptureEnabled(requestCase.capture)
				request := &http.Request{
					Method: requestCase.method,
					URL:    &url.URL{Scheme: "https", Host: hostname, Path: requestCase.path, RawQuery: requestCase.rawQuery},
					Host:   hostname,
					Header: make(http.Header),
				}
				request.Header.Set("X-Client-Request", "visible")
				if requestCase.body != "" {
					request.Body = io.NopCloser(strings.NewReader(requestCase.body))
					request.ContentLength = int64(len(requestCase.body))
					request.Header.Set("Content-Type", "text/plain")
				}
				var response *http.Response
				if testCase.h2 {
					response, err = h2Client.RoundTrip(request)
					require.NoError(t, err)
				} else {
					require.NoError(t, request.Write(tlsClient))
					response, err = http.ReadResponse(responseReader, request)
					require.NoError(t, err)
				}
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				require.NoError(t, response.Body.Close())
				if requestCase.local {
					require.Equal(t, http.StatusNoContent, response.StatusCode)
					require.Empty(t, body)
					require.Empty(t, response.Header.Get("X-MITM-Response"))
				} else {
					require.Equal(t, http.StatusOK, response.StatusCode)
					require.Equal(t, "handled", response.Header.Get("X-MITM-Response"))
					if testCase.h2 {
						require.Equal(t, "handled|HTTP/2.0", string(body))
					} else {
						require.Equal(t, "handled|HTTP/1.1", string(body))
					}
				}
				requestURL := "https://" + hostname + requestCase.path + "?" + requestCase.rawQuery
				if !requestCase.local {
					if requestCase.dial {
						require.Equal(t, requestURL, <-dialedURLs)
					}
					require.Equal(t, requestCase.connectionID, <-handler.connectionIDs)
				}
				require.Equal(t, requestURL, <-handler.requestURLs)
			}
			require.Empty(t, dialedURLs)
			require.Equal(t, hostname, handler.hostname)
			if h2Client != nil {
				require.NoError(t, h2Client.Close())
			}
			_ = tlsClient.Close()

			select {
			case err := <-handleResult:
				require.NoError(t, err)
			case <-time.After(3 * time.Second):
				t.Fatal("MITM handler did not stop after the client closed")
			}
			require.Empty(t, handler.errors)

			snapshot := mitm.CapturedSessionsSnapshot()
			require.False(t, snapshot.Capture)
			require.Equal(t, mitm.CaptureHistoryLimit, snapshot.Limit)
			require.Len(t, snapshot.Sessions, len(requestCases))
			var previousRequestIndex uint64
			for index, requestCase := range requestCases {
				captured := snapshot.Sessions[index]
				require.Equal(t, requestCase.connectionID, captured.ConnectionID)
				require.Greater(t, captured.RequestIndex, previousRequestIndex)
				previousRequestIndex = captured.RequestIndex
				require.Equal(t, requestCase.capture, captured.Capture)
				require.Equal(t, requestCase.method, captured.Request.Method)
				require.Equal(t, "https://"+hostname+requestCase.path+"?"+requestCase.rawQuery, captured.Request.URL)
				require.Equal(t, hostname, captured.Request.Headers.Get("Host"))
				require.Equal(t, "visible", captured.Request.Headers.Get("X-Client-Request"))
				require.NotNil(t, captured.Response)
				if requestCase.local {
					require.Equal(t, http.StatusNoContent, captured.Response.StatusCode)
				} else {
					require.Equal(t, http.StatusOK, captured.Response.StatusCode)
					require.Equal(t, "visible", captured.Response.Headers.Get("X-Upstream-Response"))
					require.Equal(t, "handled", captured.Response.Headers.Get("X-MITM-Response"))
				}
				require.NotNil(t, captured.CompletedAt)
				if requestCase.capture {
					require.NotNil(t, captured.Request.Body)
					require.Equal(t, requestCase.body, captured.Request.Body.Content)
					require.Equal(t, "utf8", captured.Request.Body.Encoding)
					require.True(t, captured.Request.Body.Complete)
					require.NotNil(t, captured.Response.Body)
					require.Equal(t, "utf8", captured.Response.Body.Encoding)
					require.True(t, captured.Response.Body.Complete)
					if testCase.h2 {
						require.Equal(t, "handled|HTTP/2.0", captured.Response.Body.Content)
					} else {
						require.Equal(t, "handled|HTTP/1.1", captured.Response.Body.Content)
					}
				} else {
					require.Nil(t, captured.Request.Body)
					require.Nil(t, captured.Response.Body)
				}
			}
		})
	}
}

func TestMitmUserPlainHTTPAPI(t *testing.T) {
	const (
		passphrase   = "password"
		hostname     = "plain.example.com"
		connectionID = "33333333-3333-4333-8333-333333333333"
	)
	_, _, caP12 := newTestAuthority(t, passphrase)

	for _, testCase := range []struct {
		name string
		h2   bool
	}{
		{name: "HTTP/1.1"},
		{name: "unencrypted HTTP/2", h2: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			mitm.SetCaptureEnabled(false)
			mitm.ClearCapturedSessions()
			t.Cleanup(func() {
				mitm.SetCaptureEnabled(false)
				mitm.ClearCapturedSessions()
			})

			rawConfig := fmt.Sprintf(`
mitm:
  h2: %t
  capture: true
  hostname:
    - %s
  passphrase: %q
  ca-p12: %q
`, testCase.h2, hostname, passphrase, base64.StdEncoding.EncodeToString(caP12))
			parsedConfig, err := config.Parse([]byte(rawConfig))
			require.NoError(t, err)
			require.NotNil(t, parsedConfig.Mitm)
			require.True(t, parsedConfig.Mitm.MatchHostPort(hostname, 80, netip.MustParseAddr("127.0.0.1")))

			upstreamHandler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				writer.Header().Set("X-Upstream-Response", "visible")
				_, _ = fmt.Fprintf(writer, "%s|%s", request.Header.Get("X-MITM-Request"), request.Proto)
			})
			upstream := httptest.NewUnstartedServer(upstreamHandler)
			if testCase.h2 {
				protocols := new(http.Protocols)
				protocols.SetUnencryptedHTTP2(true)
				upstream.Config.Protocols = protocols
				require.NoError(t, http2.ConfigureServer(upstream.Config, &http2.Server{}))
			}
			upstream.Start()
			defer upstream.Close()

			handler := &apiHandler{
				requestURLs:   make(chan string, 1),
				connectionIDs: make(chan string, 1),
			}
			mitm.SetCaptureEnabled(parsedConfig.Mitm.Capture)
			interceptor := mitm.New(parsedConfig.Mitm, handler)
			require.NotNil(t, interceptor)

			clientConn, serverConn := net.Pipe()
			handleResult := make(chan error, 1)
			dialedURLs := make(chan string, 1)
			go func() {
				handled, err := interceptor.Handle(N.NewBufferedConn(serverConn), &C.Metadata{
					NetWork: C.TCP,
					Type:    C.HTTP,
					SrcIP:   netip.MustParseAddr("127.0.0.1"),
					SrcPort: 54321,
					Host:    hostname,
					DstPort: 80,
				}, func(ctx context.Context, _, _ string) (net.Conn, error) {
					dialedURLs <- mitm.RequestURLFromContext(ctx)
					connection, err := (&net.Dialer{}).DialContext(ctx, "tcp", upstream.Listener.Addr().String())
					if err != nil {
						return nil, err
					}
					return &apiTrackedConnection{Conn: connection, id: connectionID}, nil
				})
				if !handled && err == nil {
					err = fmt.Errorf("connection was not handled")
				}
				handleResult <- err
			}()

			requestURL := "http://" + hostname + "/index.html?source=plain-http"
			request := &http.Request{
				Method:        http.MethodPost,
				URL:           mustParseURL(t, requestURL),
				Host:          hostname,
				Header:        make(http.Header),
				Body:          io.NopCloser(strings.NewReader("plain-request-body")),
				ContentLength: int64(len("plain-request-body")),
			}
			request.Header.Set("Content-Type", "text/plain")
			request.Header.Set("X-Client-Request", "visible")

			var response *http.Response
			var h2Client *http2.ClientConn
			if testCase.h2 {
				h2Transport := &http2.Transport{}
				h2Client, err = h2Transport.NewClientConn(clientConn)
				require.NoError(t, err)
				response, err = h2Client.RoundTrip(request)
			} else {
				require.NoError(t, request.Write(clientConn))
				response, err = http.ReadResponse(bufio.NewReader(clientConn), request)
			}
			require.NoError(t, err)
			responseBody, err := io.ReadAll(response.Body)
			require.NoError(t, err)
			require.NoError(t, response.Body.Close())
			require.Equal(t, http.StatusOK, response.StatusCode)
			require.Equal(t, "handled", response.Header.Get("X-MITM-Response"))
			if testCase.h2 {
				require.Equal(t, "handled|HTTP/2.0", string(responseBody))
			} else {
				require.Equal(t, "handled|HTTP/1.1", string(responseBody))
			}
			require.Equal(t, requestURL, <-dialedURLs)
			require.Equal(t, requestURL, <-handler.requestURLs)
			require.Equal(t, connectionID, <-handler.connectionIDs)
			require.Equal(t, hostname, handler.hostname)

			if h2Client != nil {
				require.NoError(t, h2Client.Close())
			}
			_ = clientConn.Close()
			select {
			case err := <-handleResult:
				require.NoError(t, err)
			case <-time.After(3 * time.Second):
				t.Fatal("plain HTTP handler did not stop after the client closed")
			}
			require.Empty(t, handler.errors)

			snapshot := mitm.CapturedSessionsSnapshot()
			require.True(t, snapshot.Capture)
			require.Len(t, snapshot.Sessions, 1)
			captured := snapshot.Sessions[0]
			require.Equal(t, connectionID, captured.ConnectionID)
			require.Equal(t, requestURL, captured.Request.URL)
			require.Equal(t, hostname, captured.Request.Headers.Get("Host"))
			require.Equal(t, "visible", captured.Request.Headers.Get("X-Client-Request"))
			require.NotNil(t, captured.Request.Body)
			require.Equal(t, "plain-request-body", captured.Request.Body.Content)
			require.NotNil(t, captured.Response)
			require.Equal(t, "visible", captured.Response.Headers.Get("X-Upstream-Response"))
			require.NotNil(t, captured.Response.Body)
			require.Equal(t, string(responseBody), captured.Response.Body.Content)
			require.NotNil(t, captured.CompletedAt)
		})
	}
}

func TestMitmUserUpstreamFailureTransactionAPI(t *testing.T) {
	const (
		passphrase = "password"
		hostname   = "failure.example.com"
	)
	_, _, caP12 := newTestAuthority(t, passphrase)
	parsedConfig, err := config.Parse([]byte(fmt.Sprintf(`
mitm:
  hostname:
    - %s
  passphrase: %q
  ca-p12: %q
`, hostname, passphrase, base64.StdEncoding.EncodeToString(caP12))))
	require.NoError(t, err)

	mitm.SetCaptureEnabled(false)
	mitm.ClearCapturedSessions()
	t.Cleanup(func() {
		mitm.SetCaptureEnabled(false)
		mitm.ClearCapturedSessions()
	})

	interceptor := mitm.New(parsedConfig.Mitm, nil)
	require.NotNil(t, interceptor)
	clientConnection, serverConnection := net.Pipe()
	handleResult := make(chan error, 1)
	go func() {
		handled, handleErr := interceptor.Handle(N.NewBufferedConn(serverConnection), &C.Metadata{
			NetWork: C.TCP,
			Type:    C.HTTP,
			SrcIP:   netip.MustParseAddr("127.0.0.1"),
			SrcPort: 54321,
			Host:    hostname,
			DstPort: 80,
		}, func(context.Context, string, string) (net.Conn, error) {
			return nil, errors.New("upstream unavailable")
		})
		if !handled && handleErr == nil {
			handleErr = errors.New("connection was not handled")
		}
		handleResult <- handleErr
	}()

	request := &http.Request{
		Method: http.MethodGet,
		URL:    mustParseURL(t, "http://"+hostname+"/failure"),
		Host:   hostname,
		Header: make(http.Header),
	}
	require.NoError(t, request.Write(clientConnection))
	response, err := http.ReadResponse(bufio.NewReader(clientConnection), request)
	require.NoError(t, err)
	require.Equal(t, http.StatusBadGateway, response.StatusCode)
	_, err = io.ReadAll(response.Body)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Contains(t, response.Header.Get("Warning"), "upstream unavailable")
	require.NoError(t, clientConnection.Close())

	select {
	case handleErr := <-handleResult:
		require.NoError(t, handleErr)
	case <-time.After(3 * time.Second):
		t.Fatal("failed HTTP transaction did not stop after the client closed")
	}

	snapshot := mitm.CapturedSessionsSnapshot()
	require.Len(t, snapshot.Sessions, 1)
	transaction := snapshot.Sessions[0]
	require.NotEmpty(t, transaction.TransactionID)
	require.Empty(t, transaction.ConnectionID)
	require.Equal(t, "failed", string(transaction.State))
	require.NotNil(t, transaction.CompletedAt)
	require.False(t, transaction.Modified)
	require.Equal(t, "upstream unavailable", transaction.Error)
	require.NotNil(t, transaction.Failure)
	require.Equal(t, "upstream", transaction.Failure.Stage)
	require.Equal(t, "upstream", string(transaction.Failure.Source))
	require.Equal(t, "upstream unavailable", transaction.Failure.Message)
	require.NotNil(t, transaction.Response)
	require.Equal(t, http.StatusBadGateway, transaction.Response.StatusCode)
}

func TestMitmUserConfigurationDefaultsAndHostnamePorts(t *testing.T) {
	const passphrase = "password"
	_, _, caP12 := newTestAuthority(t, passphrase)
	rawConfig := fmt.Sprintf(`
mitm:
  hostname:
    - standard.example.com
    - plain.example.com:80
    - secure.example.com:443
    - custom.example.com:8443
    - "*.any.example.com:0"
    - blocked.example.com:0
  hostname-exclude:
    - blocked.example.com:0
  passphrase: %q
  ca-p12: %q
`, passphrase, base64.StdEncoding.EncodeToString(caP12))

	parsedConfig, err := config.Parse([]byte(rawConfig))
	require.NoError(t, err)
	require.NotNil(t, parsedConfig.Mitm)
	require.False(t, parsedConfig.Mitm.Capture)

	ipv4 := netip.MustParseAddr("192.0.2.1")
	ipv6 := netip.MustParseAddr("2001:db8::1")
	require.True(t, parsedConfig.Mitm.Match("standard.example.com", ipv4))
	require.True(t, parsedConfig.Mitm.Match("standard.example.com", ipv6))
	require.True(t, parsedConfig.Mitm.MatchHostPort("standard.example.com", 80, ipv4))
	require.False(t, parsedConfig.Mitm.MatchHostPort("standard.example.com", 8443, ipv4))
	require.True(t, parsedConfig.Mitm.MatchHostPort("plain.example.com", 80, ipv4))
	require.False(t, parsedConfig.Mitm.MatchHostPort("plain.example.com", 443, ipv4))
	require.True(t, parsedConfig.Mitm.MatchHostPort("secure.example.com", 443, ipv6))
	require.False(t, parsedConfig.Mitm.MatchHostPort("secure.example.com", 80, ipv6))
	require.True(t, parsedConfig.Mitm.MatchHostPort("custom.example.com", 8443, ipv6))
	require.False(t, parsedConfig.Mitm.MatchHostPort("custom.example.com", 443, ipv4))
	require.True(t, parsedConfig.Mitm.MatchHostPort("edge.any.example.com", 443, ipv4))
	require.True(t, parsedConfig.Mitm.MatchHostPort("edge.any.example.com", 9443, ipv6))
	require.False(t, parsedConfig.Mitm.MatchHostPort("blocked.example.com", 443, ipv4))
	require.False(t, parsedConfig.Mitm.MatchHostPort("blocked.example.com", 8443, ipv6))
}

func TestURLRegexUserRule(t *testing.T) {
	parsedConfig, err := config.Parse([]byte(`
rules:
  - URL-REGEX,^https?:\/\/update\.pan\.baidu\.com\/statistics,REJECT
`))
	require.NoError(t, err)
	require.Len(t, parsedConfig.Rules, 1)

	rule := parsedConfig.Rules[0]
	require.Equal(t, C.URLRegex, rule.RuleType())
	matched, adapter := rule.Match(&C.Metadata{
		URL: "https://update.pan.baidu.com/statistics?event=started",
	}, C.RuleMatchHelper{})
	require.True(t, matched)
	require.Equal(t, "REJECT", adapter)

	matched, _ = rule.Match(&C.Metadata{URL: "https://update.pan.baidu.com/download"}, C.RuleMatchHelper{})
	require.False(t, matched)
	matched, _ = rule.Match(&C.Metadata{Host: "update.pan.baidu.com"}, C.RuleMatchHelper{})
	require.False(t, matched)
}

type apiHandler struct {
	hostname      string
	requestURLs   chan string
	connectionIDs chan string
	errors        []error
}

type apiTrackedConnection struct {
	net.Conn
	id string
}

func (c *apiTrackedConnection) ID() string {
	return c.id
}

func (h *apiHandler) HandleRequest(session *mitm.Session) (*http.Request, *http.Response) {
	h.hostname = session.Metadata().Host
	h.requestURLs <- session.Metadata().RemoteDestination()
	if session.Request().URL.Path == "/local" {
		return nil, session.NewResponse(http.StatusNoContent, nil)
	}
	session.Request().Header.Set("X-MITM-Request", "handled")
	return nil, nil
}

func (h *apiHandler) HandleResponse(session *mitm.Session) *http.Response {
	h.connectionIDs <- session.Metadata().UUID
	session.Response().Header.Set("X-MITM-Response", "handled")
	return nil
}

func (h *apiHandler) HandleError(_ *mitm.Session, err error) {
	h.errors = append(h.errors, err)
}

func newTestAuthority(t *testing.T, passphrase string) (*x509.Certificate, *rsa.PrivateKey, []byte) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "mihomo MITM test CA"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, key.Public(), key)
	require.NoError(t, err)
	certificate, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	p12, err := pkcs12.Modern.Encode(key, certificate, nil, passphrase)
	require.NoError(t, err)
	return certificate, key, p12
}

func mustParseURL(t *testing.T, value string) *url.URL {
	t.Helper()
	parsed, err := url.Parse(value)
	require.NoError(t, err)
	return parsed
}

func newTestServerCertificate(t *testing.T, hostname string, authority *x509.Certificate, authorityKey *rsa.PrivateKey) tls.Certificate {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: hostname},
		DNSNames:     []string{hostname},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, authority, key.Public(), authorityKey)
	require.NoError(t, err)
	leaf, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return tls.Certificate{
		Certificate: [][]byte{der, authority.Raw},
		PrivateKey:  key,
		Leaf:        leaf,
	}
}

var _ mitm.Handler = (*apiHandler)(nil)
