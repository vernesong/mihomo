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
	"fmt"
	"io"
	"math/big"
	"net"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
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
		passphrase = "password"
		hostname   = "api.example.com"
	)

	caCertificate, caKey, caP12 := newTestAuthority(t, passphrase)
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
			require.True(t, parsedConfig.Mitm.Match(hostname, netip.MustParseAddr("127.0.0.1")))
			require.False(t, parsedConfig.Mitm.Match("a.pinned.example.com", netip.MustParseAddr("127.0.0.1")))
			require.False(t, parsedConfig.Mitm.Match(hostname, netip.MustParseAddr("10.0.0.1")))

			upstreamCertificate := newTestServerCertificate(t, hostname, caCertificate, caKey)
			upstream := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				_, _ = fmt.Fprintf(writer, "%s|%s", request.Header.Get("X-MITM-Request"), request.Proto)
			}))
			upstream.EnableHTTP2 = testCase.h2
			upstream.TLS = &tls.Config{Certificates: []tls.Certificate{upstreamCertificate}}
			upstream.StartTLS()
			defer upstream.Close()

			handler := &apiHandler{requestURLs: make(chan string, 1)}
			interceptor := mitm.New(parsedConfig.Mitm, handler)
			require.NotNil(t, interceptor)

			clientConn, serverConn := net.Pipe()
			handleResult := make(chan error, 1)
			dialedURLs := make(chan string, 1)
			go func() {
				handled, err := interceptor.Handle(N.NewBufferedConn(serverConn), &C.Metadata{
					NetWork: C.TCP,
					Type:    C.TUN,
					SrcIP:   netip.MustParseAddr("127.0.0.1"),
					SrcPort: 54321,
					Host:    hostname,
					DstPort: 443,
				}, func(ctx context.Context, _, _ string) (net.Conn, error) {
					dialedURLs <- mitm.RequestURLFromContext(ctx)
					return (&net.Dialer{}).DialContext(ctx, "tcp", upstream.Listener.Addr().String())
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
			for _, requestCase := range []struct {
				path     string
				rawQuery string
			}{
				{path: "/index.html", rawQuery: "source=mitm"},
				{path: "/statistics", rawQuery: "event=finished"},
			} {
				request := &http.Request{
					Method: http.MethodGet,
					URL:    &url.URL{Scheme: "https", Host: hostname, Path: requestCase.path, RawQuery: requestCase.rawQuery},
					Host:   hostname,
					Header: make(http.Header),
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
				require.Equal(t, "handled", response.Header.Get("X-MITM-Response"))
				if testCase.h2 {
					require.Equal(t, "handled|HTTP/2.0", string(body))
				} else {
					require.Equal(t, "handled|HTTP/1.1", string(body))
				}
				requestURL := "https://" + hostname + requestCase.path + "?" + requestCase.rawQuery
				require.Equal(t, requestURL, <-dialedURLs)
				require.Equal(t, requestURL, <-handler.requestURLs)
			}
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
		})
	}
}

func TestMitmUserConfigurationDefaultsAndHostnamePorts(t *testing.T) {
	const passphrase = "password"
	_, _, caP12 := newTestAuthority(t, passphrase)
	rawConfig := fmt.Sprintf(`
mitm:
  hostname:
    - standard.example.com
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

	ipv4 := netip.MustParseAddr("192.0.2.1")
	ipv6 := netip.MustParseAddr("2001:db8::1")
	require.True(t, parsedConfig.Mitm.Match("standard.example.com", ipv4))
	require.True(t, parsedConfig.Mitm.Match("standard.example.com", ipv6))
	require.False(t, parsedConfig.Mitm.MatchHostPort("standard.example.com", 8443, ipv4))
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
	hostname    string
	requestURLs chan string
	errors      []error
}

func (h *apiHandler) HandleRequest(session *mitm.Session) (*http.Request, *http.Response) {
	h.hostname = session.Metadata().Host
	h.requestURLs <- session.Metadata().RemoteDestination()
	session.Request().Header.Set("X-MITM-Request", "handled")
	return nil, nil
}

func (*apiHandler) HandleResponse(session *mitm.Session) *http.Response {
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
