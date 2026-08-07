package mekya

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/metacubex/mihomo/common/httputils"
	"github.com/metacubex/mihomo/component/ca"
	tlsC "github.com/metacubex/mihomo/component/tls"
	"github.com/metacubex/mihomo/transport/mkcp"

	"github.com/metacubex/http"
	"github.com/metacubex/tls"
	utls "github.com/metacubex/utls"
	"github.com/stretchr/testify/require"
)

func TestRoundTrip(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	cfg := testConfig()
	server, err := Listen(ctx, ln, cfg)
	require.NoError(t, err)
	defer server.Close()

	serverErr := make(chan error, 1)
	go func() {
		conn, err := server.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer conn.Close()
		serverErr <- echo(conn)
	}()

	client, err := NewClient(ctx, func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", server.Addr().String())
	}, cfg)
	require.NoError(t, err)
	defer client.Close()

	conn, err := client.Dial(ctx)
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))

	payload := bytes.Repeat([]byte("m"), 64*1024)
	_, err = conn.Write(payload)
	require.NoError(t, err)
	got := make([]byte, len(payload))
	_, err = io.ReadFull(conn, got)
	require.NoError(t, err)
	require.Equal(t, payload, got)
	require.NotNil(t, conn.LocalAddr())
	require.NotNil(t, conn.RemoteAddr())
	require.NoError(t, conn.Close())
}

func TestNewRoundTripperALPNRouting(t *testing.T) {
	addr, seenProto := startALPNTestHTTPServer(t)
	url := "https://" + addr + "/mekya"
	h1Addr, h1SeenProto := startALPNTestHTTPServer(t, "http/1.1")
	h1URL := "https://" + h1Addr + "/mekya"
	h2Addr, h2SeenProto := startALPNTestHTTPServer(t, http2NextProtoTLS)
	h2URL := "https://" + h2Addr + "/mekya"

	testCases := []struct {
		name    string
		newConn alpnTestConnFactory
	}{
		{name: "tls", newConn: newTLSALPNTestConn},
		{name: "utls", newConn: newUTLSALPNTestConn},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("h2 direct", func(t *testing.T) {
				dial, dialCount := newALPNTestDialer(t, tc.newConn, addr, http2NextProtoTLS)
				rt := newALPNTestRoundTripper(t, dial)

				requireALPNRoundTrip(t, rt, url, seenProto, "HTTP/2.0")

				require.Equal(t, 1, dialCount())
				require.False(t, rt.shouldConnectWithH1(addr))
			})

			t.Run("http1 fallback", func(t *testing.T) {
				dial, dialCount := newALPNTestDialer(t, tc.newConn, addr, "http/1.1")
				rt := newALPNTestRoundTripper(t, dial)

				requireALPNRoundTrip(t, rt, url, seenProto, "HTTP/1.1")

				require.Equal(t, 1, dialCount())
				require.True(t, rt.shouldConnectWithH1(addr))
			})

			t.Run("empty alpn fallback", func(t *testing.T) {
				dial, dialCount := newALPNTestDialer(t, tc.newConn, addr, "")
				rt := newALPNTestRoundTripper(t, dial)

				requireALPNRoundTrip(t, rt, url, seenProto, "HTTP/1.1")

				require.Equal(t, 1, dialCount())
				require.True(t, rt.shouldConnectWithH1(addr))
			})

			t.Run("h1 roundtripper sends only http1", func(t *testing.T) {
				dial, dialCount := newALPNTestDialer(t, tc.newConn, addr, "http/1.1")
				rt := newALPNTestRoundTripper(t, dial)
				rt.mu.Lock()
				rt.connectWithH1[addr] = true
				rt.mu.Unlock()

				requireALPNRoundTrip(t, rt.h1, url, seenProto, "HTTP/1.1")

				require.Equal(t, 1, dialCount())
			})

			t.Run("h1 roundtripper sends http1 to http1-only server", func(t *testing.T) {
				dial, dialCount := newALPNTestDialerWithClientProtocols(t, tc.newConn, h1Addr, []string{http2NextProtoTLS, "http/1.1"}, "http/1.1")
				rt := newALPNTestRoundTripper(t, dial)
				rt.mu.Lock()
				rt.connectWithH1[h1Addr] = true
				rt.mu.Unlock()

				requireALPNRoundTrip(t, rt.h1, h1URL, h1SeenProto, "HTTP/1.1")

				require.Equal(t, 1, dialCount())
			})

			t.Run("h1 roundtripper rejects h2 alpn", func(t *testing.T) {
				dial, dialCount := newALPNTestDialer(t, tc.newConn, addr, http2NextProtoTLS)
				rt := newALPNTestRoundTripper(t, dial)
				rt.mu.Lock()
				rt.connectWithH1[addr] = true
				rt.mu.Unlock()

				requireALPNRoundTripError(t, rt.h1, url, errUnexpectedALPN)

				require.Equal(t, 1, dialCount())
				require.False(t, rt.shouldConnectWithH1(addr))
			})

			t.Run("h1 roundtripper rejects h2-only server", func(t *testing.T) {
				dial, dialCount := newALPNTestDialerWithClientProtocols(t, tc.newConn, h2Addr, []string{http2NextProtoTLS, "http/1.1"}, http2NextProtoTLS)
				rt := newALPNTestRoundTripper(t, dial)
				rt.mu.Lock()
				rt.connectWithH1[h2Addr] = true
				rt.mu.Unlock()

				requireALPNRoundTripError(t, rt.h1, h2URL, errUnexpectedALPN)
				requireNoALPNRequest(t, h2SeenProto)

				require.Equal(t, 1, dialCount())
				require.False(t, rt.shouldConnectWithH1(h2Addr))
			})

			t.Run("h2 roundtripper sends only http2", func(t *testing.T) {
				dial, dialCount := newALPNTestDialer(t, tc.newConn, addr, http2NextProtoTLS)
				rt := newALPNTestRoundTripper(t, dial)

				requireALPNRoundTrip(t, rt.h2, url, seenProto, "HTTP/2.0")

				require.Equal(t, 1, dialCount())
			})

			t.Run("h2 roundtripper sends http2 to h2-only server", func(t *testing.T) {
				dial, dialCount := newALPNTestDialerWithClientProtocols(t, tc.newConn, h2Addr, []string{http2NextProtoTLS, "http/1.1"}, http2NextProtoTLS)
				rt := newALPNTestRoundTripper(t, dial)

				requireALPNRoundTrip(t, rt.h2, h2URL, h2SeenProto, "HTTP/2.0")

				require.Equal(t, 1, dialCount())
			})

			t.Run("h2 roundtripper rejects http1 alpn", func(t *testing.T) {
				dial, dialCount := newALPNTestDialer(t, tc.newConn, addr, "http/1.1")
				rt := newALPNTestRoundTripper(t, dial)

				requireALPNRoundTripError(t, rt.h2, url, errUnexpectedALPN)

				require.Equal(t, 1, dialCount())
				require.True(t, rt.shouldConnectWithH1(addr))
			})

			t.Run("h2 roundtripper rejects http1-only server", func(t *testing.T) {
				dial, dialCount := newALPNTestDialerWithClientProtocols(t, tc.newConn, h1Addr, []string{http2NextProtoTLS, "http/1.1"}, "http/1.1")
				rt := newALPNTestRoundTripper(t, dial)

				requireALPNRoundTripError(t, rt.h2, h1URL, errUnexpectedALPN)
				requireNoALPNRequest(t, h1SeenProto)

				require.Equal(t, 1, dialCount())
				require.True(t, rt.shouldConnectWithH1(h1Addr))
			})

			t.Run("h2 roundtripper rejects empty alpn", func(t *testing.T) {
				dial, dialCount := newALPNTestDialer(t, tc.newConn, addr, "")
				rt := newALPNTestRoundTripper(t, dial)

				requireALPNRoundTripError(t, rt.h2, url, errUnexpectedALPN)

				require.Equal(t, 1, dialCount())
				require.True(t, rt.shouldConnectWithH1(addr))
			})

			t.Run("remembered http1 stays http1", func(t *testing.T) {
				dial, dialCount := newALPNTestDialer(t, tc.newConn, addr, "http/1.1", "http/1.1")
				rt := newALPNTestRoundTripper(t, dial)

				requireALPNRoundTrip(t, rt, url, seenProto, "HTTP/1.1")
				require.True(t, rt.shouldConnectWithH1(addr))
				httputils.CloseTransport(rt.h1)

				requireALPNRoundTrip(t, rt, url, seenProto, "HTTP/1.1")

				require.Equal(t, 2, dialCount())
				require.True(t, rt.shouldConnectWithH1(addr))
			})

			t.Run("remembered http1 can switch to h2", func(t *testing.T) {
				dial, dialCount := newALPNTestDialer(t, tc.newConn, addr, "http/1.1", http2NextProtoTLS)
				rt := newALPNTestRoundTripper(t, dial)

				requireALPNRoundTrip(t, rt, url, seenProto, "HTTP/1.1")
				require.True(t, rt.shouldConnectWithH1(addr))
				httputils.CloseTransport(rt.h1)

				requireALPNRoundTrip(t, rt, url, seenProto, "HTTP/2.0")

				require.Equal(t, 2, dialCount())
				require.False(t, rt.shouldConnectWithH1(addr))
			})
		})
	}
}

func testConfig() Config {
	return Config{
		KCP: mkcp.Config{
			TTI: 15,
		},
		URL:                            "https://example.invalid/mekya",
		H2PoolSize:                     2,
		MaxWriteDelay:                  20,
		MaxRequestSize:                 96000,
		PollingIntervalInitial:         20,
		MaxWriteSize:                   1 << 20,
		MaxWriteDurationMs:             100,
		MaxSimultaneousWriteConnection: 16,
		PacketWritingBuffer:            1024,
	}
}

func echo(conn net.Conn) error {
	_, err := io.Copy(conn, conn)
	return err
}

type alpnTestConnFactory func(ctx context.Context, addr string, protocols []string, negotiated string) (net.Conn, error)

func newALPNTestRoundTripper(t *testing.T, dial DialFunc) *alpnAwareRoundTripper {
	t.Helper()
	rt, ok := newRoundTripper(dial, 0).(*alpnAwareRoundTripper)
	require.True(t, ok)
	t.Cleanup(func() {
		require.NoError(t, rt.Close())
	})
	return rt
}

func newALPNTestDialer(t *testing.T, newConn alpnTestConnFactory, addr string, protocols ...string) (DialFunc, func() int) {
	t.Helper()
	var mu sync.Mutex
	dialCount := 0
	dial := func(ctx context.Context) (net.Conn, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		mu.Lock()
		index := dialCount
		dialCount++
		mu.Unlock()

		if index >= len(protocols) {
			return nil, fmt.Errorf("unexpected ALPN test dial %d", index+1)
		}
		return newConn(ctx, addr, alpnTestClientProtocols(protocols[index]), protocols[index])
	}
	count := func() int {
		mu.Lock()
		defer mu.Unlock()
		return dialCount
	}
	return dial, count
}

func newALPNTestDialerWithClientProtocols(t *testing.T, newConn alpnTestConnFactory, addr string, clientProtocols []string, negotiatedProtocols ...string) (DialFunc, func() int) {
	t.Helper()
	var mu sync.Mutex
	dialCount := 0
	dial := func(ctx context.Context) (net.Conn, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		mu.Lock()
		index := dialCount
		dialCount++
		mu.Unlock()

		if index >= len(negotiatedProtocols) {
			return nil, fmt.Errorf("unexpected ALPN test dial %d", index+1)
		}
		return newConn(ctx, addr, clientProtocols, negotiatedProtocols[index])
	}
	count := func() int {
		mu.Lock()
		defer mu.Unlock()
		return dialCount
	}
	return dial, count
}

func requireALPNRoundTrip(t *testing.T, rt http.RoundTripper, url string, seenProto <-chan string, expectedProto string) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := rt.RoundTrip(req)
	require.NoError(t, err)
	_, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	select {
	case proto := <-seenProto:
		require.Equal(t, expectedProto, proto)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for ALPN test request")
	}
}

func requireALPNRoundTripError(t *testing.T, rt http.RoundTripper, url string, target error) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := rt.RoundTrip(req)
	if resp != nil && resp.Body != nil {
		require.NoError(t, resp.Body.Close())
	}
	require.ErrorIs(t, err, target)
}

func requireNoALPNRequest(t *testing.T, seenProto <-chan string) {
	t.Helper()
	select {
	case proto := <-seenProto:
		t.Fatalf("unexpected ALPN test request with %s", proto)
	default:
	}
}

func newTLSALPNTestConn(ctx context.Context, addr string, protocols []string, negotiated string) (net.Conn, error) {
	rawConn, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	conn := tls.Client(rawConn, &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         protocols,
	})
	if err := conn.HandshakeContext(ctx); err != nil {
		_ = rawConn.Close()
		return nil, err
	}
	if got := tlsC.GetTLSConnectionState(conn).NegotiatedProtocol; got != negotiated {
		_ = conn.Close()
		return nil, fmt.Errorf("negotiated ALPN %q, want %q", got, negotiated)
	}
	return conn, nil
}

func newUTLSALPNTestConn(ctx context.Context, addr string, protocols []string, negotiated string) (net.Conn, error) {
	rawConn, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	conn := utls.UClient(rawConn, &utls.Config{
		InsecureSkipVerify: true,
		NextProtos:         protocols,
	}, utls.HelloGolang)
	if err := conn.HandshakeContext(ctx); err != nil {
		_ = rawConn.Close()
		return nil, err
	}
	if got := tlsC.GetTLSConnectionState(conn).NegotiatedProtocol; got != negotiated {
		_ = conn.Close()
		return nil, fmt.Errorf("negotiated ALPN %q, want %q", got, negotiated)
	}
	return conn, nil
}

func startALPNTestHTTPServer(t *testing.T, nextProtos ...string) (string, <-chan string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	if len(nextProtos) == 0 {
		nextProtos = []string{http2NextProtoTLS, "http/1.1"}
	}

	seenProto := make(chan string, 4)
	protocols := new(http.Protocols)
	for _, nextProto := range nextProtos {
		switch nextProto {
		case "http/1.1":
			protocols.SetHTTP1(true)
		case http2NextProtoTLS:
			protocols.SetHTTP2(true)
			protocols.SetUnencryptedHTTP2(true)
		}
	}
	server := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			seenProto <- r.Proto
			_, _ = w.Write([]byte("ok"))
		}),
		Protocols: protocols,
	}
	tlsListener := tls.NewListener(ln, &tls.Config{
		Certificates: []tls.Certificate{alpnTestCertificate(t)},
		NextProtos:   nextProtos,
	})
	done := make(chan error, 1)
	go func() {
		done <- server.Serve(tlsListener)
	}()
	t.Cleanup(func() {
		_ = server.Close()
		select {
		case err := <-done:
			require.ErrorIs(t, err, http.ErrServerClosed)
		case <-time.After(time.Second):
			t.Error("timed out waiting for ALPN test HTTP server")
		}
	})
	return ln.Addr().String(), seenProto
}

func alpnTestClientProtocols(protocol string) []string {
	if protocol == "" {
		return nil
	}
	return []string{protocol}
}

var (
	alpnTestCertificateOnce  sync.Once
	alpnTestCertificateValue tls.Certificate
	alpnTestCertificateErr   error
)

func alpnTestCertificate(t *testing.T) tls.Certificate {
	t.Helper()
	alpnTestCertificateOnce.Do(func() {
		certPEM, keyPEM, _, err := ca.NewRandomTLSKeyPair(ca.KeyPairTypeP256)
		if err != nil {
			alpnTestCertificateErr = err
			return
		}
		alpnTestCertificateValue, alpnTestCertificateErr = tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
	})
	require.NoError(t, alpnTestCertificateErr)
	return alpnTestCertificateValue
}
