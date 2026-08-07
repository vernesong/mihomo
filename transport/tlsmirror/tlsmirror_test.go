package tlsmirror

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/http"
	"github.com/metacubex/mihomo/component/ca"
	"github.com/metacubex/tls"

	"github.com/stretchr/testify/require"
)

var testPrimaryKey = GeneratePrimaryKey()

func TestTLSMirrorConnDeadline(t *testing.T) {
	for _, tc := range []struct {
		name string
		set  func(*Conn, time.Time) error
		do   func(*Conn) error
	}{
		{
			name: "read",
			set:  (*Conn).SetReadDeadline,
			do: func(conn *Conn) error {
				_, err := conn.Read(make([]byte, 1))
				return err
			},
		},
		{
			name: "write",
			set:  (*Conn).SetWriteDeadline,
			do: func(conn *Conn) error {
				_, err := conn.Write([]byte{1})
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			client, server := net.Pipe()
			t.Cleanup(func() { _ = client.Close() })
			t.Cleanup(func() { _ = server.Close() })

			mirror := newMirrorConn(ctx, client, server, Config{}, nil, nil, nil, nil)
			conn, err := newHiddenConn(ctx, mirror, make([]byte, 32), false, Config{})
			require.NoError(t, err)
			require.NoError(t, tc.set(conn, time.Now().Add(10*time.Millisecond)))

			err = tc.do(conn)
			require.True(t, errors.Is(err, os.ErrDeadlineExceeded), "unexpected error: %v", err)
		})
	}
}

func TestTLSMirrorRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		cfg                    Config
		firstWriteDelayAtLeast time.Duration
		configureForwardTLS    []func(*tls.Config)
	}{
		{name: "default"},
		{name: "transport layer padding", cfg: Config{
			TransportLayerPadding: TransportLayerPadding{Enabled: true},
		}},
		{name: "sequence watermark", cfg: Config{
			SequenceWatermarkingEnabled: true,
		}},
		{
			name: "first write delay",
			cfg: Config{
				DeferInstanceDerivedWrite: TimeSpec{BaseNanoseconds: uint64((50 * time.Millisecond).Nanoseconds())},
			},
			firstWriteDelayAtLeast: 40 * time.Millisecond,
		},
		{
			name: "tls12 explicit nonce",
			cfg: Config{
				ExplicitNonceCipherSuites: RecommendedExplicitNonceCipherSuites,
			},
			configureForwardTLS: []func(*tls.Config){func(config *tls.Config) {
				config.MinVersion = tls.VersionTLS12
				config.MaxVersion = tls.VersionTLS12
				config.CipherSuites = []uint16{tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256}
			}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testTLSMirrorRoundTrip(t, tc.cfg, tc.firstWriteDelayAtLeast, tc.configureForwardTLS...)
		})
	}
}

func TestTLSMirrorFallback(t *testing.T) {
	type fallbackPipe struct {
		client   net.Conn
		forward  net.Conn
		writeErr chan error
	}

	newPipe := func(t *testing.T, c2sHook func(*record) (bool, error)) fallbackPipe {
		t.Helper()
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		client, mirrorClient := net.Pipe()
		mirrorServer, forward := net.Pipe()
		t.Cleanup(func() { _ = client.Close() })
		t.Cleanup(func() { _ = mirrorClient.Close() })
		t.Cleanup(func() { _ = mirrorServer.Close() })
		t.Cleanup(func() { _ = forward.Close() })

		deadline := time.Now().Add(time.Second)
		require.NoError(t, client.SetDeadline(deadline))
		require.NoError(t, forward.SetDeadline(deadline))

		mirror := newMirrorConn(ctx, mirrorClient, mirrorServer, Config{}, c2sHook, nil, nil, nil)
		mirror.start()
		return fallbackPipe{client: client, forward: forward, writeErr: make(chan error, 4)}
	}
	writeAsync := func(p fallbackPipe, conn net.Conn, payload []byte) {
		go func() {
			_, err := conn.Write(payload)
			p.writeErr <- err
		}()
	}
	expectRead := func(t *testing.T, conn net.Conn, want []byte) {
		t.Helper()
		got := make([]byte, len(want))
		_, err := io.ReadFull(conn, got)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
	expectReadError := func(t *testing.T, conn net.Conn) {
		t.Helper()
		_, err := conn.Read(make([]byte, 1))
		require.Error(t, err)
	}
	forwardClientHello := func(t *testing.T, p fallbackPipe) {
		t.Helper()
		clientHello := encodeTestRecord(recordTypeHandshake, [2]byte{0x03, 0x03}, testClientHelloFragment())
		writeAsync(p, p.client, clientHello)
		expectRead(t, p.forward, clientHello)
		require.NoError(t, <-p.writeErr)
	}
	forwardServerHello := func(t *testing.T, p fallbackPipe) {
		t.Helper()
		serverHello := encodeTestRecord(recordTypeHandshake, [2]byte{0x03, 0x03}, testServerHelloFragment())
		writeAsync(p, p.forward, serverHello)
		expectRead(t, p.client, serverHello)
		require.NoError(t, <-p.writeErr)
	}

	t.Run("non tls", func(t *testing.T) {
		p := newPipe(t, nil)
		request := []byte("GET / HTTP/1.1\r\nHost: example.com\r\n\r\n")
		writeAsync(p, p.client, request)
		expectRead(t, p.forward, request)
		require.NoError(t, <-p.writeErr)

		response := []byte("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK")
		writeAsync(p, p.forward, response)
		expectRead(t, p.client, response)
		require.NoError(t, <-p.writeErr)
	})

	t.Run("closes after malformed first client hello", func(t *testing.T) {
		p := newPipe(t, nil)
		malformedClientHello := encodeTestRecord(recordTypeHandshake, [2]byte{0x03, 0x03}, []byte{1})
		writeAsync(p, p.client, malformedClientHello)
		expectRead(t, p.forward, malformedClientHello)
		expectReadError(t, p.forward)
		<-p.writeErr
	})

	t.Run("after c2s hook error", func(t *testing.T) {
		hookErr := errors.New("probe fallback")
		p := newPipe(t, func(rec *record) (bool, error) {
			if rec.recordType == recordTypeApplicationData {
				return false, hookErr
			}
			return false, nil
		})
		forwardClientHello(t, p)
		forwardServerHello(t, p)

		applicationData := encodeTestRecord(recordTypeApplicationData, [2]byte{0x03, 0x03}, []byte("probe-application-data"))
		rawTail := []byte("raw-tail-after-fallback")
		writeAsync(p, p.client, append(applicationData, rawTail...))
		expectRead(t, p.forward, applicationData)
		expectRead(t, p.forward, rawTail)
		require.NoError(t, <-p.writeErr)
	})

	t.Run("keeps invalid record header", func(t *testing.T) {
		p := newPipe(t, nil)
		forwardClientHello(t, p)
		forwardServerHello(t, p)

		invalidHeader := []byte{recordTypeApplicationData, 0x03, 0x03, 0x40, 0x01}
		rawTail := []byte("tail-after-invalid-header")
		writeAsync(p, p.client, append(invalidHeader, rawTail...))
		expectRead(t, p.forward, invalidHeader)
		expectRead(t, p.forward, rawTail)
		require.NoError(t, <-p.writeErr)
	})

	t.Run("c2s change cipher spec before server hello", func(t *testing.T) {
		p := newPipe(t, nil)
		forwardClientHello(t, p)

		changeCipherSpec := encodeTestRecord(recordTypeChangeCipherSpec, [2]byte{0x03, 0x03}, []byte{1})
		rawTail := []byte("raw-tail-after-early-ccs")
		writeAsync(p, p.client, append(changeCipherSpec, rawTail...))
		expectRead(t, p.forward, changeCipherSpec)
		expectRead(t, p.forward, rawTail)
		require.NoError(t, <-p.writeErr)
	})

	t.Run("forwards alert before close", func(t *testing.T) {
		p := newPipe(t, nil)
		forwardClientHello(t, p)
		forwardServerHello(t, p)

		alert := encodeTestRecord(recordTypeAlert, [2]byte{0x03, 0x03}, []byte{1, 0})
		writeAsync(p, p.client, alert)
		expectRead(t, p.forward, alert)
		require.NoError(t, <-p.writeErr)
	})
}

func TestTLSMirrorCaptureFirstHandshakeRecordForwardsFragments(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	src, srcWriter := net.Pipe()
	dstReader, dst := net.Pipe()
	t.Cleanup(func() { _ = src.Close() })
	t.Cleanup(func() { _ = srcWriter.Close() })
	t.Cleanup(func() { _ = dstReader.Close() })
	t.Cleanup(func() { _ = dst.Close() })

	deadline := time.Now().Add(time.Second)
	require.NoError(t, src.SetDeadline(deadline))
	require.NoError(t, srcWriter.SetDeadline(deadline))
	require.NoError(t, dstReader.SetDeadline(deadline))
	require.NoError(t, dst.SetDeadline(deadline))

	mirror := newMirrorConn(ctx, src, dst, Config{}, nil, nil, nil, nil)
	type captureResult struct {
		rec    *record
		reader *bufio.Reader
		raw    []byte
		err    error
	}
	captured := make(chan captureResult, 1)
	go func() {
		rec, reader, raw, err := mirror.captureFirstHandshakeRecord(src, bufio.NewWriterSize(dst, 65536))
		captured <- captureResult{rec: rec, reader: reader, raw: raw, err: err}
	}()

	clientHello := encodeTestRecord(recordTypeHandshake, [2]byte{0x03, 0x03}, testClientHelloFragment())
	firstPart := clientHello[:8]
	restPart := clientHello[8:]
	nextRecord := encodeTestRecord(recordTypeApplicationData, [2]byte{0x03, 0x03}, []byte("coalesced-next-record"))
	writeErr := make(chan error, 2)
	go func() {
		_, err := srcWriter.Write(firstPart)
		writeErr <- err
	}()
	gotFirstPart := make([]byte, len(firstPart))
	_, err := io.ReadFull(dstReader, gotFirstPart)
	require.NoError(t, err)
	require.Equal(t, firstPart, gotFirstPart)
	require.NoError(t, <-writeErr)

	go func() {
		_, err := srcWriter.Write(append(append([]byte(nil), restPart...), nextRecord...))
		writeErr <- err
	}()
	require.NoError(t, <-writeErr)

	result := <-captured
	require.NoError(t, result.err)
	require.Equal(t, byte(recordTypeHandshake), result.rec.recordType)
	require.Equal(t, testClientHelloFragment(), result.rec.fragment)
	require.Equal(t, restPart, result.raw)

	go func() {
		writeErr <- writeRawFlush(bufio.NewWriterSize(dst, 65536), result.raw)
	}()
	gotRestPart := make([]byte, len(restPart))
	_, err = io.ReadFull(dstReader, gotRestPart)
	require.NoError(t, err)
	require.Equal(t, restPart, gotRestPart)
	require.NoError(t, <-writeErr)

	rec, raw, err := readRecord(result.reader)
	require.NoError(t, err)
	require.Equal(t, byte(recordTypeApplicationData), rec.recordType)
	require.Equal(t, []byte("coalesced-next-record"), rec.fragment)
	require.Equal(t, nextRecord, raw)
}

func TestTLSMirrorRoundTripWithEmbeddedTrafficGenerator(t *testing.T) {
	for _, tc := range []struct {
		name                         string
		startForward                 func(*testing.T, *atomic.Int32) string
		alpn                         []string
		h2DoNotWaitForDownloadFinish bool
	}{
		{
			name:         "http1",
			startForward: startTestForwardHTTPS,
		},
		{
			name:                         "h2",
			startForward:                 startTestForwardHTTP2,
			alpn:                         []string{"h2"},
			h2DoNotWaitForDownloadFinish: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var requests atomic.Int32
			forwardAddr := tc.startForward(t, &requests)
			testTLSMirrorRoundTripWithEmbeddedTrafficGenerator(t, forwardAddr, &requests, tc.alpn, tc.h2DoNotWaitForDownloadFinish)
		})
	}
}

func TestTLSMirrorTrafficGenerator(t *testing.T) {
	t.Run("http1 does not add connection header", func(t *testing.T) {
		var got *http.Request
		err := runTrafficStep(context.Background(), roundTripFunc(func(req *http.Request) (*http.Response, error) {
			got = req
			return &http.Response{
				StatusCode: http.StatusNoContent,
				Body:       io.NopCloser(bytes.NewReader(nil)),
			}, nil
		}), TrafficStep{
			Host:   "example.com",
			Path:   "/",
			Method: http.MethodGet,
		}, "http/1.1")
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Empty(t, got.Header.Values("Connection"))
	})

	t.Run("wait time includes request time", func(t *testing.T) {
		now := time.Unix(0, 0)
		requestTime := 120 * time.Millisecond
		waitTime := 200 * time.Millisecond
		fakeNow := func() time.Time {
			return now
		}
		var waited time.Duration
		fakeWait := func(ctx context.Context, delay time.Duration) error {
			waited = delay
			return ctx.Err()
		}

		err := runTrafficStepWithClock(context.Background(), roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusNoContent,
				Body: &readHookEOFBody{read: func() {
					now = now.Add(requestTime)
				}},
			}, nil
		}), TrafficStep{
			Host:   "example.com",
			Path:   "/",
			Method: http.MethodGet,
			WaitTime: TimeSpec{
				BaseNanoseconds: uint64(waitTime.Nanoseconds()),
			},
		}, "http/1.1", fakeNow, fakeWait)
		require.NoError(t, err)
		require.Equal(t, waitTime-requestTime, waited)
	})

	t.Run("uses v2ray host semantics", func(t *testing.T) {
		var got *http.Request
		err := runTrafficStep(context.Background(), roundTripFunc(func(req *http.Request) (*http.Response, error) {
			got = req
			return &http.Response{
				StatusCode: http.StatusNoContent,
				Body:       io.NopCloser(bytes.NewReader(nil)),
			}, nil
		}), TrafficStep{
			Host:   "example.com:8443",
			Path:   "/carrier",
			Method: http.MethodGet,
		}, "http/1.1")
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Equal(t, "example.com:8443", got.URL.Host)
		require.Equal(t, "example.com", got.Host)
	})

	t.Run("invalid next step weight", func(t *testing.T) {
		_, _, err := chooseNextTrafficStep(TrafficStep{
			NextStep: []TrafficTransferCandidate{{Weight: 0, GotoLocation: 0}},
		}, 0)
		require.Error(t, err)
	})
}

func testTLSMirrorRoundTripWithEmbeddedTrafficGenerator(t *testing.T, forwardAddr string, requests *atomic.Int32, alpn []string, h2DoNotWaitForDownloadFinish bool) {
	t.Helper()
	serverLn, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = serverLn.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	serverDone := make(chan error, 1)
	go func() {
		carrier, err := serverLn.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		forward, err := (&net.Dialer{}).DialContext(ctx, "tcp", forwardAddr)
		if err != nil {
			serverDone <- err
			return
		}
		conn, err := ServeConnReady(ctx, carrier, forward, ServerConfig{PrimaryKey: testPrimaryKey})
		if err != nil {
			serverDone <- err
			return
		}
		buf := make([]byte, 1024)
		_, err = io.ReadFull(conn, buf)
		if err != nil {
			serverDone <- err
			return
		}
		_, err = conn.Write(buf)
		serverDone <- err
	}()

	raw, err := net.Dial("tcp", serverLn.Addr().String())
	require.NoError(t, err)
	client, err := Dial(ctx, raw, ClientConfig{
		Config: Config{
			PrimaryKey: testPrimaryKey,
			EmbeddedTrafficGenerator: &TrafficGenerator{Steps: []TrafficStep{{
				Host:                 "localhost",
				Path:                 "/carrier",
				Method:               "GET",
				ConnectionReady:      true,
				ConnectionRecallExit: true,
				WaitTime: TimeSpec{
					BaseNanoseconds: uint64((10 * time.Millisecond).Nanoseconds()),
				},
				NextStep: []TrafficTransferCandidate{{
					Weight:       1,
					GotoLocation: 0,
				}},
				Headers: []TrafficHeader{{
					Name:  "User-Agent",
					Value: "tlsmirror-test",
				}},
				H2DoNotWaitForDownloadFinish: h2DoNotWaitForDownloadFinish,
			}}},
		},
		ServerName:     "localhost",
		SkipCertVerify: true,
		ALPN:           alpn,
	})
	require.NoError(t, err)
	require.GreaterOrEqual(t, requests.Load(), int32(1))

	payload := bytes.Repeat([]byte{7}, 1024)
	_, err = client.Write(payload)
	require.NoError(t, err)
	got := make([]byte, len(payload))
	_, err = io.ReadFull(client, got)
	require.NoError(t, err)
	require.Equal(t, payload, got)

	select {
	case err := <-serverDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("server handler timeout")
	}
}

func testTLSMirrorRoundTrip(t *testing.T, cfg Config, firstWriteDelayAtLeast time.Duration, configureForwardTLS ...func(*tls.Config)) {
	cfg.PrimaryKey = testPrimaryKey

	forwardAddr := startTestForwardTLS(t, configureForwardTLS...)
	serverLn, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = serverLn.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	serverDone := make(chan error, 1)
	go func() {
		carrier, err := serverLn.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		dialer := net.Dialer{}
		forward, err := dialer.DialContext(ctx, "tcp", forwardAddr)
		if err != nil {
			serverDone <- err
			return
		}
		conn, err := ServeConnReady(ctx, carrier, forward, cfg)
		if err != nil {
			serverDone <- err
			return
		}
		for i := 0; i < 8; i++ {
			size := 4 + i*8192
			buf := make([]byte, size)
			_, err := io.ReadFull(conn, buf)
			if err != nil {
				serverDone <- err
				return
			}
			if !bytes.Equal(buf, bytes.Repeat([]byte{byte(i)}, size)) {
				serverDone <- bytes.ErrTooLarge
				return
			}
			_, err = conn.Write(bytes.Repeat([]byte{byte(255 - i)}, size))
			if err != nil {
				serverDone <- err
				return
			}
		}
		serverDone <- nil
	}()

	raw, err := net.Dial("tcp", serverLn.Addr().String())
	require.NoError(t, err)
	client, err := Dial(ctx, raw, ClientConfig{
		Config:         cfg,
		ServerName:     "localhost",
		SkipCertVerify: true,
	})
	require.NoError(t, err)
	for i := 0; i < 8; i++ {
		size := 4 + i*8192
		start := time.Now()
		_, err = client.Write(bytes.Repeat([]byte{byte(i)}, size))
		require.NoError(t, err)
		if i == 0 && firstWriteDelayAtLeast > 0 {
			require.GreaterOrEqual(t, time.Since(start), firstWriteDelayAtLeast)
		}
		buf := make([]byte, size)
		_, err = io.ReadFull(client, buf)
		require.NoError(t, err)
		require.Equal(t, bytes.Repeat([]byte{byte(255 - i)}, size), buf)
	}

	select {
	case err := <-serverDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("server handler timeout")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type readHookEOFBody struct {
	read func()
	done bool
}

func (b *readHookEOFBody) Read([]byte) (int, error) {
	if !b.done {
		b.done = true
		if b.read != nil {
			b.read()
		}
	}
	return 0, io.EOF
}

func (b *readHookEOFBody) Close() error {
	return nil
}

func testClientHelloFragment() []byte {
	fragment := make([]byte, 38)
	fragment[0] = 1
	for i := 6; i < 38; i++ {
		fragment[i] = byte(i)
	}
	return fragment
}

func testServerHelloFragment() []byte {
	fragment := make([]byte, 41)
	fragment[0] = 2
	for i := 6; i < 38; i++ {
		fragment[i] = byte(0x80 + i)
	}
	fragment[38] = 0
	fragment[39] = 0x13
	fragment[40] = 0x01
	return fragment
}

func encodeTestRecord(recordType byte, version [2]byte, fragment []byte) []byte {
	record := make([]byte, 5+len(fragment))
	record[0] = recordType
	record[1] = version[0]
	record[2] = version[1]
	binary.BigEndian.PutUint16(record[3:5], uint16(len(fragment)))
	copy(record[5:], fragment)
	return record
}

func startTestForwardHTTPS(t *testing.T, requests *atomic.Int32) string {
	t.Helper()
	certPEM, keyPEM, _, err := ca.NewRandomTLSKeyPair(ca.KeyPairTypeP256)
	require.NoError(t, err)
	cert, err := tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
	require.NoError(t, err)
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				reader := bufio.NewReader(conn)
				for {
					line, err := reader.ReadString('\n')
					if err != nil {
						return
					}
					if line == "\r\n" {
						break
					}
				}
				requests.Add(1)
				_, _ = conn.Write([]byte("HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n"))
				_, _ = io.Copy(io.Discard, reader)
			}()
		}
	}()
	return ln.Addr().String()
}

func startTestForwardHTTP2(t *testing.T, requests *atomic.Int32) string {
	t.Helper()
	certPEM, keyPEM, _, err := ca.NewRandomTLSKeyPair(ca.KeyPairTypeP256)
	require.NoError(t, err)
	cert, err := tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
	require.NoError(t, err)
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
		NextProtos:   []string{"h2"},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	server := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requests.Add(1)
			w.WriteHeader(http.StatusOK)
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
			time.Sleep(200 * time.Millisecond)
			_, _ = w.Write([]byte("ok"))
		}),
		Protocols: new(http.Protocols),
	}
	server.Protocols.SetHTTP2(true)
	server.Protocols.SetUnencryptedHTTP2(true)
	go func() { _ = server.Serve(ln) }()
	t.Cleanup(func() { _ = server.Close() })
	return ln.Addr().String()
}

func startTestForwardTLS(t *testing.T, configure ...func(*tls.Config)) string {
	t.Helper()
	certPEM, keyPEM, _, err := ca.NewRandomTLSKeyPair(ca.KeyPairTypeP256)
	require.NoError(t, err)
	cert, err := tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
	require.NoError(t, err)
	config := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
	}
	for _, configure := range configure {
		configure(config)
	}
	ln, err := tls.Listen("tcp", "127.0.0.1:0", config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = io.Copy(io.Discard, conn)
			}()
		}
	}()
	return ln.Addr().String()
}
