package mitm

import (
	"bytes"
	"container/list"
	"context"
	"errors"
	"io"
	stdlog "log"
	"net"
	"net/netip"
	"strconv"
	"sync"
	"time"

	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/component/ca"
	R "github.com/metacubex/mihomo/component/rewrite"
	"github.com/metacubex/mihomo/component/sniffer"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/ntp"

	"github.com/metacubex/http"
	"github.com/metacubex/http/http2"
	"github.com/metacubex/http/httptrace"
	"github.com/metacubex/http/httputil"
	"github.com/metacubex/tls"
)

const (
	maxSniffBufferSize         = 64 * 1024
	maxCachedRequestTransports = 16
)

var http2ClientPreface = []byte("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")

var errSingleConnectionDone = errors.New("MITM connection closed")

type DialContext func(ctx context.Context, network, address string) (net.Conn, error)

type connectionIdentifier interface {
	ID() string
}

type netConnUnwrapper interface {
	NetConn() net.Conn
}

type downstreamProtocol uint8

const (
	downstreamUnknown downstreamProtocol = iota
	downstreamTLS
	downstreamHTTP1
	downstreamHTTP2
)

func (p downstreamProtocol) scheme() string {
	if p == downstreamTLS {
		return "https"
	}
	return "http"
}

func (p downstreamProtocol) standardPort() uint16 {
	if p == downstreamTLS {
		return standardHTTPSPort
	}
	return standardHTTPPort
}

type Interceptor struct {
	config  *Config
	rewrite *R.Config
	handler Handler
}

func New(config *Config, handler Handler, rewriteConfig ...*R.Config) *Interceptor {
	var rewrite *R.Config
	if len(rewriteConfig) != 0 {
		rewrite = rewriteConfig[0]
	}
	return NewWithRewrite(config, handler, rewrite)
}

// NewWithRewrite constructs an interceptor with an explicit rewrite configuration.
func NewWithRewrite(config *Config, handler Handler, rewrite *R.Config) *Interceptor {
	if config == nil || !config.Enabled() {
		return nil
	}
	if handler == nil {
		handler = NopHandler{}
	}
	return &Interceptor{config: config, rewrite: rewrite, handler: handler}
}

func (i *Interceptor) Handle(conn *N.BufferedConn, metadata *C.Metadata, dial DialContext) (bool, error) {
	if i == nil || conn == nil || metadata == nil || dial == nil || !i.matchSource(metadata.SrcIP) {
		return false, nil
	}

	serverName, protocol := sniffDownstream(conn)
	if protocol == downstreamUnknown || (protocol == downstreamHTTP2 && !i.config.H2) {
		return false, nil
	}
	if serverName == "" {
		serverName = metadata.RuleHost()
	}
	serverName = normalizeCertificateHost(serverName)
	if !i.config.matchHostnamePort(serverName, metadata.DstPort, protocol.standardPort()) {
		return false, nil
	}

	metadata.SniffHost = serverName
	metadata.Host = serverName
	metadata.DstIP = netip.Addr{}
	metadata.DNSMode = C.DNSNormal

	roundTripper := newRequestRoundTripper(i, dial)
	defer roundTripper.CloseIdleConnections()
	proxy := i.newReverseProxy(roundTripper, metadata, protocol.scheme())
	if protocol != downstreamTLS {
		return i.servePlainHTTP(conn, proxy)
	}

	return i.serveTLS(conn, serverName, proxy)
}

func (i *Interceptor) serveTLS(conn net.Conn, serverName string, proxy http.Handler) (bool, error) {
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetHTTP2(i.config.H2)
	server := &http.Server{
		Handler:   proxy,
		TLSConfig: i.config.authority.tlsConfig(serverName, i.config.H2),
		ErrorLog:  stdlog.New(io.Discard, "", 0),
		Protocols: protocols,
	}
	if i.config.H2 {
		if err := http2.ConfigureServer(server, &http2.Server{}); err != nil {
			return true, err
		}
	}

	tlsConn := tls.Server(conn, server.TLSConfig)
	handshakeContext, cancel := context.WithTimeout(context.Background(), C.DefaultTLSTimeout)
	err := tlsConn.HandshakeContext(handshakeContext)
	cancel()
	if err != nil {
		i.handler.HandleError(nil, err)
		return true, err
	}

	return i.serveSingleConnection(server, tlsConn)
}

func (i *Interceptor) servePlainHTTP(conn net.Conn, proxy http.Handler) (bool, error) {
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(i.config.H2)
	server := &http.Server{
		Handler:   proxy,
		ErrorLog:  stdlog.New(io.Discard, "", 0),
		Protocols: protocols,
	}
	if i.config.H2 {
		if err := http2.ConfigureServer(server, &http2.Server{}); err != nil {
			return true, err
		}
	}
	return i.serveSingleConnection(server, conn)
}

func (i *Interceptor) serveSingleConnection(server *http.Server, conn net.Conn) (bool, error) {
	listener := newSingleConnListener(conn)
	err := server.Serve(listener)
	_ = listener.Close()
	if errors.Is(err, errSingleConnectionDone) || errors.Is(err, net.ErrClosed) || errors.Is(err, http.ErrServerClosed) {
		return true, nil
	}
	if err != nil {
		i.handler.HandleError(nil, err)
	}
	return true, err
}

func (i *Interceptor) matchSource(source netip.Addr) bool {
	if !source.IsValid() {
		return false
	}
	source = source.WithZone("")
	for _, prefix := range i.config.ClientSourceAddress {
		if prefix.Contains(source) {
			return true
		}
	}
	return false
}

func sniffDownstream(conn *N.BufferedConn) (string, downstreamProtocol) {
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	defer func() { _ = conn.SetReadDeadline(time.Time{}) }()

	httpSniffer, err := sniffer.NewHTTPSniffer(sniffer.SnifferConfig{})
	if err != nil {
		return "", downstreamUnknown
	}

	want := 1
	protocol := downstreamUnknown
	for want <= maxSniffBufferSize {
		conn.Grow(want)
		_, peekErr := conn.Peek(want)
		buffered := conn.Buffered()
		if buffered == 0 {
			return "", downstreamUnknown
		}
		data, _ := conn.Peek(buffered)
		if protocol == downstreamUnknown {
			if data[0] == 0x16 {
				protocol = downstreamTLS
			} else {
				protocol = downstreamHTTP1
			}
		}

		if protocol == downstreamTLS {
			if len(data) >= 2 && data[1] != 3 {
				return "", downstreamUnknown
			}

			serverName, sniffErr := sniffer.SniffTLS(data)
			if sniffErr == nil && serverName != nil {
				return *serverName, downstreamTLS
			}
			if !errors.Is(sniffErr, sniffer.ErrNoClue) {
				return "", downstreamTLS
			}
		} else {
			serverName, sniffErr := httpSniffer.SniffData(data)
			if sniffErr == nil {
				if bytes.HasPrefix(data, http2ClientPreface) {
					return serverName, downstreamHTTP2
				}
				return serverName, downstreamHTTP1
			}
			if !errors.Is(sniffErr, sniffer.ErrNoClue) {
				return "", downstreamUnknown
			}
		}

		if peekErr != nil {
			if protocol == downstreamTLS {
				return "", downstreamTLS
			}
			return "", downstreamUnknown
		}

		if buffered >= maxSniffBufferSize {
			break
		}
		want *= 2
		if want <= buffered {
			want = buffered + 1
		}
		if want > maxSniffBufferSize {
			want = maxSniffBufferSize
		}
	}
	if protocol == downstreamTLS {
		return "", downstreamTLS
	}
	return "", downstreamUnknown
}

type sessionContextKey struct{}

func (i *Interceptor) newReverseProxy(transport http.RoundTripper, metadata *C.Metadata, defaultScheme string) http.Handler {
	reverseProxy := &httputil.ReverseProxy{
		Transport: transport,
		Rewrite: func(proxyRequest *httputil.ProxyRequest) {
			setRequestURLDefaults(proxyRequest.Out, defaultScheme)
			proxyRequest.Out.Host = proxyRequest.In.Host
		},
		ModifyResponse: func(response *http.Response) error {
			session, _ := response.Request.Context().Value(sessionContextKey{}).(*Session)
			if session == nil {
				return nil
			}
			session.SetResponse(response)
			replacement := i.handler.HandleResponse(session)
			if replacement != nil && replacement != response {
				oldBody := response.Body
				if replacement.Request == nil {
					replacement.Request = response.Request
				}
				if replacement.Body == nil {
					replacement.Body = http.NoBody
				}
				*response = *replacement
				if oldBody != nil && oldBody != response.Body {
					_ = oldBody.Close()
				}
			}
			if i.rewrite != nil {
				i.rewrite.RewriteResponse(session.Request(), response)
			}
			session.SetResponse(response)
			session.capture.observeResponse(response)
			return nil
		},
		ErrorHandler: func(writer http.ResponseWriter, request *http.Request, err error) {
			session, _ := request.Context().Value(sessionContextKey{}).(*Session)
			if session == nil {
				session = newSession(request, metadata)
			}
			session.SetResponse(session.NewErrorResponse(err))
			i.handler.HandleError(session, err)
			session.capture.setError(err)
			session.capture.observeResponse(session.Response())
			writeResponse(writer, session.Response())
		},
		ErrorLog: stdlog.New(io.Discard, "", 0),
	}

	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		setRequestURLDefaults(request, defaultScheme)
		session := newSession(request, metadata)
		var response *http.Response
		if i.rewrite != nil {
			response = i.rewrite.RewriteRequest(request)
			session.SetRequest(request)
		}
		if response == nil {
			newRequest, handlerResponse := i.handler.HandleRequest(session)
			response = handlerResponse
			if newRequest != nil {
				setRequestURLDefaults(newRequest, defaultScheme)
				request = newRequest
				session.SetRequest(newRequest)
			}
		}
		if response != nil {
			if i.rewrite != nil {
				i.rewrite.RewriteResponse(request, response)
			}
			session.SetResponse(response)
			session.capture.observeResponse(response)
			writeResponse(writer, response)
			return
		}

		requestContext := context.WithValue(request.Context(), sessionContextKey{}, session)
		reverseProxy.ServeHTTP(writer, request.WithContext(requestContext))
	})
}

func setRequestURLDefaults(request *http.Request, scheme string) {
	if request == nil || request.URL == nil {
		return
	}
	if request.URL.Scheme == "" {
		request.URL.Scheme = scheme
	}
	if request.URL.Host == "" {
		request.URL.Host = request.Host
	}
}

type requestRoundTripper struct {
	interceptor *Interceptor
	dial        DialContext

	mutex      sync.Mutex
	transports map[requestTransportKey]*requestTransport
	recent     list.List
	closed     bool
}

type requestTransportKey struct {
	// URL stays in the key because URL-REGEX rules can route paths and queries
	// on the same origin through different outbound connections.
	url                  string
	unencryptedHTTP2Only bool
}

type requestTransport struct {
	key       requestTransportKey
	transport *http.Transport
	element   *list.Element
	inFlight  int
	retired   bool
}

func newRequestRoundTripper(interceptor *Interceptor, dial DialContext) *requestRoundTripper {
	return &requestRoundTripper{
		interceptor: interceptor,
		dial:        dial,
	}
}

func (t *requestRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	requestURL := fullRequestURL(request)
	session, _ := request.Context().Value(sessionContextKey{}).(*Session)
	useUnencryptedHTTP2 := t.interceptor.config.H2 && request.URL.Scheme == "http" && request.ProtoMajor == 2
	entry := t.acquire(requestTransportKey{url: requestURL, unencryptedHTTP2Only: useUnencryptedHTTP2})

	trace := &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) {
		if session != nil {
			session.setConnectionID(connectionID(info.Conn))
		}
	}}
	requestContext := withRequestURL(request.Context(), requestURL)
	request = request.WithContext(httptrace.WithClientTrace(requestContext, trace))
	response, err := entry.transport.RoundTrip(request)
	if err != nil {
		t.release(entry)
		return nil, err
	}
	if response.Body == nil {
		t.release(entry)
		return response, nil
	}
	response.Body = &releaseResponseBody{
		ReadCloser: response.Body,
		release:    func() { t.release(entry) },
	}
	return response, nil
}

func (t *requestRoundTripper) acquire(key requestTransportKey) *requestTransport {
	var evicted *http.Transport

	t.mutex.Lock()
	if entry := t.transports[key]; entry != nil {
		entry.inFlight++
		t.recent.MoveToFront(entry.element)
		t.mutex.Unlock()
		return entry
	}

	if len(t.transports) >= maxCachedRequestTransports {
		for element := t.recent.Back(); element != nil; element = element.Prev() {
			candidate := element.Value.(*requestTransport)
			if candidate.inFlight != 0 {
				continue
			}
			delete(t.transports, candidate.key)
			t.recent.Remove(element)
			candidate.retired = true
			evicted = candidate.transport
			break
		}
	}

	entry := &requestTransport{
		key:       key,
		transport: t.interceptor.newTransport(t.dial, key.unencryptedHTTP2Only),
		inFlight:  1,
	}
	if t.closed || len(t.transports) >= maxCachedRequestTransports {
		entry.retired = true
	} else {
		if t.transports == nil {
			t.transports = make(map[requestTransportKey]*requestTransport)
		}
		entry.element = t.recent.PushFront(entry)
		t.transports[key] = entry
	}
	t.mutex.Unlock()

	if evicted != nil {
		evicted.CloseIdleConnections()
	}
	return entry
}

func (t *requestRoundTripper) release(entry *requestTransport) {
	closeIdle := false
	t.mutex.Lock()
	if entry.inFlight > 0 {
		entry.inFlight--
	}
	closeIdle = entry.retired && entry.inFlight == 0
	t.mutex.Unlock()
	if closeIdle {
		entry.transport.CloseIdleConnections()
	}
}

func (t *requestRoundTripper) CloseIdleConnections() {
	t.mutex.Lock()
	if t.closed {
		t.mutex.Unlock()
		return
	}
	t.closed = true
	transports := make([]*http.Transport, 0, len(t.transports))
	for _, entry := range t.transports {
		entry.retired = true
		transports = append(transports, entry.transport)
	}
	t.transports = nil
	t.recent.Init()
	t.mutex.Unlock()

	for _, transport := range transports {
		transport.CloseIdleConnections()
	}
}

type releaseResponseBody struct {
	io.ReadCloser
	release     func()
	releaseOnce sync.Once
}

func (b *releaseResponseBody) Write(buffer []byte) (int, error) {
	if writer, ok := b.ReadCloser.(io.Writer); ok {
		return writer.Write(buffer)
	}
	return 0, io.ErrClosedPipe
}

func (b *releaseResponseBody) Close() error {
	err := b.ReadCloser.Close()
	b.releaseOnce.Do(b.release)
	return err
}

func connectionID(connection net.Conn) string {
	for connection != nil {
		if identified, ok := connection.(connectionIdentifier); ok {
			return identified.ID()
		}
		unwrapped, ok := connection.(netConnUnwrapper)
		if !ok {
			return ""
		}
		connection = unwrapped.NetConn()
	}
	return ""
}

func (i *Interceptor) newTransport(dial DialContext, useUnencryptedHTTP2 bool) *http.Transport {
	nextProtos := []string{"http/1.1"}
	if i.config.H2 {
		nextProtos = []string{"h2", "http/1.1"}
	}
	baseTLSConfig := &tls.Config{
		RootCAs:    ca.GetCertPool(),
		Time:       ntp.Now,
		NextProtos: nextProtos,
	}
	dialConnection := func(ctx context.Context, network, address string) (net.Conn, error) {
		return dial(ctx, network, address)
	}

	transport := &http.Transport{
		Proxy: nil,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			return dialConnection(ctx, network, address)
		},
		ForceAttemptHTTP2:     i.config.H2,
		MaxIdleConns:          2,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   C.DefaultTLSTimeout,
		ExpectContinueTimeout: time.Second,
		TLSClientConfig:       baseTLSConfig,
		DialTLSContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			rawConn, err := dialConnection(ctx, network, address)
			if err != nil {
				return nil, err
			}
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				_ = rawConn.Close()
				return nil, err
			}
			tlsConfig := baseTLSConfig.Clone()
			tlsConfig.ServerName = host
			tlsConn := tls.Client(rawConn, tlsConfig)
			if err = tlsConn.HandshakeContext(ctx); err != nil {
				_ = rawConn.Close()
				return nil, err
			}
			return tlsConn, nil
		},
	}
	if useUnencryptedHTTP2 {
		protocols := new(http.Protocols)
		protocols.SetUnencryptedHTTP2(true)
		transport.Protocols = protocols
	}
	return transport
}

func writeResponse(writer http.ResponseWriter, response *http.Response) {
	if response == nil {
		writer.WriteHeader(http.StatusBadGateway)
		return
	}
	if response.Body == nil {
		response.Body = http.NoBody
	}
	defer response.Body.Close()
	for key, values := range response.Header {
		for _, value := range values {
			writer.Header().Add(key, value)
		}
	}
	if response.ContentLength >= 0 && writer.Header().Get("Content-Length") == "" {
		writer.Header().Set("Content-Length", strconv.FormatInt(response.ContentLength, 10))
	}
	statusCode := response.StatusCode
	if statusCode == 0 {
		statusCode = http.StatusOK
	}
	writer.WriteHeader(statusCode)
	_, _ = io.Copy(writer, response.Body)
}

type singleConnListener struct {
	conn     net.Conn
	mutex    sync.Mutex
	accepted bool
	done     chan struct{}
	doneOnce sync.Once
}

func newSingleConnListener(conn net.Conn) *singleConnListener {
	return &singleConnListener{conn: conn, done: make(chan struct{})}
}

func (l *singleConnListener) Accept() (net.Conn, error) {
	l.mutex.Lock()
	if !l.accepted {
		l.accepted = true
		l.mutex.Unlock()
		if tlsConn, ok := l.conn.(http.TLSConn); ok {
			return &notifyCloseTLSConn{TLSConn: tlsConn, onClose: l.finish}, nil
		}
		return &notifyCloseConn{Conn: l.conn, onClose: l.finish}, nil
	}
	l.mutex.Unlock()
	<-l.done
	return nil, errSingleConnectionDone
}

func (l *singleConnListener) Close() error {
	l.finish()
	return l.conn.Close()
}

func (l *singleConnListener) Addr() net.Addr {
	return l.conn.LocalAddr()
}

func (l *singleConnListener) finish() {
	l.doneOnce.Do(func() { close(l.done) })
}

type notifyCloseTLSConn struct {
	http.TLSConn
	onClose func()
}

type notifyCloseConn struct {
	net.Conn
	onClose func()
}

func (c *notifyCloseConn) Close() error {
	c.onClose()
	return c.Conn.Close()
}

func (c *notifyCloseTLSConn) Close() error {
	c.onClose()
	return c.TLSConn.Close()
}
