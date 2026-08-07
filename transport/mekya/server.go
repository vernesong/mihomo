package mekya

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/metacubex/mihomo/transport/mkcp"

	"github.com/metacubex/http"
)

type Listener struct {
	outer      net.Listener
	packetConn *wrappedPacketConn
	mkcp       *mkcp.Listener
	server     *http.Server
	done       chan struct{}
	once       sync.Once
}

func Listen(ctx context.Context, ln net.Listener, cfg Config) (*Listener, error) {
	packetConn := newWrappedPacketConn(ctx, ln.Addr())
	handler := newServer(ctx, cfg, packetConn)
	mkcpListener, err := mkcp.Listen(ctx, packetConn, cfg.KCP)
	if err != nil {
		return nil, err
	}

	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetHTTP2(true)
	protocols.SetUnencryptedHTTP2(true)
	server := &http.Server{
		Handler:           handler,
		Protocols:         protocols,
		ReadHeaderTimeout: 240 * time.Second,
		ReadTimeout:       240 * time.Second,
		WriteTimeout:      240 * time.Second,
		IdleTimeout:       240 * time.Second,
	}
	l := &Listener{
		outer:      ln,
		packetConn: packetConn,
		mkcp:       mkcpListener,
		server:     server,
		done:       make(chan struct{}),
	}
	go func() {
		defer close(l.done)
		err := server.Serve(ln)
		if err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
			_ = mkcpListener.Close()
			_ = packetConn.Close()
		}
	}()
	go func() {
		select {
		case <-ctx.Done():
			_ = l.Close()
		case <-l.done:
		}
	}()
	return l, nil
}

func (l *Listener) Accept() (net.Conn, error) {
	return l.mkcp.Accept()
}

func (l *Listener) Close() error {
	var err error
	l.once.Do(func() {
		err = errors.Join(l.server.Close(), l.mkcp.Close(), l.packetConn.Close(), l.outer.Close())
		<-l.done
	})
	return err
}

func (l *Listener) Addr() net.Addr {
	return l.outer.Addr()
}

type server struct {
	ctx        context.Context
	cfg        Config
	packetConn *wrappedPacketConn
	sessions   sync.Map
}

func newServer(ctx context.Context, cfg Config, packetConn *wrappedPacketConn) *server {
	return &server{ctx: ctx, cfg: cfg, packetConn: packetConn}
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	sessionID, err := base64.RawURLEncoding.DecodeString(r.Header.Get("X-Session-ID"))
	if err != nil {
		http.Error(w, "invalid session id", http.StatusBadRequest)
		return
	}
	body, err := io.ReadAll(r.Body)
	_ = r.Body.Close()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	session, err := s.getSession(r.Context(), sessionID, parseRemoteAddr(r.RemoteAddr))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := session.ingest(body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
	session.writeResponse(r.Context(), w)
}

func (s *server) getSession(_ context.Context, sessionID []byte, remoteAddr *net.TCPAddr) (*serverSession, error) {
	key := string(sessionID)
	if session, ok := s.sessions.Load(key); ok {
		return session.(*serverSession), nil
	}
	sessionCtx, cancel := context.WithCancel(s.ctx)
	session := &serverSession{
		ctx:                            sessionCtx,
		cancel:                         cancel,
		sessionID:                      append([]byte(nil), sessionID...),
		remoteAddr:                     remoteAddr,
		server:                         s,
		writerChan:                     make(chan []byte, s.cfg.PacketWritingBuffer),
		readerChan:                     make(chan []byte, 256),
		maxWriteSize:                   s.cfg.MaxWriteSize,
		maxWriteDuration:               time.Duration(s.cfg.MaxWriteDurationMs) * time.Millisecond,
		maxSimultaneousWriteConnection: s.cfg.MaxSimultaneousWriteConnection,
	}
	actual, loaded := s.sessions.LoadOrStore(key, session)
	if loaded {
		cancel()
		return actual.(*serverSession), nil
	}
	if err := s.packetConn.addSession(session); err != nil {
		cancel()
		s.sessions.Delete(key)
		return nil, err
	}
	return session, nil
}

func (s *server) removeSession(sessionID []byte) {
	s.sessions.Delete(string(sessionID))
}

func parseRemoteAddr(addr string) *net.TCPAddr {
	if tcpAddr, err := net.ResolveTCPAddr("tcp", addr); err == nil {
		return tcpAddr
	}
	return &net.TCPAddr{}
}

type serverSession struct {
	ctx                            context.Context
	cancel                         context.CancelFunc
	sessionID                      []byte
	remoteAddr                     *net.TCPAddr
	server                         *server
	writerChan                     chan []byte
	readerChan                     chan []byte
	maxWriteSize                   int
	maxWriteDuration               time.Duration
	maxSimultaneousWriteConnection int
	writingMu                      sync.Mutex
	writingConns                   []*writingConnection
}

func (s *serverSession) ingest(body []byte) error {
	reader := bytes.NewReader(body)
	for reader.Len() > 0 {
		packet, err := readPacketBundle(reader)
		if err != nil {
			return err
		}
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		case s.readerChan <- packet:
		}
	}
	return nil
}

type writingConnection struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *serverSession) beginWritingConnection(ctx context.Context) (*writingConnection, context.Context) {
	writeCtx, cancel := context.WithCancel(ctx)
	conn := &writingConnection{ctx: writeCtx, cancel: cancel}

	var stale []*writingConnection
	s.writingMu.Lock()
	s.writingConns = append(s.writingConns, conn)
	if s.maxSimultaneousWriteConnection > 0 {
		for len(s.writingConns) > s.maxSimultaneousWriteConnection {
			old := s.writingConns[0]
			s.writingConns[0] = nil
			s.writingConns = s.writingConns[1:]
			stale = append(stale, old)
		}
	}
	s.writingMu.Unlock()

	for _, old := range stale {
		old.cancel()
	}
	return conn, writeCtx
}

func (s *serverSession) finishWritingConnection(conn *writingConnection) {
	conn.cancel()

	s.writingMu.Lock()
	for i, item := range s.writingConns {
		if item == conn {
			copy(s.writingConns[i:], s.writingConns[i+1:])
			s.writingConns[len(s.writingConns)-1] = nil
			s.writingConns = s.writingConns[:len(s.writingConns)-1]
			break
		}
	}
	s.writingMu.Unlock()
}

func (s *serverSession) writeResponse(ctx context.Context, w http.ResponseWriter) {
	writeConn, writeCtx := s.beginWritingConnection(ctx)
	defer s.finishWritingConnection(writeConn)

	flusher, _ := w.(http.Flusher)
	timer := time.NewTimer(s.maxWriteDuration)
	defer timer.Stop()
	bytesSent := 0
	for {
		select {
		case <-writeCtx.Done():
			return
		case <-s.ctx.Done():
			return
		case packet := <-s.writerChan:
			if err := writePacketBundle(w, packet); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
			bytesSent += packetBundleOverhead + len(packet)
			if s.maxWriteSize > 0 && bytesSent >= s.maxWriteSize {
				return
			}
		case <-timer.C:
			return
		}
	}
}

func (s *serverSession) Read(p []byte) (int, error) {
	select {
	case <-s.ctx.Done():
		return 0, s.ctx.Err()
	case packet := <-s.readerChan:
		return copy(p, packet), nil
	}
}

func (s *serverSession) Write(p []byte) (int, error) {
	packet := append([]byte(nil), p...)
	select {
	case <-s.ctx.Done():
		return 0, s.ctx.Err()
	case s.writerChan <- packet:
		return len(p), nil
	default:
		return len(p), nil
	}
}

func (s *serverSession) Close() error {
	s.server.removeSession(s.sessionID)
	s.cancel()
	return nil
}

func (s *serverSession) Network() string {
	if s.remoteAddr == nil {
		return ""
	}
	return s.remoteAddr.Network()
}

func (s *serverSession) String() string {
	if s.remoteAddr == nil {
		return ""
	}
	return s.remoteAddr.String()
}

var _ net.Listener = (*Listener)(nil)
var _ net.Addr = (*serverSession)(nil)
var _ io.ReadWriteCloser = (*serverSession)(nil)
