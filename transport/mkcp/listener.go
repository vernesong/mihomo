package mkcp

import (
	"context"
	"net"
	"sync"
)

type Listener struct {
	pc             net.PacketConn
	cfg            Config
	reader         packetReader
	securityWriter func(ioWriter) packetWriter
	mu             sync.Mutex
	sessions       map[sessionID]*Conn
	packets        chan packetPayload
	accept         chan net.Conn
	closed         chan struct{}
	once           sync.Once
}

type ioWriter interface {
	Write([]byte) (int, error)
}

type sessionID struct {
	addr string
	conv uint16
}

type packetPayload struct {
	payload []byte
	addr    net.Addr
}

func Listen(ctx context.Context, pc net.PacketConn, cfg Config) (*Listener, error) {
	security, err := cfg.security()
	if err != nil {
		return nil, err
	}
	l := &Listener{
		pc:       pc,
		cfg:      cfg,
		reader:   packetReader{security: security, header: cfg.packetHeader()},
		sessions: make(map[sessionID]*Conn),
		packets:  make(chan packetPayload, 1024),
		accept:   make(chan net.Conn, 1024),
		closed:   make(chan struct{}),
	}
	l.securityWriter = func(w ioWriter) packetWriter {
		return packetWriter{security: security, header: cfg.packetHeader(), writer: w}
	}
	go l.readLoop()
	go l.packetLoop()
	go func() {
		select {
		case <-ctx.Done():
			_ = l.Close()
		case <-l.closed:
		}
	}()
	return l, nil
}

func (l *Listener) readLoop() {
	buf := make([]byte, 64*1024)
	for {
		n, addr, err := l.pc.ReadFrom(buf)
		if err != nil {
			_ = l.Close()
			return
		}
		payload := append([]byte(nil), buf[:n]...)
		select {
		case l.packets <- packetPayload{payload: payload, addr: addr}:
		case <-l.closed:
			return
		default:
		}
	}
}

func (l *Listener) packetLoop() {
	for {
		select {
		case packet := <-l.packets:
			l.onReceive(packet.payload, packet.addr)
		case <-l.closed:
			return
		}
	}
}

func (l *Listener) onReceive(payload []byte, addr net.Addr) {
	segments := l.reader.read(payload)
	if len(segments) == 0 {
		return
	}
	conv := segments[0].conversation()
	id := sessionID{addr: addr.String(), conv: conv}

	l.mu.Lock()
	if l.sessions == nil {
		l.mu.Unlock()
		return
	}
	conn := l.sessions[id]
	if conn == nil {
		if segments[0].command() == commandTerminate {
			l.mu.Unlock()
			return
		}
		writer := &packetConnWriter{pc: l.pc, addr: addr, remove: func() {
			l.remove(id)
		}}
		conn = newConn(l.pc.LocalAddr(), addr, conv, l.securityWriter(writer), writer, l.cfg)
		l.sessions[id] = conn
		select {
		case l.accept <- conn:
		case <-l.closed:
			l.mu.Unlock()
			_ = conn.Close()
			return
		}
	}
	l.mu.Unlock()
	conn.Input(segments)
}

func (l *Listener) remove(id sessionID) {
	l.mu.Lock()
	delete(l.sessions, id)
	l.mu.Unlock()
}

func (l *Listener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.accept:
		return conn, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *Listener) Close() error {
	l.once.Do(func() {
		close(l.closed)
		_ = l.pc.Close()
		l.mu.Lock()
		sessions := make([]*Conn, 0, len(l.sessions))
		for _, conn := range l.sessions {
			sessions = append(sessions, conn)
		}
		l.sessions = nil
		l.mu.Unlock()
		for _, conn := range sessions {
			conn.terminate()
		}
	})
	return nil
}

func (l *Listener) Addr() net.Addr {
	return l.pc.LocalAddr()
}

type packetConnWriter struct {
	pc     net.PacketConn
	addr   net.Addr
	remove func()
}

func (w *packetConnWriter) Write(b []byte) (int, error) {
	return w.pc.WriteTo(b, w.addr)
}

func (w *packetConnWriter) Close() error {
	if w.remove != nil {
		w.remove()
	}
	return nil
}

var _ net.Listener = (*Listener)(nil)
var _ net.Conn = (*Conn)(nil)
