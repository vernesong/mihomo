package mekya

import (
	"context"
	"io"
	"net"
	"os"
	"sync"
	"time"
)

type packet struct {
	addr net.Addr
	data []byte
}

type wrappedPacketConn struct {
	ctx       context.Context
	cancel    context.CancelFunc
	mu        sync.Mutex
	sessions  map[string]*serverSession
	readChan  chan packet
	local     net.Addr
	deadlines pipeDeadlines
}

func newWrappedPacketConn(ctx context.Context, local net.Addr) *wrappedPacketConn {
	ctx, cancel := context.WithCancel(ctx)
	return &wrappedPacketConn{
		ctx:       ctx,
		cancel:    cancel,
		sessions:  make(map[string]*serverSession),
		readChan:  make(chan packet, 16),
		local:     local,
		deadlines: newPipeDeadlines(),
	}
}

func (c *wrappedPacketConn) addSession(session *serverSession) error {
	select {
	case <-c.ctx.Done():
		return net.ErrClosed
	default:
	}
	c.mu.Lock()
	if c.sessions == nil {
		c.mu.Unlock()
		return net.ErrClosed
	}
	c.sessions[string(session.sessionID)] = session
	c.mu.Unlock()
	go c.readSession(session)
	return nil
}

func (c *wrappedPacketConn) readSession(session *serverSession) {
	buf := make([]byte, 2000)
	for {
		n, err := session.Read(buf)
		if err != nil || n > len(buf) {
			return
		}
		payload := append([]byte(nil), buf[:n]...)
		select {
		case <-c.ctx.Done():
			return
		case c.readChan <- packet{addr: session, data: payload}:
		}
	}
}

func (c *wrappedPacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	select {
	case <-c.ctx.Done():
		return 0, nil, c.ctx.Err()
	case <-c.deadlines.read.Wait():
		return 0, nil, os.ErrDeadlineExceeded
	case packet := <-c.readChan:
		n := copy(p, packet.data)
		if n < len(packet.data) {
			return n, packet.addr, io.ErrShortBuffer
		}
		return n, packet.addr, nil
	}
}

func (c *wrappedPacketConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	select {
	case <-c.ctx.Done():
		return 0, c.ctx.Err()
	case <-c.deadlines.write.Wait():
		return 0, os.ErrDeadlineExceeded
	default:
	}
	session, ok := addr.(*serverSession)
	if !ok {
		return 0, net.ErrClosed
	}
	c.mu.Lock()
	session = c.sessions[string(session.sessionID)]
	c.mu.Unlock()
	if session == nil {
		return 0, net.ErrClosed
	}
	return session.Write(p)
}

func (c *wrappedPacketConn) Close() error {
	c.cancel()
	c.mu.Lock()
	sessions := make([]*serverSession, 0, len(c.sessions))
	for _, session := range c.sessions {
		sessions = append(sessions, session)
	}
	c.sessions = nil
	c.mu.Unlock()
	for _, session := range sessions {
		_ = session.Close()
	}
	return nil
}

func (c *wrappedPacketConn) LocalAddr() net.Addr {
	return c.local
}

func (c *wrappedPacketConn) SetDeadline(t time.Time) error {
	return c.deadlines.SetDeadline(t)
}

func (c *wrappedPacketConn) SetReadDeadline(t time.Time) error {
	return c.deadlines.SetReadDeadline(t)
}

func (c *wrappedPacketConn) SetWriteDeadline(t time.Time) error {
	return c.deadlines.SetWriteDeadline(t)
}

var _ net.PacketConn = (*wrappedPacketConn)(nil)
