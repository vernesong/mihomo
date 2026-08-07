package tcpstats

import (
	"net"
	"syscall"
	"testing"
)

type fakeRawConn struct{}

func (f *fakeRawConn) Control(fn func(fd uintptr)) error { return nil }
func (f *fakeRawConn) Read(fn func(fd uintptr) bool) error { return nil }
func (f *fakeRawConn) Write(fn func(fd uintptr) bool) error { return nil }
func (f *fakeRawConn) Close() error { return nil }

type syscallConn struct {
	net.Conn
	syscallCalled bool
}

func (c *syscallConn) SyscallConn() (syscall.RawConn, error) {
	c.syscallCalled = true
	return &fakeRawConn{}, nil
}

type upstreamWrapper struct {
	net.Conn
	upstream any
}

func (w *upstreamWrapper) Upstream() any { return w.upstream }

type netConnFallbackWrapper struct {
	net.Conn
	inner net.Conn
}

func (w *netConnFallbackWrapper) Upstream() any { return "not a net.Conn" }
func (w *netConnFallbackWrapper) NetConn() net.Conn { return w.inner }

type reflectFallbackWrapper struct {
	net.Conn
	extra any
}

type cyclicWrapper struct {
	net.Conn
}

func (w *cyclicWrapper) Upstream() any { return w }

func newBaseConn(t *testing.T) *syscallConn {
	t.Helper()
	client, peer := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = peer.Close()
	})
	return &syscallConn{Conn: client}
}

func TestGetTCPStats_UnwrapUpstreamChain(t *testing.T) {
	base := newBaseConn(t)
	inner := &upstreamWrapper{Conn: base, upstream: base}
	outer := &upstreamWrapper{Conn: inner, upstream: inner}

	getTCPStats(outer)
	if !base.syscallCalled {
		t.Fatal("expected getTCPStats to unwrap through the Upstream chain to SyscallConn")
	}
}

func TestGetTCPStats_UpstreamFallbackToNetConn(t *testing.T) {
	base := newBaseConn(t)
	w := &netConnFallbackWrapper{Conn: base, inner: base}

	getTCPStats(w)
	if !base.syscallCalled {
		t.Fatal("expected getTCPStats to fall back to NetConn() when Upstream() returns a non-net.Conn")
	}
}

func TestGetTCPStats_ReflectFallback(t *testing.T) {
	base := newBaseConn(t)
	w := &reflectFallbackWrapper{Conn: base}

	getTCPStats(w)
	if !base.syscallCalled {
		t.Fatal("expected getTCPStats to unwrap the embedded net.Conn via reflection")
	}
}

func TestGetTCPStats_CyclicUpstream(t *testing.T) {
	client, peer := net.Pipe()
	defer client.Close()
	defer peer.Close()
	w := &cyclicWrapper{Conn: client}

	if stats := getTCPStats(w); stats != nil {
		t.Fatal("expected nil stats for a cyclic unwrap chain")
	}
}
