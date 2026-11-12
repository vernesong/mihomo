package outboundgroup

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/adapter/provider"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
	"github.com/metacubex/mihomo/tunnel/statistic"
)

// Shared test helpers for Smart group tests: scripted fake nodes and a group built on them.

type closeRecorderConn struct {
	net.Conn
	closed atomic.Bool
}

func (c *closeRecorderConn) Close() error {
	c.closed.Store(true)
	return c.Conn.Close()
}

type closeRecorderPacketConn struct {
	net.PacketConn
	closed atomic.Bool
}

func (c *closeRecorderPacketConn) Close() error {
	c.closed.Store(true)
	return c.PacketConn.Close()
}

// fakeNode is an outbound whose TCP/UDP dial results are scripted by the test.
type fakeNode struct {
	*outbound.Base
	tcpFail  atomic.Bool
	udpFail  atomic.Bool
	tcpDelay time.Duration
	noRecord bool // do not keep conns (long-running memory tests)
	tcpDials atomic.Int64
	udpDials atomic.Int64
	mu       sync.Mutex
	tcpConns []*closeRecorderConn
	udpConns []*closeRecorderPacketConn
}

func newFakeNode(name string) *fakeNode {
	return &fakeNode{Base: outbound.NewBase(outbound.BaseOption{Name: name, Type: C.Vmess, UDP: true})}
}

var errFakeDial = errors.New("fake node: dial refused")

func (n *fakeNode) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	n.tcpDials.Add(1)
	if n.tcpDelay > 0 {
		select {
		case <-time.After(n.tcpDelay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if n.tcpFail.Load() {
		return nil, errFakeDial
	}
	a, b := net.Pipe()
	go func() { _, _ = b.Read(make([]byte, 1)); _ = b.Close() }()
	rc := &closeRecorderConn{Conn: a}
	if !n.noRecord {
		n.mu.Lock()
		n.tcpConns = append(n.tcpConns, rc)
		n.mu.Unlock()
	}
	return outbound.NewConn(rc, n), nil
}

func (n *fakeNode) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
	n.udpDials.Add(1)
	if n.udpFail.Load() {
		return nil, errFakeDial
	}
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	rc := &closeRecorderPacketConn{PacketConn: pc}
	n.mu.Lock()
	n.udpConns = append(n.udpConns, rc)
	n.mu.Unlock()
	return outbound.NewPacketConn(rc, n), nil
}

var testGroupSeq atomic.Int64

func newTestSmart(t *testing.T, nodes ...*fakeNode) *Smart {
	t.Helper()
	proxies := make([]C.Proxy, 0, len(nodes))
	for _, n := range nodes {
		proxies = append(proxies, adapter.NewProxy(n))
	}
	hc := provider.NewHealthCheck(proxies, "", 0, 0, true, nil)
	pd, err := provider.NewCompatibleProvider("smart-test", proxies, hc)
	if err != nil {
		t.Fatal(err)
	}
	// Smart keeps learned state in a process-global store keyed by group name, so every
	// instance gets a unique name (go test -count=N reruns tests in the same process).
	name := fmt.Sprintf("smart-test-%s-%d", t.Name(), testGroupSeq.Add(1))
	s, err := NewSmart(GroupCommonOption{Name: name}, SmartOption{}, adapter.NewProxy(outbound.NewCompatible()), []P.ProxyProvider{pd})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func tcpMeta(host string) *C.Metadata {
	return &C.Metadata{
		NetWork: C.TCP,
		Type:    C.HTTP,
		Host:    host,
		DstPort: 443,
		SrcIP:   netip.MustParseAddr("192.168.1.10"),
		SrcPort: 50000,
	}
}

func udpMeta(host string, srcPort uint16) *C.Metadata {
	return &C.Metadata{
		NetWork: C.UDP,
		Type:    C.TUN,
		Host:    host,
		DstPort: 8801, // Zoom media
		SrcIP:   netip.MustParseAddr("192.168.1.10"),
		SrcPort: srcPort,
	}
}

func lastNode(chains C.Chain) string {
	if len(chains) == 0 {
		return ""
	}
	return chains[0]
}

// openTrackedUDP opens a UDP session through the group and registers it with the global
// connection manager the way the tunnel does, so the group can find it.
func openTrackedUDP(t *testing.T, s *Smart, srcPort uint16) (C.PacketConn, *C.Metadata) {
	t.Helper()
	m := udpMeta("zoom.us", srcPort)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pc, err := s.ListenPacketContext(ctx, m)
	if err != nil {
		t.Fatal(err)
	}
	tracker := statistic.NewUDPTracker(pc, statistic.DefaultManager, m, nil, 0, 0, false)
	t.Cleanup(func() { _ = tracker.Close() })
	return pc, m
}

// listenUntilNode opens new UDP flows to the call's target until one lands on the wanted
// node (at most 3 tries); flows that fail are part of the scenario.
func listenUntilNode(t *testing.T, s *Smart, node string, srcPort uint16) C.PacketConn {
	t.Helper()
	for i := uint16(0); i < 3; i++ {
		m := udpMeta("zoom.us", srcPort+i)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		pc, err := s.ListenPacketContext(ctx, m)
		cancel()
		if err != nil {
			continue
		}
		tracker := statistic.NewUDPTracker(pc, statistic.DefaultManager, m, nil, 0, 0, false)
		t.Cleanup(func() { _ = tracker.Close() })
		if lastNode(pc.Chains()) == node {
			return pc
		}
	}
	t.Fatalf("setup: no new flow landed on %s", node)
	return nil
}

func proxyByName(s *Smart, name string) C.Proxy {
	for _, p := range s.GetProxies(true) {
		if p.Name() == name {
			return p
		}
	}
	return nil
}
