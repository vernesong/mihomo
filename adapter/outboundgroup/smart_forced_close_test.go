package outboundgroup

import (
	"context"
	"errors"
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/tunnel/statistic"
)

// Repeated failures of node b for a target must not close the target's healthy connections
// on node a (here a TCP connection and a UDP call).
func TestSmartFailureOnOneNodeKeepsOtherNodesConnections(t *testing.T) {
	a, b := newFakeNode("a"), newFakeNode("b")
	s := newTestSmart(t, a, b)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// a healthy TCP connection and UDP call on a
	b.tcpFail.Store(true)
	b.udpFail.Store(true)
	mt := tcpMeta("zoom.us")
	ct, err := s.DialContext(ctx, mt)
	if err != nil || lastNode(ct.Chains()) != "a" {
		t.Fatalf("setup tcp: %v %v", err, ct)
	}
	tt := statistic.NewTCPTracker(ct, statistic.DefaultManager, mt, nil, 0, 0, false)
	t.Cleanup(func() { _ = tt.Close() })
	call, _ := openTrackedUDP(t, s, 50100)
	if lastNode(call.Chains()) != "a" {
		t.Fatal("setup: call not on a")
	}
	// let the async stats of the setup dials settle
	time.Sleep(100 * time.Millisecond)

	// node b now fails repeatedly for the same target (driven synchronously)
	pb := proxyByName(s, "b")
	for i := 0; i < 12; i++ {
		m := tcpMeta("zoom.us")
		m.SrcPort = uint16(51000 + i)
		s.selectProxies(m, s.GetProxies(true))
		s.recordConnectionStats(m, pb, 30, 0, 0, 0, 0, 0, 0, nil, nil, errors.New("b: connection refused"))
	}

	a.mu.Lock()
	tcpClosed := a.tcpConns[0].closed.Load()
	udpClosed := a.udpConns[0].closed.Load()
	a.mu.Unlock()
	if tcpClosed || udpClosed {
		t.Fatalf("node b failing closed node a's healthy connections (tcp closed=%v, udp call closed=%v)", tcpClosed, udpClosed)
	}
}

// The scoping must still close the failing node's own connections on the failure's network,
// and a TCP failure must not close a UDP flow on the same node.
func TestSmartForcedCloseStillClosesFailingNode(t *testing.T) {
	a, b := newFakeNode("a"), newFakeNode("b")
	s := newTestSmart(t, a, b)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	a.tcpFail.Store(true)
	a.udpFail.Store(true)
	m := tcpMeta("zoom.us")
	c, err := s.DialContext(ctx, m)
	if err != nil || lastNode(c.Chains()) != "b" {
		t.Fatalf("setup tcp on b: %v", err)
	}
	tr := statistic.NewTCPTracker(c, statistic.DefaultManager, m, nil, 0, 0, false)
	t.Cleanup(func() { _ = tr.Close() })
	call, _ := openTrackedUDP(t, s, 50200)
	if lastNode(call.Chains()) != "b" {
		t.Fatal("setup: udp not on b")
	}

	s.closeSameConnection(&C.Metadata{UUID: "failure", NetWork: C.TCP}, "b", m.SmartTarget, true)

	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.tcpConns[0].closed.Load() {
		t.Fatal("the failing node's TCP connection to the target was not closed")
	}
	if b.udpConns[0].closed.Load() {
		t.Fatal("a TCP failure closed the node's UDP flow")
	}
}
