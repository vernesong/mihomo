package outboundgroup

import (
	"net"
	"testing"
	"time"

	N "github.com/metacubex/mihomo/common/net"
	C "github.com/metacubex/mihomo/constant"
)

type smartMonitorTestConn struct {
	N.ExtendedConn
}

func (*smartMonitorTestConn) Chains() C.Chain               { return nil }
func (*smartMonitorTestConn) ProviderChains() C.Chain       { return nil }
func (*smartMonitorTestConn) AppendToChains(C.ProxyAdapter) {}
func (*smartMonitorTestConn) RemoteDestination() string     { return "" }

func TestSmartMonitoredConnPreservesRelayUnwrap(t *testing.T) {
	upstreamReader, upstreamWriter := net.Pipe()
	defer upstreamReader.Close()
	defer upstreamWriter.Close()
	base := N.NewExtendedConn(upstreamReader)
	inner := &smartMonitorTestConn{ExtendedConn: base}
	monitored := &smartMonitoredConn{Conn: inner}

	if got := N.UnwrapReader(monitored); got != inner {
		t.Fatalf("reader did not unwrap to base connection: %T", got)
	}
	if got := N.UnwrapWriter(monitored); got != inner {
		t.Fatalf("writer did not unwrap to base connection: %T", got)
	}
}

func TestSmartMonitoredConnNoResponseNeedsMeaningfulWrite(t *testing.T) {
	start := time.Unix(100, 0)
	conn := &smartMonitoredConn{}

	conn.observeTotals(smartMonitorMinUploadBytes-1, 0, start)
	if signal, ok := conn.sample(start.Add(smartMonitorNoResponseAfter)); ok {
		t.Fatalf("tiny write produced signal: %s", signal)
	}

	conn.observeTotals(smartMonitorMinUploadBytes, 0, start.Add(time.Second))
	signal, ok := conn.sample(start.Add(time.Second + smartMonitorNoResponseAfter))
	if !ok || signal.reason != "no-response-after-write" {
		t.Fatalf("meaningful unanswered write did not produce expected signal: ok=%v signal=%s", ok, signal)
	}
}

func TestSmartMonitoredConnDistinguishesSlowAndHealthyTransfer(t *testing.T) {
	start := time.Unix(200, 0)
	slow := &smartMonitoredConn{}
	slow.observeTotals(smartMonitorMinUploadBytes, 0, start)
	slow.observeTotals(smartMonitorMinUploadBytes, smartMonitorMinDownloadBytes, start.Add(time.Second))
	signal, ok := slow.sample(start.Add(3 * time.Second))
	if !ok || signal.reason != "sustained-low-throughput" {
		t.Fatalf("slow transfer did not produce expected signal: ok=%v signal=%s", ok, signal)
	}

	healthy := &smartMonitoredConn{}
	healthy.observeTotals(smartMonitorMinUploadBytes, 0, start)
	healthy.observeTotals(smartMonitorMinUploadBytes, 128*1024, start.Add(time.Second))
	if signal, ok := healthy.sample(start.Add(3 * time.Second)); ok {
		t.Fatalf("healthy transfer produced signal: %s", signal)
	}
}

func TestSmartActiveDegradationRejectsRepeatedEvidenceFromSameConnection(t *testing.T) {
	s := &Smart{}
	conn := &smartMonitoredConn{}
	now := time.Unix(300, 0)
	const key = "node\x00*.example.com"

	if s.recordActiveDegradationEvidence(key, conn, 1, "sustained-low-throughput", now) {
		t.Fatal("first observation confirmed degradation")
	}
	if s.recordActiveDegradationEvidence(key, conn, 1, "sustained-low-throughput", now.Add(time.Second)) {
		t.Fatal("duplicate observation confirmed degradation")
	}
	if s.recordActiveDegradationEvidence(key, conn, 2, "sustained-low-throughput", now.Add(2*time.Second)) {
		t.Fatal("same connection confirmed degradation")
	}
	conn.closed = true
	if !s.recordActiveDegradationEvidence(key, &smartMonitoredConn{}, 1, "sustained-low-throughput", now.Add(3*time.Second)) {
		t.Fatal("closed prior connection and distinct new connection did not confirm degradation")
	}
}

func TestSmartActiveDegradationEvidenceExpires(t *testing.T) {
	s := &Smart{}
	conn := &smartMonitoredConn{}
	now := time.Unix(400, 0)
	const key = "node\x00*.example.com"

	if s.recordActiveDegradationEvidence(key, conn, 1, "no-response-after-write", now) {
		t.Fatal("first observation confirmed degradation")
	}
	if s.recordActiveDegradationEvidence(key, conn, 2, "no-response-after-write", now.Add(smartMonitorEvidenceWindow+time.Second)) {
		t.Fatal("expired evidence confirmed degradation")
	}
}

func TestSmartActiveDegradationDoesNotConfirmNoResponseEvidence(t *testing.T) {
	s := &Smart{}
	first := &smartMonitoredConn{}
	second := &smartMonitoredConn{}
	now := time.Unix(450, 0)
	const key = "node\x00*.example.com"

	if s.recordActiveDegradationEvidence(key, first, 1, "no-response-after-write", now) {
		t.Fatal("first no-response observation confirmed degradation")
	}
	if s.recordActiveDegradationEvidence(key, second, 1, "no-response-after-write", now.Add(time.Second)) {
		t.Fatal("concurrent no-response connection confirmed degradation")
	}
	first.closed = true
	if s.recordActiveDegradationEvidence(key, second, 1, "no-response-after-write", now.Add(2*time.Second)) {
		t.Fatal("no-response evidence confirmed degradation")
	}
}

func TestSmartActiveDegradationCooldownIsScopedByNodeAndWildcard(t *testing.T) {
	s := &Smart{}
	now := time.Unix(500, 0)
	key := "node-a\x00*.example.com"
	s.activeMonitor.evidence = map[string]smartDegradationEvidence{
		key: {confirmedUntil: now.Add(smartMonitorCooldown)},
	}

	if !s.activeNodeCoolingDown("node-a", "*.example.com", now) {
		t.Fatal("confirmed node and wildcard were not cooling down")
	}
	if s.activeNodeCoolingDown("node-b", "*.example.com", now) {
		t.Fatal("cooldown leaked to another node")
	}
	if s.activeNodeCoolingDown("node-a", "*.other.example", now) {
		t.Fatal("cooldown leaked to another wildcard")
	}
	if s.activeNodeCoolingDown("node-a", "*.example.com", now.Add(smartMonitorCooldown)) {
		t.Fatal("expired cooldown remained active")
	}
}
