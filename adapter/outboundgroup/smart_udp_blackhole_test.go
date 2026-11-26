package outboundgroup

import (
	"bytes"
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/metacubex/mihomo/component/smart"
	"github.com/metacubex/mihomo/tunnel/statistic"
)

// A node that accepts UDP sessions but never returns a packet (a UDP blackhole, e.g. a
// congested node during a video call) must not be scored as a good node: such a session
// closes without error and with no latency.
func TestSmartUDPBlackholeIsNotScoredHealthy(t *testing.T) {
	a, b := newFakeNode("a"), newFakeNode("b")
	b.udpFail.Store(true) // b refuses UDP: a is the only node that ever accepts a session
	s := newTestSmart(t, a, b)

	target := ""
	sessions := 0
	for i := 0; i < 6; i++ {
		m := udpMeta("zoom.us", uint16(52000+i))
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		pc, err := s.ListenPacketContext(ctx, m)
		cancel()
		target = m.SmartTarget
		if err != nil {
			// a never refuses, so a failed listen means Smart stopped offering a: fixed behaviour
			break
		}
		sessions++
		tr := statistic.NewUDPTracker(pc, statistic.DefaultManager, m, nil, 0, 0, false)
		// the client sends media for 10 s and never receives anything back
		_, _ = tr.WriteTo(bytes.Repeat([]byte{1}, 1200), &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9})
		tr.Info().Start = time.Now().Add(-10 * time.Second)
		_ = tr.Close()
		time.Sleep(20 * time.Millisecond)
	}
	if sessions == 0 {
		t.Fatal("setup: no session was ever opened on a")
	}
	// close callbacks record asynchronously: judge only once every session is in a's record
	key := smart.FormatDBKey(smart.KeyTypeStats, s.configName, s.Name(), target, "a")
	rec := s.store.GetOrCreateAtomicRecord(key, s.Name(), s.configName, target, "a")
	for deadline := time.Now().Add(5 * time.Second); rec.Success()+rec.Failure() < int64(sessions); {
		if time.Now().After(deadline) {
			t.Fatalf("setup: only %d of %d sessions recorded", rec.Success()+rec.Failure(), sessions)
		}
		time.Sleep(20 * time.Millisecond)
	}

	names, weights, err := s.store.GetBestProxyForTarget(s.Name(), s.configName, target, true)
	if err != nil && !strings.Contains(err.Error(), "no best node") {
		t.Fatal(err)
	}
	healthy := false
	for i, n := range names {
		if n == "a" && i < len(weights) && weights[i] >= smart.AllowedWeight {
			healthy = true
		}
	}
	offered := false
	cands, _, _ := s.selectProxies(udpMeta("zoom.us", 52100), s.GetProxies(true))
	for _, p := range cands {
		offered = offered || p.Name() == "a"
	}
	if healthy && offered {
		t.Fatalf("node a never answered %d UDP sessions yet is still offered and ranked %s with weight >= AllowedWeight",
			sessions, strings.Join(names, ","))
	}
}
