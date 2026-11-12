package outboundgroup

import (
	"context"
	"sync"
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/tunnel/statistic"
)

// A forced close writes another connection's Metadata.SmartBlock while that connection's own
// close callback reads it on another goroutine. Run with -race: without the accessors the
// race detector reports the unsynchronized string read/write.
func TestSmartForcedCloseRacesCloseCallback(t *testing.T) {
	a, b := newFakeNode("a"), newFakeNode("b")
	s := newTestSmart(t, a, b)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		m := tcpMeta("race.example")
		m.SrcPort = uint16(53000 + i)
		c, err := s.DialContext(ctx, m)
		if err != nil {
			t.Fatal(err)
		}
		tr := statistic.NewTCPTracker(c, statistic.DefaultManager, m, nil, 0, 0, false)
		wg.Add(2)
		go func() { defer wg.Done(); _ = tr.Close() }()
		go func(m *C.Metadata, node string) {
			defer wg.Done()
			s.closeSameConnection(&C.Metadata{UUID: "other", NetWork: C.TCP}, node, m.SmartTarget, true)
		}(m, lastNode(c.Chains()))
	}
	wg.Wait()
	time.Sleep(200 * time.Millisecond)
}
