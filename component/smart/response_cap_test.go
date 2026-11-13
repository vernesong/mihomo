package smart

import (
	"strconv"
	"testing"
	"time"
)

// ProbeThrottle only trims entries older than probeRecentFailureWindow (10 min), at most
// once a minute, so its maps are bounded by the number of unique targets seen in 10 minutes,
// not by a count. A burst of unique IP targets (P2P/DHT, scanners) grows it without limit
// (about 43 MB per 200k unique targets in a heap profile).
func TestProbeThrottleHasAHardCap(t *testing.T) {
	var th ProbeThrottle
	now := time.Now()
	for i := 0; i < 50000; i++ {
		target := "203.0." + strconv.Itoa(i/250) + "." + strconv.Itoa(i%250)
		now = now.Add(time.Millisecond) // 50k targets within one minute
		if th.AllowNode(target, "a", now) {
			th.MarkPending(target, "a", now)
		}
	}
	th.mu.Lock()
	nodes, targets := len(th.nodes), len(th.targets)
	th.mu.Unlock()
	limit := 2 * probeMaxEntries
	if nodes > limit || targets > limit {
		t.Fatal("after 50k unique targets in 50 s: nodes=" + strconv.Itoa(nodes) + " targets=" + strconv.Itoa(targets) + " (cap " + strconv.Itoa(probeMaxEntries) + ")")
	}
}
