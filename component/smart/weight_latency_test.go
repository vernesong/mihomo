package smart

import (
	"math"
	"testing"
	"time"
)

// baseline is a healthy, recently used short web connection.
func baseline() ModelInput {
	return ModelInput{
		Success:            20,
		Failure:            0,
		ConnectTime:        80,
		Latency:            120,
		UploadTotal:        0.05,
		DownloadTotal:      0.8,
		MaxuploadRate:      40,
		MaxdownloadRate:    600,
		ConnectionDuration: 0.5,
		LastUsed:           time.Now().Unix(),
		IsTCP:              true,
	}
}

func weightOf(t *testing.T, in ModelInput, priority float64) float64 {
	t.Helper()
	w, _ := CalculateWeight(&in, priority)
	if math.IsNaN(w) || math.IsInf(w, 0) {
		t.Fatalf("weight is %v for input %+v", w, in)
	}
	return w
}

// W1: a connection that never received a byte has no first-read latency (0) and
// is scored as if it had 1 ms, i.e. better than any measured node. (A connect time of 0 is
// different: it is a real sub-millisecond dial on multiplexed transports and stays fast.)
func TestCalculateWeightMissingLatencyIsNotBest(t *testing.T) {
	missing := baseline()
	missing.Latency = 0
	wx := weightOf(t, missing, 1)
	// 20 ms also earns the low-latency bonus; 200 ms does not, so it checks the score itself
	for _, ms := range []int64{20, 200} {
		measured := baseline()
		measured.Latency = ms
		if wm := weightOf(t, measured, 1); wx >= wm {
			t.Fatalf("node with no measured latency weighs %.4f >= a measured %d ms node (%.4f)", wx, ms, wm)
		}
	}
	// but an unanswered connection is not scored like a failed one either
	failed := baseline()
	failed.Latency, failed.ConnectionFailed = 0, true
	if wf := weightOf(t, failed, 1); wf >= wx {
		t.Fatalf("unknown latency (%.4f) should score above a failed connection (%.4f)", wx, wf)
	}
}

// W2: the latency score is clamped at 0.8, so every latency below ~335 ms
// scores the same and a 30 ms node is indistinguishable from a 300 ms one (apart from a flat
// +0.1 bonus under 100 ms, which cannot separate 120 ms from 330 ms).
func TestCalculateWeightDistinguishesLatencyBelow335ms(t *testing.T) {
	// interactive UDP range: each step must cost a visible amount of weight (>= 1%)
	steps := []int64{30, 120, 330}
	prev := -1.0
	for _, ms := range steps {
		in := baseline()
		in.IsUDP, in.IsTCP, in.Latency = true, false, ms
		w := weightOf(t, in, 1)
		if prev > 0 && (prev-w)/prev < 0.01 {
			t.Fatalf("UDP %d ms weighs %.4f, not meaningfully below the previous step (%.4f)", ms, w, prev)
		}
		prev = w
	}
}
