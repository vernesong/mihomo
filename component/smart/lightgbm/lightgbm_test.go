package lightgbm

import (
	"testing"
	"time"

	"github.com/metacubex/mihomo/component/smart"
	"github.com/vernesong/leaves"
)

// PredictWeight's recover() only wraps PredictSingle and assigns its fallback to a local
// variable, so (a) a panic anywhere else in the model path (NFeatures, transforms) escapes —
// and PredictWeight runs inside recordConnectionStats' goroutine, where an escaping panic
// exits the whole process — and (b) a recovered panic returns (0, false), not the heuristic.
// A zero-value Ensemble is the smallest model that panics.
func TestPredictWeightFallsBackToHeuristicOnModelPanic(t *testing.T) {
	m := &WeightModel{model: &leaves.Ensemble{}}
	in := &smart.ModelInput{Success: 10, ConnectTime: 40, Latency: 60, LastUsed: time.Now().Unix(), IsTCP: true}
	want, _ := smart.CalculateWeight(in, 1)

	var (
		got      float64
		escaped  any
		finished bool
	)
	func() {
		defer func() { escaped = recover() }()
		got, _ = m.PredictWeight(in, 1)
		finished = true
	}()

	if escaped != nil || !finished {
		t.Fatalf("a model panic escaped PredictWeight: %v", escaped)
	}
	if got != want {
		t.Fatalf("after a model panic got weight %v, want the heuristic %v", got, want)
	}
}
