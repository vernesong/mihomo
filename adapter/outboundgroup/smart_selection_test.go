package outboundgroup

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSmartSelectionFlightKeyMatchesTargetCacheIdentity(t *testing.T) {
	const (
		config = "config"
		group  = "smart-group"
		target = "GeoSite [geolocation-!cn]"
	)

	got := smartSelectionFlightKey(config, group, target)
	want := "smart/config/smart-group/GeoSite [geolocation-!cn]"
	if got != want {
		t.Fatalf("smartSelectionFlightKey() = %q, want %q", got, want)
	}
}

func TestSmartDialBatchBoundsLeaderRacesAllCandidatesOnce(t *testing.T) {
	begin, end := smartDialBatchBounds(10, 0, true)
	if begin != 0 || end != 10 {
		t.Fatalf("leader first batch = [%d:%d], want [0:10]", begin, end)
	}
	begin, end = smartDialBatchBounds(10, 1, true)
	if begin != 0 || end != 0 {
		t.Fatalf("leader second batch = [%d:%d], want empty", begin, end)
	}
}

func TestSmartDialBatchBoundsCachedPathKeepsBoundedRetries(t *testing.T) {
	tests := []struct {
		iteration int
		begin     int
		end       int
	}{
		{iteration: 0, begin: 0, end: 1},
		{iteration: 1, begin: 1, end: 6},
		{iteration: 2, begin: 6, end: 10},
	}
	for _, test := range tests {
		begin, end := smartDialBatchBounds(10, test.iteration, false)
		if begin != test.begin || end != test.end {
			t.Fatalf("iteration %d batch = [%d:%d], want [%d:%d]", test.iteration, begin, end, test.begin, test.end)
		}
	}
}

func TestShouldRecordSmartDialFailureIgnoresCanceledRaceLoser(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if shouldRecordSmartDialFailure(ctx, errors.New("transport returned after race cancellation")) {
		t.Fatal("canceled race loser must not be recorded as a node failure")
	}
	if !shouldRecordSmartDialFailure(context.Background(), errors.New("real dial failure")) {
		t.Fatal("live-context dial failure must be recorded")
	}
}

func TestSmartStatusProbeAcceptedTreatsEndpointPolicyAsReachable(t *testing.T) {
	for _, status := range []uint16{403, 405} {
		if !smartStatusProbeAccepted(status, false) {
			t.Fatalf("status %d must prove the proxy path is reachable", status)
		}
	}
	if smartStatusProbeAccepted(503, false) {
		t.Fatal("status 503 must remain an abnormal probe response")
	}
	if !smartStatusProbeAccepted(204, true) {
		t.Fatal("StatusTest success must remain accepted")
	}
}

func TestSmartSelectionCoordinatorPublishesOneWinner(t *testing.T) {
	var coordinator smartSelectionCoordinator
	const callers = 64

	start := make(chan struct{})
	results := make(chan string, callers)
	errs := make(chan error, callers)
	var leaders atomic.Int64
	var wg sync.WaitGroup

	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start

			flight, leader := coordinator.begin("config/group/target")
			if leader {
				leaders.Add(1)
				time.Sleep(10 * time.Millisecond)
				coordinator.finish("config/group/target", flight, "winner", nil)
				results <- "winner"
				return
			}

			winner, err := flight.wait(context.Background())
			if err != nil {
				errs <- err
				return
			}
			results <- winner
		}()
	}

	close(start)
	wg.Wait()
	close(results)
	close(errs)

	if got := leaders.Load(); got != 1 {
		t.Fatalf("leaders = %d, want 1", got)
	}
	for err := range errs {
		t.Fatalf("wait error: %v", err)
	}
	count := 0
	for result := range results {
		count++
		if result != "winner" {
			t.Fatalf("winner = %q, want winner", result)
		}
	}
	if count != callers {
		t.Fatalf("results = %d, want %d", count, callers)
	}
}

func TestSmartSelectionCoordinatorPropagatesFailure(t *testing.T) {
	var coordinator smartSelectionCoordinator
	flight, leader := coordinator.begin("target")
	if !leader {
		t.Fatal("first caller must be leader")
	}

	wantErr := errors.New("selection failed")
	coordinator.finish("target", flight, "", wantErr)
	if _, err := flight.wait(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("wait error = %v, want %v", err, wantErr)
	}
}

func TestSmartSelectionCoordinatorRejectsMissingWinner(t *testing.T) {
	var coordinator smartSelectionCoordinator
	flight, leader := coordinator.begin("target")
	if !leader {
		t.Fatal("first caller must be leader")
	}

	coordinator.finish("target", flight, "", nil)
	if _, err := flight.wait(context.Background()); !errors.Is(err, errSmartSelectionMissingWinner) {
		t.Fatalf("wait error = %v, want %v", err, errSmartSelectionMissingWinner)
	}
}

func TestSmartSelectionCoordinatorWaitHonorsContext(t *testing.T) {
	var coordinator smartSelectionCoordinator
	flight, leader := coordinator.begin("target")
	if !leader {
		t.Fatal("first caller must be leader")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := flight.wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("wait error = %v, want context canceled", err)
	}

	coordinator.finish("target", flight, "winner", nil)
}
