package xhttp

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/http"
)

type testRoundTripper struct {
	id int64
}

func (t *testRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	panic("not used in reuse manager unit tests")
}

func makeTestTransportFactory(counter *atomic.Int64) TransportMaker {
	return func() http.RoundTripper {
		id := counter.Add(1)
		return &testRoundTripper{id: id}
	}
}

func transportID(rt http.RoundTripper) int64 {
	return rt.(*testRoundTripper).id
}

func TestManagerReuseSameEntry(t *testing.T) {
	var created atomic.Int64

	manager, err := NewReuseManager(&ReuseConfig{
		MaxConnections:   "1",
		MaxConcurrency:   "1",
		HMaxRequestTimes: "10",
	}, makeTestTransportFactory(&created))
	if err != nil {
		t.Fatal(err)
	}

	transport1, err := manager.GetTransport()
	if err != nil {
		t.Fatal(err)
	}
	id1 := transportID(transport1.entry.transport)

	transport1.Close()

	transport2, err := manager.GetTransport()
	if err != nil {
		t.Fatal(err)
	}
	id2 := transportID(transport2.entry.transport)

	if id1 != id2 {
		t.Fatalf("expected same transport to be reused, got %d and %d", id1, id2)
	}

	transport2.Close()
	manager.Close()
}

func TestManagerRespectMaxConnections(t *testing.T) {
	var created atomic.Int64

	manager, err := NewReuseManager(&ReuseConfig{
		MaxConnections:   "2",
		MaxConcurrency:   "1",
		HMaxRequestTimes: "100",
	}, makeTestTransportFactory(&created))
	if err != nil {
		t.Fatal(err)
	}

	transport1, err := manager.GetTransport()
	if err != nil {
		t.Fatal(err)
	}
	if transport1 == nil {
		t.Fatal("expected first entry")
	}

	transport2, err := manager.GetTransport()
	if err != nil {
		t.Fatal(err)
	}
	if transport2 == nil {
		t.Fatal("expected second entry")
	}

	if transport1.entry == transport2.entry {
		t.Fatal("expected different entries for first two allocations")
	}

	transport3, err := manager.GetTransport()
	if err == nil {
		t.Fatal("expected error when max-connections reached and all entries are at max-concurrency")
	}
	if transport3 != nil {
		t.Fatal("expected nil entry on allocation failure")
	}

	transport1.Close()
	transport2.Close()
	manager.Close()
}

func TestManagerRotateOnRequestLimit(t *testing.T) {
	var created atomic.Int64

	manager, err := NewReuseManager(&ReuseConfig{
		MaxConnections:   "1",
		MaxConcurrency:   "1",
		HMaxRequestTimes: "1",
	}, makeTestTransportFactory(&created))
	if err != nil {
		t.Fatal(err)
	}

	transport1, err := manager.GetTransport()
	if err != nil {
		t.Fatal(err)
	}
	id1 := transportID(transport1.entry.transport)

	transport1.Close()

	transport2, err := manager.GetTransport()
	if err != nil {
		t.Fatal(err)
	}
	id2 := transportID(transport2.entry.transport)

	if id1 == id2 {
		t.Fatalf("expected new transport after request limit, got same id %d", id1)
	}

	transport2.Close()
	manager.Close()
}

func TestManagerRotateOnReusableSecs(t *testing.T) {
	var created atomic.Int64

	manager, err := NewReuseManager(&ReuseConfig{
		MaxConnections:   "1",
		MaxConcurrency:   "1",
		HMaxRequestTimes: "100",
		HMaxReusableSecs: "1",
	}, makeTestTransportFactory(&created))
	if err != nil {
		t.Fatal(err)
	}

	transport1, err := manager.GetTransport()
	if err != nil {
		t.Fatal(err)
	}
	id1 := transportID(transport1.entry.transport)

	time.Sleep(1100 * time.Millisecond)
	transport1.Close()

	transport2, err := manager.GetTransport()
	if err != nil {
		t.Fatal(err)
	}
	id2 := transportID(transport2.entry.transport)

	if id1 == id2 {
		t.Fatalf("expected new transport after reusable timeout, got same id %d", id1)
	}

	transport2.Close()
	manager.Close()
}

func TestManagerRotateOnConnReuseLimit(t *testing.T) {
	var created atomic.Int64

	manager, err := NewReuseManager(&ReuseConfig{
		MaxConnections:   "1",
		MaxConcurrency:   "1",
		CMaxReuseTimes:   "1",
		HMaxRequestTimes: "100",
	}, makeTestTransportFactory(&created))
	if err != nil {
		t.Fatal(err)
	}

	transport1, err := manager.GetTransport()
	if err != nil {
		t.Fatal(err)
	}
	id1 := transportID(transport1.entry.transport)

	transport1.Close()

	transport2, err := manager.GetTransport()
	if err != nil {
		t.Fatal(err)
	}
	id2 := transportID(transport2.entry.transport)

	if id1 != id2 {
		t.Fatalf("expected first reuse to use same transport, got %d and %d", id1, id2)
	}

	transport2.Close()

	transport3, err := manager.GetTransport()
	if err != nil {
		t.Fatal(err)
	}
	id3 := transportID(transport3.entry.transport)

	if id3 == id2 {
		t.Fatalf("expected new transport after c-max-reuse-times limit, got same id %d", id3)
	}

	transport3.Close()
	manager.Close()
}
