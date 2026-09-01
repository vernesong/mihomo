package outboundgroup

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/metacubex/mihomo/component/smart"
	C "github.com/metacubex/mihomo/constant"
)

var errSmartSelectionMissingWinner = errors.New("smart selection completed without a winner")

type smartProxySelectionState struct {
	key          string
	cacheHit     bool
	cacheExpired bool
	fixed        bool
}

type smartDialSelection struct {
	proxies       []C.Proxy
	asnNumber     string
	persistWinner bool
	flightKey     string
	flight        *smartSelectionFlight
	leader        bool
}

func shouldRecordSmartDialFailure(ctx context.Context, err error) bool {
	return err != nil && !errors.Is(err, context.Canceled) && ctx.Err() == nil
}

func smartStatusProbeAccepted(status uint16, ok bool) bool {
	if ok {
		return true
	}
	// StatusTest probes an arbitrary host root rather than the original request
	// path. A 403/405 response proves the proxy completed TCP, TLS, and HTTP; many
	// CDN roots intentionally return these statuses, so they cannot identify a
	// failed proxy node.
	return status == 403 || status == 405
}

type smartSelectionFlight struct {
	done   chan struct{}
	winner string
	err    error
}

type smartSelectionCoordinator struct {
	mu      sync.Mutex
	flights map[string]*smartSelectionFlight
}

func smartSelectionFlightKey(config, group, target string) string {
	// StoreUnwrapResult persists one winner per config/group/SmartTarget. The
	// in-flight key must use the same identity: ASN can differ between parallel
	// connections before DNS/CDN metadata settles, but those calls still share
	// the target-level winner.
	return smart.FormatDBKey(config, group, target)
}

func smartDialBatchBounds(total, iteration int, leader bool) (int, int) {
	if total <= 0 || iteration < 0 {
		return 0, 0
	}
	if total == 1 {
		if iteration == 0 {
			return 0, 1
		}
		return 0, 0
	}
	if leader {
		// There is exactly one leader per target, so racing the complete bounded
		// candidate set avoids making every follower inherit a slow first-node
		// timeout while still publishing only one successful winner.
		if iteration == 0 {
			return 0, total
		}
		return 0, 0
	}
	if iteration == 0 {
		return 0, 1
	}

	begin := 1 + (iteration-1)*parallelDials
	if begin >= total {
		return 0, 0
	}
	end := begin + parallelDials
	if end > total {
		end = total
	}
	return begin, end
}

func (c *smartSelectionCoordinator) begin(key string) (*smartSelectionFlight, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.flights == nil {
		c.flights = make(map[string]*smartSelectionFlight)
	}
	if flight, found := c.flights[key]; found {
		return flight, false
	}

	flight := &smartSelectionFlight{done: make(chan struct{})}
	c.flights[key] = flight
	return flight, true
}

func (c *smartSelectionCoordinator) finish(key string, flight *smartSelectionFlight, winner string, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	current, found := c.flights[key]
	if !found || current != flight {
		return
	}
	if err == nil && winner == "" {
		err = errSmartSelectionMissingWinner
	}

	flight.winner = winner
	flight.err = err
	delete(c.flights, key)
	close(flight.done)
}

func (f *smartSelectionFlight) wait(ctx context.Context) (string, error) {
	select {
	case <-f.done:
		if f.err != nil {
			return "", f.err
		}
		if f.winner == "" {
			return "", errSmartSelectionMissingWinner
		}
		return f.winner, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func findSmartProxyByName(proxies []C.Proxy, name string) (C.Proxy, error) {
	for _, proxy := range proxies {
		if proxy.Name() == name {
			return proxy, nil
		}
	}
	return nil, fmt.Errorf("smart selection winner unavailable: %s", name)
}
