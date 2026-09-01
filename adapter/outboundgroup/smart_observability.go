package outboundgroup

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/metacubex/mihomo/log"
)

const smartTimingUnobserved = "unobserved"

var smartTimingSequence atomic.Uint64

type smartTimingEvent struct {
	phase                    string
	streamReadyElapsed       time.Duration
	firstWriteToRead         time.Duration
	firstWriteToReadObserved bool
	err                      error
}

type smartConnectionTiming struct {
	traceID       string
	group         string
	node          string
	target        string
	wildcard      string
	proxyConnect  time.Duration
	streamReadyAt time.Time
	firstWriteAt  atomic.Int64
}

func newSmartConnectionTiming(group, node, target, wildcard string, proxyConnect time.Duration, streamReadyAt time.Time) *smartConnectionTiming {
	return &smartConnectionTiming{
		traceID:       fmt.Sprintf("%x-%x", streamReadyAt.UnixNano(), smartTimingSequence.Add(1)),
		group:         group,
		node:          node,
		target:        target,
		wildcard:      wildcard,
		proxyConnect:  proxyConnect,
		streamReadyAt: streamReadyAt,
	}
}

func (t *smartConnectionTiming) firstWriteEvent(now time.Time, err error) smartTimingEvent {
	t.firstWriteAt.CompareAndSwap(0, now.UnixNano())
	return smartTimingEvent{
		phase:              "first-write",
		streamReadyElapsed: now.Sub(t.streamReadyAt),
		err:                err,
	}
}

func (t *smartConnectionTiming) firstReadEvent(now time.Time, err error) smartTimingEvent {
	event := smartTimingEvent{
		phase:              "first-read",
		streamReadyElapsed: now.Sub(t.streamReadyAt),
		err:                err,
	}
	if firstWriteAt := t.firstWriteAt.Load(); firstWriteAt != 0 {
		event.firstWriteToRead = now.Sub(time.Unix(0, firstWriteAt))
		event.firstWriteToReadObserved = true
	}
	return event
}

func (t *smartConnectionTiming) logEvent(event smartTimingEvent) {
	firstWriteToRead := smartTimingUnobserved
	if event.firstWriteToReadObserved {
		firstWriteToRead = formatTimeUnit(float64(event.firstWriteToRead.Milliseconds()))
	}
	log.Debugln("[SmartTiming] phase=[%s] trace=[%s] group=[%s] node=[%s] target=[%s] wildcard=[%s] proxy-connect=[%s] stream-ready-elapsed=[%s] first-write-to-read=[%s] error=[%s]",
		event.phase,
		t.traceID,
		t.group,
		t.node,
		t.target,
		t.wildcard,
		formatTimeUnit(float64(t.proxyConnect.Milliseconds())),
		formatTimeUnit(float64(event.streamReadyElapsed.Milliseconds())),
		firstWriteToRead,
		formatSmartTimingError(event.err),
	)
}

func (t *smartConnectionTiming) logClose(now time.Time, connectionID string, connectionDuration int64, uploadTotal, downloadTotal int64) {
	log.Debugln("[SmartTiming] phase=[close] trace=[%s] id=[%s] group=[%s] node=[%s] target=[%s] wildcard=[%s] proxy-connect=[%s] stream-lifetime=[%s] tracker-duration=[%s] up=[%s] down=[%s]",
		t.traceID,
		connectionID,
		t.group,
		t.node,
		t.target,
		t.wildcard,
		formatTimeUnit(float64(t.proxyConnect.Milliseconds())),
		formatTimeUnit(float64(now.Sub(t.streamReadyAt).Milliseconds())),
		formatTimeUnit(float64(connectionDuration)),
		formatTrafficUnit(float64(uploadTotal), false),
		formatTrafficUnit(float64(downloadTotal), false),
	)
}

func formatSmartTimingError(err error) string {
	if err == nil {
		return "none"
	}
	return fmt.Sprintf("%v", err)
}
