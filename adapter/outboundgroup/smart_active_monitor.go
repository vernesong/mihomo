package outboundgroup

import (
	"fmt"
	"sync"
	"time"

	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
	"github.com/metacubex/mihomo/tunnel"
	"github.com/metacubex/mihomo/tunnel/statistic"
)

const (
	smartMonitorInterval           = time.Second
	smartMonitorEvidenceWindow     = 2 * time.Minute
	smartMonitorCooldown           = 5 * time.Minute
	smartMonitorNoResponseAfter    = 5 * time.Second
	smartMonitorSlowResponseAfter  = 2500 * time.Millisecond
	smartMonitorRequestSeparation  = 500 * time.Millisecond
	smartMonitorMinUploadBytes     = 128
	smartMonitorMinDownloadBytes   = 16 * 1024
	smartMonitorSlowBytesPerSecond = 16 * 1024
)

type smartActiveMonitor struct {
	mu          sync.Mutex
	connections map[*smartMonitoredConn]struct{}
	evidence    map[string]smartDegradationEvidence
}

type smartDegradationEvidence struct {
	connection     *smartMonitoredConn
	observation    uint64
	reason         string
	observedAt     time.Time
	confirmedUntil time.Time
}

type smartMonitorSignal struct {
	reason        string
	observation   uint64
	duration      time.Duration
	uploadBytes   int64
	downloadBytes int64
}

type smartMonitoredConn struct {
	C.Conn
	smart          *Smart
	proxyName      string
	target         string
	wildcardTarget string
	metadata       *C.Metadata
	tracker        statistic.Tracker

	mu               sync.Mutex
	closed           bool
	observing        bool
	signaled         bool
	observation      uint64
	startedAt        time.Time
	lastReadAt       time.Time
	uploadBaseline   int64
	downloadBaseline int64
	uploadTotal      int64
	downloadTotal    int64
}

func (s *Smart) startActiveConnectionMonitor() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		waitTicker := time.NewTicker(100 * time.Millisecond)
		defer waitTicker.Stop()
		for tunnel.Status() != tunnel.Running {
			select {
			case <-waitTicker.C:
			case <-s.ctx.Done():
				return
			}
		}

		ticker := time.NewTicker(smartMonitorInterval)
		defer ticker.Stop()
		for {
			select {
			case now := <-ticker.C:
				s.sampleActiveConnections(now)
			case <-s.ctx.Done():
				return
			}
		}
	}()
}

func (s *Smart) monitorActiveConnection(conn C.Conn, proxy C.Proxy, metadata *C.Metadata) C.Conn {
	if metadata == nil || metadata.NetWork != C.TCP || metadata.DstPort != 443 || metadata.Type == C.INNER || metadata.WildcardTarget == "" {
		return conn
	}

	monitored := &smartMonitoredConn{
		Conn:           conn,
		smart:          s,
		proxyName:      proxy.Name(),
		target:         metadata.SmartTarget,
		wildcardTarget: metadata.WildcardTarget,
		metadata:       metadata,
	}
	s.activeMonitor.mu.Lock()
	if s.activeMonitor.connections == nil {
		s.activeMonitor.connections = make(map[*smartMonitoredConn]struct{})
	}
	s.activeMonitor.connections[monitored] = struct{}{}
	s.activeMonitor.mu.Unlock()
	return monitored
}

func (c *smartMonitoredConn) Close() error {
	c.mu.Lock()
	remove := !c.closed
	if remove {
		c.closed = true
	}
	c.mu.Unlock()
	if remove {
		c.smart.removeActiveConnection(c)
	}
	return c.Conn.Close()
}

func (c *smartMonitoredConn) ReaderReplaceable() bool {
	return true
}

func (c *smartMonitoredConn) WriterReplaceable() bool {
	return true
}

func (c *smartMonitoredConn) Upstream() any {
	return c.Conn
}

func (c *smartMonitoredConn) observeTotals(uploadTotal, downloadTotal int64, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	previousDownloadTotal := c.downloadTotal
	uploadDelta := uploadTotal - c.uploadTotal
	downloadDelta := downloadTotal - c.downloadTotal
	c.uploadTotal = uploadTotal
	c.downloadTotal = downloadTotal
	if downloadDelta > 0 {
		c.lastReadAt = now
	}
	if uploadDelta <= 0 {
		return
	}
	if !c.observing || c.signaled || (!c.lastReadAt.IsZero() && now.Sub(c.lastReadAt) >= smartMonitorRequestSeparation && downloadTotal > c.downloadBaseline) {
		c.observation++
		c.observing = true
		c.signaled = false
		c.startedAt = now
		c.uploadBaseline = uploadTotal - uploadDelta
		c.downloadBaseline = previousDownloadTotal
	}
}

func (c *smartMonitoredConn) sample(now time.Time) (smartMonitorSignal, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || !c.observing || c.signaled {
		return smartMonitorSignal{}, false
	}

	duration := now.Sub(c.startedAt)
	upload := c.uploadTotal - c.uploadBaseline
	download := c.downloadTotal - c.downloadBaseline
	if upload < smartMonitorMinUploadBytes {
		return smartMonitorSignal{}, false
	}

	reason := ""
	if download == 0 && duration >= smartMonitorNoResponseAfter {
		reason = "no-response-after-write"
	} else if download >= smartMonitorMinDownloadBytes && duration >= smartMonitorSlowResponseAfter {
		rate := float64(download) / duration.Seconds()
		if rate < smartMonitorSlowBytesPerSecond {
			reason = "sustained-low-throughput"
		} else {
			c.observing = false
			return smartMonitorSignal{}, false
		}
	}
	if reason == "" {
		return smartMonitorSignal{}, false
	}

	c.signaled = true
	return smartMonitorSignal{
		reason:        reason,
		observation:   c.observation,
		duration:      duration,
		uploadBytes:   upload,
		downloadBytes: download,
	}, true
}

func (s *Smart) removeActiveConnection(conn *smartMonitoredConn) {
	s.activeMonitor.mu.Lock()
	delete(s.activeMonitor.connections, conn)
	s.activeMonitor.mu.Unlock()
}

func (s *Smart) sampleActiveConnections(now time.Time) {
	s.activeMonitor.mu.Lock()
	connections := make([]*smartMonitoredConn, 0, len(s.activeMonitor.connections))
	for conn := range s.activeMonitor.connections {
		connections = append(connections, conn)
	}
	s.activeMonitor.mu.Unlock()

	for _, conn := range connections {
		tracker := s.findActiveTracker(conn)
		if tracker == nil {
			continue
		}
		info := tracker.Info()
		conn.observeTotals(info.UploadTotal.Load(), info.DownloadTotal.Load(), now)
		if signal, ok := conn.sample(now); ok {
			s.handleActiveDegradation(conn, signal, now)
		}
	}
}

func (s *Smart) handleActiveDegradation(conn *smartMonitoredConn, signal smartMonitorSignal, now time.Time) {
	if signal.reason != "sustained-low-throughput" {
		log.Warnln("[SmartMonitor] Observed unanswered active connection without automatic action: group=[%s] node=[%s] wildcard=[%s] duration=[%s] upload=[%d] observation=[%d]",
			s.Name(), conn.proxyName, conn.wildcardTarget, signal.duration, signal.uploadBytes, signal.observation)
		return
	}
	key := conn.proxyName + "\x00" + conn.wildcardTarget
	confirmed := s.recordActiveDegradationEvidence(key, conn, signal.observation, signal.reason, now)
	if !confirmed {
		log.Warnln("[SmartMonitor] Suspect active connection: group=[%s] node=[%s] wildcard=[%s] reason=[%s] duration=[%s] upload=[%d] download=[%d] observation=[%d]",
			s.Name(), conn.proxyName, conn.wildcardTarget, signal.reason, signal.duration, signal.uploadBytes, signal.downloadBytes, signal.observation)
		return
	}

	log.Warnln("[SmartMonitor] Confirmed active degradation: group=[%s] node=[%s] wildcard=[%s] reason=[%s] duration=[%s] upload=[%d] download=[%d] observation=[%d]",
		s.Name(), conn.proxyName, conn.wildcardTarget, signal.reason, signal.duration, signal.uploadBytes, signal.downloadBytes, signal.observation)

	s.store.DeleteUnwrapResult(s.Name(), s.configName, conn.target, "", conn.wildcardTarget)

	tracker := s.findActiveTracker(conn)
	if tracker == nil {
		log.Warnln("[SmartMonitor] Unable to close confirmed degraded connection: group=[%s] node=[%s] wildcard=[%s] error=[tracker-not-found]",
			s.Name(), conn.proxyName, conn.wildcardTarget)
		return
	}
	tracker.Info().Metadata.SmartBlock = "degraded"
	if err := tracker.Close(); err != nil {
		log.Warnln("[SmartMonitor] Failed to close confirmed degraded connection: id=[%s] error=[%v]", tracker.ID(), err)
	} else {
		log.Warnln("[SmartMonitor] Closed confirmed degraded connection: id=[%s] group=[%s] node=[%s] wildcard=[%s]",
			tracker.ID(), s.Name(), conn.proxyName, conn.wildcardTarget)
	}
}

func (s *Smart) recordActiveDegradationEvidence(key string, conn *smartMonitoredConn, observation uint64, reason string, now time.Time) bool {
	s.activeMonitor.mu.Lock()
	defer s.activeMonitor.mu.Unlock()
	if s.activeMonitor.evidence == nil {
		s.activeMonitor.evidence = make(map[string]smartDegradationEvidence)
	}
	previous, found := s.activeMonitor.evidence[key]
	if found && now.Before(previous.confirmedUntil) {
		return false
	}
	withinWindow := found && now.Sub(previous.observedAt) <= smartMonitorEvidenceWindow
	if withinWindow {
		if previous.connection == conn || !previous.connection.isClosed() || previous.reason != "sustained-low-throughput" || reason != "sustained-low-throughput" {
			return false
		}
		s.activeMonitor.evidence[key] = smartDegradationEvidence{confirmedUntil: now.Add(smartMonitorCooldown)}
		return true
	}
	s.activeMonitor.evidence[key] = smartDegradationEvidence{
		connection:  conn,
		observation: observation,
		reason:      reason,
		observedAt:  now,
	}
	return false
}

func (c *smartMonitoredConn) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func (s *Smart) activeNodeCoolingDown(proxyName, wildcardTarget string, now time.Time) bool {
	key := proxyName + "\x00" + wildcardTarget
	s.activeMonitor.mu.Lock()
	defer s.activeMonitor.mu.Unlock()
	evidence, found := s.activeMonitor.evidence[key]
	if !found || evidence.confirmedUntil.IsZero() {
		return false
	}
	if now.Before(evidence.confirmedUntil) {
		return true
	}
	delete(s.activeMonitor.evidence, key)
	return false
}

func (s *Smart) findActiveTracker(conn *smartMonitoredConn) statistic.Tracker {
	conn.mu.Lock()
	if conn.tracker != nil {
		tracker := conn.tracker
		conn.mu.Unlock()
		return tracker
	}
	conn.mu.Unlock()

	var matched statistic.Tracker
	statistic.DefaultManager.RangeSmartTarget(conn.target, func(id string) bool {
		tracker := statistic.DefaultManager.Get(id)
		if tracker != nil && tracker.Info().Metadata == conn.metadata {
			matched = tracker
			return false
		}
		return true
	})
	if matched != nil {
		conn.mu.Lock()
		conn.tracker = matched
		conn.mu.Unlock()
	}
	return matched
}

func (s smartMonitorSignal) String() string {
	return fmt.Sprintf("%s duration=%s upload=%d download=%d observation=%d", s.reason, s.duration, s.uploadBytes, s.downloadBytes, s.observation)
}
