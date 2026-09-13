package outboundgroup

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/metacubex/mihomo/component/smart"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
)

// Response-aware active probing for smart groups (see component/smart/response.go
// for the decision table). Probes run outside the (target, node) stats lock, are
// throttled per (target, node), per target and per group, and only ever look at
// what the site actually answered: connection duration or missing download are
// never used as failure evidence, they only decide whether a probe is worth it.

// connections downloading less than this may be a block page / rate limit answer
const responseProbeDownloadThresholdMB = 0.25

type statusProber interface {
	StatusProbe(ctx context.Context, rawURL string, opt smart.ProbeOptions) (*smart.ProbeResult, error)
}

type probeURLSuffix struct {
	suffix string // ".example.com"
	url    string
}

// responseProbeURLs maps a host (exact or "*." wildcard for subdomains) to the URL probed for it.
type responseProbeURLs struct {
	exact    map[string]string
	wildcard []probeURLSuffix // longest suffix first
}

func parseResponseProbeURLs(raw map[string]string) (*responseProbeURLs, error) {
	r := &responseProbeURLs{exact: make(map[string]string)}
	for host, rawURL := range raw {
		key := strings.ToLower(strings.TrimSpace(host))
		wildcard := strings.HasPrefix(key, "*.")
		name := strings.TrimPrefix(key, "*.")
		if name == "" || strings.ContainsAny(name, "*/:[] ") {
			return nil, fmt.Errorf("invalid host [%s]", host)
		}
		u, err := url.Parse(strings.TrimSpace(rawURL))
		if err != nil {
			return nil, fmt.Errorf("invalid url [%s] for host [%s]: %w", rawURL, host, err)
		}
		if (u.Scheme != "https" && u.Scheme != "http") || u.Hostname() == "" {
			return nil, fmt.Errorf("invalid url [%s] for host [%s]: must be an absolute http(s) url", rawURL, host)
		}
		if wildcard {
			r.wildcard = append(r.wildcard, probeURLSuffix{suffix: "." + name, url: u.String()})
		} else {
			r.exact[name] = u.String()
		}
	}
	sort.Slice(r.wildcard, func(i, j int) bool {
		if len(r.wildcard[i].suffix) != len(r.wildcard[j].suffix) {
			return len(r.wildcard[i].suffix) > len(r.wildcard[j].suffix)
		}
		return r.wildcard[i].suffix < r.wildcard[j].suffix
	})
	return r, nil
}

func (r *responseProbeURLs) match(host string) (string, bool) {
	if r == nil || host == "" {
		return "", false
	}
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	if u, ok := r.exact[host]; ok {
		return u, true
	}
	for _, w := range r.wildcard {
		if strings.HasSuffix(host, w.suffix) {
			return w.url, true
		}
	}
	return "", false
}

type probeNodeState struct {
	lastProbe   time.Time
	lastFailure time.Time
}

type probeWindow struct {
	start time.Time
	count int
}

// probeThrottle bounds probe volume. Reserving the (target, node) slot before a probe
// starts also collapses concurrent probes for the same pair into one.
type probeThrottle struct {
	mu         sync.Mutex
	nodes      map[string]*probeNodeState
	targets    map[string]*probeWindow
	suppress   map[string]time.Time
	tokens     float64
	lastRefill time.Time
}

const probeThrottleMaxEntries = 4096

func probeKey(a, b string) string {
	return a + "\x00" + b
}

func (t *probeThrottle) initLocked(now time.Time) {
	if t.nodes == nil {
		t.nodes = make(map[string]*probeNodeState)
		t.targets = make(map[string]*probeWindow)
		t.suppress = make(map[string]time.Time)
		t.tokens = smart.ProbeGroupBurst
		t.lastRefill = now
	}
	if len(t.nodes) > probeThrottleMaxEntries {
		for k, st := range t.nodes {
			if now.Sub(st.lastProbe) > smart.ProbeRecentFailureWindow && now.Sub(st.lastFailure) > smart.ProbeRecentFailureWindow {
				delete(t.nodes, k)
			}
		}
	}
	if len(t.targets) > probeThrottleMaxEntries {
		for k, w := range t.targets {
			if now.Sub(w.start) >= time.Minute {
				delete(t.targets, k)
			}
		}
	}
	if len(t.suppress) > probeThrottleMaxEntries {
		for k, until := range t.suppress {
			if !now.Before(until) {
				delete(t.suppress, k)
			}
		}
	}
}

// budgetLocked checks and optionally consumes the per-target and per-group budgets.
func (t *probeThrottle) budgetLocked(target string, now time.Time, consume bool) bool {
	elapsed := now.Sub(t.lastRefill)
	if elapsed > 0 {
		t.tokens += elapsed.Minutes() * smart.ProbeGroupPerMinute
		if t.tokens > smart.ProbeGroupBurst {
			t.tokens = smart.ProbeGroupBurst
		}
		t.lastRefill = now
	}
	if t.tokens < 1 {
		return false
	}
	w := t.targets[target]
	if w == nil || now.Sub(w.start) >= time.Minute {
		w = &probeWindow{start: now}
		t.targets[target] = w
	}
	if w.count >= smart.ProbePerTargetPerMinute {
		return false
	}
	if consume {
		w.count++
		t.tokens--
	}
	return true
}

// allowNode reserves a probe of node for target if every limit allows it.
func (t *probeThrottle) allowNode(target, node string, now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.initLocked(now)

	key := probeKey(target, node)
	st := t.nodes[key]
	if st != nil {
		interval := smart.ProbeMinIntervalPerNode
		if !st.lastFailure.IsZero() && now.Sub(st.lastFailure) < smart.ProbeRecentFailureWindow {
			interval = smart.ProbeMinIntervalAfterFailure
		}
		if now.Sub(st.lastProbe) < interval {
			return false
		}
	}
	if !t.budgetLocked(target, now, true) {
		return false
	}
	if st == nil {
		st = &probeNodeState{}
		t.nodes[key] = st
	}
	st.lastProbe = now
	return true
}

// allowControl reserves a differential confirmation probe for target.
func (t *probeThrottle) allowControl(target string, now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.initLocked(now)
	return t.budgetLocked(target, now, true)
}

func (t *probeThrottle) noteFailure(target, node string, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.initLocked(now)
	key := probeKey(target, node)
	st := t.nodes[key]
	if st == nil {
		st = &probeNodeState{lastProbe: now}
		t.nodes[key] = st
	}
	st.lastFailure = now
}

func (t *probeThrottle) suppressed(target, class string, now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.initLocked(now)
	until, ok := t.suppress[probeKey(target, class)]
	return ok && now.Before(until)
}

func (t *probeThrottle) suppressClass(target, class string, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.initLocked(now)
	t.suppress[probeKey(target, class)] = now.Add(smart.ProbeSiteLevelSuppress)
}

// responseProbeEligible decides whether a closed connection is worth a probe.
func (s *Smart) responseProbeEligible(metadata *C.Metadata, downloadTotalMB float64, isUDP bool) bool {
	if isUDP || metadata.Host == "" || metadata.Type == C.INNER || downloadTotalMB >= responseProbeDownloadThresholdMB {
		return false
	}
	if metadata.DstPort == 443 {
		return true
	}
	// without looking at the payload a non-443 port is only known to be http(s) when configured
	_, ok := s.probeURLs.match(metadata.Host)
	return ok
}

func (s *Smart) responseProbeURL(host string, port uint16) string {
	if u, ok := s.probeURLs.match(host); ok {
		return u
	}
	hostPort := host
	if port != 0 && port != 443 {
		hostPort = net.JoinHostPort(host, strconv.FormatUint(uint64(port), 10))
	}
	return "https://" + hostPort + "/?z=" + strconv.FormatInt(rand.Int63(), 10)
}

// scheduleResponseProbe starts an asynchronous probe if the throttle allows it.
func (s *Smart) scheduleResponseProbe(metadata *C.Metadata, proxy C.Proxy, asnNumber string) {
	if !s.probeThrottle.allowNode(metadata.WildcardTarget, proxy.Name(), time.Now()) {
		return
	}
	probeURL := s.responseProbeURL(metadata.Host, metadata.DstPort)
	go s.runResponseProbe(metadata, proxy, probeURL, asnNumber)
}

func (s *Smart) probeVerdict(proxy C.Proxy, probeURL string) smart.ResponseVerdict {
	prober, ok := proxy.(statusProber)
	if !ok {
		return smart.ResponseVerdict{Kind: smart.VerdictIgnore, Class: "unsupported"}
	}
	parent := s.ctx
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, C.DefaultTCPTimeout)
	defer cancel()
	result, err := prober.StatusProbe(ctx, probeURL, smart.ProbeOptions{MaxBodyBytes: smart.ProbeMaxBodyBytes})
	if err != nil {
		return smart.ClassifyProbeError(err)
	}
	return smart.ClassifyResponse(result.StatusCode, result.Header, result.Body, time.Now())
}

// pickControlNode returns another node without failure records for the target.
func (s *Smart) pickControlNode(metadata *C.Metadata, proxyName string) C.Proxy {
	wtFailNodes, _, _, _ := s.store.GetHostStatus(s.Name(), s.configName, metadata.WildcardTarget, int(s.hostFailLimit.Load()), metadata.SmartTarget)
	blockedNodes := s.store.GetBlockedNodes(s.Name(), s.configName)
	var candidates []C.Proxy
	for _, p := range s.GetProxies(false) {
		name := p.Name()
		if name == proxyName || wtFailNodes[name] != 0 || blockedNodes[name] || !p.AliveForTestUrl(s.testUrl) {
			continue
		}
		candidates = append(candidates, p)
	}
	if len(candidates) == 0 {
		return nil
	}
	return candidates[rand.Intn(len(candidates))]
}

// confirmSuspect runs the differential check: a suspect answer only blocks the node when
// another node gets a normal answer for the same URL. The same answer on the control node
// means the site treats everyone that way: nothing is recorded and the class is suppressed.
// Without a usable control result the evidence is only counted.
func (s *Smart) confirmSuspect(metadata *C.Metadata, proxyName, probeURL string, v smart.ResponseVerdict) smart.ResponseVerdict {
	target := metadata.WildcardTarget
	now := time.Now()
	if s.probeThrottle.suppressed(target, v.Class, now) {
		return smart.ResponseVerdict{Kind: smart.VerdictIgnore, Class: v.Class, Reason: v.Reason + " (site-level, suppressed)"}
	}
	control := s.pickControlNode(metadata, proxyName)
	if control == nil || !s.probeThrottle.allowControl(target, now) {
		return smart.ResponseVerdict{Kind: smart.VerdictCount, Class: v.Class, Reason: v.Reason + " (no control node)"}
	}
	cv := s.probeVerdict(control, probeURL)
	switch cv.Kind {
	case smart.VerdictOK:
		return smart.ResponseVerdict{Kind: smart.VerdictBlocked, Class: v.Class,
			Reason: fmt.Sprintf("%s (confirmed, control node [%s] %s)", v.Reason, control.Name(), cv.Reason)}
	case smart.VerdictSuspect, smart.VerdictRateLimited, smart.VerdictBlocked:
		s.probeThrottle.suppressClass(target, v.Class, now)
		return smart.ResponseVerdict{Kind: smart.VerdictIgnore, Class: v.Class,
			Reason: fmt.Sprintf("%s (site-level, control node [%s] %s)", v.Reason, control.Name(), cv.Reason)}
	default:
		return smart.ResponseVerdict{Kind: smart.VerdictCount, Class: v.Class,
			Reason: fmt.Sprintf("%s (control node [%s] inconclusive: %s)", v.Reason, control.Name(), cv.Reason)}
	}
}

func (s *Smart) runResponseProbe(metadata *C.Metadata, proxy C.Proxy, probeURL string, asnNumber string) smart.ResponseVerdict {
	v := s.probeVerdict(proxy, probeURL)
	if v.Kind == smart.VerdictSuspect {
		v = s.confirmSuspect(metadata, proxy.Name(), probeURL, v)
	}
	s.applyProbeVerdict(metadata, proxy.Name(), probeURL, asnNumber, v)
	return v
}

func (s *Smart) applyProbeVerdict(metadata *C.Metadata, proxyName, probeURL, asnNumber string, v smart.ResponseVerdict) {
	// results arrive after the connection closed: re-check pin and stop-loss
	if s.selected != "" {
		return
	}
	_, _, _, wtBlocked := s.store.GetHostStatus(s.Name(), s.configName, metadata.WildcardTarget, int(s.hostFailLimit.Load()), metadata.SmartTarget)
	if wtBlocked {
		return
	}

	var isDegraded bool
	var code int64
	var ttl time.Duration
	switch v.Kind {
	case smart.VerdictOK:
		code = 0
	case smart.VerdictRateLimited:
		isDegraded, code, ttl = true, smart.HostCodeRateLimited, v.Cooldown
	case smart.VerdictBlocked:
		isDegraded, code = true, 2
	case smart.VerdictCount:
		code = 3
	default:
		if v.Reason != "" {
			log.Debugln("[Smart] Probe Group: [%s] - Node: [%s] - URL: [%s] ignored response [%s]", s.Name(), proxyName, probeURL, v.Reason)
		}
		return
	}

	if v.Kind != smart.VerdictOK {
		s.probeThrottle.noteFailure(metadata.WildcardTarget, proxyName, time.Now())
		log.Debugln("[Smart] Probe Group: [%s] - Node: [%s] - Host: [%s] - URL: [%s] detected %s response [%s] cooldown [%s]...",
			s.Name(), proxyName, metadata.Host, probeURL, v.Kind, v.Reason, ttl)
	}

	failedBlock := s.markNodeFailureTTL(metadata, proxyName, isDegraded, true, code, ttl)
	if isDegraded || failedBlock {
		s.closeSameConnection(metadata, proxyName, metadata.SmartTarget, asnNumber, true)
		s.store.DeleteUnwrapResult(s.Name(), s.configName, metadata.SmartTarget, asnNumber, metadata.WildcardTarget)
	}
}
