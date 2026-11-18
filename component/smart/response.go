package smart

import (
	"bytes"
	"context"
	"crypto/x509"
	"errors"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"

	"github.com/metacubex/http"
)

const (
	responseProbeDownloadThresholdMB = 0.03

	ProbeTimeout = 8 * time.Second

	probePerTargetPerMinute = 6

	probeGlobalPerMinute = 120
	probeGlobalBurst     = 10
	probeMaxInflight     = 4

	probeMaxEntries = 4096
	probePath       = "/robots.txt"

	ProbeBodyLimit = 4096

	// a refusal page is usually sent at once, waiting longer is not worth the probe
	ProbeBodyWait = time.Second

	probeMaxBlockTTL = 30 * time.Minute

	ReasonForbidden     = "forbidden"
	ReasonOriginError   = "origin error"
	ReasonEdgeError     = "edge error"
	ReasonCertAuthority = "unknown certificate authority"
	ReasonCertHostname  = "certificate hostname mismatch"
)

// A host that several independent nodes refuse, and that no node has answered since its
// last success, is held until the ttl; one probation refusal then arms the next hold.
const (
	hostBackoffMinNodes   = 3
	hostBackoffMaxEntries = 4096
)

// The intervals are variables so a test can shrink them instead of waiting them out.
var (
	probeDefaultBlockTTL   = 10 * time.Minute
	probeTransientBlockTTL = 5 * time.Minute
	probeRegionalBlockTTL  = 30 * time.Minute
	probeMinBlockTTL       = 60 * time.Second

	probeMinIntervalPerNode      = 120 * time.Second
	probeMinIntervalAfterFailure = 30 * time.Second
	probeRecentFailureWindow     = 10 * time.Minute
	probeTrimInterval            = time.Minute

	hostBackoffWindow = time.Hour
	hostBackoffTTL    = 30 * time.Minute
)

var (
	globalProbeThrottle struct {
		mu         sync.Mutex
		tokens     float64
		lastRefill time.Time
	}

	probeInflight = make(chan struct{}, probeMaxInflight)

	hostBackoffTable hostBackoff
)

type VerdictAction uint8

const (
	VerdictIgnore VerdictAction = iota
	VerdictReachable
	VerdictRecord
)

type (
	Verdict struct {
		Action         VerdictAction
		TTL            time.Duration
		Reason         string
		ControlSuccess bool
		Unproven       bool
	}

	ProbeResult struct {
		StatusCode int
		Header     http.Header
		Body       []byte
	}

	StatusProber interface {
		StatusProbe(ctx context.Context, rawURL string) (*ProbeResult, error)
	}

	probeNodeState struct {
		lastProbe     time.Time
		lastFailure   time.Time
		pendingSince  time.Time
		unprovenSince time.Time
	}

	probeNodeKey struct {
		target string
		node   string
	}

	probeWindow struct {
		start time.Time
		count int
	}

	ProbeThrottle struct {
		mu       sync.Mutex
		nodes    map[probeNodeKey]*probeNodeState
		targets  map[string]*probeWindow
		lastTrim time.Time
	}

	hostBackoffKey struct {
		config string
		group  string
		host   string
	}

	hostBackoffState struct {
		nodes     map[string]int64
		updated   int64
		until     int64
		probation bool
	}

	hostBackoff struct {
		mu      sync.RWMutex
		entries map[hostBackoffKey]hostBackoffState
	}
)

// ClassifyResponse turns the answer of a node into a verdict: only what identifies the node
// itself may record a block, a redirect that leaves the host must neither record nor clear.
func ClassifyResponse(status int, header http.Header, body []byte, now time.Time) Verdict {
	switch {
	case isChallenge(header):
		return record(probeDefaultBlockTTL, "challenge")
	case status == http.StatusTooManyRequests:
		return record(rateLimitTTL(header, now), "rate limited")
	case status == http.StatusForbidden && isRateLimited(header, now):
		return record(rateLimitTTL(header, now), "rate limited")
	case status == http.StatusOK && strings.TrimSpace(header.Get("X-Ratelimit-Remaining")) == "0":
		return record(rateLimitTTL(header, now), "quota exhausted")
	case IsRegionUnavailableText(body):
		return record(probeRegionalBlockTTL, ReasonRegionUnavailable)
	case status == http.StatusForbidden:
		return record(probeDefaultBlockTTL, ReasonForbidden)
	case status == http.StatusUnavailableForLegalReasons:
		return record(probeRegionalBlockTTL, "unavailable for legal reasons")
	case status == http.StatusMisdirectedRequest:
		return ignore("misdirected request")
	case status >= 300 && status < 400:
		if IsRegionUnavailableLocation(header.Get("Location")) {
			return record(probeRegionalBlockTTL, ReasonRegionUnavailable)
		}
		return ignore("redirect")
	case status == http.StatusMethodNotAllowed || status == http.StatusNotImplemented:
		return ignore("method not allowed")
	case status >= 500:
		if _, ok := parseRetryAfter(header.Get("Retry-After"), now); ok {
			return record(rateLimitTTL(header, now), "overloaded")
		}
		if isEdgeAnswer(body) {
			return record(probeTransientBlockTTL, ReasonEdgeError)
		}
		return record(probeTransientBlockTTL, ReasonOriginError)
	case status >= 200:
		return reachable("answered", status >= 200 && status < 300)
	default:
		return ignore("unknown status")
	}
}

// RegionEvidence reports whether a refusal may be pooled per region: an answer that blames
// the region, or a refusal only a cross region comparison can tell apart from an outage.
func RegionEvidence(reason string) bool {
	switch reason {
	case ReasonRegionUnavailable, ReasonForbidden, ReasonOriginError:
		return true
	}
	return false
}

func ClassifyProbeError(err error) Verdict {
	if err == nil {
		return ignore("no probe")
	}
	var hostnameErr x509.HostnameError
	if errors.As(err, &hostnameErr) {
		return record(probeDefaultBlockTTL, ReasonCertHostname)
	}
	var authorityErr x509.UnknownAuthorityError
	if errors.As(err, &authorityErr) {
		return record(probeDefaultBlockTTL, ReasonCertAuthority)
	}
	if isTimeout(err) {
		return record(probeTransientBlockTTL, "probe timed out")
	}
	return unproven("probe failed")
}

// A cancelled probe means the group is shutting down, not that the node timed out.
func isTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func parseRetryAfter(value string, now time.Time) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
		if seconds <= 0 {
			return 0, false
		}
		if seconds > int64(probeMaxBlockTTL/time.Second) {
			return probeMaxBlockTTL, true
		}
		return time.Duration(seconds) * time.Second, true
	}
	if t, err := http.ParseTime(value); err == nil && t.After(now) {
		return t.Sub(now), true
	}
	return 0, false
}

func rateLimitTTL(header http.Header, now time.Time) time.Duration {
	if d, ok := parseRetryAfter(header.Get("Retry-After"), now); ok {
		return d
	}
	if epoch, err := strconv.ParseInt(strings.TrimSpace(header.Get("X-Ratelimit-Reset")), 10, 64); err == nil && epoch > now.Unix() {
		seconds := epoch - now.Unix()
		if seconds > int64(probeMaxBlockTTL/time.Second) {
			return probeMaxBlockTTL
		}
		return time.Duration(seconds) * time.Second
	}
	return probeDefaultBlockTTL
}

// A CDN answers a broken origin with its own error page that every node gets: the refusal
// says nothing about a node or a region even though it is recorded as one.
func isEdgeAnswer(body []byte) bool {
	return bytes.Contains(body, []byte("cf-error-details")) ||
		bytes.Contains(body, []byte("cf-error-overview")) ||
		bytes.Contains(body, []byte("cf.errors.css"))
}

func isChallenge(header http.Header) bool {
	if strings.EqualFold(strings.TrimSpace(header.Get("Cf-Mitigated")), "challenge") {
		return true
	}
	switch strings.ToLower(strings.TrimSpace(header.Get("X-Amzn-Waf-Action"))) {
	case "challenge", "captcha":
		return true
	}
	return false
}

func isRateLimited(header http.Header, now time.Time) bool {
	if strings.TrimSpace(header.Get("X-Ratelimit-Remaining")) == "0" {
		return true
	}
	_, ok := parseRetryAfter(header.Get("Retry-After"), now)
	return ok
}

func ignore(reason string) Verdict {
	return Verdict{Action: VerdictIgnore, Reason: reason}
}

func unproven(reason string) Verdict {
	return Verdict{Action: VerdictIgnore, Reason: reason, Unproven: true}
}

func reachable(reason string, controlSuccess bool) Verdict {
	return Verdict{Action: VerdictReachable, Reason: reason, ControlSuccess: controlSuccess}
}

func record(ttl time.Duration, reason string) Verdict {
	if ttl < probeMinBlockTTL {
		ttl = probeMinBlockTTL
	}
	if ttl > probeMaxBlockTTL {
		ttl = probeMaxBlockTTL
	}
	return Verdict{Action: VerdictRecord, TTL: ttl, Reason: reason}
}

func (t *ProbeThrottle) ensureMaps() {
	if t.nodes == nil {
		t.nodes = make(map[probeNodeKey]*probeNodeState)
		t.targets = make(map[string]*probeWindow)
	}
}

func (t *ProbeThrottle) nodeState(target, node string) *probeNodeState {
	return t.nodes[probeNodeKey{target: target, node: node}]
}

func (t *ProbeThrottle) ensureNodeState(target, node string, now time.Time) *probeNodeState {
	key := probeNodeKey{target: target, node: node}
	state := t.nodes[key]
	if state == nil {
		t.enforceCap()
		state = &probeNodeState{lastProbe: now}
		t.nodes[key] = state
	}
	return state
}

func (t *ProbeThrottle) AllowNode(target, node string, now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.ensureMaps()
	if (len(t.nodes) > probeMaxEntries || len(t.targets) > probeMaxEntries) &&
		(t.lastTrim.IsZero() || now.Sub(t.lastTrim) >= probeTrimInterval) {
		t.trim(now)
		t.lastTrim = now
	}
	t.enforceCap()

	key := probeNodeKey{target: target, node: node}
	state := t.nodes[key]
	if state != nil {
		interval := probeMinIntervalPerNode
		if !state.lastFailure.IsZero() && now.Sub(state.lastFailure) < probeRecentFailureWindow {
			interval = probeMinIntervalAfterFailure
		}
		if now.Sub(state.lastProbe) < interval {
			return false
		}
	}

	window := t.targets[target]
	if window == nil || now.Sub(window.start) >= time.Minute {
		window = &probeWindow{start: now}
		t.targets[target] = window
	}
	if window.count >= probePerTargetPerMinute {
		return false
	}

	if state == nil {
		state = &probeNodeState{}
		t.nodes[key] = state
	}
	state.lastProbe = now
	window.count++
	return true
}

// hostRefusalEvidence: refusals that blame the host itself, not a per node quota.
func hostRefusalEvidence(reason string) bool {
	switch reason {
	case ReasonRegionUnavailable, ReasonForbidden, ReasonOriginError, ReasonCertAuthority, ReasonCertHostname:
		return true
	}
	return false
}

// NoteHostRefusal records a refusal, true while the host is held back.
func NoteHostRefusal(config, group, host, node, reason string) bool {
	if host == "" || node == "" || !hostRefusalEvidence(reason) {
		return false
	}
	now := time.Now()
	nowNano := now.UnixNano()

	hostBackoffTable.mu.Lock()
	defer hostBackoffTable.mu.Unlock()

	if hostBackoffTable.entries == nil {
		hostBackoffTable.entries = make(map[hostBackoffKey]hostBackoffState)
	}
	key := hostBackoffKey{config: config, group: group, host: host}
	state, exists := hostBackoffTable.entries[key]
	if state.until > nowNano {
		log.Debugln("[Smart] Host backoff [%s] node [%s] dropped (%s)", host, node, reason)
		return true
	}
	if state.probation {
		state.probation = false
		state.nodes = nil
		state.updated = nowNano
		state.until = now.Add(hostBackoffTTL).UnixNano()
		hostBackoffTable.entries[key] = state
		log.Debugln("[Smart] Host backoff [%s] node [%s] re-armed (%s)", host, node, reason)
		return true
	}
	if !exists && len(hostBackoffTable.entries) >= hostBackoffMaxEntries {
		hostBackoffTable.pruneLocked(nowNano)
		if len(hostBackoffTable.entries) >= hostBackoffMaxEntries {
			return false
		}
	}
	if state.nodes == nil {
		state.nodes = make(map[string]int64)
	}
	state.nodes[node] = nowNano
	for seen, at := range state.nodes {
		if nowNano-at > int64(hostBackoffWindow) {
			delete(state.nodes, seen)
		}
	}
	state.updated = nowNano
	if len(state.nodes) < hostBackoffMinNodes {
		hostBackoffTable.entries[key] = state
		return false
	}
	state.nodes = nil
	state.probation = true
	state.until = now.Add(hostBackoffTTL).UnixNano()
	hostBackoffTable.entries[key] = state
	log.Debugln("[Smart] Host backoff [%s] node [%s] raised (%s)", host, node, reason)
	return true
}

// HostBackedOff reports a held host; an answer drops its evidence and the ttl expires it.
func HostBackedOff(config, group, host string) bool {
	if host == "" {
		return false
	}
	nowNano := time.Now().UnixNano()
	hostBackoffTable.mu.RLock()
	defer hostBackoffTable.mu.RUnlock()
	return hostBackoffTable.entries[hostBackoffKey{config: config, group: group, host: host}].until > nowNano
}

// NoteHostAnswer drops the evidence once a node serves the host, failures start fresh.
func NoteHostAnswer(config, group, host string) {
	if host == "" {
		return
	}
	hostBackoffTable.mu.Lock()
	defer hostBackoffTable.mu.Unlock()
	if hostBackoffTable.entries != nil {
		delete(hostBackoffTable.entries, hostBackoffKey{config: config, group: group, host: host})
	}
}

func (t *hostBackoff) pruneLocked(nowNano int64) {
	for key, state := range t.entries {
		if nowNano-state.updated > int64(hostBackoffWindow) {
			delete(t.entries, key)
		}
	}
}

func (t *ProbeThrottle) NoteFailure(target, node string, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.ensureMaps()

	key := probeNodeKey{target: target, node: node}
	state := t.nodes[key]
	if state == nil {
		state = &probeNodeState{lastProbe: now}
		t.nodes[key] = state
	}
	state.lastFailure = now
}

func (t *ProbeThrottle) MarkPending(target, node string, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.ensureMaps()

	state := t.ensureNodeState(target, node, now)
	state.pendingSince = now
	state.unprovenSince = time.Time{}
}

func (t *ProbeThrottle) ClearPending(target, node string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if state := t.nodeState(target, node); state != nil {
		state.pendingSince = time.Time{}
	}
}

func (t *ProbeThrottle) PendingFor(target, node string, now time.Time) time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()

	state := t.nodeState(target, node)
	if state == nil || state.pendingSince.IsZero() {
		return 0
	}
	return now.Sub(state.pendingSince)
}

func (t *ProbeThrottle) MarkUnproven(target, node string, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.ensureMaps()

	t.ensureNodeState(target, node, now).unprovenSince = now
}

func (t *ProbeThrottle) ClearUnproven(target, node string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if state := t.nodeState(target, node); state != nil {
		state.unprovenSince = time.Time{}
	}
}

func (t *ProbeThrottle) Unproven(target, node string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	state := t.nodeState(target, node)
	return state != nil && !state.unprovenSince.IsZero()
}

// enforceCap is the count bound the age-based trim lacks: past twice probeMaxEntries the
// oldest half is evicted, so a burst of unique targets (P2P, scanners) cannot grow the maps
// without limit (M1). Losing a throttle entry only lets that pair be probed a little sooner.
func (t *ProbeThrottle) enforceCap() {
	if len(t.nodes) >= 2*probeMaxEntries {
		keys := make([]probeNodeKey, 0, len(t.nodes))
		for k := range t.nodes {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool {
			return probeNodeAge(t.nodes[keys[i]]).Before(probeNodeAge(t.nodes[keys[j]]))
		})
		for _, k := range keys[:len(keys)-probeMaxEntries] {
			delete(t.nodes, k)
		}
	}
	if len(t.targets) >= 2*probeMaxEntries {
		keys := make([]string, 0, len(t.targets))
		for k := range t.targets {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool {
			return t.targets[keys[i]].start.Before(t.targets[keys[j]].start)
		})
		for _, k := range keys[:len(keys)-probeMaxEntries] {
			delete(t.targets, k)
		}
	}
}

// probeNodeAge is the most recent activity of a throttle entry; an in-flight probe or an open
// suspicion counts as activity, so eviction prefers idle entries.
func probeNodeAge(st *probeNodeState) time.Time {
	latest := st.lastProbe
	for _, t := range []time.Time{st.lastFailure, st.pendingSince, st.unprovenSince} {
		if t.After(latest) {
			latest = t
		}
	}
	return latest
}

func (t *ProbeThrottle) trim(now time.Time) {
	if len(t.nodes) > probeMaxEntries {
		for key, state := range t.nodes {
			if now.Sub(state.lastProbe) > probeRecentFailureWindow && now.Sub(state.lastFailure) > probeRecentFailureWindow {
				delete(t.nodes, key)
			}
		}
	}
	if len(t.targets) > probeMaxEntries {
		for target, window := range t.targets {
			if now.Sub(window.start) >= time.Minute {
				delete(t.targets, target)
			}
		}
	}
}

func AllowGlobalProbe(now time.Time) bool {
	globalProbeThrottle.mu.Lock()
	defer globalProbeThrottle.mu.Unlock()

	if globalProbeThrottle.lastRefill.IsZero() {
		globalProbeThrottle.tokens = probeGlobalBurst
		globalProbeThrottle.lastRefill = now
	}
	if elapsed := now.Sub(globalProbeThrottle.lastRefill); elapsed > 0 {
		globalProbeThrottle.tokens += elapsed.Minutes() * probeGlobalPerMinute
		if globalProbeThrottle.tokens > probeGlobalBurst {
			globalProbeThrottle.tokens = probeGlobalBurst
		}
		globalProbeThrottle.lastRefill = now
	}
	if globalProbeThrottle.tokens < 1 {
		return false
	}
	globalProbeThrottle.tokens--
	return true
}

func TryStartProbe() (func(), bool) {
	select {
	case probeInflight <- struct{}{}:
		return func() { <-probeInflight }, true
	default:
		return nil, false
	}
}

// A connection that downloaded almost nothing may have been answered with an error or limit page.
func ResponseProbeEligible(metadata *C.Metadata, downloadTotalMB float64, isUDP bool) bool {
	return !isUDP &&
		metadata.Host != "" &&
		metadata.Type != C.INNER &&
		metadata.DstPort == 443 &&
		downloadTotalMB < responseProbeDownloadThresholdMB
}

// API and CDN subdomains answer the root path with a 403 that says nothing about the node.
func ProbeURL(host string) string {
	return "https://" + host + probePath
}
