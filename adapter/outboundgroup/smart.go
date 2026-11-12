package outboundgroup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/common/callback"
	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/common/singleflight"
	"github.com/metacubex/mihomo/common/xsync"
	"github.com/metacubex/mihomo/component/geodata"
	"github.com/metacubex/mihomo/component/mmdb"
	"github.com/metacubex/mihomo/component/profile/cachefile"
	"github.com/metacubex/mihomo/component/resolver"
	"github.com/metacubex/mihomo/component/smart"
	"github.com/metacubex/mihomo/component/smart/lightgbm"
	"github.com/metacubex/mihomo/component/smart/tcpstats"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/constant/provider"
	"github.com/metacubex/mihomo/log"
	"github.com/metacubex/mihomo/tunnel"
	"github.com/metacubex/mihomo/tunnel/statistic"

	"github.com/dlclark/regexp2"
	"github.com/samber/lo"
)

const (
	cleanupInterval          = 120 * time.Minute
	cacheParamAdjustInterval = 5 * time.Minute
	recoveryCheckInterval    = 10 * time.Minute
	hostStatusCheckInterval  = 30 * time.Minute
	checkInterval            = 10 * time.Minute
	prefetchInterval         = 15 * time.Minute
	claimInterval            = 2 * time.Minute
	flushQueueInterval       = 5 * time.Minute
	rankingInterval          = 5 * time.Minute

	maxRetries  = 5
	maxSelected = 10

	deterministicDialPrefix = 3
	parallelDials           = 5
	connectThreshold        = 5.0

	floodWindow    = 2 * time.Second
	floodThreshold = 50

	stalledProbeGrace      = 2 * time.Second
	stalledConnectionGrace = 4 * time.Second
	stalledProbeTTL        = 2 * time.Minute
	udpFailureTTL          = 3 * time.Minute

	asnDiversityCacheLimit = 4096
	siteKeyCacheLimit      = 4096 // an evicted site is relearned on its next connection
)

const (
	blockCodeManual    = 1
	blockCodeAnswer    = 2
	blockCodeError     = 3
	blockCodeLowWeight = 4
	blockCodeLoss      = 5
	blockCodeStalled   = 6
)

var (
	flushQueueOnce atomic.Bool
	smartInitOnce  sync.Once
)

type SmartOption struct {
	PolicyPriority string  `group:"policy-priority,omitempty"`
	UseLightGBM    bool    `group:"uselightgbm,omitempty"`
	CollectData    bool    `group:"collectdata,omitempty"`
	SampleRate     float64 `group:"sample-rate,omitempty"`
	PreferASN      bool    `group:"prefer-asn,omitempty"`
	Tolerance      uint16  `group:"tolerance,omitempty"`
}

type Smart struct {
	*GroupBase
	store *smart.Store

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	configName     string
	selected       string
	testUrl        string
	testUrlHost    string
	expectedStatus string
	disableUDP     bool

	dataCollector  *lightgbm.DataCollector
	weightModel    *lightgbm.WeightModel
	policyPriority []priorityRule
	priorityCache  xsync.Map[string, float64]
	sampleRate     float64
	useLightGBM    bool
	collectData    bool
	preferASN      bool
	hostFailLimit  atomic.Int32
	tolerance      uint16

	freshNodesGroup singleflight.Group[nodeResult]

	ruleCountCache    xsync.Map[string, int]
	geoSiteCountCache xsync.Map[string, int]
	asnDiversity      xsync.Map[string, *xsync.Map[string, bool]]
	asnRule           xsync.Map[string, string]
	siteKeyCache      xsync.Map[string, bool]

	suppressStats atomic.Bool
	suppressCount atomic.Int64
	suppressLast  atomic.Int64

	probeThrottle smart.ProbeThrottle
	lossSampler   *tcpstats.LossSampler

	exitWatch *smart.ExitWatcher
}

type dialResult struct {
	proxyIndex  int
	conn        C.Conn
	connectTime int64
	error       error
}

type priorityRule struct {
	pattern string
	regex   *regexp2.Regexp
	factor  float64
	isRegex bool
}

type nodeResult struct {
	names   []string
	weights []float64
}

// targetState is the store side of the judgement of one finished connection, read once by
// the caller.
type targetState struct {
	failNodes map[string]int
	blocked   bool
	suspected bool
}

// connQuality is the connection side of the judgement of one finished connection.
type connQuality struct {
	err            error
	address        string // display string for the debug lines only
	weight         float64
	oldWeight      float64
	duration       int64
	uploadMB       float64
	downloadMB     float64
	lossRate       float64 // the socket's lifetime rate as read, for the debug line
	emaLossRate    float64
	strikeRate     float64 // rate of the judged window the strike gate acts on
	strikeEvidence bool
}

func NewSmart(option GroupCommonOption, smartOption SmartOption, emptyFallback C.Proxy, providers []provider.ProxyProvider) (*Smart, error) {
	if option.URL == "" {
		option.URL = C.DefaultTestURL
	}

	configFile := C.Path.Config()
	configName := strings.TrimSuffix(filepath.Base(configFile), filepath.Ext(configFile))

	s := &Smart{
		GroupBase: NewGroupBase(GroupBaseOption{
			Name:           option.Name,
			Type:           C.Smart,
			Hidden:         option.Hidden,
			Icon:           option.Icon,
			Filter:         option.Filter,
			ExcludeFilter:  option.ExcludeFilter,
			ExcludeType:    option.ExcludeType,
			TestTimeout:    option.TestTimeout,
			MaxFailedTimes: option.MaxFailedTimes,
			EmptyFallback:  emptyFallback,
			Providers:      providers,
		}),
		testUrl:        option.URL,
		expectedStatus: option.ExpectedStatus,
		configName:     configName,
		disableUDP:     option.DisableUDP,
		policyPriority: make([]priorityRule, 0),
		sampleRate:     1,
		useLightGBM:    smartOption.UseLightGBM,
		collectData:    smartOption.CollectData,
		preferASN:      smartOption.PreferASN,
		tolerance:      smartOption.Tolerance,
		lossSampler:    tcpstats.NewLossSampler(),
	}

	s.hostFailLimit.Store(int32(s.maxFailedTimes))
	s.testUrlHost = testUrlHostOf(s.testUrl)

	if smartOption.SampleRate > 0 && smartOption.SampleRate <= 1 {
		s.sampleRate = smartOption.SampleRate
	}

	if smartOption.PolicyPriority != "" {
		s.applyPolicyPriority(smartOption.PolicyPriority)
	}

	s.InitSmart()

	return s, nil
}

func (s *Smart) GetConfigFilename() string {
	return s.configName
}

func (s *Smart) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	getBatch := func(proxies []C.Proxy, i int, pinned bool) ([]C.Proxy, time.Duration) {
		begin, end := smartDialBatchBounds(len(proxies), i, pinned)
		if begin == end {
			return nil, 0
		}

		batch := proxies[begin:end]
		var timeout time.Duration
		for _, p := range batch {
			if t := s.dialTimeout(metadata, p, C.DefaultTCPTimeout); t > timeout {
				timeout = t
			}
		}

		return batch, timeout
	}

	tryDial := func(proxies []C.Proxy, pinned bool, keepPin bool) (C.Conn, error) {
		var finalErr error
		for i := 0; i < maxRetries; i++ {
			batch, timeout := getBatch(proxies, i, pinned)
			if len(batch) == 0 {
				break
			}

			ctxDial, cancel := context.WithTimeout(ctx, timeout)
			start := time.Now()
			p, c, connectTime, err := s.ParallelDialContext(ctxDial, batch, metadata, start, s.singleDialContext)
			cancel()

			if err != nil {
				if tunnel.ShouldStopRetry(err) {
					return nil, err
				}
				finalErr = err
			} else {
				if !keepPin {
					s.adoptUnwrapWinner(metadata, p)
				}
				s.onDialSuccess()
				return s.WrapConnWithMetric(c, p, metadata, connectTime), nil
			}
		}

		s.store.DeleteUnwrapResult(s.Name(), s.configName, s.unwrapKey(metadata))

		s.groupDialFailed(proxies, finalErr)

		return nil, finalErr
	}

	proxies, pinned, keepPin := s.selectProxies(metadata, s.GetProxies(true))

	return tryDial(proxies, pinned, keepPin)
}

func (s *Smart) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (pc C.PacketConn, err error) {
	var finalErr error

	proxies, _, keepPin := s.selectProxies(metadata, s.GetProxies(true))

	limit := len(proxies)
	if limit > maxSelected {
		limit = maxSelected
	}

	singleProxyRetry := (len(proxies) == 1)

	for i := 0; i < limit; i++ {
		proxy := proxies[i]
		attempts := 1
		if singleProxyRetry {
			attempts = maxRetries
		}

		for a := 0; a < attempts; a++ {
			timeout := s.dialTimeout(metadata, proxy, C.DefaultUDPTimeout)
			ctxDial, cancel := context.WithTimeout(ctx, timeout)
			start := time.Now()
			pc, err = proxy.ListenPacketContext(ctxDial, metadata)
			cancel()
			connectTime := time.Since(start).Milliseconds()

			if err != nil {
				if tunnel.ShouldStopRetry(err) {
					return nil, err
				}
				finalErr = err
				go s.recordConnectionStats(metadata.Clone(), proxy, connectTime, 0, 0, 0, 0, 0, 0, nil, nil, err)
				continue
			}

			if !keepPin {
				s.adoptUnwrapWinner(metadata, proxy)
			}
			s.onDialSuccess()
			return s.WrapPacketConnWithMetric(pc, proxy, metadata, connectTime), nil
		}

		if singleProxyRetry {
			break
		}
	}

	s.store.DeleteUnwrapResult(s.Name(), s.configName, s.unwrapKey(metadata))

	s.groupDialFailed(proxies, finalErr)

	return nil, finalErr
}

// ref: component/dialer/dialer.go parallelDialContext
func (s *Smart) ParallelDialContext(ctx context.Context, proxies []C.Proxy, metadata *C.Metadata, start time.Time, singleDialFunc func(context.Context, C.Proxy, *C.Metadata, time.Time) (C.Conn, int64, error)) (C.Proxy, C.Conn, int64, error) {
	if len(proxies) == 1 {
		conn, connectTime, err := singleDialFunc(ctx, proxies[0], metadata, start)
		return proxies[0], conn, connectTime, err
	}

	n := len(proxies)
	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make(chan dialResult, n)

	for i := 0; i < n; i++ {
		go func(proxyIndex int) {
			conn, connectTime, err := singleDialFunc(childCtx, proxies[proxyIndex], metadata, start)
			results <- dialResult{
				proxyIndex:  proxyIndex,
				conn:        conn,
				connectTime: connectTime,
				error:       err,
			}
		}(i)
	}

	drainRemaining := func(pending int) {
		go func() {
			for i := 0; i < pending; i++ {
				if r := <-results; r.conn != nil && r.error == nil {
					_ = r.conn.Close()
				}
			}
		}()
	}

	errs := make([]error, 0, n)
	for received := 0; received < n; received++ {
		select {
		case res := <-results:
			if res.error == nil {
				cancel()
				drainRemaining(n - received - 1)
				return proxies[res.proxyIndex], res.conn, res.connectTime, nil
			}
			errs = append(errs, res.error)

		case <-ctx.Done():
			drainRemaining(n - received)
			return nil, nil, 0, ctx.Err()
		}
	}

	if len(errs) > 0 {
		return nil, nil, 0, errors.Join(errs...)
	}
	return nil, nil, 0, os.ErrDeadlineExceeded
}

func (s *Smart) singleDialContext(ctx context.Context, proxy C.Proxy, metadata *C.Metadata, start time.Time) (c C.Conn, connectTime int64, err error) {
	c, err = proxy.DialContext(ctx, metadata)
	connectTime = time.Since(start).Milliseconds()

	if err != nil {
		// errors that stop the retry loop are not recorded as node failures
		if tunnel.ShouldStopRetry(err) {
			return nil, connectTime, err
		}
		if !errors.Is(err, context.Canceled) && ctx.Err() == nil {
			// metadata may be re-written by a later retry's selectProxies.
			go s.recordConnectionStats(metadata.Clone(), proxy, connectTime, 0, 0, 0, 0, 0, 0, nil, nil, err)
		}
		return nil, connectTime, err
	}

	return c, connectTime, nil
}

// dialTimeout converts the remembered connect time into a deadline: a node that was slow
// before fails fast instead of holding the whole batch until the default timeout.
func (s *Smart) dialTimeout(metadata *C.Metadata, proxy C.Proxy, max time.Duration) time.Duration {
	historyConnectTime := s.getHistoryConnectStats(metadata, proxy)
	if historyConnectTime <= 0 {
		return max
	}
	timeout := time.Duration(float64(historyConnectTime)*connectThreshold) * time.Millisecond
	if timeout > max {
		return max
	}
	return timeout
}

func (s *Smart) groupDialFailed(proxies []C.Proxy, err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	for _, p := range proxies {
		t := p.Type()
		if t == C.Direct || t == C.Compatible || t == C.Reject || t == C.Pass || t == C.RejectDrop {
			continue
		}
		s.onDialFailed(t, err, s.healthCheck)
		return
	}
}

// smartDialBatchBounds returns the dial batch of one iteration, a pinned target is
// dialed one node at a time so it cannot end up on a second exit.
func smartDialBatchBounds(total, iteration int, pinned bool) (begin, end int) {
	if total <= 0 {
		return 0, 0
	}
	if pinned {
		if iteration >= total {
			return 0, 0
		}
		return iteration, iteration + 1
	}
	if total == 1 {
		return 0, 1
	}
	if iteration < deterministicDialPrefix {
		if iteration >= total {
			return 0, 0
		}
		return iteration, iteration + 1
	}
	begin = deterministicDialPrefix + (iteration-deterministicDialPrefix)*parallelDials
	if begin >= total {
		return 0, 0
	}
	end = begin + parallelDials
	if end > total {
		end = total
	}
	return begin, end
}

func (s *Smart) unwrapKey(metadata *C.Metadata) string {
	return smart.NetScopedTarget(metadata.SmartTarget, metadata.NetWork == C.UDP)
}

func failLookupTargets(metadata *C.Metadata, isUDP bool) (string, []string) {
	expected := []string{metadata.SmartTarget}
	if isUDP {
		expected = append(expected, smart.NetScopedTarget(metadata.SmartTarget, true))
	}
	return smart.NetScopedTarget(metadata.WildcardTarget, isUDP), expected
}

func (s *Smart) dropUnwrapPins(target string) {
	s.store.DeleteUnwrapResult(s.Name(), s.configName, target)
	s.store.DeleteUnwrapResult(s.Name(), s.configName, smart.NetScopedTarget(target, true))
}

func (s *Smart) adoptUnwrapWinner(metadata *C.Metadata, p C.Proxy) {
	target := metadata.SmartTarget
	key := s.unwrapKey(metadata)
	existing, _ := s.store.GetUnwrapResult(s.Name(), s.configName, key)

	switch {
	case len(existing) == 0:
		s.store.StoreUnwrapResult(s.Name(), s.configName, key, []C.Proxy{p})
	case existing[0] == p.Name():
	default:
		s.store.DeleteUnwrapResult(s.Name(), s.configName, key)
		s.store.StoreUnwrapResult(s.Name(), s.configName, key, []C.Proxy{p})
	}
	s.closeSameConnection(metadata, p.Name(), target, false)
}

func (s *Smart) Unwrap(metadata *C.Metadata, touch bool) C.Proxy {
	proxies := s.GetProxies(touch)

	if metadata == nil {
		return proxies[0]
	}

	if s.selected != "" {
		for _, p := range proxies {
			if p.Name() == s.selected {
				return p
			}
		}
	}

	proxies, _, _ = s.selectProxies(metadata, proxies)

	return proxies[0]
}

func (s *Smart) IsL3Protocol(metadata *C.Metadata) bool {
	return s.Unwrap(metadata, false).IsL3Protocol(metadata)
}

func (s *Smart) SupportUDP() bool {
	return !s.disableUDP
}

func (s *Smart) WrapConnWithMetric(c C.Conn, proxy C.Proxy, metadata *C.Metadata, connectTime int64) C.Conn {
	s.exitWatch.EnsureLoaded(proxy.Name())
	if region, asn := s.exitWatch.Label(metadata.WildcardTarget, proxy.Name()); region != "" || asn != "" {
		metadata.ExitGeoIP = region
		if s.preferASN {
			metadata.ExitIPASN = asn
		}
	}
	c.AppendToChains(s)

	start := time.Now()

	var firstReadErr atomic.TypedValue[error]
	var firstReadLatency atomic.Int64
	var observedReadErr atomic.TypedValue[error]
	var observedWriteErr atomic.TypedValue[error]

	needHandshake := N.NeedHandshake(c)
	// a raw relay must keep its splice fast path: the copy engine peels wrappers
	// down to the socket, so the observer would disable it; skip those chains
	stop := N.UnwrapWriter(c)
	if _, isRawRelay := stop.(syscall.Conn); !isRawRelay {
		if u, ok := stop.(interface{ Upstream() any }); !ok || u.Upstream() != nil {
			c = callback.NewErrorCallBackConn(c, &observedReadErr, &observedWriteErr)
		}
	}

	if needHandshake {
		c = callback.NewFirstWriteCallBackConn(c, nil)
	}

	c = callback.NewFirstReadCallBackConn(c, func(err error) {
		firstReadLatency.Store(time.Since(start).Milliseconds())
		if err != nil {
			firstReadErr.Store(err)
		}
	})

	return s.registerClosureMetricsCallback(
		c, proxy, metadata, connectTime,
		&firstReadLatency, &firstReadErr,
		&observedReadErr, &observedWriteErr,
	)
}

func (s *Smart) WrapPacketConnWithMetric(pc C.PacketConn, proxy C.Proxy, metadata *C.Metadata, connectTime int64) C.PacketConn {
	s.exitWatch.EnsureLoaded(proxy.Name())
	if region, asn := s.exitWatch.Label(metadata.WildcardTarget, proxy.Name()); region != "" || asn != "" {
		metadata.ExitGeoIP = region
		if s.preferASN {
			metadata.ExitIPASN = asn
		}
	}
	pc.AppendToChains(s)

	var udpLatency atomic.Int64
	var observedReadErr atomic.TypedValue[error]
	var observedWriteErr atomic.TypedValue[error]

	pc = callback.NewErrorCallBackPacketConn(pc, &observedReadErr, &observedWriteErr)
	pc = callback.NewFirstReadCallBackPacketConn(pc, func(latency int64) {
		udpLatency.Store(latency)
	})

	return s.registerPacketClosureMetricsCallback(pc, proxy, metadata, connectTime, &udpLatency, &observedReadErr, &observedWriteErr)
}

func (s *Smart) Set(name string) error {
	var p C.Proxy
	for _, proxy := range s.GetProxies(false) {
		if proxy.Name() == name {
			p = proxy
			break
		}
	}

	if p == nil {
		return errors.New("proxy not exist")
	}

	s.ForceSet(name)

	return nil
}

func (s *Smart) ForceSet(name string) {
	s.selected = name
}

func (s *Smart) Now() string {
	if s.selected != "" {
		for _, p := range s.GetProxies(false) {
			if p.Name() == s.selected {
				return p.Name()
			}
		}
		s.selected = ""
	}

	return "Smart - Select"
}

func (s *Smart) MarshalJSON() ([]byte, error) {
	proxies := s.GetProxies(false)
	all := make([]string, len(proxies))
	for i, proxy := range proxies {
		all[i] = proxy.Name()
	}

	var policyPriorityBuf strings.Builder
	for i, rule := range s.policyPriority {
		if i > 0 {
			policyPriorityBuf.WriteByte(';')
		}
		fmt.Fprintf(&policyPriorityBuf, "%s:%.2f", rule.pattern, rule.factor)
	}

	return json.Marshal(map[string]any{
		"type":            s.Type().String(),
		"now":             s.Now(),
		"all":             all,
		"testUrl":         s.testUrl,
		"expectedStatus":  s.expectedStatus,
		"fixed":           s.selected,
		"hidden":          s.Hidden(),
		"icon":            s.Icon(),
		"emptyFallback":   s.EmptyFallback().Name(),
		"policy-priority": policyPriorityBuf.String(),
		"useLightGBM":     s.useLightGBM,
		"collectData":     s.collectData,
		"sampleRate":      s.sampleRate,
		"preferASN":       s.preferASN,
		"tolerance":       s.tolerance,
	})
}

func (s *Smart) Providers() []provider.ProxyProvider {
	return s.providers
}

func (s *Smart) Proxies() []C.Proxy {
	return s.GetProxies(false)
}

func (s *Smart) filterProxies(metadata *C.Metadata, wildcardTarget string, names []string, weights []float64, all []C.Proxy, minCount int, isUDP bool) ([]C.Proxy, bool) {
	blockedNodes := s.store.GetBlockedNodes(s.Name(), s.configName)
	lookupTarget, extraTargets := failLookupTargets(metadata, isUDP)
	wtFailNodes, wtBlocked := s.store.GetHostStatus(s.Name(), s.configName, lookupTarget, int(s.hostFailLimit.Load()), extraTargets...)
	now := time.Now()
	s.exitWatch.Backfill(s.ctx, wildcardTarget, all)

	nameIndex := make(map[string]int, len(names))
	for i, name := range names {
		nameIndex[name] = i
	}

	var proxiesByName []C.Proxy
	if len(names) > 0 {
		proxiesByName = make([]C.Proxy, len(names))
		for _, p := range all {
			if idx, ok := nameIndex[p.Name()]; ok {
				proxiesByName[idx] = p
			}
		}
	}

	selected := make([]C.Proxy, 0, minCount+1)
	softSkipped := false

	for i, name := range names {
		proxy := proxiesByName[i]
		if proxy == nil || blockedNodes[name] || !proxy.AliveForTestUrl(s.testUrl) || (isUDP && !proxy.SupportUDP()) {
			continue
		}
		w := 0.0
		if weights != nil && i < len(weights) {
			w = weights[i]
		}
		if weights != nil && w < smart.AllowedWeight {
			continue
		}
		if wtFailNodes[name] != 0 {
			if !wtBlocked || wtFailNodes[name] == 1 {
				if i == 0 {
					softSkipped = true
				}
				continue
			}
		}
		if s.exitWatch.Defer(wildcardTarget, name, now, smart.ProbeTimeout) {
			if i == 0 {
				softSkipped = true
			}
			continue
		}
		selected = append(selected, proxy)
	}
	// a pinned candidate list is used as is, it is not padded from the rest
	if weights == nil && len(selected) > 0 {
		return selected, softSkipped
	}

	if len(selected) >= len(all) {
		return selected, softSkipped
	}

	if len(selected) >= minCount {
		return selected[:minCount], softSkipped
	}

	hasPriority := len(s.policyPriority) > 0

	type sortKey struct {
		delay     uint16
		suspected bool
		factor    float64
		index     int
	}
	type keyedProxy struct {
		proxy C.Proxy
		key   sortKey
	}

	ranked := make([]keyedProxy, len(all))
	for i, p := range all {
		key := sortKey{
			delay:     p.LastDelayForTestUrl(s.testUrl),
			index:     i,
			suspected: s.exitWatch.Suspected(wildcardTarget, p.Name()),
		}
		if hasPriority {
			key.factor = s.getPriorityFactor(p.Name())
		}
		ranked[i] = keyedProxy{proxy: p, key: key}
	}

	// delays within the tolerance count as equal, so a jittering delay does not reorder the list
	less := func(a, b sortKey) bool {
		if a.suspected != b.suspected {
			return !a.suspected
		}
		if hasPriority && a.factor != b.factor {
			return a.factor > b.factor
		}
		if s.tolerance > 0 {
			var diff uint16
			if a.delay > b.delay {
				diff = a.delay - b.delay
			} else {
				diff = b.delay - a.delay
			}
			if diff <= s.tolerance {
				return a.index < b.index
			}
		}
		if a.delay != b.delay {
			return a.delay < b.delay
		}
		return a.index < b.index
	}

	filtered := make([]keyedProxy, 0, len(all))
	for _, kp := range ranked {
		name := kp.proxy.Name()
		if _, isCandidate := nameIndex[name]; isCandidate {
			continue
		}
		if wtFailNodes[name] != 0 {
			if !wtBlocked || wtFailNodes[name] == 1 {
				continue
			}
		}
		if blockedNodes[name] {
			continue
		}
		if !kp.proxy.AliveForTestUrl(s.testUrl) || (isUDP && !kp.proxy.SupportUDP()) {
			continue
		}
		if s.exitWatch.Defer(wildcardTarget, name, now, smart.ProbeTimeout) {
			continue
		}
		filtered = append(filtered, kp)
	}

	sort.SliceStable(filtered, func(i, j int) bool { return less(filtered[i].key, filtered[j].key) })

	for _, kp := range filtered {
		selected = append(selected, kp.proxy)
		if len(selected) >= minCount {
			break
		}
	}

	rankedSorted := false
	sortFallback := func() {
		if !rankedSorted {
			sort.SliceStable(ranked, func(i, j int) bool { return less(ranked[i].key, ranked[j].key) })
			rankedSorted = true
		}
	}

	if len(selected) == 0 {
		sortFallback()
		for _, kp := range ranked {
			p := kp.proxy
			if (wtFailNodes[p.Name()] == 0 || (wtBlocked && wtFailNodes[p.Name()] != 1)) &&
				p.AliveForTestUrl(s.testUrl) && (!isUDP || p.SupportUDP()) &&
				!s.exitWatch.Defer(wildcardTarget, p.Name(), now, smart.ProbeTimeout) {
				selected = append(selected, p)
			}
			if len(selected) >= minCount {
				break
			}
		}

		if len(selected) == 0 {
			for _, kp := range ranked {
				p := kp.proxy
				if p.AliveForTestUrl(s.testUrl) && !s.exitWatch.Defer(wildcardTarget, p.Name(), now, smart.ProbeTimeout) {
					selected = append(selected, p)
				}
				if len(selected) >= minCount {
					break
				}
			}
		}

		if len(selected) == 0 {
			for _, kp := range ranked {
				p := kp.proxy
				if wtFailNodes[p.Name()] == 1 || s.exitWatch.Defer(wildcardTarget, p.Name(), now, smart.ProbeTimeout) {
					continue
				}
				selected = append(selected, p)
				if len(selected) >= minCount {
					break
				}
			}
		}
	}

	if len(selected) < minCount {
		sortFallback()
		for _, kp := range ranked {
			p := kp.proxy
			if !p.AliveForTestUrl(s.testUrl) || (isUDP && !p.SupportUDP()) {
				continue
			}
			if s.exitWatch.Defer(wildcardTarget, p.Name(), now, smart.ProbeTimeout) &&
				s.exitWatch.AllowFallback(wildcardTarget, p.Name(), now, smart.ProbeTimeout) {
				selected = append(selected, p)
				break
			}
		}
	}
	return selected, softSkipped
}

// selectProxies returns the nodes to dial and whether the target is already pinned to
// one node: a pinned target is dialed node by node, an unpinned one may race on its
// first dial, which is the only moment a key can show more than one exit.
func (s *Smart) selectProxies(metadata *C.Metadata, proxies []C.Proxy) ([]C.Proxy, bool, bool) {
	asnNumber := s.getASNCode(metadata)
	dstIP := ""
	if metadata.Host == "" {
		dstIP = metadata.DstIP.String()
	}
	wildcardTarget := smart.GetEffectiveTarget(metadata.Host, dstIP)
	metadata.WildcardTarget = wildcardTarget
	if metadata.SmartTarget == "" {
		metadata.SmartTarget = wildcardTarget
	}
	needsASNKey := s.preferASN && s.needsASNKey(metadata.SmartTarget, asnNumber)
	if claimedRule, ok := s.claimedRule(asnNumber, needsASNKey); ok {
		metadata.SmartTarget = claimedRule
	} else {
		// a shared or unknown network cannot identify the site, so the site is kept as its key
		site := ""
		if needsASNKey && metadata.Host != "" {
			if asnNumber == "" || smart.SharedASNs[asnNumber] {
				s.recordSiteKey(metadata, wildcardTarget)
			} else if _, recorded := s.siteKeyCache.Load(wildcardTarget); recorded {
				site = wildcardTarget
			}
		}
		metadata.SmartTarget = smart.SmartTargetKey(s.preferASN, asnNumber, metadata.SmartTarget, wildcardTarget, site, needsASNKey)
	}

	if s.selected != "" {
		for _, p := range proxies {
			if p.Name() == s.selected {
				return []C.Proxy{p}, true, false
			}
		}
	}

	computeFreshNodes := func(isUDP bool) ([]string, []float64) {
		if proxiesName, weights := s.store.GetPrefetchResult(s.Name(), s.configName, metadata.SmartTarget, isUDP); len(proxiesName) > 0 {
			return proxiesName, weights
		}
		if proxiesName, weights, err := s.store.GetBestProxyForTarget(s.Name(), s.configName, metadata.SmartTarget, isUDP); err == nil && len(proxiesName) > 0 {
			return proxiesName, weights
		}
		return nil, nil
	}

	computeFreshSingleFlight := func(isUDP bool) ([]string, []float64) {
		sfKey := metadata.SmartTarget + "|t"
		if isUDP {
			sfKey = metadata.SmartTarget + "|u"
		}
		res, _, _ := s.freshNodesGroup.Do(sfKey, func() (nodeResult, error) {
			names, weights := computeFreshNodes(isUDP)
			return nodeResult{names: names, weights: weights}, nil
		})
		return res.names, res.weights
	}

	// asynchronously update expired cache (stale-while-revalidate)
	refreshUnwrapCache := func(isUDP bool) {
		names, _ := computeFreshSingleFlight(isUDP)
		if len(names) == 0 {
			return
		}
		allProxies := s.GetProxies(true)
		nameSet := make(map[string]bool, len(names))
		for _, name := range names {
			nameSet[name] = true
		}
		proxyByName := make(map[string]C.Proxy, len(names))
		for _, p := range allProxies {
			if name := p.Name(); nameSet[name] {
				proxyByName[name] = p
			}
		}
		resultProxies := make([]C.Proxy, 0, len(names))
		for _, name := range names {
			if p, ok := proxyByName[name]; ok {
				resultProxies = append(resultProxies, p)
			}
		}
		if len(resultProxies) > 0 {
			s.store.StoreUnwrapResult(s.Name(), s.configName, smart.NetScopedTarget(metadata.SmartTarget, isUDP), resultProxies)
		}
	}

	trySelector := func(isUDP bool) ([]string, []float64, bool) {
		if proxiesName, expired := s.store.GetUnwrapResult(s.Name(), s.configName, smart.NetScopedTarget(metadata.SmartTarget, isUDP)); len(proxiesName) > 0 {
			if expired {
				go refreshUnwrapCache(isUDP)
			}
			return proxiesName, nil, true
		}
		names, weights := computeFreshSingleFlight(isUDP)
		return names, weights, false
	}

	isUDP := metadata.NetWork == C.UDP
	resultNames, resultWeights, pinned := trySelector(isUDP)

	// a pin that cannot serve this network leaves it to the other one, so the candidates are
	// taken per network instead; the winner of this dial still replaces the pin
	if pinned && isUDP && len(resultNames) > 0 && lo.ContainsBy(proxies, func(p C.Proxy) bool {
		return p.Name() == resultNames[0] && !p.SupportUDP()
	}) {
		resultNames, resultWeights = computeFreshSingleFlight(isUDP)
		pinned = false
	}

	filtered, softSkipped := s.filterProxies(metadata, wildcardTarget, resultNames, resultWeights, proxies, maxSelected, isUDP)
	return filtered, pinned, pinned && softSkipped
}

func (s *Smart) getPriorityFactor(proxyName string) float64 {
	if len(s.policyPriority) == 0 {
		return 1.0
	}
	if v, ok := s.priorityCache.Load(proxyName); ok {
		return v
	}
	factor := 1.0
	for _, rule := range s.policyPriority {
		if rule.isRegex && rule.regex != nil {
			if matched, _ := rule.regex.MatchString(proxyName); matched {
				factor = rule.factor
				break
			}
		} else if strings.Contains(proxyName, rule.pattern) {
			factor = rule.factor
			break
		}
	}
	s.priorityCache.Store(proxyName, factor)
	return factor
}

func (s *Smart) applyHostFailLimit() {
	if proxyCount := len(s.GetProxies(true)); proxyCount > 0 {
		hostFailLimit := proxyCount / 3
		if hostFailLimit < 2 {
			hostFailLimit = 2
		}
		s.hostFailLimit.Store(int32(hostFailLimit))
	}
}

func (s *Smart) applyPolicyPriority(policyPriority string) {
	lastUnescapedColon := func(str string) int {
		for i := len(str) - 1; i >= 0; i-- {
			if str[i] == ':' {
				bs := 0
				j := i - 1
				for j >= 0 && str[j] == '\\' {
					bs++
					j--
				}
				if bs%2 == 0 {
					return i
				}
			}
		}
		return -1
	}

	unescapePattern := func(p string) string {
		var b strings.Builder
		for i := 0; i < len(p); i++ {
			if p[i] == '\\' && i+1 < len(p) {
				b.WriteByte(p[i+1])
				i++
			} else {
				b.WriteByte(p[i])
			}
		}
		return b.String()
	}

	pairs := strings.Split(policyPriority, ";")
	for _, pair := range pairs {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}

		idx := lastUnescapedColon(pair)
		if idx <= 0 || idx == len(pair)-1 {
			log.Warnln("[Smart] Invalid policy-priority rule: [%s], must be in 'pattern:factor' format and factor is required", pair)
			continue
		}

		patternRaw := strings.TrimSpace(pair[:idx])
		factorStr := strings.TrimSpace(pair[idx+1:])

		factor, err := strconv.ParseFloat(factorStr, 64)
		if err != nil {
			log.Warnln("[Smart] Invalid priority factor format for pattern [%s:%v]", patternRaw, err)
			continue
		}
		if factor <= 0 {
			log.Warnln("[Smart] Invalid priority factor [%.2f] for pattern [%s], factor must be positive", factor, patternRaw)
			continue
		}

		rule := priorityRule{
			pattern: unescapePattern(patternRaw),
			factor:  factor,
		}

		if re, err := regexp2.Compile(rule.pattern, regexp2.None); err == nil {
			rule.regex = re
			rule.isRegex = true
		}

		s.policyPriority = append(s.policyPriority, rule)
	}
}

func (s *Smart) getASNCode(metadata *C.Metadata) string {
	if metadata.DstIPASN == "unknown" {
		return ""
	}

	if metadata.DstIPASN == "" {
		if !s.preferASN {
			return ""
		}
		var ip netip.Addr
		if metadata.Host != "" && !metadata.Resolved() {
			ctx, cancel := context.WithTimeout(context.Background(), resolver.DefaultDNSTimeout)
			defer cancel()
			var err error
			ip, err = resolver.ResolveIP(ctx, metadata.Host)
			if err != nil {
				log.Debugln("[DNS] resolve %s error: %s", metadata.Host, err.Error())
				metadata.DstIPASN = "unknown"
				return ""
			} else {
				log.Debugln("[DNS] %s --> %s", metadata.Host, ip.String())
				if !ip.IsValid() {
					metadata.DstIPASN = "unknown"
					return ""
				}
			}
		} else {
			ip = metadata.DstIP
		}

		asn, aso := mmdb.ASNInstance().LookupASN(ip.AsSlice())
		if asn == "" {
			metadata.DstIPASN = "unknown"
		} else {
			metadata.DstIPASN = asn + " " + aso
		}
		return asn
	}

	if idx := strings.IndexByte(metadata.DstIPASN, ' '); idx >= 0 {
		return metadata.DstIPASN[:idx]
	}
	return metadata.DstIPASN
}

// recordSiteKey marks a site as keyed by itself; the first mark also closes the site
// connections that still key by a network, so the service converges on one exit.
func (s *Smart) recordSiteKey(metadata *C.Metadata, site string) {
	if site == "" {
		return
	}
	if _, recorded := s.siteKeyCache.Load(site); recorded {
		return
	}
	if s.siteKeyCache.Size() >= siteKeyCacheLimit {
		return
	}
	if _, recorded := s.siteKeyCache.LoadOrStore(site, true); recorded {
		return
	}

	statistic.DefaultManager.Range(func(tracker statistic.Tracker) bool {
		if tracker.ID() == metadata.UUID {
			return true
		}
		info := tracker.Info()
		if info == nil || info.Metadata == nil || info.Metadata.WildcardTarget != site {
			return true
		}
		target := info.Metadata.SmartTarget
		if target == site || target == "" || smart.IsRuleTarget(target) {
			return true
		}
		if !lo.Contains(tracker.Chains(), s.Name()) {
			return true
		}
		info.Metadata.SetSmartBlock("degraded")
		_ = tracker.Close()
		return true
	})
}

func (s *Smart) needsASNKey(target, asn string) bool {
	ruleCount := 0
	if ruleType, payload, ok := smart.RulePayload(target); ok {
		ruleCount = s.ruleEntryCount(ruleType, payload)
	}
	return smart.NeedsASNKey(target, ruleCount, s.asnDiversityOf(target, asn))
}

// ruleEntryCount counts provider rules for RuleSet and matcher domains for geosite.
func (s *Smart) ruleEntryCount(ruleType, payload string) int {
	switch ruleType {
	case "RuleSet":
		if count, loaded := s.ruleCountCache.Load(payload); loaded {
			return count
		}
		if rp, ok := tunnel.RuleProviders()[payload]; ok && rp != nil {
			if count := rp.Count(); count > 0 {
				s.ruleCountCache.Store(payload, count)
				return count
			}
		}
	case "GeoSite":
		if count, loaded := s.geoSiteCountCache.Load(payload); loaded {
			return count
		}
		if matcher, err := geodata.LoadGeoSiteMatcher(payload); err == nil {
			if count := matcher.Count(); count > 0 {
				s.geoSiteCountCache.Store(payload, count)
				return count
			}
		}
	}
	return 0
}

// asnDiversityOf counts the unrelated networks a target was seen on, which is how a
// rule set with a provider defined name is told apart from a collection.
func (s *Smart) asnDiversityOf(target, asn string) int {
	if asn == "" || smart.SharedASNs[asn] {
		return 0
	}
	set, loaded := s.asnDiversity.Load(target)
	if !loaded {
		if s.asnDiversity.Size() >= asnDiversityCacheLimit {
			return 0
		}
		set, _ = s.asnDiversity.LoadOrStore(target, xsync.NewMap[string, bool]())
	}
	if _, loaded := set.Load(asn); !loaded && set.Size() < smart.BroadASNDiversity {
		set.Store(asn, true)
	}
	return set.Size()
}

// claimedRule returns the service rule that owns this network, so that a host of the
// same service which matched a broad rule keeps the exit of the service.
func (s *Smart) claimedRule(asn string, needsASNKey bool) (string, bool) {
	if !s.preferASN || !needsASNKey || asn == "" {
		return "", false
	}
	rule, ok := s.asnRule.Load(asn)
	if !ok || rule == "" || rule == smart.ASNClaimAmbiguous {
		return "", false
	}
	return rule, true
}

// claimASNEvidence rebuilds the network to service rule claims from the evidence
// collected per target (see TargetASNEvidence), it runs on its own timer.
func (s *Smart) claimASNEvidence() {
	claims := smart.ClaimedASNRules(s.store.TargetASNEvidence(s.Name(), s.configName))
	if len(claims) == 0 {
		return
	}

	s.asnRule.Range(func(asn, _ string) bool {
		if _, ok := claims[asn]; !ok {
			s.asnRule.Delete(asn)
		}
		return true
	})

	ambiguous := 0
	for asn, rule := range claims {
		if rule == smart.ASNClaimAmbiguous {
			ambiguous++
		}
		s.asnRule.Store(asn, rule)
	}

	log.Debugln("[Smart] Group: [%s] - network claims updated: [%d] networks, [%d] ambiguous", s.Name(), len(claims), ambiguous)
}

func (s *Smart) getHistoryConnectStats(metadata *C.Metadata, proxy C.Proxy) int64 {
	target := metadata.SmartTarget
	proxyName := proxy.Name()
	cacheKey := smart.FormatDBKey(smart.KeyTypeStats, s.configName, s.Name(), target, proxyName)
	atomicRecord := s.store.GetOrCreateAtomicRecord(cacheKey, s.Name(), s.configName, target, proxyName)
	return atomicRecord.ConnectTime()
}

func (s *Smart) updateConnectionDuration(record *smart.AtomicStatsRecord, connectionDuration int64) {
	durationMinutes := float64(connectionDuration) / 60000.0
	if currentDuration := record.Duration(); currentDuration > 0 {
		record.SetDuration((currentDuration + durationMinutes) / 2.0)
	} else {
		record.SetDuration(durationMinutes)
	}
}

func (s *Smart) saveStatsRecord(target string, proxy C.Proxy, record *smart.StatsRecord) {
	if data, err := json.Marshal(record); err == nil {
		s.store.AppendToGlobalQueue(smart.StoreOperation{
			Type:   smart.OpSaveStats,
			Group:  s.Name(),
			Config: s.configName,
			Target: target,
			Node:   proxy.Name(),
			Data:   data,
		})
	}
}

func formatTrafficUnit(val float64, isSpeed bool) string {
	units := []string{"B", "KB", "MB", "GB", "TB"}
	base := 1024.0
	i := 0
	for val >= base && i < len(units)-1 {
		val /= base
		i++
	}
	if isSpeed {
		return fmt.Sprintf("%.2f %s/s", val, units[i])
	}
	return fmt.Sprintf("%.2f %s", val, units[i])
}

func formatTimeUnit(val float64) string {
	units := []string{"ms", "s", "min", "h"}
	thresholds := []float64{1000.0, 60.0, 60.0}
	i := 0
	for i < len(units)-1 && val >= thresholds[i] {
		val /= thresholds[i]
		i++
	}
	return fmt.Sprintf("%.2f %s", val, units[i])
}

func (s *Smart) logConnectionStats(err error, record *smart.StatsRecord, metadata *C.Metadata, baseWeight, priorityFactor float64,
	addressDisplay, proxyName string, connectTime int64, latency int64, uploadTotal, downloadTotal, maxUploadRate, maxDownloadRate float64,
	connectionDuration int64, ModelPredicted bool, lossRate, cumulLossRate float64,
) {
	weightSource := "Traditional"
	if ModelPredicted {
		weightSource = "LightGBM"
	}

	statusStr := "closed"
	if err != nil {
		statusStr = "failed"
	}

	log.Debugln("[Smart] Connection status: [%s], Updated weights: (Model: [%s], TCP: [%.4f], UDP: [%.4f], Base: [%.4f], Priority: [%.2f]) "+
		"For (Group: [%s] - Node: [%s] - Network: [%s] - Address: [%s]) "+
		"- Current: (Connect: [%s], Latency: [%s], LossRate: [%.2f%%], Up: [%s], Down: [%s], Max Up Speed: [%s], Max Down Speed: [%s], Duration: [%s]) "+
		"- History: (Success: [%d], Failure: [%d], EMA Connect: [%s], EMA Latency: [%s], Cumul LossRate: [%.2f%%], Total Up: [%s], Total Down: [%s], Max Up Speed: [%s], Max Down Speed: [%s], Avg Duration: [%s])",
		statusStr, weightSource, record.Weights[smart.WeightTypeTCP], record.Weights[smart.WeightTypeUDP], baseWeight, priorityFactor,
		s.Name(), proxyName, metadata.NetWork.String(), addressDisplay,
		formatTimeUnit(float64(connectTime)),
		formatTimeUnit(float64(latency)),
		lossRate*100,
		formatTrafficUnit(uploadTotal*1024*1024, false),
		formatTrafficUnit(downloadTotal*1024*1024, false),
		formatTrafficUnit(maxUploadRate*1024, true),
		formatTrafficUnit(maxDownloadRate*1024, true),
		formatTimeUnit(float64(connectionDuration)),
		record.Success, record.Failure,
		formatTimeUnit(float64(record.ConnectTime)),
		formatTimeUnit(float64(record.Latency)),
		cumulLossRate*100,
		formatTrafficUnit(record.UploadTotal*1024*1024, false),
		formatTrafficUnit(record.DownloadTotal*1024*1024, false),
		formatTrafficUnit(record.MaxUploadRate*1024, true),
		formatTrafficUnit(record.MaxDownloadRate*1024, true),
		formatTimeUnit(record.ConnectionDuration*60000),
	)
}

func (s *Smart) collectConnectionData(input *smart.ModelInput, metadata *C.Metadata,
	baseWeight float64, proxyName string, ModelPredicted bool,
) {
	if s.sampleRate < 1.0 && rand.Float64() > s.sampleRate {
		return
	}

	input.GroupName = s.Name()
	input.NodeName = proxyName
	weightSource := "Traditional"

	if ModelPredicted {
		weightSource = "LightGBM"
	}

	s.dataCollector.AddSample(input, metadata, baseWeight, weightSource)
}

func updateEMA[T int64 | float64](oldValue, newValue T) T {
	if oldValue > 0 {
		return (oldValue*2 + newValue*4) / 6
	}
	return newValue
}

func testUrlHostOf(raw string) string {
	if u, err := url.Parse(raw); err == nil {
		return strings.ToLower(u.Hostname())
	}
	return ""
}

func (s *Smart) recordConnectionStats(metadata *C.Metadata, proxy C.Proxy,
	connectTime, latency, uploadTotal, downloadTotal, maxUploadRate, maxDownloadRate,
	connectionDuration int64, tcpStats, windowStats *tcpstats.Stats, err error,
) {
	if proxy.Type() == C.Compatible || proxy.Type() == C.Reject || proxy.Type() == C.Pass || proxy.Type() == C.RejectDrop {
		return
	}

	s.exitWatch.MaybeProbe(s.ctx, proxy)
	if s.testUrlHost != "" && strings.EqualFold(metadata.Host, s.testUrlHost) {
		return
	}
	// failure flood suppression: short-circuit heavy work when recent failures reach the threshold to avoid a disconnect storm (including Direct nodes)
	now := time.Now().Unix()
	if err == nil {
		s.suppressStats.Store(false)
		s.suppressCount.Store(0)
	} else {
		if now-s.suppressLast.Load() > int64(floodWindow.Seconds()) {
			s.suppressCount.Store(0)
		}
		s.suppressLast.Store(now)
		if s.suppressCount.Add(1) >= floodThreshold {
			if s.suppressStats.CompareAndSwap(false, true) {
				s.store.ClearFloodRecordsByGroup(s.Name(), s.configName)
			}
		}
		if s.suppressStats.Load() {
			return
		}
	}

	var lossRate float64
	var cumulLossRate float64
	var calculatedWeight float64
	var ModelPredicted bool

	proxyName := proxy.Name()
	isUDP := metadata.NetWork == C.UDP

	target := metadata.SmartTarget
	wildcardTarget := metadata.WildcardTarget
	cacheKey := smart.FormatDBKey(smart.KeyTypeStats, s.configName, s.Name(), target, proxyName)
	asnNumber := s.getASNCode(metadata)
	priorityFactor := s.getPriorityFactor(proxyName)

	asnDisplay := "unknown"
	if asnNumber != "" {
		asnDisplay = asnNumber
	}
	exitGeoIP := metadata.ExitGeoIP
	exitIPASN := metadata.ExitIPASN
	if exitGeoIP == "" {
		exitGeoIP = "unknown"
	}
	if exitIPASN == "" {
		exitIPASN = "unknown"
	}
	var addressDisplay string
	if metadata.Host != "" {
		addressDisplay = fmt.Sprintf("Host: [%s] - Target: [%s] - WildcardTarget: [%s] - ASN: [%s] - ExitGeoIP: [%s] - ExitIPASN: [%s]", metadata.Host, target, wildcardTarget, asnDisplay, exitGeoIP, exitIPASN)
	} else {
		addressDisplay = fmt.Sprintf("IP: [%s] - Target: [%s] - ASN: [%s] - ExitGeoIP: [%s] - ExitIPASN: [%s]", metadata.DstIP.String(), target, asnDisplay, exitGeoIP, exitIPASN)
	}

	weightType := smart.WeightTypeTCP
	if isUDP {
		weightType = smart.WeightTypeUDP
	}

	lock := smart.GetTargetNodeLock(target, s.Name(), proxyName)
	lock.Lock()
	locked := true
	defer func() {
		if locked {
			lock.Unlock()
		}
	}()

	atomicRecord := s.store.GetOrCreateAtomicRecord(cacheKey, s.Name(), s.configName, target, proxyName)

	switch {
	case err != nil:
		atomicRecord.AddFailure(1)
	default:
		atomicRecord.AddSuccess(1)
		if asnNumber != "" && !smart.SharedASNs[asnNumber] {
			if kind := smart.ClassifyTargetName(target); kind == smart.TargetKindRuleName || kind == smart.TargetKindService {
				atomicRecord.AddASNEvidence(asnNumber)
				s.store.RecordASNEvidence(s.Name(), s.configName, target, asnNumber)
			}
		}
	}

	if connectTime > 0 {
		atomicRecord.SetConnectTime(updateEMA(atomicRecord.ConnectTime(), connectTime))
	}

	if latency > 0 {
		atomicRecord.SetLatency(updateEMA(atomicRecord.Latency(), latency))
	}

	if connectionDuration > 0 {
		s.updateConnectionDuration(atomicRecord, connectionDuration)
	}

	if tcpStats != nil {
		lossRate = tcpStats.LossRate()
	}

	// a judged window feeds the ema, the strike gate and the cumulative counters; the
	// sampler holds increments below the window floor until they can be judged
	var windowRate float64
	var windowEvidence bool
	if windowStats != nil {
		windowRate = windowStats.LossRate()
		windowEvidence = true
		atomicRecord.AddCumulSent(int64(windowStats.TotalSent()))
		atomicRecord.AddCumulRetrans(int64(windowStats.TotalRetrans()))
	}

	if sent := atomicRecord.CumulSent(); sent > 0 {
		cumulLossRate = float64(atomicRecord.CumulRetrans()) / float64(sent)
		if cumulLossRate > 1 {
			cumulLossRate = 1
		}
	}

	emaLossRate := atomicRecord.LossRate()
	if windowEvidence {
		emaLossRate = updateEMA(emaLossRate, windowRate)
		atomicRecord.SetLossRate(emaLossRate)
	}

	oldWeight := atomicRecord.GetWeight(weightType)
	uploadTotalMB := float64(uploadTotal) / (1024.0 * 1024.0)
	downloadTotalMB := float64(downloadTotal) / (1024.0 * 1024.0)
	maxUploadRateKB := float64(maxUploadRate) / 1024.0
	maxDownloadRateKB := float64(maxDownloadRate) / 1024.0

	atomicRecord.AddUploadTotal(uploadTotalMB)
	atomicRecord.AddDownloadTotal(downloadTotalMB)

	if maxUploadRateKB > atomicRecord.MaxUploadRate() {
		atomicRecord.SetMaxUploadRate(maxUploadRateKB)
	}

	if maxDownloadRateKB > atomicRecord.MaxDownloadRate() {
		atomicRecord.SetMaxDownloadRate(maxDownloadRateKB)
	}

	// the model and the collected data only see judged windows
	evidenceLossRate := windowRate
	input := lightgbm.CreateModelInputFromStatsRecord(
		atomicRecord, metadata,
		uploadTotalMB, downloadTotalMB, maxUploadRateKB, maxDownloadRateKB, float64(connectionDuration)/60000.0, wildcardTarget,
		evidenceLossRate, cumulLossRate,
	)
	input.ConnectionFailed = err != nil

	if s.useLightGBM && s.weightModel != nil {
		calculatedWeight, ModelPredicted = s.weightModel.PredictWeight(input, priorityFactor)
	} else {
		calculatedWeight, ModelPredicted = smart.CalculateWeight(input, priorityFactor)
	}

	// a pinned group and a force-closed connection never reach the host checks, so their
	// snapshot is not read at all
	var state targetState
	if s.selected == "" && metadata.GetSmartBlock() != "blocked" && metadata.GetSmartBlock() != "degraded" {
		state = s.readTargetState(metadata, proxyName)
	}
	adjWeight, isDegraded, checked, blockCode := s.checkNodeQuality(state, metadata, proxy, connQuality{
		err:            err,
		address:        addressDisplay,
		weight:         calculatedWeight,
		oldWeight:      oldWeight,
		duration:       connectionDuration,
		uploadMB:       uploadTotalMB,
		downloadMB:     downloadTotalMB,
		lossRate:       lossRate,
		emaLossRate:    emaLossRate,
		strikeRate:     windowRate,
		strikeEvidence: windowEvidence,
	})

	// block node for the specific domain/IP (wildcardTarget + SmartTarget two-level records)
	var blockTTL time.Duration
	if blockCode == blockCodeStalled {
		blockTTL = stalledProbeTTL
	}
	failedBlock := s.markNodeFailure(metadata, proxyName, isDegraded, checked, blockCode, blockTTL)

	newWeight := updateEMA(oldWeight, adjWeight)
	atomicRecord.SetLastUsed(time.Now().Unix())
	atomicRecord.SetWeight(weightType, newWeight)
	statsSnapshot := atomicRecord.CreateStatsSnapshot(cacheKey)

	lock.Unlock()
	locked = false

	// loss needs a second strike; a stall is witnessed by the connection itself, and its
	// record stays host scoped: it neither closes the target's connections nor drops the pin
	needsConfirm := blockCode == blockCodeLoss || blockCode == blockCodeStalled
	if failedBlock || (isDegraded && !needsConfirm) {
		if failedBlock && blockCode == blockCodeLoss {
			log.Debugln("[Smart] Connection Group: [%s] - Node: [%s] - Address: [%s] repeated strike confirmed, node excluded for the target",
				s.Name(), proxyName, addressDisplay)
		}
		if !isUDP {
			s.closeSameConnection(metadata, proxyName, target, true)
		}
		s.store.DeleteUnwrapResult(s.Name(), s.configName, s.unwrapKey(metadata))
	}

	s.saveStatsRecord(target, proxy, statsSnapshot)

	if s.collectData {
		collectedWeight := adjWeight / priorityFactor
		if isDegraded || failedBlock {
			// forcefully adjust for abnormal connections so the model can recognize them during training
			if collectedWeight >= smart.AllowedWeight {
				collectedWeight = collectedWeight * 0.1
			} else {
				if collectedWeight == 0 {
					collectedWeight = smart.AllowedWeight * rand.Float64()
				}
			}
		}
		s.collectConnectionData(input, metadata, collectedWeight, proxyName, ModelPredicted)
	}

	// the event must always be published: an API log subscriber filters by its own level,
	// so a console level above debug does not mean the line is unwanted
	s.logConnectionStats(err, statsSnapshot, metadata, calculatedWeight/priorityFactor, priorityFactor, addressDisplay, proxyName,
		connectTime, latency, uploadTotalMB, downloadTotalMB, maxUploadRateKB, maxDownloadRateKB, connectionDuration, ModelPredicted, lossRate, cumulLossRate)
}

func (s *Smart) registerClosureMetricsCallback(c C.Conn, proxy C.Proxy, metadata *C.Metadata, connectTime int64, firstReadLatency *atomic.Int64, firstReadErr *atomic.TypedValue[error], observedReadErr *atomic.TypedValue[error], observedWriteErr *atomic.TypedValue[error]) C.Conn {
	return callback.NewCloseCallbackConn(c, func() {
		tracker := statistic.DefaultManager.Get(metadata.UUID)
		if tracker != nil {
			info := tracker.Info()
			uploadTotal := info.UploadTotal.Load()
			downloadTotal := info.DownloadTotal.Load()
			connectionDuration := time.Since(info.Start).Milliseconds()
			maxUploadRate := info.MaxUploadRate.Load()
			maxDownloadRate := info.MaxDownloadRate.Load()

			latency := firstReadLatency.Load()
			readErr := firstReadErr.Load()
			// the observer covers mid-transfer errors the one-shot callbacks miss
			if err := observedReadErr.Load(); err != nil {
				readErr = err
			}
			writeErr := observedWriteErr.Load()

			var tcpStats, windowStats *tcpstats.Stats
			if trackerConn, ok := tracker.(net.Conn); ok {
				stats, source := tcpstats.GetTCPStatsWithSource(trackerConn)
				tcpStats = stats
				windowStats = s.lossSampler.Consume(source, stats)
			}

			var closeErr error
			if readErr != nil {
				if readErr == io.EOF {
					if writeErr != nil && writeErr != io.EOF {
						closeErr = writeErr
					}
				} else {
					closeErr = readErr
				}
			}

			// a connection the group closed on purpose is not a transfer failure
			if metadata.GetSmartBlock() == "degraded" {
				closeErr = nil
			}

			go s.recordConnectionStats(metadata, proxy, connectTime, latency, uploadTotal, downloadTotal, maxUploadRate, maxDownloadRate, connectionDuration, tcpStats, windowStats, closeErr)
			return
		}
	})
}

func (s *Smart) registerPacketClosureMetricsCallback(pc C.PacketConn, proxy C.Proxy, metadata *C.Metadata, connectTime int64, udpLatency *atomic.Int64, observedReadErr *atomic.TypedValue[error], observedWriteErr *atomic.TypedValue[error]) C.PacketConn {
	return callback.NewCloseCallbackPacketConn(pc, func() {
		tracker := statistic.DefaultManager.Get(metadata.UUID)
		if tracker != nil {
			info := tracker.Info()
			uploadTotal := info.UploadTotal.Load()
			downloadTotal := info.DownloadTotal.Load()
			connectionDuration := time.Since(info.Start).Milliseconds()
			maxUploadRate := info.MaxUploadRate.Load()
			maxDownloadRate := info.MaxDownloadRate.Load()

			closeErr := observedReadErr.Load()
			if closeErr == nil {
				closeErr = observedWriteErr.Load()
			}
			// a connection the group closed on purpose is not a transfer failure
			if metadata.GetSmartBlock() == "degraded" {
				closeErr = nil
			}

			go s.recordConnectionStats(metadata, proxy, connectTime, udpLatency.Load(),
				uploadTotal, downloadTotal, maxUploadRate, maxDownloadRate, connectionDuration, nil, nil, closeErr)
			return
		}
	})
}

func (s *Smart) calcMADMetrics(delays []float64) (currentAnomaly bool, unstable bool, threshold float64, grade int) {
	calcGrade := func(t float64) int {
		if t <= 500 {
			return 1
		} else if t <= 1000 {
			return 2
		} else if t <= 2000 {
			return 3
		}
		return 4
	}

	n := len(delays)
	if n == 0 {
		return false, false, 0, 0
	}

	var median float64
	var mad float64
	var robustCV float64

	const scale = 1.4826
	const defaultK = 2.5
	const smallK = 3.5
	const cvThreshold = 0.6
	const minSamples = 3
	const SentinelThreshold = 0.5
	const sentinel = float64(0xffff)

	recentCount := minSamples
	if n < recentCount {
		recentCount = n
	}
	recentStart := n - recentCount
	recentSentinels := 0

	filtered := make([]float64, 0, n)
	for i, v := range delays {
		if v >= sentinel {
			if i >= recentStart {
				recentSentinels++
			}
			continue
		}
		filtered = append(filtered, v)
	}

	m := len(filtered)
	if m == 0 {
		return true, true, 0, 0
	}

	if float64(recentSentinels)/float64(recentCount) > SentinelThreshold {
		unstable = true
	}

	if m < minSamples {
		last := delays[n-1]
		currentAnomaly = last >= sentinel
		return currentAnomaly, unstable, 0, 0
	}

	sort.Float64s(filtered)

	if m%2 == 1 {
		median = filtered[m/2]
	} else {
		median = (filtered[m/2-1] + filtered[m/2]) / 2
	}

	devs := make([]float64, 0, m)
	for _, v := range filtered {
		devs = append(devs, math.Abs(v-median))
	}
	sort.Float64s(devs)

	if m%2 == 1 {
		mad = devs[m/2]
	} else {
		mad = (devs[m/2-1] + devs[m/2]) / 2
	}

	if mad == 0 {
		mean := 0.0
		for _, v := range filtered {
			mean += v
		}
		mean /= float64(m)
		varSum := 0.0
		for _, v := range filtered {
			d := v - mean
			varSum += d * d
		}
		std := math.Sqrt(varSum / float64(m))
		threshold = mean + 2*std
		last := delays[n-1]
		if last >= sentinel {
			currentAnomaly = true
		} else {
			currentAnomaly = last > threshold && delays[n-2] > threshold
		}

		return currentAnomaly, unstable, threshold, calcGrade(threshold)
	}

	k := defaultK
	if m < maxSelected {
		k = smallK
	}

	threshold = median + k*scale*mad

	if median > 0 {
		robustCV = scale * mad / median
	} else {
		robustCV = 0
	}

	last := delays[n-1]
	if last >= sentinel {
		currentAnomaly = true
	} else {
		currentAnomaly = last > threshold && delays[n-2] > threshold
	}

	if !unstable {
		unstable = robustCV >= cvThreshold
	}

	return currentAnomaly, unstable, threshold, calcGrade(threshold)
}

func (s *Smart) checkNodesStable() {
	if s.suppressStats.Load() {
		return
	}

	proxies := s.GetProxies(true)
	nodesToBlock := make(map[string]*smart.NodeState, len(proxies))
	now := time.Now().Unix()
	blockedUntil := time.Now().Add(checkInterval + 2*time.Minute).Unix()

	for _, p := range proxies {
		if !p.AliveForTestUrl(s.testUrl) {
			continue
		}

		histories := p.DelayHistoryForTestUrl(s.testUrl)
		if len(histories) == 0 {
			continue
		}

		delays := make([]float64, 0, len(histories))
		for _, h := range histories {
			if h.Delay == 0 {
				h.Delay = 0xffff
			}
			delays = append(delays, float64(h.Delay))
		}

		currentAnomaly, unstable, _, newGrade := s.calcMADMetrics(delays)

		proxyName := p.Name()
		var blockCopy *smart.NodeState
		blockSignals := ""
		s.store.UpdateNodeState(s.Name(), s.configName, proxyName, func(state *smart.NodeState) {
			prevGrade := state.ThresholdGrade
			state.LastChecked = now
			if newGrade > 0 {
				state.ThresholdGrade = newGrade
			}

			gradeWorsened := (prevGrade > 0 && newGrade > prevGrade) || (prevGrade <= 2 && newGrade > 2)
			if currentAnomaly || unstable || gradeWorsened {
				state.BlockedUntil = blockedUntil
				copied := *state
				blockCopy = &copied
				blockSignals = fmt.Sprintf("anomaly [%t], unstable [%t], grade [%d->%d]", currentAnomaly, unstable, prevGrade, newGrade)
			}
		})
		if blockCopy != nil {
			nodesToBlock[proxyName] = blockCopy
			log.Debugln("[Smart] Node [%s] blocked for [%s] on target [%s] (%s)",
				proxyName, (checkInterval + 2*time.Minute).String(), s.testUrl, blockSignals)
		}
	}

	if len(nodesToBlock) > 0 {
		s.store.UpdateBlockedNodesCache(s.Name(), s.configName, nodesToBlock)
	}
}

func (s *Smart) checkBlockedNodes() {
	stateData, err := s.store.GetNodeStates(s.Name(), s.configName)
	if err != nil {
		return
	}

	nodesToUpdate := make(map[string]*smart.NodeState)

	for nodeName, data := range stateData {
		var state smart.NodeState
		err := json.Unmarshal(data, &state)
		if err != nil {
			continue
		}

		if state.BlockedUntil > 0 && state.BlockedUntil <= time.Now().Unix() {
			s.store.UpdateNodeState(s.Name(), s.configName, nodeName, func(state *smart.NodeState) {
				state.BlockedUntil = 0
			})
			nodesToUpdate[nodeName] = &smart.NodeState{}
			log.Debugln("[Smart] Node [%s] block period expired, unblocking", nodeName)
		}
	}

	if len(nodesToUpdate) > 0 {
		s.store.UpdateBlockedNodesCache(s.Name(), s.configName, nodesToUpdate)
	}
}

// readTargetState reads one snapshot for the judgement of a finished connection: the host
// failure view of the target and the exit suspicion of the node.
func (s *Smart) readTargetState(metadata *C.Metadata, proxyName string) targetState {
	lookupTarget, extraTargets := failLookupTargets(metadata, metadata.NetWork == C.UDP)
	failNodes, blocked := s.store.GetHostStatus(s.Name(), s.configName, lookupTarget, int(s.hostFailLimit.Load()), extraTargets...)
	return targetState{
		failNodes: failNodes,
		blocked:   blocked,
		suspected: s.exitWatch.Suspected(metadata.WildcardTarget, proxyName),
	}
}

// checkNodeQuality turns one finished connection into its weight adjustment and the host
// record to write; the store and the exit trackers are read by the caller, so this only
// decides.
func (s *Smart) checkNodeQuality(state targetState, metadata *C.Metadata, proxy C.Proxy, q connQuality) (float64, bool, bool, int64) {
	if s.selected != "" {
		return q.weight, false, false, 0
	}

	proxyName := proxy.Name()

	// user manual block
	if metadata.GetSmartBlock() == "blocked" {
		log.Debugln("[Smart] Connection Group: [%s] - Node: [%s] - Network: [%s] - Address: [%s] detected manual block...",
			s.Name(), proxyName, metadata.NetWork.String(), q.address)
		return q.weight, true, true, blockCodeManual
	}

	// force-closed connection, skip quality check to avoid erroneous downgrade
	if metadata.GetSmartBlock() == "degraded" {
		return q.oldWeight, false, false, 0
	}

	if state.blocked {
		return q.weight, false, false, 0
	}

	if q.weight > 0 && q.weight < smart.AllowedWeight {
		return q.weight, true, true, blockCodeLowWeight
	}

	if q.err != nil {
		return q.weight, false, true, blockCodeError
	}

	if state.failNodes[proxyName] != 0 {
		return q.weight, false, false, 0
	}

	// probes need a Host, so a silent answer on an IP-only target counts on every port
	if q.duration > stalledConnectionGrace.Milliseconds() && q.uploadMB > 0 && q.downloadMB == 0 &&
		metadata.NetWork != C.UDP && (metadata.DstPort == 443 || metadata.Host == "") {
		log.Debugln("[Smart] Connection Group: [%s] - Node: [%s] - Network: [%s] - Address: [%s] stalled without a response...",
			s.Name(), proxyName, metadata.NetWork.String(), q.address)
		return q.weight, true, true, blockCodeStalled
	}

	// a suspected exit re-checks the suspicion: it carries traffic only when the rest failed;
	// a connection that downloaded almost nothing may have been answered by an error page
	eligible := smart.ResponseProbeEligible(metadata, q.downloadMB, metadata.NetWork == C.UDP)
	if state.suspected || eligible {
		s.probeAfterClose(metadata, proxy, eligible)
		return q.weight, false, false, 0
	}

	// a strike acts only on a judged window the sampler released; the decaying ema alone
	// must not confirm it
	if q.strikeEvidence && (q.strikeRate >= 0.1 || (q.strikeRate >= 0.05 && q.emaLossRate >= 0.05)) {
		log.Debugln("[Smart] Connection Group: [%s] - Node: [%s] - Network: [%s] - Address: [%s] detected high packet loss [window: %.2f%%, read: %.2f%%, history EMA: %.2f%%]...",
			s.Name(), proxyName, metadata.NetWork.String(), q.address, q.strikeRate*100, q.lossRate*100, q.emaLossRate*100)
		return q.weight, true, true, blockCodeLoss
	}

	return q.weight, false, false, 0
}

// markNodeFailure records a node failure for the host and for the target, otherwise every
// host of a rule set picks its own fallback node.
func (s *Smart) markNodeFailure(metadata *C.Metadata, proxyName string, isDegraded bool, checked bool, blockCode int64, ttl time.Duration) bool {
	isUDP := metadata.NetWork == C.UDP
	if isUDP && ttl == 0 {
		ttl = udpFailureTTL
	}
	wildcardTarget := smart.NetScopedTarget(metadata.WildcardTarget, isUDP)
	target := smart.NetScopedTarget(metadata.SmartTarget, isUDP)

	failedBlock := s.store.UpdateHostStatus(s.Name(), s.configName, wildcardTarget, metadata, proxyName, s.maxFailedTimes, int(s.hostFailLimit.Load()), isDegraded, checked, blockCode, ttl)

	hostEvidence := blockCode == blockCodeAnswer || blockCode == blockCodeStalled
	if (failedBlock || (isDegraded && !hostEvidence)) && target != "" && target != wildcardTarget {
		// a target level block must also drive the caller's close and pin drop
		if s.store.UpdateHostStatus(s.Name(), s.configName, target, metadata, proxyName, s.maxFailedTimes, int(s.hostFailLimit.Load()), isDegraded, checked, blockCode, ttl) {
			failedBlock = true
		}
	}

	return failedBlock
}

func (s *Smart) ruleExcluded(target, node string) bool {
	if target == "" {
		return false
	}
	failNodes, _ := s.store.GetHostStatus(s.Name(), s.configName, target, int(s.hostFailLimit.Load()))
	return failNodes[node] != 0
}

// closeSameConnection closes the other connections of the target so it keeps a single exit:
// force drops them all because the node was dropped, otherwise only the ones on another node.
func (s *Smart) closeSameConnection(metadata *C.Metadata, proxyName, target string, force bool) {
	statistic.DefaultManager.RangeSmartTarget(target, func(id string) bool {
		if id == metadata.UUID {
			return true
		}
		tracker := statistic.DefaultManager.Get(id)
		if tracker == nil {
			return true
		}
		if !lo.Contains(tracker.Chains(), s.Name()) {
			return true
		}
		if force {
			tracker.Info().Metadata.SetSmartBlock("degraded")
			_ = tracker.Close()
		} else if proxyName != "" && !lo.Contains(tracker.Chains(), proxyName) {
			_ = tracker.Close()
		}
		return true
	})
}

// applyNodeAnswer lands one probe answer: reachable clears the host failure and the node's
// exit suspicion, a refusal is dropped while the host is backed off, else recorded.
func (s *Smart) applyNodeAnswer(proxy C.Proxy, metadata *C.Metadata, target, node string, verdict smart.Verdict) (cleared bool) {
	switch verdict.Action {
	case smart.VerdictReachable:
		smart.NoteHostAnswer(s.configName, s.Name(), metadata.Host)
		s.markNodeFailure(metadata, node, false, true, 0, 0)
		s.noteExitSuccess(proxy, metadata.Host, target, node, verdict)
		cleared = s.exitWatch.Suspected(target, node)
		s.exitWatch.Clear(target, node)
	case smart.VerdictRecord:
		if smart.NoteHostRefusal(s.configName, s.Name(), metadata.Host, node, verdict.Reason) {
			return false
		}
		s.markNodeFailure(metadata, node, true, true, blockCodeAnswer, verdict.TTL)
		s.noteExitEvidence(proxy, metadata.Host, target, node, verdict.Reason)
	}
	return cleared
}

func (s *Smart) checkHostStatus() {
	proxies := s.GetProxies(false)
	proxyMap := make(map[string]C.Proxy, len(proxies))
	for _, p := range proxies {
		proxyMap[p.Name()] = p
	}

	toCheck, err := s.store.CheckHostStatus(s.Name(), s.configName, int(s.hostFailLimit.Load()))
	if err != nil {
		return
	}

	type checkItem struct {
		wildcardTarget string
		nodeName       string
		host           string
	}

	var items []checkItem
	for wildcardTarget, nodeMap := range toCheck {
		for nodeName, host := range nodeMap {
			items = append(items, checkItem{wildcardTarget, nodeName, host})
		}
	}

	var toProbe []checkItem
	for _, it := range items {
		if smart.HostBackedOff(s.configName, s.Name(), it.host) {
			continue
		}
		if rand.Float64() < 0.5 {
			toProbe = append(toProbe, it)
		}
	}
	if len(toProbe) == 0 {
		return
	}

	jobs := make(chan checkItem)
	var wg sync.WaitGroup
	for i := 0; i < parallelDials; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for it := range jobs {
				select {
				case <-s.ctx.Done():
					return
				default:
				}
				p, ok := proxyMap[it.nodeName]
				if !ok {
					continue
				}
				metadata := &C.Metadata{Host: it.host, WildcardTarget: it.wildcardTarget}
				verdict := s.probeVerdict(p, it.host)
				cleared := s.applyNodeAnswer(p, metadata, it.wildcardTarget, it.nodeName, verdict)
				switch verdict.Action {
				case smart.VerdictReachable:
					if cleared {
						log.Debugln("[Smart] Exit suspicion [%s] target [%s] cleared by node [%s] (recheck)", s.Name(), it.wildcardTarget, it.nodeName)
					}
					log.Debugln("[Smart] Recheck Group: [%s] - Node: [%s] - Host: [%s] recovered [%s]", s.Name(), it.nodeName, it.host, verdict.Reason)
				case smart.VerdictRecord:
					log.Debugln("[Smart] Recheck Group: [%s] - Node: [%s] - Host: [%s] avoided for [%s]: [%s]", s.Name(), it.nodeName, it.host, verdict.TTL, verdict.Reason)
				default:
					log.Debugln("[Smart] Recheck Group: [%s] - Node: [%s] - Host: [%s] left unchanged [%s]", s.Name(), it.nodeName, it.host, verdict.Reason)
				}
			}
		}()
	}

sendLoop:
	for _, it := range toProbe {
		select {
		case jobs <- it:
		case <-s.ctx.Done():
			break sendLoop
		}
	}
	close(jobs)
	wg.Wait()
}

// The verdict lands after its connection is gone, outside the (target, node) stats lock, so the pin,
// the membership and the stop-loss have to be re-checked before anything is written.
func (s *Smart) probeAfterClose(metadata *C.Metadata, proxy C.Proxy, eligible bool) {
	now := time.Now()
	if smart.HostBackedOff(s.configName, s.Name(), metadata.Host) {
		log.Debugln("[Smart] Probe skipped for host [%s] on node [%s], the host is backed off", metadata.Host, proxy.Name())
		return
	}
	if !s.probeThrottle.AllowNode(metadata.WildcardTarget, proxy.Name(), now) {
		if eligible {
			reason := ""
			if s.probeThrottle.PendingFor(metadata.WildcardTarget, proxy.Name(), now) > stalledProbeGrace {
				reason = "stalled probe"
			} else if s.probeThrottle.Unproven(metadata.WildcardTarget, proxy.Name()) {
				reason = "probe unproven"
			}
			if reason != "" {
				s.probeThrottle.ClearUnproven(metadata.WildcardTarget, proxy.Name())
				s.markNodeFailure(metadata, proxy.Name(), true, true, blockCodeStalled, stalledProbeTTL)
				if s.ruleExcluded(metadata.SmartTarget, proxy.Name()) {
					s.dropUnwrapPins(metadata.SmartTarget)
				}
				log.Debugln("[Smart] Probe Group: [%s] - Node: [%s] - Host: [%s] avoided for [%s]: [%s]", s.Name(), proxy.Name(), metadata.Host, stalledProbeTTL, reason)
			}
		}
		return
	}
	if !smart.AllowGlobalProbe(now) {
		return
	}
	done, ok := smart.TryStartProbe()
	if !ok {
		return
	}

	s.probeThrottle.MarkPending(metadata.WildcardTarget, proxy.Name(), now)
	clone := metadata.Clone()
	nodeName := proxy.Name()
	go func() {
		defer done()
		defer s.probeThrottle.ClearPending(clone.WildcardTarget, nodeName)

		verdict := s.probeVerdict(proxy, clone.Host)
		if verdict.Action == smart.VerdictIgnore {
			if verdict.Unproven {
				at := time.Now()
				s.probeThrottle.MarkUnproven(clone.WildcardTarget, nodeName, at)
				s.probeThrottle.NoteFailure(clone.WildcardTarget, nodeName, at)
			}
			log.Debugln("[Smart] Probe Group: [%s] - Node: [%s] - Host: [%s] ignored answer [%s]", s.Name(), nodeName, clone.Host, verdict.Reason)
			return
		}
		s.probeThrottle.ClearUnproven(clone.WildcardTarget, nodeName)
		if s.selected != "" || !lo.ContainsBy(s.GetProxies(false), func(p C.Proxy) bool { return p.Name() == nodeName }) {
			return
		}
		if _, wtBlocked := s.store.GetHostStatus(s.Name(), s.configName, clone.WildcardTarget, int(s.hostFailLimit.Load()), clone.SmartTarget); wtBlocked {
			return
		}

		if verdict.Action == smart.VerdictReachable {
			if cleared := s.applyNodeAnswer(proxy, clone, clone.WildcardTarget, nodeName, verdict); cleared {
				log.Debugln("[Smart] Exit suspicion [%s] target [%s] cleared by node [%s]", s.Name(), clone.WildcardTarget, nodeName)
			}
			log.Debugln("[Smart] Probe Group: [%s] - Node: [%s] - Host: [%s] recovered [%s]", s.Name(), nodeName, clone.Host, verdict.Reason)
			return
		}

		s.probeThrottle.NoteFailure(clone.WildcardTarget, nodeName, time.Now())
		s.applyNodeAnswer(proxy, clone, clone.WildcardTarget, nodeName, verdict)
		if s.ruleExcluded(clone.SmartTarget, nodeName) {
			s.closeSameConnection(clone, nodeName, clone.SmartTarget, true)
			s.dropUnwrapPins(clone.SmartTarget)
		}
		log.Debugln("[Smart] Probe Group: [%s] - Node: [%s] - Host: [%s] avoided for [%s]: [%s]", s.Name(), nodeName, clone.Host, verdict.TTL, verdict.Reason)
	}()
}

func (s *Smart) noteExitEvidence(proxy C.Proxy, host, target, node, reason string) {
	if smart.RegionEvidence(reason) {
		s.exitWatch.Note(target, node)
		s.exitWatch.MaybeProbeTarget(s.ctx, proxy, target, node, host)
	}
}

func (s *Smart) noteExitSuccess(proxy C.Proxy, host, target, node string, verdict smart.Verdict) {
	if verdict.ControlSuccess {
		s.exitWatch.NoteSuccess(target, node)
		s.exitWatch.MaybeProbeTarget(s.ctx, proxy, target, node, host)
	}
}

func (s *Smart) probeVerdict(proxy C.Proxy, host string) smart.Verdict {
	prober, ok := proxy.(smart.StatusProber)
	if !ok {
		return smart.Verdict{Action: smart.VerdictIgnore, Reason: "no probe support"}
	}

	parent := s.ctx
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, smart.ProbeTimeout)
	defer cancel()

	result, err := prober.StatusProbe(ctx, smart.ProbeURL(host))
	if err != nil {
		return smart.ClassifyProbeError(err)
	}
	return smart.ClassifyResponse(result.StatusCode, result.Header, result.Body, time.Now())
}

func (s *Smart) InitSmart() {
	s.store = cachefile.GetSmartStore()

	s.ctx, s.cancel = context.WithCancel(context.Background())

	s.exitWatch = smart.NewExitWatcher(smart.ExitWatcherOptions{
		Name:    s.Name(),
		Config:  s.configName,
		Store:   s.store,
		WantASN: func() bool { return s.preferASN },
	})

	smartInitOnce.Do(func() {
		s.startTimedTask(5*time.Minute, checkInterval, "Global orphaned groups Clean up", s.cleanupOrphanedGroups, true)
		s.startTimedTask(5*time.Second, cacheParamAdjustInterval, "Global cache parameters adjustment", s.store.AdjustCacheParameters, false)
		s.startTimedTask(5*time.Minute, flushQueueInterval, "Global queues flush", func() {
			s.store.FlushQueue(true)
		}, false)
		// the database belongs to the process, so one load serves every group
		if s.preferASN {
			if err := geodata.InitASN(); err != nil {
				log.Warnln("[Smart] Failed to load ASN database: %v", err)
			}
		}
	})

	s.startTimedTask(10*time.Minute, cleanupInterval, "Group orphaned nodes clean up", s.cleanupOrphanedNodeCache, true)
	s.startTimedTask(5*time.Minute, prefetchInterval, "Group targets prefetch", s.runPrefetch, false)
	s.startTimedTask(1*time.Minute, claimInterval, "Group network claims refresh", s.claimASNEvidence, false)
	s.startTimedTask(5*time.Minute, checkInterval, "Group nodes stable check", s.checkNodesStable, false)
	s.startTimedTask(5*time.Minute, rankingInterval, "Group nodes Ranking", s.updateNodeRanking, false)
	s.startTimedTask(5*time.Minute, recoveryCheckInterval, "Group nodes recovery check", s.checkBlockedNodes, false)
	s.startTimedTask(15*time.Minute, hostStatusCheckInterval, "Group host status check", s.checkHostStatus, false)
	s.startTimedTask(1*time.Minute, prefetchInterval, "Group hostFailLimit refresh", s.applyHostFailLimit, false)
	s.startTimedTask(10*time.Minute, cleanupInterval, "Group old records clean up", func() {
		s.store.CleanupOldRecords(s.Name(), s.configName)
	}, false)
	if s.collectData {
		s.dataCollector = lightgbm.GetCollector()
	}

	if s.useLightGBM {
		s.weightModel = lightgbm.GetModel()
	}
}

// startTimedTask waits for tunnel.Running before the first run, then repeats on its interval.
func (s *Smart) startTimedTask(initialDelay, interval time.Duration, taskName string, task func(), runOnce bool) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		waitTicker := time.NewTicker(100 * time.Millisecond)
		for tunnel.Status() != tunnel.Running {
			select {
			case <-waitTicker.C:
			case <-s.ctx.Done():
				waitTicker.Stop()
				return
			}
		}
		waitTicker.Stop()

		jitterRange := 30.0
		intervalJitter := time.Duration(rand.Float64() * jitterRange * float64(time.Second))

		adjustedInitialDelay := initialDelay + intervalJitter
		adjustedInterval := interval + intervalJitter

		select {
		case <-time.After(adjustedInitialDelay):
		case <-s.ctx.Done():
			return
		}

		if tunnel.Status() == tunnel.Running {
			task()
		}

		if runOnce {
			log.Debugln("[Smart] Task [%s] completed", taskName)
			return
		}
		log.Debugln("[Smart] Task [%s] for group [%s] started, interval: %s", taskName, s.Name(), adjustedInterval.Round(time.Second).String())

		ticker := time.NewTicker(adjustedInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if tunnel.Status() == tunnel.Running {
					task()
				}
			case <-s.ctx.Done():
				return
			}
		}
	}()
}

func (s *Smart) runPrefetch() {
	proxies := s.GetProxies(true)
	proxyMap := make(map[string]bool, len(proxies))
	for _, proxy := range proxies {
		proxyMap[proxy.Name()] = true
	}
	s.store.RunPrefetch(s.Name(), s.configName, proxyMap)
}

func (s *Smart) updateNodeRanking() {
	proxies := s.GetProxies(true)
	rankingWrapper, _ := s.store.GetNodeWeightRankingCache(s.Name(), s.configName)

	if len(rankingWrapper.Result) > 0 {
		now := time.Now().Unix()
		lastUpdated := rankingWrapper.LastUpdated
		cacheAge := time.Duration(now-lastUpdated) * time.Second

		if cacheAge < 30*time.Minute {
			rankedNodes := make(map[string]bool, len(rankingWrapper.Result))
			for _, r := range rankingWrapper.Result {
				rankedNodes[r.Name] = true
			}
			hasUnrankedProxy := false
			for _, p := range proxies {
				if !rankedNodes[p.Name()] {
					hasUnrankedProxy = true
					break
				}
			}

			if !hasUnrankedProxy {
				if cacheAge <= 10*time.Minute {
					return
				}
				proxyMap := make(map[string]C.Proxy, len(proxies))
				for _, p := range proxies {
					proxyMap[p.Name()] = p
				}
				hasDeadRankedNode := false
				for _, r := range rankingWrapper.Result {
					if r.Rank != smart.RankRarelyUsed {
						if p, exists := proxyMap[r.Name]; exists {
							if !p.AliveForTestUrl(s.testUrl) {
								hasDeadRankedNode = true
								break
							}
						}
					}
				}
				if !hasDeadRankedNode {
					return
				}
			}
		}
	}

	log.Debugln("[Smart] Starting node ranking update for policy group [%s]", s.Name())

	rankingWrapper, err := s.store.GetNodeWeightRanking(s.Name(), s.configName, s.testUrl, proxies)
	if err != nil {
		log.Warnln("[Smart] Failed to update node ranking: %v", err)
		return
	}
	if len(rankingWrapper.Result) == 0 {
		log.Debugln("[Smart] Policy group [%s] doesn't have enough data to generate node ranking", s.Name())
		return
	}
	categoryCounts := make(map[string]int)
	for _, rank := range rankingWrapper.Result {
		categoryCounts[rank.Rank]++
	}

	log.Debugln("[Smart] Policy group [%s] node ranking update completed: %d nodes total (%s: %d, %s: %d, %s: %d)",
		s.Name(), len(rankingWrapper.Result),
		smart.RankMostUsed, categoryCounts[smart.RankMostUsed],
		smart.RankOccasional, categoryCounts[smart.RankOccasional],
		smart.RankRarelyUsed, categoryCounts[smart.RankRarelyUsed])
}

func (s *Smart) cleanupOrphanedGroups() {
	allProxies := tunnel.Proxies()
	existingSmartGroups := make(map[string]bool)

	for name, proxy := range allProxies {
		if proxy.Type() == C.Smart {
			existingSmartGroups[name] = true
		}
	}

	cachedGroups, err := s.store.GetAllGroupsForConfig(s.configName)
	if err != nil {
		return
	}

	var orphanedGroups []string
	for _, groupName := range cachedGroups {
		if !existingSmartGroups[groupName] {
			orphanedGroups = append(orphanedGroups, groupName)
		}
	}

	if len(orphanedGroups) > 0 {
		for _, group := range orphanedGroups {
			log.Debugln("[Smart] Cleaning up cache data for non-existent policy group [%s]", group)
			err := s.store.FlushByGroup(group, s.configName)
			if err != nil {
				log.Warnln("[Smart] Failed to clean up policy group [%s] cache: %v", group, err)
			}
		}
	}
}

func (s *Smart) cleanupOrphanedNodeCache() {
	proxies := s.GetProxies(true)
	proxyMap := make(map[string]bool, len(proxies))
	for _, proxy := range proxies {
		proxyMap[proxy.Name()] = true
	}

	cachedNodes, err := s.store.GetAllNodesForGroup(s.Name(), s.configName)
	if err != nil {
		return
	}

	var orphanedNodes []string
	for _, nodeName := range cachedNodes {
		if !proxyMap[nodeName] {
			orphanedNodes = append(orphanedNodes, nodeName)
		}
	}

	if len(orphanedNodes) > 0 {
		for _, node := range orphanedNodes {
			log.Debugln("[Smart] Cleaning up cache data for non-existent node [%s]", node)
		}

		err := s.store.RemoveNodesData(s.Name(), s.configName, int(s.hostFailLimit.Load()), orphanedNodes)
		if err != nil {
			log.Warnln("[Smart] Failed to clean up non-existent node caches: %v", err)
		}
	}
}

func (s *Smart) Close() error {
	if s.cancel != nil {
		s.cancel()
	}

	s.wg.Wait()

	if !flushQueueOnce.Swap(true) {
		s.store.FlushQueue(true)
	}

	lightgbm.CloseAllCollectors()

	smartInitOnce = sync.Once{}

	return nil
}
