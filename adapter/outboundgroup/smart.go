package outboundgroup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/netip"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dlclark/regexp2"
	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/common/callback"
	"github.com/metacubex/mihomo/common/xsync"
	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/component/geodata"
	"github.com/metacubex/mihomo/component/mmdb"
	"github.com/metacubex/mihomo/component/profile/cachefile"
	"github.com/metacubex/mihomo/component/resolver"
	"github.com/metacubex/mihomo/component/smart"
	"github.com/metacubex/mihomo/component/smart/lightgbm"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/constant/provider"
	"github.com/metacubex/mihomo/log"
	"github.com/metacubex/mihomo/tunnel"
	"github.com/metacubex/mihomo/tunnel/statistic"
	"github.com/samber/lo"
)

const (
	cleanupInterval          = 120 * time.Minute
	cacheParamAdjustInterval = 5 * time.Minute
	recoveryCheckInterval    = 10 * time.Minute
	hostStatusCheckInterval  = 30 * time.Minute
	checkInterval            = 10 * time.Minute
	prefetchInterval         = 15 * time.Minute
	flushQueueInterval       = 5 * time.Minute
	rankingInterval          = 5 * time.Minute

	maxRetries               = 3
	maxSelected              = 10

	parallelDials            = 5
	connectThreshold         = 5.0
)

var (
	flushQueueOnce       atomic.Bool
	smartInitOnce        sync.Once
)

type smartOption func(*Smart)

type Smart struct {
	*GroupBase
	store                  *smart.Store

	wg                     sync.WaitGroup
	ctx                    context.Context
	cancel                 context.CancelFunc

	configName             string
	selected               string
	testUrl                string
	expectedStatus         string
	disableUDP             bool

	dataCollector          *lightgbm.DataCollector
	weightModel            *lightgbm.WeightModel
	policyPriority         []priorityRule
	typePriority         map[string]float64
	priorityCache          xsync.Map[string, float64]
	sampleRate             float64
	useLightGBM            bool
	collectData            bool
	preferASN	           bool
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

type nodeWithWeight struct {
	node   string
	weight float64
}

func getConfigFilename() string {
	configFile := C.Path.Config()
	baseName := filepath.Base(configFile)
	filename := strings.TrimSuffix(baseName, filepath.Ext(baseName))
	return filename
}

func NewSmart(option *GroupCommonOption, emptyFallback C.Proxy, providers []provider.ProxyProvider, options ...smartOption) (*Smart, error) {
	if option.URL == "" {
		option.URL = C.DefaultTestURL
	}

	configName := getConfigFilename()

	s := &Smart{
		GroupBase: NewGroupBase(GroupBaseOption{
			Name:            option.Name,
			Type:            C.Smart,
			Hidden:          option.Hidden,
			Icon:            option.Icon,
			Filter:          option.Filter,
			ExcludeFilter:   option.ExcludeFilter,
			ExcludeType:     option.ExcludeType,
			TestTimeout:     option.TestTimeout,
			MaxFailedTimes:  option.MaxFailedTimes,
			EmptyFallback:   emptyFallback,
			Providers:       providers,
		}),
		testUrl:              option.URL,
		expectedStatus:       option.ExpectedStatus,
		configName:           configName,
		disableUDP:           option.DisableUDP,
		policyPriority:       make([]priorityRule, 0),
		sampleRate:           1,
	}

	for _, option := range options {
		option(s)
	}

	s.InitSmart()

	return s, nil
}

func (s *Smart) GetConfigFilename() string {
	return s.configName
}

// ref: component/dialer/dialer.go:314
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
		// err if ShouldStopRetry should not record as failed in node stats and stop retry
		if tunnel.ShouldStopRetry(err) {
			return nil, connectTime, err
		}
		if !errors.Is(err, context.Canceled) {
			go s.recordConnectionStats("failed", metadata, proxy, connectTime, 0, 0, 0, 0, 0, 0, err)
		}
		return nil, connectTime, err
	}

	return c, connectTime, nil
}

func (s *Smart) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	getBatch := func(proxies []C.Proxy, i int) ([]C.Proxy, time.Duration) {
		var batch []C.Proxy
		var historyConnectTime int64
		var timeout time.Duration
		if len(proxies) == 1 {
			batch = proxies[0:1]
		} else if i == 0 {
			batch = proxies[0:1]
		} else {
			begin := 1 + (i-1) * parallelDials
			if begin >= len(proxies) {
				return nil, 0
			}
			end := begin + parallelDials
			if end > len(proxies) {
				end = len(proxies)
			}
			batch = proxies[begin:end]
		}

		for _, p := range batch {
			hct := s.getHistoryConnectStats(metadata, p)
			if hct > historyConnectTime {
				historyConnectTime = hct
			}
		}

		if historyConnectTime > 0 {
			timeout = time.Duration(float64(historyConnectTime) * connectThreshold) * time.Millisecond
		}

		if timeout > C.DefaultTCPTimeout || timeout <= 0 {
			timeout = C.DefaultTCPTimeout
		}

		return batch, timeout
	}

	tryDial := func(proxies []C.Proxy, asnNumber string) (C.Conn, error) {
		var finalErr error
		for i := 0; i < maxRetries; i++ {
			batch, timeout := getBatch(proxies, i)
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
				s.store.StoreUnwrapResult(s.Name(), s.configName, metadata.SmartTarget, asnNumber, metadata.NetWork == C.UDP, []C.Proxy{p})
				return s.WrapConnWithMetric(c, p, metadata, connectTime), nil
			}
		}

		if len(proxies) == 1 {
			s.store.DeleteUnwrapResult(s.Name(), s.configName, metadata.SmartTarget, asnNumber, metadata.NetWork == C.UDP)
		}

		return nil, finalErr
	}

	proxies, asnNumber := s.selectProxies(metadata, s.GetProxies(true))
	if len(proxies) == 0 {
		return s.EmptyFallback().DialContext(ctx, metadata)
	}
	return tryDial(proxies, asnNumber)
}

func (s *Smart) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (pc C.PacketConn, err error) {
	var finalErr error

	proxies, asnNumber := s.selectProxies(metadata, s.GetProxies(true))
	if len(proxies) == 0 {
		return s.EmptyFallback().ListenPacketContext(ctx, metadata)
	}
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
			historyConnectTime := s.getHistoryConnectStats(metadata, proxy)
			var timeout time.Duration
			if historyConnectTime > 0 {
				timeout = time.Duration(float64(historyConnectTime)*connectThreshold) * time.Millisecond
			}
			if timeout > C.DefaultUDPTimeout || timeout <= 0 {
				timeout = C.DefaultUDPTimeout
			}
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
				go s.recordConnectionStats("failed", metadata, proxy, connectTime, 0, 0, 0, 0, 0, 0, err)
				continue
			}

			s.store.StoreUnwrapResult(s.Name(), s.configName, metadata.SmartTarget, asnNumber, metadata.NetWork == C.UDP, []C.Proxy{proxy})
			return s.WrapPacketConnWithMetric(pc, proxy, metadata, connectTime), nil
		}

		if singleProxyRetry {
			s.store.DeleteUnwrapResult(s.Name(), s.configName, metadata.SmartTarget, asnNumber, metadata.NetWork == C.UDP)
			break
		}
	}

	return nil, finalErr
}

func (s *Smart) Unwrap(metadata *C.Metadata, touch bool) C.Proxy {
	proxies := s.GetProxies(touch)

	if metadata == nil {
		if len(proxies) > 0 {
			return proxies[0]
		}
		return s.EmptyFallback()
	}

	if s.selected != "" {
		for _, p := range proxies {
			if p.Name() == s.selected {
				return p
			}
		}
	}

	proxies, _ = s.selectProxies(metadata, proxies)
	if len(proxies) == 0 {
		return s.EmptyFallback()
	}
	return proxies[0]
}

func (s *Smart) IsL3Protocol(metadata *C.Metadata) bool {
	return s.Unwrap(metadata, false).IsL3Protocol(metadata)
}

func (s *Smart) SupportUDP() bool {
	return !s.disableUDP
}

func (s *Smart) WrapConnWithMetric(c C.Conn, proxy C.Proxy, metadata *C.Metadata, connectTime int64) C.Conn {
	c.AppendToChains(s)

	start := time.Now()

	var firstWriteErr atomic.TypedValue[error]
	var firstReadErr atomic.TypedValue[error]
	var firstReadLatency atomic.Int64

	if N.NeedHandshake(c) {
		c = callback.NewFirstWriteCallBackConn(c, func(err error) {
			if err != nil {
				firstWriteErr.Store(err)
			}
		})
	}

	c = callback.NewFirstReadCallBackConn(c, func(err error) {
		firstReadLatency.Store(time.Since(start).Milliseconds())
		if err != nil {
			firstReadErr.Store(err)
		}
	})

	return s.registerClosureMetricsCallback(
		c, proxy, metadata, connectTime,
		&firstReadLatency, &firstReadErr, &firstWriteErr,
	)
}

func (s *Smart) WrapPacketConnWithMetric(pc C.PacketConn, proxy C.Proxy, metadata *C.Metadata, connectTime int64) C.PacketConn {
	pc.AppendToChains(s)
	
	var udpLatency atomic.Int64

	pc = callback.NewFirstReadCallBackPacketConn(pc, func(latency int64) {
		udpLatency.Store(latency)
	})

	return s.registerPacketClosureMetricsCallback(pc, proxy, metadata, connectTime, &udpLatency)
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
	
	var typePriorityBuf strings.Builder
	typeCount := 0
	for t, f := range s.typePriority {
		if typeCount > 0 {
			typePriorityBuf.WriteByte(';')
		}
		fmt.Fprintf(&typePriorityBuf, "%s:%.2f", t, f)
		typeCount++
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
	})
}

func (s *Smart) Providers() []provider.ProxyProvider {
	return s.providers
}

func (s *Smart) Proxies() []C.Proxy {
	return s.GetProxies(false)
}

func (s *Smart) filterProxies(metadata *C.Metadata, wildcardTarget string, names []string, weights []float64, all []C.Proxy, minCount int, isUDP bool) []C.Proxy {
	blockedNodes := s.store.GetBlockedNodes(s.Name(), s.configName)
	wtFailNodes, _, _, wtBlocked := s.store.GetHostStatus(s.Name(), s.configName, wildcardTarget)

	var proxyByName map[string]C.Proxy
	if len(names) > 0 {
		proxyByName = make(map[string]C.Proxy, len(all))
		for _, p := range all {
			proxyByName[p.Name()] = p
		}
	}

	selected := make([]C.Proxy, 0, minCount+1)
	for i, name := range names {
		proxy := proxyByName[name]
		if proxy != nil && !blockedNodes[name] && (wtBlocked || !wtFailNodes[name]) && proxy.AliveForTestUrl(s.testUrl) && (!isUDP || proxy.SupportUDP()) {
			w := 0.0
			if weights != nil && i < len(weights) {
				w = weights[i]
			}
			if weights == nil || w >= smart.AllowedWeight {
				selected = append(selected, proxy)
			}
		}
	}

	// Unwrap result should not filled
	if weights == nil && len(selected) > 0 {
		return selected
	}

	if len(selected) >= len(all) {
		return selected
	}

	if len(selected) >= minCount {
		return selected[:minCount]
	}

	hasPriority := len(s.policyPriority) > 0 || len(s.typePriority) > 0

	type sortKey struct {
		delay  uint16
		factor float64
	}
	allKeys := make(map[string]sortKey, len(all))
	for _, p := range all {
		name := p.Name()
		k := sortKey{delay: p.LastDelayForTestUrl(s.testUrl)}
		if hasPriority {
			k.factor = s.getPriorityFactor(name)
		}
		allKeys[name] = k
	}

	defaultSort := func(proxies []C.Proxy) []C.Proxy {
		sort.Slice(proxies, func(i, j int) bool {
			ni, nj := proxies[i].Name(), proxies[j].Name()
			ki, kj := allKeys[ni], allKeys[nj]
			if hasPriority && ki.factor != kj.factor {
				return ki.factor > kj.factor
			}
			if ki.delay != kj.delay {
				return ki.delay < kj.delay
			}
			return ni < nj
		})
		return proxies
	}

	filteredAll := make([]C.Proxy, 0, len(all))

	checkNodeUsed := make(map[string]bool, len(names)+len(wtFailNodes))
	for _, name := range names {
		checkNodeUsed[name] = true
	}
	if len(wtFailNodes) > 0 && !wtBlocked {
		for name := range wtFailNodes {
			checkNodeUsed[name] = true
		}
	}

	for _, p := range all {
		name := p.Name()
		if checkNodeUsed[name] {
			continue
		}
		if blockedNodes[name] {
			continue
		}
		if !p.AliveForTestUrl(s.testUrl) || (isUDP && !p.SupportUDP()) {
			continue
		}
		filteredAll = append(filteredAll, p)
	}

	filteredAll = defaultSort(filteredAll)

	if !hasPriority {
		if len(filteredAll) < len(all)/3 {
			rand.Shuffle(len(filteredAll), func(i, j int) {
				filteredAll[i], filteredAll[j] = filteredAll[j], filteredAll[i]
			})
		}
	}

	var prependProxy C.Proxy
	var hasPrepend bool

	for _, p := range filteredAll {
		if !hasPrepend && (len(selected) < minCount/2 || len(wtFailNodes) <= 0 || wtBlocked) {
			prependProxy = p
			hasPrepend = true
		} else {
			selected = append(selected, p)
		}
		total := len(selected)
		if hasPrepend {
			total++
		}
		if total >= minCount {
			break
		}
	}
	if hasPrepend {
		selected = append(selected, nil)
		copy(selected[1:], selected)
		selected[0] = prependProxy
		if len(selected) > minCount {
			selected = selected[:minCount]
		}
	}

	if len(selected) == 0 {
		fallbackAll := defaultSort(all)
		for _, p := range fallbackAll {
			if p.AliveForTestUrl(s.testUrl) {
				selected = append(selected, p)
			}
			if len(selected) >= minCount {
				break
			}
		}

		if len(selected) == 0 {
			return nil
		}
	}

	return selected
}

// 节点选择
func (s *Smart) selectProxies(metadata *C.Metadata, proxies []C.Proxy) ([]C.Proxy, string) {
	// 添加ASN信息
	asnNumber := s.getASNCode(metadata)
	wildcardTarget := smart.GetEffectiveTarget(metadata.Host, metadata.DstIP.String())
	if metadata.SmartTarget == "" {
		metadata.SmartTarget = wildcardTarget
	}

	if s.selected != "" {
		for _, p := range proxies {
			if p.Name() == s.selected {
				return []C.Proxy{p}, asnNumber
			}
		}
	}

	trySelector := func(isUDP bool) ([]string, []float64) {
		// 检查匹配缓存
		if proxiesName := s.store.GetUnwrapResult(s.Name(), s.configName, metadata.SmartTarget, asnNumber, isUDP); len(proxiesName) > 0 {
			return proxiesName, nil
		}

		// 检查预解析缓存
		if proxiesName, weights := s.store.GetPrefetchResult(s.Name(), s.configName, metadata.SmartTarget, asnNumber, isUDP); len(proxiesName) > 0 {
			return proxiesName, weights
		}

		// 实时计算最佳节点
		if proxiesName, weights, err := s.store.GetBestProxyForTarget(s.Name(), s.configName, metadata.SmartTarget, asnNumber, isUDP); err == nil && len(proxiesName) > 0 {
			return proxiesName, weights
		}

		return nil, nil
	}

	isUDP := metadata.NetWork == C.UDP
	resultNames, resultWeights := trySelector(isUDP)
	result := s.filterProxies(metadata, wildcardTarget, resultNames, resultWeights, proxies, maxSelected, isUDP)

	return result, asnNumber
}

func (s *Smart) InitSmart() {
	s.store = cachefile.GetSmartStore()

	s.ctx, s.cancel = context.WithCancel(context.Background())

	smartInitOnce.Do(func() {
		s.startTimedTask(5*time.Minute, checkInterval, "Global orphaned groups Clean up", s.cleanupOrphanedGroups, true)
		s.startTimedTask(5*time.Second, cacheParamAdjustInterval, "Global cache parameters adjustment", s.store.AdjustCacheParameters, false)
		s.startTimedTask(5*time.Minute, flushQueueInterval, "Global queues flush", func() {
			s.store.FlushQueue(true)
		}, false)
		// try load ASN database
		if s.preferASN {
			if err := geodata.InitASN(); err != nil {
				log.Warnln("[Smart] Failed to load ASN database: %v", err)
			}
		}
	})

	s.startTimedTask(10*time.Minute, cleanupInterval, "Group orphaned nodes clean up", s.cleanupOrphanedNodeCache, true)
	s.startTimedTask(5*time.Minute, prefetchInterval, "Group targets prefetch", s.runPrefetch, false)
	s.startTimedTask(5*time.Minute, checkInterval, "Group nodes stable check", s.checkNodesStable, false)
	s.startTimedTask(5*time.Minute, rankingInterval, "Group nodes Ranking", s.updateNodeRanking, false)
	s.startTimedTask(5*time.Minute, recoveryCheckInterval, "Group nodes recovery check", s.checkBlockedNodes, false)
	s.startTimedTask(15*time.Minute, hostStatusCheckInterval, "Group host status check", s.checkHostStatus, false)
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

// task run after tunnel.Running
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
		} else {
			log.Debugln("[Smart] Task [%s] for group [%s] started, interval: %s", taskName, s.Name(), adjustedInterval.Round(time.Second).String())
		}

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
		cacheAge := time.Duration(now - lastUpdated) * time.Second

		if cacheAge < 30 * time.Minute {
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
				if cacheAge <= 10 * time.Minute {
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

		err := s.store.RemoveNodesData(s.Name(), s.configName, orphanedNodes)
		if err != nil {
			log.Warnln("[Smart] Failed to clean up non-existent node caches: %v", err)
		}
	}
}

// 获取历史 connectTime
func (s *Smart) getHistoryConnectStats(metadata *C.Metadata, proxy C.Proxy) int64 {
	target := metadata.SmartTarget
	proxyName := proxy.Name()
	cacheKey := smart.FormatDBKey(smart.KeyTypeStats, s.configName, s.Name(), target, proxyName)
	atomicRecord := s.store.GetOrCreateAtomicRecord(cacheKey, s.Name(), s.configName, target, proxyName)
	return atomicRecord.Get("connectTime").(int64)
}

// 连接持续时间更新
func (s *Smart) updateConnectionDuration(record *smart.AtomicStatsRecord, connectionDuration int64) {
	durationMinutes := float64(connectionDuration) / 60000.0
	currentDuration := record.Get("duration").(float64)

	if currentDuration > 0 {
		record.Set("duration", (currentDuration+durationMinutes)/2.0)
	} else {
		record.Set("duration", durationMinutes)
	}
}

// 记录保存
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

	if float64(recentSentinels) / float64(recentCount) > SentinelThreshold {
		unstable = true
	}

	if m < minSamples {
		last := delays[n - 1]
		currentAnomaly = last >= sentinel
		return currentAnomaly, unstable, 0, 0
	}

	sort.Float64s(filtered)

	if m % 2 == 1 {
		median = filtered[m / 2]
	} else {
		median = (filtered[m / 2 - 1] + filtered[m / 2]) / 2
	}

	devs := make([]float64, 0, m)
	for _, v := range filtered {
		devs = append(devs, math.Abs(v - median))
	}
	sort.Float64s(devs)

	if m % 2 == 1 {
		mad = devs[m / 2]
	} else {
		mad = (devs[m / 2 - 1] + devs[m / 2]) / 2
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
		threshold = mean + 2 * std
		last := delays[n - 1]
		if last >= sentinel {
			currentAnomaly = true
		} else {
			currentAnomaly = last > threshold && delays[n - 2] > threshold
		}

		return currentAnomaly, unstable, threshold, calcGrade(threshold)
	}

	k := defaultK
	if m < maxSelected {
		k = smallK
	}

	threshold = median + k * scale * mad

	if median > 0 {
		robustCV = scale * mad / median
	} else {
		robustCV = 0
	}

	last := delays[n - 1]
	if last >= sentinel {
		currentAnomaly = true
	} else {
		currentAnomaly = last > threshold && delays[n - 2] > threshold
	}

	if !unstable {
		unstable = robustCV >= cvThreshold
	}

	return currentAnomaly, unstable, threshold, calcGrade(threshold)
}

func (s *Smart) checkNodesStable() {
	proxies := s.GetProxies(true)
	operations := make([]smart.StoreOperation, 0, len(proxies))
	nodesToBlock := make(map[string]*smart.NodeState, len(proxies))
	now := time.Now().Unix()
	blockedUntil := time.Now().Add(checkInterval + 2 * time.Minute).Unix()

	nodeStateData, _ := s.store.GetNodeStates(s.Name(), s.configName)

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
		var state smart.NodeState
		if data, exists := nodeStateData[proxyName]; exists {
			json.Unmarshal(data, &state)
		}
		prevGrade := state.ThresholdGrade
		state.Name = proxyName
		state.LastChecked = now
		if newGrade > 0 {
			state.ThresholdGrade = newGrade
		}

		gradeDecreased := (prevGrade > 0 && newGrade > 0 && newGrade > prevGrade) || newGrade > 2
		if currentAnomaly || unstable || gradeDecreased {
			state.BlockedUntil = blockedUntil
			blockCopy := state
			nodesToBlock[proxyName] = &blockCopy
		}

		data, err := json.Marshal(&state)
		if err != nil {
			continue
		}
		operations = append(operations, smart.StoreOperation{
			Type:   smart.OpSaveNodeState,
			Group:  s.Name(),
			Config: s.configName,
			Node:   proxyName,
			Data:   data,
		})
	}

	if len(operations) > 0 {
		s.store.AppendToGlobalQueue(operations...)
	}
	if len(nodesToBlock) > 0 {
		s.store.UpdateBlockedNodesCache(s.Name(), s.configName, nodesToBlock)
	}
}

// 检查节点屏蔽状态
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

		if state.BlockedUntil > 0 && state.BlockedUntil > time.Now().Unix() {
			state.BlockedUntil = 0
			nodesToUpdate[nodeName] = &state
			log.Debugln("[Smart] Node [%s] block period expired, unblocking", nodeName)
		}
	}

	if len(nodesToUpdate) > 0 {
		operations := make([]smart.StoreOperation, 0, len(nodesToUpdate))
		for nodeName, state := range nodesToUpdate {
			data, err := json.Marshal(state)
			if err != nil {
				continue
			}
			operations = append(operations, smart.StoreOperation{
				Type:   smart.OpSaveNodeState,
				Group:  s.Name(),
				Config: s.configName,
				Node:   nodeName,
				Data:   data,
			})
		}
		s.store.AppendToGlobalQueue(operations...)
		s.store.UpdateBlockedNodesCache(s.Name(), s.configName, nodesToUpdate)
	}
}

// 单位转换
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
	base := 1000.0
	i := 0
	for val >= base && i < len(units)-1 {
		if i == 0 {
			val /= base
		} else if i == 1 {
			val /= 60
		} else if i == 2 {
			val /= 60
		}
		i++
	}
	return fmt.Sprintf("%.2f %s", val, units[i])
}

// 日志记录
func (s *Smart) logConnectionStats(status string, record *smart.StatsRecord, metadata *C.Metadata, baseWeight, priorityFactor float64,
	addressDisplay, proxyName string, connectTime int64, latency int64, uploadTotal, downloadTotal, maxUploadRate, maxDownloadRate float64,
	connectionDuration int64, asnInfo string, ModelPredicted bool) {

	var tcpAsnWeight, udpAsnWeight float64

	if asnInfo != "" {
		tcpAsnWeightKey := smart.WeightTypeTCPASN + ":" + asnInfo
		udpAsnWeightKey := smart.WeightTypeUDPASN + ":" + asnInfo
		if record.Weights != nil {
			if w, ok := record.Weights[tcpAsnWeightKey]; ok {
				tcpAsnWeight = w
			}
			if w, ok := record.Weights[udpAsnWeightKey]; ok {
				udpAsnWeight = w
			}
		}
	}

	weightSource := "Traditional"
	if ModelPredicted {
		weightSource = "LightGBM"
	}

	log.Debugln("[Smart] Connection status: [%s], Updated weights: (Model: [%s], TCP: [%.4f], UDP: [%.4f], TCP ASN: [%.4f], UDP ASN: [%.4f], Base: [%.4f], Priority: [%.2f]) "+
		"For (Group: [%s] - Node: [%s] - Network: [%s] - Address: [%s]) "+
		"- Current: (Connect: [%s], Latency: [%s], Up: [%s], Down: [%s], Max Up Speed: [%s], Max Down Speed: [%s], Duration: [%s]) "+
		"- History: (Success: [%d], Failure: [%d], Avg Connect: [%s], Avg Latency: [%s], Total Up: [%s], Total Down: [%s], Max Up Speed: [%s], Max Down Speed: [%s], Avg Duration: [%s])",
		status, weightSource, record.Weights[smart.WeightTypeTCP], record.Weights[smart.WeightTypeUDP], tcpAsnWeight, udpAsnWeight, baseWeight, priorityFactor,
		s.Name(), proxyName, metadata.NetWork.String(), addressDisplay,
		formatTimeUnit(float64(connectTime)),
		formatTimeUnit(float64(latency)),
		formatTrafficUnit(uploadTotal*1024*1024, false),
		formatTrafficUnit(downloadTotal*1024*1024, false),
		formatTrafficUnit(maxUploadRate*1024, true),
		formatTrafficUnit(maxDownloadRate*1024, true),
		formatTimeUnit(float64(connectionDuration)),
		record.Success, record.Failure,
		formatTimeUnit(float64(record.ConnectTime)),
		formatTimeUnit(float64(record.Latency)),
		formatTrafficUnit(record.UploadTotal*1024*1024, false),
		formatTrafficUnit(record.DownloadTotal*1024*1024, false),
		formatTrafficUnit(record.MaxUploadRate*1024, true),
		formatTrafficUnit(record.MaxDownloadRate*1024, true),
		formatTimeUnit(record.ConnectionDuration*60000),
	)
}

// 数据收集
func (s *Smart) collectConnectionData(input *smart.ModelInput, metadata *C.Metadata,
	baseWeight float64, proxyName string, ModelPredicted bool) {

	// 采样率控制
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

func updateAverageValueInt(oldValue int64, newValue int64) int64 {
	if oldValue > 0 {
		return (oldValue*2 + newValue*4) / 6
	}
	return newValue
}

func updateAverageValueFloat(oldValue, newValue float64) float64 {
	if oldValue > 0 {
		return (oldValue*2 + newValue*4) / 6
	}
	return newValue
}

func (s *Smart) recordConnectionStats(status string, metadata *C.Metadata, proxy C.Proxy,
	connectTime, latency, uploadTotal, downloadTotal, maxUploadRate, maxDownloadRate,
	connectionDuration int64, err error) {

	if proxy.Type() == C.Compatible || proxy.Type() == C.Reject || proxy.Type() == C.Pass || proxy.Type() == C.RejectDrop {
		return
	}

	var calculatedWeight float64
	var ModelPredicted bool

	proxyName := proxy.Name()
	isUDP := metadata.NetWork == C.UDP
	networkStr := metadata.NetWork.String()

	target := metadata.SmartTarget
	wildcardTarget := smart.GetEffectiveTarget(metadata.Host, metadata.DstIP.String())
	cacheKey := smart.FormatDBKey(smart.KeyTypeStats, s.configName, s.Name(), target, proxyName)
	asnInfo := s.getASNCode(metadata)
	priorityFactor := s.getPriorityFactor(proxyName)

	asnDisplay := "unknown"
	if asnInfo != "" {
		asnDisplay = asnInfo
	}
	var addressDisplay string
	if metadata.Host != "" {
		addressDisplay = fmt.Sprintf("Host: [%s] - Target: [%s] - ASN: [%s]", metadata.Host, target, asnDisplay)
	} else {
		addressDisplay = fmt.Sprintf("IP: [%s] - Target: [%s] - ASN: [%s]", metadata.DstIP.String(), target, asnDisplay)
	}

	weightType := smart.WeightTypeTCP
	if asnInfo != "" {
		if isUDP {
			weightType = smart.WeightTypeUDPASN + ":" + asnInfo
		} else {
			weightType = smart.WeightTypeTCPASN + ":" + asnInfo
		}
	} else if isUDP {
		weightType = smart.WeightTypeUDP
	}

	lock := smart.GetTargetNodeLock(target, s.Name(), proxyName)
	lock.Lock()
	defer lock.Unlock()

	atomicRecord := s.store.GetOrCreateAtomicRecord(cacheKey, s.Name(), s.configName, target, proxyName)

	switch status {
	case "failed":
		s.onDialFailed(proxy.Type(), err, s.healthCheck)
		atomicRecord.Add("failure", int64(1))
	case "closed":
		s.onDialSuccess()
		atomicRecord.Add("success", int64(1))
	}

	if connectTime > 0 {
		oldConnectTime := atomicRecord.Get("connectTime").(int64)
		newConnectTime := updateAverageValueInt(oldConnectTime, connectTime)
		atomicRecord.Set("connectTime", newConnectTime)
	}

	if latency > 0 {
		oldLatency := atomicRecord.Get("latency").(int64)
		newLatency := updateAverageValueInt(oldLatency, latency)
		atomicRecord.Set("latency", newLatency)
	}

	if connectionDuration > 0 {
		s.updateConnectionDuration(atomicRecord, connectionDuration)
	}

	oldWeight := atomicRecord.GetWeight(weightType)
	uploadTotalMB := float64(uploadTotal) / (1024.0 * 1024.0)
	downloadTotalMB := float64(downloadTotal) / (1024.0 * 1024.0)
	maxUploadRateKB := float64(maxUploadRate) / 1024.0
	maxDownloadRateKB := float64(maxDownloadRate) / 1024.0

	atomicRecord.Add("uploadTotal", uploadTotalMB)
	atomicRecord.Add("downloadTotal", downloadTotalMB)

	oldMaxUploadRate := atomicRecord.Get("maxUploadRate").(float64)
	if maxUploadRateKB > oldMaxUploadRate {
		atomicRecord.Set("maxUploadRate", maxUploadRateKB)
	}

	oldMaxDownloadRate := atomicRecord.Get("maxDownloadRate").(float64)
	if maxDownloadRateKB > oldMaxDownloadRate {
		atomicRecord.Set("maxDownloadRate", maxDownloadRateKB)
	}

	input := lightgbm.CreateModelInputFromStatsRecord(
		atomicRecord, metadata,
		uploadTotalMB, downloadTotalMB, maxUploadRateKB, maxDownloadRateKB, float64(connectionDuration) / 60000.0, wildcardTarget,
	)

	if s.useLightGBM && s.weightModel != nil {
		calculatedWeight, ModelPredicted = s.weightModel.PredictWeight(input, priorityFactor)
	} else {
		calculatedWeight, ModelPredicted = smart.CalculateWeight(input, priorityFactor)
	}

	// 额外检查和权重调整
	// 不再进行强制权重调整，仅在异常时对特定域名屏蔽节点，防止优秀节点被整个 target 完全屏蔽
	adjWeight, isDegraded, checked, blockCode := s.checkNodeQuality(
		status, metadata, proxy, wildcardTarget,
		addressDisplay, proxyName, calculatedWeight, oldWeight,
		connectionDuration, uploadTotalMB, downloadTotalMB,
		networkStr, asnInfo, isUDP)

	// 针对具体 域名/IP 屏蔽节点
	failedBlock := s.store.UpdateHostStatus(s.Name(), s.configName, wildcardTarget, metadata, proxyName, s.maxFailedTimes, isDegraded, checked, blockCode)

	if isDegraded || failedBlock {
		s.findSameConnection(metadata, proxyName, target, asnInfo, isUDP)
	}

	// 平均权重(适应 target 调整为 rule based 和 asn based 的情况)
	newWeight := updateAverageValueFloat(oldWeight, adjWeight)
	atomicRecord.Set("lastUsed", time.Now().Unix())
	atomicRecord.SetWeight(weightType, newWeight, isUDP)
	statsSnapshot := atomicRecord.CreateStatsSnapshot(cacheKey)
	s.saveStatsRecord(target, proxy, statsSnapshot)

	if s.collectData {
		collectedWeight := adjWeight / priorityFactor
		if isDegraded || failedBlock {
			// 对于异常连接强制调整，便于模型训练时进行识别
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

	s.logConnectionStats(status, statsSnapshot, metadata, calculatedWeight / priorityFactor, priorityFactor, addressDisplay, proxyName,
		connectTime, latency, uploadTotalMB, downloadTotalMB, maxUploadRateKB, maxDownloadRateKB, connectionDuration, asnInfo, ModelPredicted)
}

func (s *Smart) registerClosureMetricsCallback(c C.Conn, proxy C.Proxy, metadata *C.Metadata, connectTime int64, firstReadLatency *atomic.Int64, firstReadErr *atomic.TypedValue[error], firstWriteErr *atomic.TypedValue[error]) C.Conn {
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
			writeErr := firstWriteErr.Load()

			if readErr == nil {
				go s.recordConnectionStats("closed", metadata, proxy, connectTime, latency, uploadTotal, downloadTotal, maxUploadRate, maxDownloadRate, connectionDuration, nil)
			} else if readErr == io.EOF {
				if writeErr != nil && writeErr != io.EOF {
					go s.recordConnectionStats("failed", metadata, proxy, connectTime, latency, uploadTotal, downloadTotal, maxUploadRate, maxDownloadRate, connectionDuration, readErr)
				} else {
					go s.recordConnectionStats("closed", metadata, proxy, connectTime, latency, uploadTotal, downloadTotal, maxUploadRate, maxDownloadRate, connectionDuration, nil)
				}
			} else {
				go s.recordConnectionStats("failed", metadata, proxy, connectTime, latency, uploadTotal, downloadTotal, maxUploadRate, maxDownloadRate, connectionDuration, readErr)
			}
			return
		}
	})
}

func (s *Smart) registerPacketClosureMetricsCallback(pc C.PacketConn, proxy C.Proxy, metadata *C.Metadata, connectTime int64, udpLatency *atomic.Int64) C.PacketConn {
	return callback.NewCloseCallbackPacketConn(pc, func() {
		tracker := statistic.DefaultManager.Get(metadata.UUID)
		if tracker != nil {
			info := tracker.Info()
			uploadTotal := info.UploadTotal.Load()
			downloadTotal := info.DownloadTotal.Load()
			connectionDuration := time.Since(info.Start).Milliseconds()
			maxUploadRate := info.MaxUploadRate.Load()
			maxDownloadRate := info.MaxDownloadRate.Load()

			go s.recordConnectionStats("closed", metadata, proxy, connectTime, udpLatency.Load(),
				uploadTotal, downloadTotal, maxUploadRate, maxDownloadRate, connectionDuration, nil)
			return
		}
	})
}

func (s *Smart) checkNodeQuality(
	status string, metadata *C.Metadata, proxy C.Proxy, wildcardTarget string,
	addressDisplay, proxyName string,
	newWeight, oldWeight float64,
	connectionDuration int64, uploadTotal, downloadTotal float64,
	networkType string, asnInfo string, isUDP bool) (float64, bool, bool, int64) {

	if s.selected != "" {
		return newWeight, false, false, 0
	}

	now := time.Now().Unix()

	// 用户手动/智能屏蔽
	if metadata.SmartBlock == "blocked" || metadata.SmartBlock == "degraded" {
		if metadata.SmartBlock == "degraded" {
			return oldWeight, false, false, 0
		}
		log.Debugln("[Smart] Connection Group: [%s] - Node: [%s] - Network: [%s] - Address: [%s] detected manual block...",
			s.Name(), proxyName, networkType, addressDisplay)
		return newWeight, true, true, 1
	}

	_, wtLastCheck, wtLastFailure, wtBlocked := s.store.GetHostStatus(s.Name(), s.configName, wildcardTarget)

	if wtBlocked {
		return newWeight, false, false, 0
	}

	if newWeight < smart.AllowedWeight {
		return newWeight, true, true, 5
	}

	if status == "failed" {
		return newWeight, false, true, 3
	}

	// 零流量连接
	if connectionDuration > 100 && downloadTotal == 0 && uploadTotal == 0 && metadata.DstPort == 443 && !isUDP {
		log.Debugln("[Smart] Connection Group: [%s] - Node: [%s] - Network: [%s] - Address: [%s] detected zero-traffic...",
			s.Name(), proxyName, networkType, addressDisplay)
		return newWeight, true, true, 4
	}

	// 异常状态码检测
	if downloadTotal < 0.03 && metadata.Host != "" && metadata.DstPort == 443 && !isUDP {
		var failure bool
		var checked bool
		if now - wtLastCheck > 300 || now - wtLastFailure < 300 {
			checked = true
			status, ok, err := s.StatusTest(proxy, metadata.Host)
			if err == nil {
				failure = !ok
				if failure {
					log.Debugln("[Smart] Connection Group: [%s] - Node: [%s] - Network: [%s] - Address: [%s] detected abnormal response [%d]...",
						s.Name(), proxyName, networkType, addressDisplay, status)
				}
			}
		}
		if failure {
			return newWeight, true, checked, 2
		}
		return newWeight, false, checked, 0
	}

	return newWeight, false, false, 0
}

func (s *Smart) findSameConnection(metadata *C.Metadata, proxyName, target, asnInfo string, isUDP bool) {
	allIDs := statistic.DefaultManager.GetSmartTargetIDs(target, asnInfo)

	for id := range allIDs {
		if tracker := statistic.DefaultManager.Get(id); tracker != nil {
			if id != metadata.UUID && lo.Contains(tracker.Chains(), s.Name()) {
				if lo.Contains(tracker.Chains(), proxyName) {
					tracker.Info().Metadata.SmartBlock = "degraded"
				}
				_ = tracker.Close()
			}
		}
	}

	s.store.DeleteUnwrapResult(s.Name(), s.configName, target, asnInfo, isUDP)

}

func (s *Smart) checkHostStatus() {
	proxies := s.GetProxies(false)
	proxyMap := make(map[string]C.Proxy, len(proxies))
	for _, p := range proxies {
		proxyMap[p.Name()] = p
	}

	toCheck, err := s.store.CheckHostStatus(s.Name(), s.configName)
	if err != nil {
		return
	}

	for wildcardTarget, nodeMap := range toCheck {
		for nodeName, host := range nodeMap {
			p, ok := proxyMap[nodeName]
			if !ok {
				continue
			}
			status, okRes, err := s.StatusTest(p, host)
			if err == nil && okRes {
				s.store.UpdateHostStatus(s.Name(), s.configName, wildcardTarget, &C.Metadata{Host: host}, nodeName, s.maxFailedTimes, false, true, 0)
				log.Debugln("[Smart] Recover Group: [%s] - Node: [%s] for Host: [%s] with HTTP Status: [%d]", s.Name(), nodeName, host, status)
			} else if err == nil {
				log.Debugln("[Smart] Recover Group: [%s] - Node: [%s] for Host: [%s] still abnormal with HTTP Status: [%d]", s.Name(), nodeName, host, status)
			}
		}
	}
}

func (s *Smart) StatusTest(proxy C.Proxy, host string) (uint16, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), C.DefaultTCPTimeout)
	defer cancel()
	url := "https://" + host + "/?z=" + strconv.FormatInt(rand.Int63(), 10)
	return proxy.StatusTest(ctx, url)
}

func (s *Smart) getPriorityFactor(proxyName string) float64 {
	if len(s.policyPriority) == 0 && len(s.typePriority) == 0 {
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
	if len(s.typePriority) > 0 {
		var targetProxy C.Proxy
		for _, p := range s.GetProxies(false) {
			if p.Name() == proxyName {
				targetProxy = p
				break
			}
		}
		if targetProxy != nil {
			proxyType := strings.ToLower(targetProxy.Type().String())
			if typeFactor, ok := s.typePriority[proxyType]; ok {
				factor *= typeFactor
			}
		}
	}
	s.priorityCache.Store(proxyName, factor)
	return factor
}

func smartWithPolicyPriority(policyPriority string) smartOption {
	return func(s *Smart) {
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
}

func smartWithLightGBM(useLightGBM bool) smartOption {
	return func(s *Smart) {
		s.useLightGBM = useLightGBM
	}
}

func smartWithCollectData(collectData bool) smartOption {
	return func(s *Smart) {
		s.collectData = collectData
	}
}

func smartWithSampleRate(sampleRate float64) smartOption {
	return func(s *Smart) {
		if sampleRate <= 0 || sampleRate > 1 {
			s.sampleRate = 1
		} else {
			s.sampleRate = sampleRate
		}
	}
}

func smartWithPreferASN(preferASN bool) smartOption {
	return func(s *Smart) {
		s.preferASN = preferASN
	}
}

func smartWithTypePriority(typePriority string) smartOption {
	return func(s *Smart) {
		if s.typePriority == nil {
			s.typePriority = make(map[string]float64)
		}
		pairs := strings.Split(typePriority, ";")
		for _, pair := range pairs {
			pair = strings.TrimSpace(pair)
			if pair == "" {
				continue
			}
			parts := strings.Split(pair, ":")
			if len(parts) != 2 {
				log.Warnln("[Smart] Invalid type-priority rule: [%s], must be in 'type:factor' format", pair)
				continue
			}
			rawType := strings.ToLower(strings.TrimSpace(parts[0]))
			switch rawType {
			case "ss":
				rawType = "shadowsocks"
			case "ssr":
				rawType = "shadowsocksr"
			case "gost-relay":
				rawType = "gostrelay"
			}
			factorStr := strings.TrimSpace(parts[1])
			factor, err := strconv.ParseFloat(factorStr, 64)
			if err != nil || factor <= 0 {
				log.Warnln("[Smart] Invalid priority factor for type [%s]: %v", rawType, err)
				continue
			}
			s.typePriority[rawType] = factor
		}
	}
}

func parseSmartOption(config map[string]any) []smartOption {
	opts := []smartOption{}

	if elm, ok := config["policy-priority"]; ok {
		if policyPriority, ok := elm.(string); ok {
			opts = append(opts, smartWithPolicyPriority(policyPriority))
		}
	}
	
	if elm, ok := config["type-priority"]; ok {
		if typePriority, ok := elm.(string); ok {
			opts = append(opts, smartWithTypePriority(typePriority))
		}
	}

	if elm, ok := config["uselightgbm"]; ok {
		if useLightGBM, ok := elm.(bool); ok {
			opts = append(opts, smartWithLightGBM(useLightGBM))
		}
	}

	if elm, ok := config["collectdata"]; ok {
		if collectData, ok := elm.(bool); ok {
			opts = append(opts, smartWithCollectData(collectData))
		}
	}

	if elm, ok := config["sample-rate"]; ok {
		switch v := elm.(type) {
		case float64:
			opts = append(opts, smartWithSampleRate(v))
		case float32:
			opts = append(opts, smartWithSampleRate(float64(v)))
		case int:
			opts = append(opts, smartWithSampleRate(float64(v)))
		}
	}

	if elm, ok := config["prefer-asn"]; ok {
		if preferASN, ok := elm.(bool); ok {
			opts = append(opts, smartWithPreferASN(preferASN))
		}
	}

	return opts
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
