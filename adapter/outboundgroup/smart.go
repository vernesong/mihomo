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
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dlclark/regexp2"
	"github.com/metacubex/mihomo/common/callback"
	"github.com/metacubex/mihomo/common/singleflight"
	"github.com/metacubex/mihomo/common/utils"
	"github.com/metacubex/mihomo/component/dialer"
	"github.com/metacubex/mihomo/component/mmdb"
	"github.com/metacubex/mihomo/component/profile/cachefile"
	"github.com/metacubex/mihomo/component/smart"
	"github.com/metacubex/mihomo/component/smart/lightgbm"
	"github.com/metacubex/mihomo/component/tcp"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/constant/provider"
	"github.com/metacubex/mihomo/log"
	"github.com/metacubex/mihomo/tunnel"
	"github.com/metacubex/mihomo/tunnel/statistic"
	"github.com/samber/lo"
)

const (
	prefetchInterval         = 10 * time.Minute
	cleanupInterval          = 180 * time.Minute
	cacheParamAdjustInterval = 5 * time.Minute
	recoveryCheckInterval    = 5 * time.Minute
	checkInterval            = 10 * time.Minute
	flushQueueInterval       = 300 * time.Second
	rankingInterval          = 60 * time.Minute

	failureRecovery5min  = 5 * time.Minute
	failureRecovery10min = 10 * time.Minute
	failureRecovery15min = 15 * time.Minute
	failureRecovery30min = 30 * time.Minute

	longConnThreshold = 10 * time.Minute

	networkFailureThreshold = 5
	maxRetries              = 3

	maxCountValue       = 1000000
	maxTrafficStatValue = 10000000.0
)

var (
	longConnProcessGroup singleflight.Group[interface{}]
	flushQueueOnce       atomic.Bool
	smartInitOnce        sync.Once
	preloadOnce          sync.Once
	asnAvailable         bool
)

type smartOption func(*Smart)

type Smart struct {
	*GroupBase
	store          *smart.Store
	configName     string
	selected       string
	testUrl        string
	expectedStatus string
	disableUDP     bool
	fallback       *LoadBalance
	Hidden         bool
	Icon           string
	policyPriority []priorityRule
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	useLightGBM    bool
	collectData    bool
	dataCollector  *lightgbm.DataCollector
	weightModel    *lightgbm.WeightModel
	strategy       string
	sampleRate     float64
}

type priorityRule struct {
	pattern string
	regex   *regexp2.Regexp
	factor  float64
	isRegex bool
}

func getConfigFilename() string {
	configFile := C.Path.Config()
	baseName := filepath.Base(configFile)
	filename := strings.TrimSuffix(baseName, filepath.Ext(baseName))
	return filename
}

func NewSmart(option *GroupCommonOption, providers []provider.ProxyProvider, strategy string, options ...smartOption) (*Smart, error) {
	if strategy != "round-robin" && strategy != "sticky-sessions" {
		return nil, fmt.Errorf("%w: %s", errStrategy, strategy)
	}

	if option.URL == "" {
		option.URL = C.DefaultTestURL
	}

	lb, err := NewLoadBalance(&GroupCommonOption{
		Name:           option.Name + "-fallback",
		URL:            option.URL,
		Filter:         option.Filter,
		ExcludeFilter:  option.ExcludeFilter,
		ExcludeType:    option.ExcludeType,
		TestTimeout:    option.TestTimeout,
		MaxFailedTimes: option.MaxFailedTimes,
		DisableUDP:     option.DisableUDP,
		ExpectedStatus: option.ExpectedStatus,
	}, providers, strategy)

	if err != nil {
		return nil, err
	}

	configName := getConfigFilename()

	s := &Smart{
		GroupBase: NewGroupBase(GroupBaseOption{
			Name:           option.Name,
			Type:           C.Smart,
			Filter:         option.Filter,
			ExcludeFilter:  option.ExcludeFilter,
			ExcludeType:    option.ExcludeType,
			TestTimeout:    option.TestTimeout,
			MaxFailedTimes: option.MaxFailedTimes,
			Providers:      providers,
		}),
		testUrl:        option.URL,
		expectedStatus: option.ExpectedStatus,
		configName:     configName,
		disableUDP:     option.DisableUDP,
		fallback:       lb,
		Hidden:         option.Hidden,
		Icon:           option.Icon,
		policyPriority: make([]priorityRule, 0),
		strategy:       strategy,
		sampleRate:     1,
	}

	for _, option := range options {
		option(s)
	}

	return s, nil
}

func (s *Smart) GetConfigFilename() string {
	return s.configName
}

// ref: component/dialer/dialer.go:314
func parallelDialContext[T interface {
	comparable
	io.Closer
}](proxies []C.Proxy, fn func(C.Proxy) (T, error)) (C.Proxy, T, error) {
	results := make(chan struct {
		proxy C.Proxy
		conn  T
		error error
	})
	returned := make(chan struct{})
	defer close(returned)
	racer := func(proxy C.Proxy) {
		result := struct {
			proxy C.Proxy
			conn  T
			error error
		}{}
		defer func() {
			select {
			case results <- result:
			case <-returned:
				if result.conn != lo.Empty[T]() && result.error == nil {
					_ = result.conn.Close()
				}
			}
		}()
		result.conn, result.error = fn(proxy)
		result.proxy = proxy
	}

	for _, proxy := range proxies {
		go racer(proxy)
	}
	var errs []error
	for i := 0; i < len(proxies); i++ {
		res := <-results
		if res.error == nil {
			return res.proxy, res.conn, nil
		}
		errs = append(errs, res.error)
	}

	if len(errs) > 0 {
		return nil, lo.Empty[T](), errors.Join(errs...)
	}
	return nil, lo.Empty[T](), os.ErrDeadlineExceeded
}

func (s *Smart) dialContext(ctx context.Context, proxy C.Proxy, metadata *C.Metadata, start time.Time) (c C.Conn, err error) {
	var nc net.Conn
	if runtime.GOOS == "linux" || runtime.GOOS == "darwin" {
		d := dialer.NewDialer(dialer.WithCallback(func(conn net.Conn, err error) {
			if err == nil {
				nc = conn
			}
		}))
		c, err = proxy.DialContextWithDialer(ctx, d, metadata)
		if err == C.ErrNotSupport {
			c, err = proxy.DialContext(ctx, metadata)
		}
	} else {
		c, err = proxy.DialContext(ctx, metadata)
	}
	if err != nil {
		s.recordConnectionStats("failed", metadata, proxy, 0, 0, 0, 0, 0, 0, 0, false, err)
		return nil, err
	}
	if nc != nil {
		if tc, ok := nc.(*net.TCPConn); ok {
			if err = tcp.WaitAllAcks(ctx, tc); err != nil {
				s.recordConnectionStats("failed", metadata, proxy, 0, 0, 0, 0, 0, 0, 0, false, err)
				return nil, err
			}
		}
	}
	s.recordConnectionStats("success", metadata, proxy, time.Since(start).Milliseconds(), 0, 0, 0, 0, 0, 0, false, nil)
	return c, nil
}

func (s *Smart) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	proxies := s.GetProxies(true)
	if len(proxies) == 0 {
		return nil, errors.New("no proxy available")
	}

	tryDial := func(proxy C.Proxy, proxies []C.Proxy, triedProxies map[string]bool, maxRetries int, wrapMetric bool) (C.Conn, error) {
		var finalErr error
		var try C.Proxy
		const parallelDials = 3
		tries := make([]C.Proxy, 0, parallelDials)
		for i := 0; i < maxRetries; i++ {
			tries = tries[:0]
			maxTries := parallelDials
			if len(proxies) < maxTries {
				maxTries = len(proxies)
			}
			for i := 0; i < maxTries; i++ {
				if i == 0 {
					try = proxy
				} else {
					try = s.selectNextProxy(metadata, proxies, triedProxies)
					if triedProxies[try.Name()] {
						break
					}
				}
				tries = append(tries, try)
				triedProxies[try.Name()] = true
			}
			var historyConnectTime int64
			for _, t := range tries {
				hct := s.getHistoryConnectStats(metadata, t)
				if hct > historyConnectTime {
					historyConnectTime = hct
				}
			}
			const thresholdRatio = 2.0
			const dialTimeout = C.DefaultTCPTimeout / parallelDials // enough
			var timeout time.Duration
			if historyConnectTime > 0 {
				timeout = time.Duration(float64(historyConnectTime)*thresholdRatio) * time.Millisecond
				if timeout > dialTimeout {
					timeout = dialTimeout
				}
			} else {
				timeout = dialTimeout
			}
			ctxDial, cancel := context.WithTimeout(ctx, timeout)
			start := time.Now()
			p, c, err := parallelDialContext(tries, func(proxy C.Proxy) (c C.Conn, err error) {
				return s.dialContext(ctxDial, proxy, metadata, start)
			})
			go func() {
				<-ctxDial.Done()
				cancel()
			}()

			if err == nil {
				if s.store != nil && wrapMetric {
					wrappedConn, wrapErr := s.wrapConnWithMetric(c, p, metadata)
					if wrapErr != nil {
						c.Close()
						finalErr = wrapErr
						if i == maxRetries-1 {
							break
						}
						if s.selected != "" {
							break
						}
						proxy = s.selectNextProxy(metadata, proxies, triedProxies)
						if triedProxies[proxy.Name()] {
							break
						}
						triedProxies[proxy.Name()] = true
						continue
					}
					return wrappedConn, nil
				}
				c.AppendToChains(s)
				s.onDialSuccess()
				return c, nil
			}

			finalErr = err
			if i == maxRetries-1 {
				break
			}
			if s.selected != "" {
				break
			}
			proxy = s.selectNextProxy(metadata, proxies, triedProxies)
			if triedProxies[proxy.Name()] {
				break
			}
			triedProxies[proxy.Name()] = true
		}
		if finalErr != nil && s.store != nil {
			s.onDialFailed(proxy.Type(), finalErr, s.GroupBase.healthCheck)
			s.store.MarkConnectionFailed(s.Name(), s.configName, len(proxies), triedProxies)
		}
		return nil, finalErr
	}

	triedProxies := make(map[string]bool)

	if s.store != nil && s.store.CheckNetworkFailure(s.Name(), s.configName) {
		proxy := s.fallbackToLoadBalance(metadata, proxies)
		if proxy == nil {
			return nil, errors.New("no proxy found in network failure mode")
		}
		triedProxies[proxy.Name()] = true
		return tryDial(proxy, proxies, triedProxies, maxRetries, true)
	}

	proxy := s.selectProxy(metadata, false)
	if proxy == nil {
		proxy = proxies[0]
	}
	triedProxies[proxy.Name()] = true
	return tryDial(proxy, proxies, triedProxies, maxRetries, true)
}

func (s *Smart) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (pc C.PacketConn, err error) {
	proxies := s.GetProxies(true)
	if len(proxies) == 0 {
		return nil, errors.New("no proxy available")
	}
	triedProxies := make(map[string]bool)
	proxy := s.selectProxy(metadata, false)
	if proxy == nil {
		proxy = proxies[0]
	}
	triedProxies[proxy.Name()] = true

	var finalErr error
	for i := 0; i < maxRetries; i++ {
		historyConnectTime := s.getHistoryConnectStats(metadata, proxy)
		const thresholdRatio = 2.0
		var timeout time.Duration
		if historyConnectTime > 0 {
			timeout = time.Duration(float64(historyConnectTime)*thresholdRatio) * time.Millisecond
			if timeout > C.DefaultTCPTimeout {
				timeout = C.DefaultTCPTimeout
			}
		} else {
			timeout = C.DefaultTCPTimeout
		}
		ctxDial, cancel := context.WithTimeout(ctx, timeout)
		start := time.Now()
		pc, err = proxy.ListenPacketContext(ctxDial, metadata)
		cancel()
		connectTime := time.Since(start).Milliseconds()

		if err == nil {
			pc.AppendToChains(s)
			s.onDialSuccess()
			if s.store != nil {
				s.recordConnectionStats("success", metadata, proxy, connectTime, 0, 0, 0, 0, 0, 0, false, nil)
				pc = s.registerPacketClosureMetricsCallback(pc, proxy, metadata)
			}
			return pc, nil
		}
		finalErr = err
		if s.store != nil {
			s.recordConnectionStats("failed", metadata, proxy, 0, 0, 0, 0, 0, 0, 0, false, err)
		}
		triedProxies[proxy.Name()] = true
		if s.selected != "" {
			break
		}
		proxy = s.selectNextProxy(metadata, proxies, triedProxies)
		if triedProxies[proxy.Name()] {
			break
		}
	}
	if finalErr != nil && s.store != nil {
		s.onDialFailed(proxy.Type(), finalErr, s.GroupBase.healthCheck)
		s.store.MarkConnectionFailed(s.Name(), s.configName, len(proxies), triedProxies)
	}
	return nil, finalErr
}

func (s *Smart) Unwrap(metadata *C.Metadata, touch bool) C.Proxy {
	proxy := s.selectProxy(metadata, touch)

	if proxy != nil && s.store != nil {
		domain := ""
		if metadata != nil {
			domain, _ = smart.GetEffectiveDomain(metadata.Host, metadata.DstIP.String())
			if domain != "" {
				s.store.StoreUnwrapResult(s.Name(), s.configName, domain, proxy.Name())
			}
		}
	}

	return proxy
}

func (s *Smart) IsL3Protocol(metadata *C.Metadata) bool {
	return s.Unwrap(metadata, false).IsL3Protocol(metadata)
}

func (s *Smart) wrapConnWithMetric(c C.Conn, proxy C.Proxy, metadata *C.Metadata) (C.Conn, error) {
	c.AppendToChains(s)
	c = s.registerClosureMetricsCallback(c, proxy, metadata)

	start := time.Now()

	wrappedConn := callback.NewFirstReadCallBackConn(c, func(err error) {
		latency := time.Since(start).Milliseconds()
		if err == nil {
			s.onDialSuccess()
			s.recordConnectionStats("success", metadata, proxy, 0, latency, 0, 0, 0, 0, 0, false, nil)
		} else {
			s.onDialFailed(proxy.Type(), err, s.GroupBase.healthCheck)
			s.recordConnectionStats("failed", metadata, proxy, 0, 0, 0, 0, 0, 0, 0, false, err)
		}
	})

	return wrappedConn, nil
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

func (s *Smart) InitializeCache() {
	cacheFile := cachefile.Cache()
	if cacheFile == nil || cacheFile.DB == nil {
		return
	}

	smartStore := cachefile.NewSmartStore(cacheFile)
	if smartStore == nil {
		return
	}

	s.store = smartStore.GetStore()

	if s.configName == "" {
		s.configName = getConfigFilename()
	}

	s.ctx, s.cancel = context.WithCancel(context.Background())

	smartInitOnce.Do(func() {
		s.startTimedTask(5*time.Minute, checkInterval, "Clean up groups", s.cleanupOrphanedGroups, true)
		s.startTimedTask(5*time.Minute, cacheParamAdjustInterval, "Cache parameter adjustment", s.store.AdjustCacheParameters, false)
		s.startTimedTask(5*time.Minute, flushQueueInterval, "Queue flush", func() {
			s.store.FlushQueue(false)
		}, false)
		asnAvailable = mmdb.Verify(C.Path.ASN())
	})

	s.startTimedTask(5*time.Minute, checkInterval, "Clean up nodes", s.cleanupOrphanedNodeCache, true)
	s.startTimedTask(5*time.Second, checkInterval, "Preload frequent data", func() {
		preloadOnce.Do(func() {
			s.store.AdjustCacheParameters()
		})
		proxies := s.GetProxies(false)
		proxyNames := make([]string, 0, len(proxies))
		for _, p := range proxies {
			proxyNames = append(proxyNames, p.Name())
		}
		s.store.PreloadFrequentData(s.Name(), s.configName, proxyNames)
	}, true)
	s.startTimedTask(5*time.Minute, prefetchInterval, "prefetch", s.runPrefetch, false)
	s.startTimedTask(30*time.Second, rankingInterval, "ranking", s.updateNodeRanking, false)
	s.startTimedTask(5*time.Minute, recoveryCheckInterval, "Recovery check", s.checkAndRecoverDegradedNodes, false)
	s.startTimedTask(10*time.Minute, checkInterval, "long connection processing", func() {
		s.processLongConnections(longConnThreshold)
	}, false)
	s.startTimedTask(5*time.Minute, cleanupInterval, "Expired cleanup", func() {
		_ = s.store.CleanupExpiredStats(s.Name(), s.configName)
	}, false)
	s.startTimedTask(5*time.Minute, cleanupInterval, "OldDomains cleanup", func() {
		_ = s.store.CleanupOldDomains(s.Name(), s.configName)
	}, false)

	if s.useLightGBM {
		s.weightModel = lightgbm.GetModel()
	}

	if s.collectData {
		s.dataCollector = lightgbm.GetCollector()

		s.startTimedTask(10*time.Minute, 30*time.Minute, "Flush data collector", func() {
			if s.dataCollector != nil {
				s.dataCollector.Flush()
			}
		}, false)
	}
}

func (s *Smart) startTimedTask(initialDelay, interval time.Duration, taskName string, task func(), runOnce bool) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		jitterRange := 30.0
		intervalJitter := time.Duration(rand.Float64() * jitterRange * float64(time.Second))

		adjustedInitialDelay := initialDelay + intervalJitter
		adjustedInterval := interval + intervalJitter

		select {
		case <-time.After(adjustedInitialDelay):
		case <-s.ctx.Done():
			return
		}

		task()

		if runOnce {
			log.Debugln("[Smart] Task %s for group [%s] set to run once, exiting",
				taskName, s.Name())
			return
		}

		ticker := time.NewTicker(adjustedInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				task()
			case <-s.ctx.Done():
				return
			}
		}
	}()
}

func (s *Smart) runPrefetch() {
	if s.store == nil {
		return
	}

	proxies := s.GetProxies(true)
	proxyMap := make(map[string]string)
	for _, p := range proxies {
		if p.AliveForTestUrl(s.testUrl) {
			proxyMap[p.Name()] = p.Name()
		}
	}
	s.store.RunPrefetch(s.Name(), s.configName, proxyMap)
}

func (s *Smart) updateNodeRanking() {
	if s.store == nil {
		return
	}

	log.Debugln("[Smart] Starting node ranking update for policy group [%s]", s.Name())

	proxies := s.GetProxies(true)
	proxyNames := make([]string, 0, len(proxies))
	for _, p := range proxies {
		proxyNames = append(proxyNames, p.Name())
	}
	ranking, err := s.store.GetNodeWeightRanking(s.Name(), s.configName, false, proxyNames)
	if err != nil {
		log.Warnln("[Smart] Failed to update node ranking: %v", err)
		return
	}

	if len(ranking) == 0 {
		log.Debugln("[Smart] Policy group [%s] doesn't have enough data to generate node ranking", s.Name())
		return
	}

	categoryCounts := make(map[string]int)
	for _, rank := range ranking {
		categoryCounts[rank]++
	}

	log.Debugln("[Smart] Policy group [%s] node ranking update completed: %d nodes total (%s: %d, %s: %d, %s: %d)",
		s.Name(), len(ranking),
		smart.RankMostUsed, categoryCounts[smart.RankMostUsed],
		smart.RankOccasional, categoryCounts[smart.RankOccasional],
		smart.RankRarelyUsed, categoryCounts[smart.RankRarelyUsed])
}

// 检查节点屏蔽状态
func (s *Smart) checkAndRecoverDegradedNodes() {
	if s.store == nil {
		return
	}

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

		var shouldUpdate bool = false

		if !state.BlockedUntil.IsZero() && state.BlockedUntil.Before(time.Now()) {
			state.BlockedUntil = time.Time{}
			shouldUpdate = true
			log.Debugln("[Smart] Node [%s] block period expired, unblocking", nodeName)
		}

		if state.Degraded {
			timeSinceLastFailure := time.Since(state.LastFailure)

			var recoveryFactor float64
			var shouldRecover bool

			switch {
			case timeSinceLastFailure > failureRecovery30min:
				recoveryFactor = 1.0
				shouldRecover = true
				state.FailureCount = 0
				for k := range state.DomainFailureCount {
					state.DomainFailureCount[k] = 0
				}
			case timeSinceLastFailure > failureRecovery15min:
				if state.DegradedFactor < 0.9 {
					recoveryFactor = 0.9
					shouldRecover = true
					state.FailureCount = int(float64(state.FailureCount) * 0.5)
					for k, v := range state.DomainFailureCount {
						state.DomainFailureCount[k] = int(float64(v) * 0.5)
					}
				}
			case timeSinceLastFailure > failureRecovery10min:
				if state.DegradedFactor < 0.75 {
					recoveryFactor = 0.75
					shouldRecover = true
					state.FailureCount = int(float64(state.FailureCount) * 0.7)
					for k, v := range state.DomainFailureCount {
						state.DomainFailureCount[k] = int(float64(v) * 0.7)
					}
				}
			case timeSinceLastFailure > failureRecovery5min:
				if state.DegradedFactor < 0.5 {
					recoveryFactor = 0.5
					shouldRecover = true
					state.FailureCount = int(float64(state.FailureCount) * 0.9)
					for k, v := range state.DomainFailureCount {
						state.DomainFailureCount[k] = int(float64(v) * 0.9)
					}
				}
			}

			if shouldRecover {
				shouldUpdate = true
				if recoveryFactor >= 0.99 {
					state.Degraded = false
					state.DegradedFactor = 1.0
				} else {
					state.Degraded = true
					state.DegradedFactor = recoveryFactor
				}
			}
		} else if state.FailureCount > 0 {
			timeSinceLastFailure := time.Since(state.LastFailure)
			if timeSinceLastFailure > failureRecovery10min {
				state.FailureCount = 0
				for k := range state.DomainFailureCount {
					state.DomainFailureCount[k] = 0
				}
				shouldUpdate = true
				log.Debugln("[Smart] Reset failure count for node [%s]", nodeName)
			}
		}

		if shouldUpdate {
			nodesToUpdate[nodeName] = &state
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
		s.store.BatchSaveConnStats(operations)
	}
}

func (s *Smart) selectProxy(metadata *C.Metadata, touch bool) C.Proxy {
	proxies := s.GetProxies(touch)
	if metadata == nil || len(proxies) == 0 {
		if len(proxies) > 0 {
			return proxies[0]
		}
		return nil
	}

	if s.selected != "" {
		for _, p := range proxies {
			if p.Name() == s.selected {
				return p
			}
		}
	}

	if s.store == nil {
		return proxies[0]
	}

	blockedNodes := make(map[string]bool)
	stateData, _ := s.store.GetNodeStates(s.Name(), s.configName)
	for nodeName, data := range stateData {
		var state smart.NodeState
		if json.Unmarshal(data, &state) == nil {
			if !state.BlockedUntil.IsZero() && state.BlockedUntil.After(time.Now()) {
				blockedNodes[nodeName] = true
			}
		}
	}

	findProxyByName := func(names []string) C.Proxy {
		for _, name := range names {
			if blockedNodes[name] {
				continue
			}
			for _, p := range proxies {
				if p.Name() == name && p.AliveForTestUrl(s.testUrl) {
					return p
				}
			}
		}
		return nil
	}

	weightType := smart.WeightTypeTCP
	if metadata.NetWork == C.UDP {
		weightType = smart.WeightTypeUDP
	}

	trySelector := func(target string, weightType string) C.Proxy {
		// 检查解析缓存
		if cachedProxyName := s.store.GetUnwrapResult(s.Name(), s.configName, target); cachedProxyName != "" {
			if proxy := findProxyByName([]string{cachedProxyName}); proxy != nil {
				s.store.DeleteCacheResult(smart.KeyTypeUnwrap, s.Name(), s.configName, target)
				return proxy
			}
		}

		// 检查预解析缓存
		if cachedProxyName, _ := s.store.GetPrefetchResult(s.Name(), s.configName, target, weightType); cachedProxyName != "" {
			if proxy := findProxyByName([]string{cachedProxyName}); proxy != nil {
				return proxy
			}
		}

		// 实时计算最佳节点
		bestNodes, _, err := s.store.GetBestProxyForTarget(s.Name(), s.configName, target, weightType, false)
		if err == nil && len(bestNodes) > 0 {
			if proxy := findProxyByName(bestNodes); proxy != nil {
				return proxy
			}
		}

		return nil
	}

	// 尝试使用域名信息选择
	domain, _ := smart.GetEffectiveDomain(metadata.Host, metadata.DstIP.String())
	if domain != "" {
		if proxy := trySelector(domain, weightType); proxy != nil {
			return proxy
		}
	}

	// 尝试使用ASN信息选择（50%概率）
	if rand.Float64() < 0.5 {
		asnNumber := s.getASNCode(metadata)
		if asnNumber != "" {
			asnWeightType := weightType
			if weightType == smart.WeightTypeTCP {
				asnWeightType = smart.WeightTypeTCPASN + ":" + asnNumber
			} else {
				asnWeightType = smart.WeightTypeUDPASN + ":" + asnNumber
			}

			if proxy := trySelector(asnNumber, asnWeightType); proxy != nil {
				return proxy
			}
		}
	}

	return s.fallbackToLoadBalance(metadata, proxies)
}

func (s *Smart) selectNextProxy(metadata *C.Metadata, availableProxies []C.Proxy, triedProxies map[string]bool) C.Proxy {
	findFirstAvailable := func(names []string) C.Proxy {
		for _, name := range names {
			if name == "" || triedProxies[name] {
				continue
			}
			for _, p := range availableProxies {
				if p.Name() == name && p.AliveForTestUrl(s.testUrl) {
					return p
				}
			}
		}
		return nil
	}

	if s.store == nil {
		for _, p := range availableProxies {
			if !triedProxies[p.Name()] && p.AliveForTestUrl(s.testUrl) {
				return p
			}
		}
		return availableProxies[0]
	}

	weightType := smart.WeightTypeTCP
	if metadata.NetWork == C.UDP {
		weightType = smart.WeightTypeUDP
	}

	domain, _ := smart.GetEffectiveDomain(metadata.Host, metadata.DstIP.String())
	if domain != "" {
		bestNodes, _, err := s.store.GetBestProxyForTarget(s.Name(), s.configName, domain, weightType, false)
		if err == nil {
			if proxy := findFirstAvailable(bestNodes); proxy != nil {
				return proxy
			}
		}
	}

	asnNumber := s.getASNCode(metadata)
	if asnNumber != "" {
		asnWeightType := weightType
		if weightType == smart.WeightTypeTCP {
			asnWeightType = smart.WeightTypeTCPASN + ":" + asnNumber
		} else {
			asnWeightType = smart.WeightTypeUDPASN + ":" + asnNumber
		}
		bestNodes, _, err := s.store.GetBestProxyForTarget(s.Name(), s.configName, asnNumber, asnWeightType, false)
		if err == nil {
			if proxy := findFirstAvailable(bestNodes); proxy != nil {
				return proxy
			}
		}
	}

	for _, p := range availableProxies {
		if !triedProxies[p.Name()] {
			if p.AliveForTestUrl(s.testUrl) {
				return p
			}
		}
	}

	return availableProxies[0]
}

func (s *Smart) fallbackToLoadBalance(metadata *C.Metadata, proxies []C.Proxy) C.Proxy {
	if len(proxies) == 0 {
		return nil
	}

	if metadata == nil {
		return proxies[0]
	}

	if s.fallback != nil {
		proxy := s.fallback.Unwrap(metadata, true)
		if proxy != nil {
			return proxy
		}
	}
	return proxies[0]
}

func (s *Smart) SupportUDP() bool {
	if s.disableUDP {
		return false
	}

	return s.selectProxy(nil, false).SupportUDP()
}

func (s *Smart) MarshalJSON() ([]byte, error) {
	proxies := s.GetProxies(false)
	all := make([]string, len(proxies))
	for i, proxy := range proxies {
		all[i] = proxy.Name()
	}

	policyPriorityStr := ""
	for _, rule := range s.policyPriority {
		if policyPriorityStr != "" {
			policyPriorityStr += ";"
		}
		policyPriorityStr += fmt.Sprintf("%s:%.2f", rule.pattern, rule.factor)
	}

	return json.Marshal(map[string]any{
		"type":            s.Type().String(),
		"now":             s.Now(),
		"all":             all,
		"testUrl":         s.testUrl,
		"expectedStatus":  s.expectedStatus,
		"fixed":           s.selected,
		"hidden":          s.Hidden,
		"icon":            s.Icon,
		"policy-priority": policyPriorityStr,
		"strategy":        s.strategy,
		"useLightGBM":     s.useLightGBM,
		"collectData":     s.collectData,
		"sampleRate":      s.sampleRate,
	})
}

func (s *Smart) cleanupOrphanedGroups() {
	if s.store == nil {
		return
	}

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
	if s.store == nil {
		return
	}

	currentProxies := s.GetProxies(true)
	currentNodesMap := make(map[string]bool)
	for _, proxy := range currentProxies {
		currentNodesMap[proxy.Name()] = true
	}

	cachedNodes, err := s.store.GetAllNodesForGroup(s.Name(), s.configName)
	if err != nil {
		return
	}

	var orphanedNodes []string
	for _, nodeName := range cachedNodes {
		if !currentNodesMap[nodeName] {
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
func (s *Smart) getHistoryConnectStats(metadata *C.Metadata, proxy C.Proxy) (historyConnectTime int64) {
	if s.store == nil || proxy == nil || metadata == nil {
		return 0
	}
	domain, _ := smart.GetEffectiveDomain(metadata.Host, metadata.DstIP.String())
	if domain == "" {
		return 0
	}
	cacheKey := smart.FormatCacheKey(smart.KeyTypeStats, s.configName, s.Name(), domain, proxy.Name())
	atomicManager := smart.GetAtomicManager()
	if atomicManager == nil {
		return 0
	}
	atomicRecord := atomicManager.GetOrCreateAtomicRecord(cacheKey, s.store, s.Name(), s.configName, domain, proxy.Name())
	if atomicRecord == nil {
		return 0
	}
	historyConnectTime, _ = atomicRecord.Get("connectTime").(int64)
	return
}

func (s *Smart) checkNodeQualityDegradation(
	metadata *C.Metadata, proxy C.Proxy, atomicRecord *smart.AtomicStatsRecord,
	addressDisplay, proxyName string,
	newWeight, oldWeight float64,
	connectionDuration int64,
	uploadTotal, downloadTotal float64,
	maxUploadRateKB, maxDownloadRateKB, historyMaxUploadRateKB, historyMaxDownloadRateKB float64,
	historyUploadTotal, historyDownloadTotal float64,
	success int64, weightType string, lastStatus int64, lastUsedVal int64) (float64, bool) {

	// 零流量连接
	if connectionDuration > 100 && downloadTotal == 0 && uploadTotal == 0 {
		degradedWeight := math.Max(0.1, newWeight*0.3)
		log.Debugln("[Smart] Zero-traffic connection detected: [%s] for domain [%s], conn time: %dms, forcing weight degradation from %.4f to %.4f (%s)",
			proxyName, addressDisplay, connectionDuration, newWeight, degradedWeight, weightType)
		return degradedWeight, true
	}

	// 异常状态码检测
	if (downloadTotal < 0.03 && metadata != nil && metadata.Host != "" && metadata.DstPort == 443 && metadata.NetWork == C.TCP) ||
		(rand.Float64() < 0.05 && metadata != nil && metadata.Host != "" && metadata.DstPort == 443 && metadata.NetWork == C.TCP) {
		needTest := false
		cooldownSeconds := int64(300)
		now := time.Now().Unix()
		if lastStatus == 0 || lastStatus == 403 || lastStatus == 429 || lastStatus == 407 || lastStatus == 599 {
			if now-lastUsedVal > cooldownSeconds {
				needTest = true
			}
		} else if rand.Float64() < 0.3 {
			needTest = true
		}
		if needTest {
			expectedStatus, _ := utils.NewUnsignedRanges[uint16]("200-399")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			url := "https://" + metadata.Host + "/?z=" + strconv.FormatInt(rand.Int63(), 10)
			status, ok, err := proxy.StatusTest(ctx, url, expectedStatus)
			atomicRecord.Set("status", int64(status))
			if err == nil && !ok {
				if status == 403 || status == 429 || status == 407 || status == 599 {
					degradedWeight := math.Max(0.1, newWeight*0.3)
					log.Debugln("[Smart] Connection [%s] - [%s] - [%s] - [%s] detected abnormal response [%d], degrade weight from %.4f to %.4f",
						s.Name(), proxyName, weightType, addressDisplay, status, newWeight, degradedWeight)
					return degradedWeight, true
				}
			}
		}
	}

	// 权重显著下降
	if oldWeight > 0 && success >= 3 {
		weightChangeRatio := (newWeight - oldWeight) / oldWeight

		var thresholdRatio float64
		switch {
		case success >= 300:
			thresholdRatio = -0.6
		case success >= 100:
			thresholdRatio = -0.5
		case success >= 20:
			thresholdRatio = -0.4
		default:
			thresholdRatio = -0.3
		}

		if weightChangeRatio < thresholdRatio {
			avgDownload := 0.0
			avgUpload := 0.0
			if success > 0 {
				avgDownload = historyDownloadTotal / float64(success)
				avgUpload = historyUploadTotal / float64(success)
			}

			trafficCompareRatio := 0.3
			speedCompareRatio := 0.6

			var performanceIssues int = 0

			if avgDownload > 0 && downloadTotal < avgDownload*trafficCompareRatio {
				performanceIssues++
			}
			if avgUpload > 0 && uploadTotal < avgUpload*trafficCompareRatio {
				performanceIssues++
			}

			if historyMaxUploadRateKB > 0 && maxUploadRateKB < historyMaxUploadRateKB*speedCompareRatio {
				performanceIssues++
			}
			if historyMaxDownloadRateKB > 0 && maxDownloadRateKB < historyMaxDownloadRateKB*speedCompareRatio {
				performanceIssues++
			}

			if performanceIssues >= 2 {
				var adjustmentFactor float64
				switch {
				case weightChangeRatio < -0.7:
					adjustmentFactor = 0.7
				case weightChangeRatio < -0.6:
					adjustmentFactor = 0.75
				case weightChangeRatio < -0.5:
					adjustmentFactor = 0.8
				default:
					adjustmentFactor = 0.85
				}

				limitedWeight := math.Max(newWeight, oldWeight*adjustmentFactor)

				log.Debugln("[Smart] Node quality degraded: [%s] for domain [%s], "+
					"weight from %.4f to %.4f (%.1f%%), limited to %.4f (%s)",
					proxyName, addressDisplay, oldWeight, newWeight, weightChangeRatio*100,
					limitedWeight, weightType)

				return limitedWeight, true
			}
		}
	}

	return newWeight, false
}

// ASN权重更新
func (s *Smart) updateAsnWeights(record *smart.AtomicStatsRecord, asnInfo string, weight float64, isUDP bool) {
	var asnWeightKey string

	if isUDP {
		asnWeightKey = smart.WeightTypeUDPASN + ":" + asnInfo
	} else {
		asnWeightKey = smart.WeightTypeTCPASN + ":" + asnInfo
	}

	record.SetWeight(asnWeightKey, weight)
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

// 统计数据限制检查
func (s *Smart) checkAndLimitStats(record *smart.AtomicStatsRecord) {
	success := record.Get("success").(int64)
	failure := record.Get("failure").(int64)

	if success > 10000 {
		record.Set("success", success/2)
	}

	if failure > 10000 {
		record.Set("failure", failure/2)
	}

	connectTime := record.Get("connectTime").(int64)
	if connectTime > 60000 {
		record.Set("connectTime", int64(60000))
	}

	latency := record.Get("latency").(int64)
	if latency > 10000 {
		record.Set("latency", int64(10000))
	}
}

// 记录保存
func (s *Smart) saveStatsRecord(cacheKey, domain string, proxy C.Proxy, record *smart.StatsRecord, lastUsed time.Time) {
	record.LastUsed = lastUsed

	smart.SetCacheValue(cacheKey, record)

	go func() {
		if data, err := json.Marshal(record); err == nil {
			operation := smart.StoreOperation{
				Type:   smart.OpSaveStats,
				Group:  s.Name(),
				Config: s.configName,
				Domain: domain,
				Node:   proxy.Name(),
				Data:   data,
			}
			s.store.BatchSaveConnStats([]smart.StoreOperation{operation})
		}
	}()
}

// 失败连接处理
func (s *Smart) handleFailedConnection(proxyName, cacheKey, domain string, calculatedWeight float64, weightType string) (float64, bool) {
	nodeStateData, _ := s.store.GetNodeStates(s.Name(), s.configName)
	var nodeState smart.NodeState
	var isDegraded bool

	const domainCountThreshold = 10 // 不同失败域名数量阈值
	const mildFailureCountThreshold = 20
	const mediumFailureCountThreshold = 50
	const severeFailureCountThreshold = 80

	if data, exists := nodeStateData[proxyName]; exists {
		if json.Unmarshal(data, &nodeState) != nil {
			nodeState = smart.NodeState{
				Name:               proxyName,
				FailureCount:       1,
				LastFailure:        time.Now(),
				Degraded:           false,
				DegradedFactor:     1.0,
				DomainFailureCount: map[string]int{domain: 1},
			}
		} else {
			nodeState.FailureCount++
			nodeState.LastFailure = time.Now()
			if nodeState.DomainFailureCount == nil {
				nodeState.DomainFailureCount = make(map[string]int)
			}
			nodeState.DomainFailureCount[domain]++
		}
	} else {
		nodeState = smart.NodeState{
			Name:               proxyName,
			FailureCount:       1,
			LastFailure:        time.Now(),
			Degraded:           false,
			DegradedFactor:     1.0,
			DomainFailureCount: map[string]int{domain: 1},
		}
	}

	failedDomainCount := 0
	for _, cnt := range nodeState.DomainFailureCount {
		if cnt >= mildFailureCountThreshold {
			failedDomainCount++
		}
	}

	if failedDomainCount >= domainCountThreshold {
		switch {
		case failedDomainCount >= domainCountThreshold*4:
			nodeState.Degraded = true
			nodeState.DegradedFactor = 0.4
			nodeState.BlockedUntil = time.Now().Add(60 * time.Minute)
			isDegraded = true
		case failedDomainCount >= domainCountThreshold*2:
			nodeState.Degraded = true
			nodeState.DegradedFactor = 0.6
			nodeState.BlockedUntil = time.Now().Add(45 * time.Minute)
			isDegraded = true
		default:
			nodeState.Degraded = true
			nodeState.DegradedFactor = 0.8
			nodeState.BlockedUntil = time.Now().Add(30 * time.Minute)
			isDegraded = true
		}

		additionalBlock := 0
		if nodeState.FailureCount >= severeFailureCountThreshold {
			nodeState.DegradedFactor = nodeState.DegradedFactor * 0.7
			additionalBlock = 30
		} else if nodeState.FailureCount >= mediumFailureCountThreshold {
			nodeState.DegradedFactor = nodeState.DegradedFactor * 0.8
			additionalBlock = 20
		} else if nodeState.FailureCount >= mildFailureCountThreshold {
			nodeState.DegradedFactor = nodeState.DegradedFactor * 0.9
			additionalBlock = 10
		}
		if nodeState.BlockedUntil.After(time.Now()) {
			nodeState.BlockedUntil = nodeState.BlockedUntil.Add(time.Duration(additionalBlock) * time.Minute)
		} else {
			nodeState.BlockedUntil = time.Now().Add(time.Duration(additionalBlock) * time.Minute)
		}
	}

	if nodeStateBytes, err := json.Marshal(&nodeState); err == nil {
		operation := smart.StoreOperation{
			Type:   smart.OpSaveNodeState,
			Group:  s.Name(),
			Config: s.configName,
			Node:   proxyName,
			Data:   nodeStateBytes,
		}
		s.store.BatchSaveConnStats([]smart.StoreOperation{operation})
	}

	if isDegraded {
		return math.Max(0.1, calculatedWeight*nodeState.DegradedFactor), isDegraded
	} else {
		return calculatedWeight, isDegraded
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
func (s *Smart) logConnectionStats(record *smart.StatsRecord, metadata *C.Metadata, baseWeight, priorityFactor float64,
	addressDisplay, proxyName string, uploadTotal, downloadTotal, maxUploadRate, maxDownloadRate float64,
	connectionDuration int64, asnInfo string, isModelPredicted bool) {

	var tcpAsnWeight, udpAsnWeight float64
	var asnDisplayInfo string

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
		asnDisplayInfo = metadata.DstIPASN
	} else {
		asnDisplayInfo = "unknown"
	}

	weightSource := "Traditional"
	if isModelPredicted {
		weightSource = "LightGBM"
	}

	log.Debugln("[Smart] Updated weights: (Model: [%s], TCP: [%.4f], UDP: [%.4f], TCP ASN: [%.4f], UDP ASN: [%.4f], Base: [%.4f], Priority: [%.2f]) "+
		"For (Group: [%s] - Node: [%s] - Network: [%s] - Address: [%s] - ASN: [%s]) "+
		"- Current: (Up: [%s], Down: [%s], Max Up Speed: [%s], Max Down Speed: [%s], Duration: [%s]) "+
		"- History: (Success: [%d], Failure: [%d], Connect: [%s], Latency: [%s], Total Up: [%s], Total Down: [%s], Max Up Speed: [%s], Max Down Speed: [%s], Avg Duration: [%s])",
		weightSource, record.Weights[smart.WeightTypeTCP], record.Weights[smart.WeightTypeUDP], tcpAsnWeight, udpAsnWeight, baseWeight, priorityFactor,
		s.Name(), proxyName, strings.ToUpper(metadata.NetWork.String()), addressDisplay, asnDisplayInfo,
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
func (s *Smart) collectConnectionData(status string, record *smart.StatsRecord, metadata *C.Metadata,
	uploadTotal, downloadTotal, maxUploadRate, maxDownloadRate, baseWeight float64, proxyName string, isModelPredicted bool) {

	// 采样率控制
	if s.sampleRate < 1.0 && rand.Float64() > s.sampleRate {
		return
	}

	var input *lightgbm.ModelInput

	if status == "failed" {
		input = lightgbm.CreateModelInputFromStats(
			record.Success, record.Failure, record.ConnectTime, record.Latency,
			0, 0, 0, 0, 0, 0, 0, 0,
			0, record.LastUsed.Unix(),
			metadata.NetWork == C.UDP, metadata.NetWork == C.TCP,
			metadata,
		)
	} else if status == "closed" {
		input = lightgbm.CreateModelInputFromStatsRecord(
			record, metadata,
			uploadTotal, downloadTotal, maxUploadRate, maxDownloadRate,
		)
	}

	if input != nil {
		input.GroupName = s.Name()
		input.NodeName = proxyName
		weightSource := "Traditional"

		if isModelPredicted {
			weightSource = "LightGBM"
		}

		s.dataCollector.AddSample(input, metadata, baseWeight, weightSource)
	}
}

func updateAverageValue(oldValue int64, newValue int64, count int64) int64 {
	var newAverage int64

	if oldValue > 0 && count > 1 {
		newAverage = (oldValue*5 + newValue*5) / 6
	} else {
		newAverage = newValue
	}

	return newAverage
}

func (s *Smart) recordConnectionStats(status string, metadata *C.Metadata, proxy C.Proxy,
	connectTime int64, latency int64, uploadTotal int64, downloadTotal int64, maxUploadRate int64, maxDownloadRate int64,
	connectionDuration int64, fromLongConnProcess bool, err error) {

	if s.store == nil || proxy == nil || metadata == nil {
		return
	}

	domain, rawDomain := smart.GetEffectiveDomain(metadata.Host, metadata.DstIP.String())
	if domain == "" {
		return
	}

	addressDisplay := rawDomain
	if rawDomain != "" && domain != "" && rawDomain != domain {
		addressDisplay = fmt.Sprintf("%s (Wildcard: %s)", rawDomain, domain)
	}

	if status == "failed" {
		s.store.MarkConnectionFailed(s.Name(), s.configName, len(s.GetProxies(false)), map[string]bool{proxy.Name(): true})
	} else if status == "success" {
		s.store.MarkConnectionSuccess(s.Name(), s.configName)
	}

	cacheKey := smart.FormatCacheKey(smart.KeyTypeStats, s.configName, s.Name(), domain, proxy.Name())
	priorityFactor := s.getPriorityFactor(proxy.Name())
	weightType := smart.WeightTypeTCP
	if metadata.NetWork == C.UDP {
		weightType = smart.WeightTypeUDP
	}

	asnInfo := s.getASNCode(metadata)
	lock := smart.GetDomainNodeLock(domain, s.Name(), proxy.Name())
	lock.Lock()
	defer lock.Unlock()

	atomicManager := smart.GetAtomicManager()
	if atomicManager == nil {
		return
	}

	atomicRecord := atomicManager.GetOrCreateAtomicRecord(cacheKey, s.store, s.Name(), s.configName, domain, proxy.Name())
	if atomicRecord == nil {
		return
	}

	var baseWeight, calculatedWeight, oldWeight, uploadTotalMB, downloadTotalMB, maxUploadRateKB, maxDownloadRateKB float64
	var needCheckQuality bool
	var needDataCollection bool
	var isModelPredicted bool

	switch status {
	case "success":
		atomicRecord.Add("success", int64(1))

		if connectTime > 0 {
			success := atomicRecord.Get("success").(int64)
			oldConnectTime := atomicRecord.Get("connectTime").(int64)
			newConnectTime := updateAverageValue(oldConnectTime, connectTime, success)
			atomicRecord.Set("connectTime", newConnectTime)
		}

		if latency > 0 {
			success := atomicRecord.Get("success").(int64)
			oldLatency := atomicRecord.Get("latency").(int64)
			newLatency := updateAverageValue(oldLatency, latency, success)
			atomicRecord.Set("latency", newLatency)
		}
	case "failed":
		atomicRecord.Add("failure", int64(1))
		success := atomicRecord.Get("success").(int64)
		failure := atomicRecord.Get("failure").(int64)
		connectTimeVal := atomicRecord.Get("connectTime").(int64)
		latencyVal := atomicRecord.Get("latency").(int64)
		lastUsedVal := atomicRecord.Get("lastUsed").(int64)

		if s.useLightGBM && s.weightModel != nil {
			input := lightgbm.CreateModelInputFromStats(
				success, failure, connectTimeVal, latencyVal,
				0, 0, 0, 0, 0, 0, 0, 0, 0, lastUsedVal,
				metadata.NetWork == C.UDP, metadata.NetWork == C.TCP,
				metadata,
			)
			if input != nil {
				calculatedWeight, isModelPredicted = s.weightModel.PredictWeight(input, priorityFactor)
			} else {
				calculatedWeight = smart.CalculateWeight(
					success, failure, connectTimeVal, latencyVal,
					metadata.NetWork == C.UDP, 0, 0, 0, 0, 0, lastUsedVal) * priorityFactor
				isModelPredicted = false
			}
		} else {
			calculatedWeight = smart.CalculateWeight(
				success, failure, connectTimeVal, latencyVal,
				metadata.NetWork == C.UDP, 0, 0, 0, 0, 0, lastUsedVal) * priorityFactor
			isModelPredicted = false
		}

		needDataCollection = s.collectData && s.dataCollector != nil
	case "closed":
		weights := atomicRecord.Get("weights")
		if weights != nil {
			weightsMap := weights.(map[string]float64)
			oldWeight = weightsMap[weightType]
		}

		uploadTotalMB = float64(uploadTotal) / (1024.0 * 1024.0)
		downloadTotalMB = float64(downloadTotal) / (1024.0 * 1024.0)
		maxUploadRateKB = float64(maxUploadRate) / 1024.0
		maxDownloadRateKB = float64(maxDownloadRate) / 1024.0

		if !fromLongConnProcess {
			atomicRecord.Add("uploadTotal", uploadTotalMB)
			atomicRecord.Add("downloadTotal", downloadTotalMB)

			if connectionDuration > 0 {
				s.updateConnectionDuration(atomicRecord, connectionDuration)
			}
		}

		oldMaxUploadRate := atomicRecord.Get("maxUploadRate").(float64)
		if maxUploadRateKB > oldMaxUploadRate {
			atomicRecord.Set("maxUploadRate", maxUploadRateKB)
		}

		oldMaxDownloadRate := atomicRecord.Get("maxDownloadRate").(float64)
		if maxDownloadRateKB > oldMaxDownloadRate {
			atomicRecord.Set("maxDownloadRate", maxDownloadRateKB)
		}

		success := atomicRecord.Get("success").(int64)
		failure := atomicRecord.Get("failure").(int64)
		connectTimeVal := atomicRecord.Get("connectTime").(int64)
		latencyVal := atomicRecord.Get("latency").(int64)
		durationVal := atomicRecord.Get("duration").(float64)
		lastUsedVal := atomicRecord.Get("lastUsed").(int64)

		if s.useLightGBM && s.weightModel != nil {
			tempRecord := atomicRecord.CreateStatsSnapshot()
			input := lightgbm.CreateModelInputFromStatsRecord(
				tempRecord, metadata,
				uploadTotalMB, downloadTotalMB, maxUploadRateKB, maxDownloadRateKB,
			)
			if input != nil {
				calculatedWeight, isModelPredicted = s.weightModel.PredictWeight(input, priorityFactor)
			} else {
				calculatedWeight = smart.CalculateWeight(
					success, failure, connectTimeVal, latencyVal,
					metadata.NetWork == C.UDP, uploadTotalMB, downloadTotalMB, maxUploadRateKB, maxDownloadRateKB, durationVal, lastUsedVal) * priorityFactor
				isModelPredicted = false
			}
		} else {
			calculatedWeight = smart.CalculateWeight(
				success, failure, connectTimeVal, latencyVal,
				metadata.NetWork == C.UDP, uploadTotalMB, downloadTotalMB, maxUploadRateKB, maxDownloadRateKB, durationVal, lastUsedVal) * priorityFactor
			isModelPredicted = false
		}

		needDataCollection = s.collectData && s.dataCollector != nil
		s.checkAndLimitStats(atomicRecord)
		atomicRecord.SetWeight(weightType, calculatedWeight)

		if asnInfo != "" {
			s.updateAsnWeights(atomicRecord, asnInfo, calculatedWeight, metadata.NetWork == C.UDP)
		}

		if oldWeight > 0 && calculatedWeight > 0 {
			needCheckQuality = true
		}
	}

	// 额外检查和权重调整
	var degradedWeight float64
	var isDegraded bool

	if status == "failed" {
		degradedWeight, isDegraded = s.handleFailedConnection(proxy.Name(), cacheKey, domain, calculatedWeight, weightType)
	}

	if status == "closed" {
		if needCheckQuality {
			historyMaxUploadRateKB := atomicRecord.Get("maxUploadRate").(float64)
			historyMaxDownloadRateKB := atomicRecord.Get("maxDownloadRate").(float64)
			historyUploadTotal := atomicRecord.Get("uploadTotal").(float64)
			historyDownloadTotal := atomicRecord.Get("downloadTotal").(float64)
			success := atomicRecord.Get("success").(int64)
			status := atomicRecord.Get("status").(int64)
			lastUsedVal := atomicRecord.Get("lastUsed").(int64)

			degradedWeight, isDegraded = s.checkNodeQualityDegradation(
				metadata, proxy, atomicRecord,
				addressDisplay, proxy.Name(), calculatedWeight, oldWeight,
				connectionDuration, uploadTotalMB, downloadTotalMB,
				maxUploadRateKB, maxDownloadRateKB, historyMaxUploadRateKB, historyMaxDownloadRateKB,
				historyUploadTotal, historyDownloadTotal, success, weightType,
				status, lastUsedVal)
		}
	}

	if status == "failed" || status == "closed" {
		if isDegraded {
			calculatedWeight = degradedWeight
		}

		baseWeight = calculatedWeight / priorityFactor

		atomicRecord.SetWeight(weightType, calculatedWeight)

		if asnInfo != "" {
			s.updateAsnWeights(atomicRecord, asnInfo, calculatedWeight, metadata.NetWork == C.UDP)
		}
	}

	statsSnapshot := atomicRecord.CreateStatsSnapshot()

	if isDegraded {
		go s.cleanupDegradedNodePreferenceCache(metadata, domain, addressDisplay, proxy.Name(), calculatedWeight, weightType, asnInfo)
	}

	// 日志输出
	if status == "closed" {
		if !fromLongConnProcess {
			s.logConnectionStats(statsSnapshot, metadata, baseWeight, priorityFactor, addressDisplay, proxy.Name(),
				uploadTotalMB, downloadTotalMB, maxUploadRateKB, maxDownloadRateKB, connectionDuration, asnInfo, isModelPredicted)
		}
	}

	// 数据收集
	if needDataCollection {
		s.collectConnectionData(status, statsSnapshot, metadata, uploadTotalMB, downloadTotalMB, maxUploadRateKB, maxDownloadRateKB, baseWeight, proxy.Name(), isModelPredicted)
	}

	// 保存统计记录
	s.saveStatsRecord(cacheKey, domain, proxy, statsSnapshot, time.Now())
}

func (s *Smart) registerClosureMetricsCallback(c C.Conn, proxy C.Proxy, metadata *C.Metadata) C.Conn {
	return callback.NewCloseCallbackConn(c, func() {
		if metadata != nil && metadata.UUID != "" {
			tracker := statistic.DefaultManager.Get(metadata.UUID)
			if tracker != nil {
				info := tracker.Info()
				uploadTotal := info.UploadTotal.Load()
				downloadTotal := info.DownloadTotal.Load()
				connectionDuration := time.Since(info.Start).Milliseconds()
				maxUploadRate := info.MaxUploadRate.Load()
				maxDownloadRate := info.MaxDownloadRate.Load()

				s.recordConnectionStats("closed", metadata, proxy, 0, 0,
					uploadTotal, downloadTotal, maxUploadRate, maxDownloadRate, connectionDuration, false, nil)
				return
			}
		}
	})
}

func (s *Smart) registerPacketClosureMetricsCallback(pc C.PacketConn, proxy C.Proxy, metadata *C.Metadata) C.PacketConn {
	return callback.NewCloseCallbackPacketConn(pc, func() {
		if metadata != nil && metadata.UUID != "" {
			tracker := statistic.DefaultManager.Get(metadata.UUID)
			if tracker != nil {
				info := tracker.Info()
				uploadTotal := info.UploadTotal.Load()
				downloadTotal := info.DownloadTotal.Load()
				connectionDuration := time.Since(info.Start).Milliseconds()
				maxUploadRate := info.MaxUploadRate.Load()
				maxDownloadRate := info.MaxDownloadRate.Load()

				s.recordConnectionStats("closed", metadata, proxy, 0, 0,
					uploadTotal, downloadTotal, maxUploadRate, maxDownloadRate, connectionDuration, false, nil)
				return
			}
		}
	})
}

func (s *Smart) processLongConnections(threshold time.Duration) {
	if s.store == nil {
		return
	}

	_, err, _ := longConnProcessGroup.Do(s.Name()+"-"+s.configName, func() (interface{}, error) {
		statistic.DefaultManager.Range(func(t statistic.Tracker) bool {
			info := t.Info()
			if info == nil || info.Metadata == nil || info.Chain == nil || len(info.Chain) < 2 {
				return true
			}

			connectionAge := time.Since(info.Start)
			if connectionAge < threshold {
				return true
			}

			var throughThisPolicy bool
			var proxyName string

			for i, c := range info.Chain {
				if c == s.Name() {
					throughThisPolicy = true
					proxyName = info.Chain[i-1]
					break
				}
			}

			if !throughThisPolicy || proxyName == "" {
				return true
			}

			uploadTotal := info.UploadTotal.Load()
			downloadTotal := info.DownloadTotal.Load()
			connectionDuration := connectionAge.Milliseconds()
			maxUploadRate := info.MaxUploadRate.Load()
			maxDownloadRate := info.MaxDownloadRate.Load()

			if uploadTotal > 0 || downloadTotal > 0 {
				for _, p := range s.GetProxies(false) {
					if p.Name() == proxyName {
						s.recordConnectionStats("closed", info.Metadata, p, 0, 0,
							uploadTotal, downloadTotal, maxUploadRate, maxDownloadRate, connectionDuration, true, nil)
						break
					}
				}
			}

			return true
		})
		return nil, nil
	})

	if err != nil {
		log.Debugln("[Smart] Error processing long connections: %v", err)
	}

}

func (s *Smart) cleanupDegradedNodePreferenceCache(metadata *C.Metadata, domain, addressDisplay string, nodeName string, currentWeight float64, weightType string, asnInfo string) {
	if s.store == nil {
		return
	}

	lock := smart.GetDomainNodeLock(domain, s.Name(), nodeName)
	lock.Lock()
	defer lock.Unlock()

	s.store.DeleteCacheResult(smart.KeyTypePrefetch, s.Name(), s.configName, domain)

	// 处理域名相关缓存
	bestNodes, bestWeights, err := s.store.GetBestProxyForTarget(s.Name(), s.configName, domain, weightType, false)
	var bestNode string
	var bestWeight float64
	for i := 0; i < len(bestNodes); i++ {
		if bestNodes[i] != "" && bestNodes[i] != nodeName {
			bestNode = bestNodes[i]
			bestWeight = bestWeights[i]
			break
		}
	}
	if err == nil && bestNode != "" && bestWeight > currentWeight {
		s.store.StorePrefetchResult(s.Name(), s.configName, domain, weightType, bestNode, bestWeight)
		log.Debugln("[Smart] Added new prefetch result for domain: [%s] -> [%s] (weight: %.4f, type: %s)",
			addressDisplay, bestNode, bestWeight, weightType)
	}

	// 处理ASN相关缓存
	if asnInfo != "" {
		asnWeightType := smart.WeightTypeTCPASN
		if weightType == smart.WeightTypeUDP {
			asnWeightType = smart.WeightTypeUDPASN
		}
		fullAsnWeightType := asnWeightType + ":" + asnInfo

		s.store.DeleteCacheResult(smart.KeyTypePrefetch, s.Name(), s.configName, asnInfo)

		bestNodes, bestWeights, err := s.store.GetBestProxyForTarget(s.Name(), s.configName, asnInfo, fullAsnWeightType, false)
		var asnBestNode string
		var asnBestWeight float64
		for i := 0; i < len(bestNodes); i++ {
			if bestNodes[i] != "" && bestNodes[i] != nodeName {
				asnBestNode = bestNodes[i]
				asnBestWeight = bestWeights[i]
				break
			}
		}
		if err == nil && asnBestNode != "" && asnBestWeight > currentWeight {
			s.store.StorePrefetchResult(s.Name(), s.configName, asnInfo, fullAsnWeightType, asnBestNode, asnBestWeight)
			log.Debugln("[Smart] Added new ASN prefetch result: [%s] -> [%s] (weight: %.4f, type: %s)",
				asnInfo, asnBestNode, asnBestWeight, fullAsnWeightType)
		}
	}

	// 处理sticky-sessions的缓存
	if s.fallback != nil && s.strategy == "sticky-sessions" {
		s.fallback.ClearStickySession(metadata)
	}
}

func (s *Smart) getPriorityFactor(proxyName string) float64 {
	for _, rule := range s.policyPriority {
		if rule.isRegex && rule.regex != nil {
			if matched, _ := rule.regex.MatchString(proxyName); matched {
				return rule.factor
			}
		} else if strings.Contains(proxyName, rule.pattern) {
			return rule.factor
		}
	}
	return 1.0
}

func smartWithPolicyPriority(policyPriority string) smartOption {
	return func(s *Smart) {
		pairs := strings.Split(policyPriority, ";")
		for _, pair := range pairs {
			kv := strings.SplitN(pair, ":", 2)
			if len(kv) != 2 || strings.TrimSpace(kv[1]) == "" {
				log.Warnln("[Smart] Invalid policy-priority rule: '%s', must be in 'pattern:factor' format and factor is required", pair)
				continue
			}
			pattern := kv[0]
			if factor, err := strconv.ParseFloat(kv[1], 64); err == nil {
				if factor <= 0 {
					log.Warnln("[Smart] Invalid priority factor %.2f for pattern '%s', factor must be positive", factor, pattern)
					continue
				}

				rule := priorityRule{
					pattern: pattern,
					factor:  factor,
				}

				if re, err := regexp2.Compile(pattern, regexp2.None); err == nil {
					rule.regex = re
					rule.isRegex = true
				}

				s.policyPriority = append(s.policyPriority, rule)
			} else {
				log.Warnln("[Smart] Invalid priority factor format for pattern '%s': %v", pattern, err)
			}
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

func smartWithStrategy(config map[string]any) string {
	if strategy, ok := config["strategy"].(string); ok {
		return strategy
	}
	return "sticky-sessions"
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

func parseSmartOption(config map[string]any) ([]smartOption, string) {
	opts := []smartOption{}

	strategy := smartWithStrategy(config)

	if elm, ok := config["policy-priority"]; ok {
		if policyPriority, ok := elm.(string); ok {
			opts = append(opts, smartWithPolicyPriority(policyPriority))
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

	return opts, strategy
}

func (s *Smart) getASNCode(metadata *C.Metadata) string {
	if metadata == nil || !metadata.DstIP.IsValid() || metadata.DstIPASN == "unknown" {
		return ""
	}

	if metadata.DstIPASN == "" {
		if !asnAvailable {
			return ""
		}
		asn, aso := mmdb.ASNInstance().LookupASN(metadata.DstIP.AsSlice())
		if asn == "" {
			metadata.DstIPASN = "unknown"
		} else {
			metadata.DstIPASN = asn + " " + aso
		}
		return asn
	}

	return strings.SplitN(metadata.DstIPASN, " ", 2)[0]
}

func (s *Smart) Close() error {
	if s.cancel != nil {
		s.cancel()
	}

	s.wg.Wait()

	if s.store != nil && !flushQueueOnce.Swap(true) {
		s.store.FlushQueue(true)
	}

	lightgbm.CloseAllCollectors()

	smartInitOnce = sync.Once{}
	preloadOnce = sync.Once{}

	return nil
}
