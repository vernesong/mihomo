package outboundgroup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dlclark/regexp2"
	"github.com/metacubex/mihomo/common/callback"
	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/common/utils"
	"github.com/metacubex/mihomo/component/geodata"
	"github.com/metacubex/mihomo/component/mmdb"
	"github.com/metacubex/mihomo/component/profile/cachefile"
	"github.com/metacubex/mihomo/component/smart"
	"github.com/metacubex/mihomo/component/smart/lightgbm"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/constant/provider"
	"github.com/metacubex/mihomo/log"
	"github.com/metacubex/mihomo/tunnel"
	"github.com/metacubex/mihomo/tunnel/statistic"
)

const (
	prefetchInterval         = 10 * time.Minute
	cleanupInterval          = 180 * time.Minute
	cacheParamAdjustInterval = 5 * time.Minute
	recoveryCheckInterval    = 5 * time.Minute
	checkInterval            = 10 * time.Minute
	flushQueueInterval       = 300 * time.Second
	rankingInterval          = 60 * time.Minute

	failureRecovery5min      = 5 * time.Minute
	failureRecovery10min     = 10 * time.Minute
	failureRecovery15min     = 15 * time.Minute
	failureRecovery30min     = 30 * time.Minute

	maxRetries               = 3
	maxSelected              = 9
	allowedWeight            = 0.4
)

var (
	flushQueueOnce       atomic.Bool
	smartInitOnce        sync.Once
	preloadOnce          sync.Once
	asnAvailable         bool
)

var cdnASNs = map[string]struct{}{
	"13335": {}, // Cloudflare
	"12222": {}, // Akamai
	"16625": {}, // Akamai
	"20940": {}, // Akamai
	"31110": {}, // Akamai
	"35994": {}, // Akamai
	"54113": {}, // Fastly
	"22822": {}, // Limelight Networks
	"15133": {}, // EdgeCast (Verizon)
	"19551": {}, // Incapsula (Imperva)
	"20446": {}, // StackPath / Bunny
	"60068": {}, // CDN77
	"16509": {}, // Amazon CloudFront
	"36408": {}, // CDNetworks
	"4809":  {}, // ChinaCache
	"199524":{}, // Gcore
	"212238":{}, // BelugaCDN
	"55933": {}, // QUANTIL
	"43260": {}, // Medianova
	"43317": {}, // CDNvideo
	"43996": {}, // CDNsun
	"52320": {}, // GlobeNet
}

type smartOption func(*Smart)

type Smart struct {
	*GroupBase
	store          *smart.Store
	fallback       *LoadBalance
	dataCollector  *lightgbm.DataCollector
	weightModel    *lightgbm.WeightModel
	ctx            context.Context
	cancel         context.CancelFunc

	configName     string
	selected       string
	testUrl        string
	expectedStatus string
	strategy       string
	Icon           string

	policyPriority []priorityRule
	wg             sync.WaitGroup

	sampleRate     float64

	disableUDP     bool
	Hidden         bool
	useLightGBM    bool
	collectData    bool
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

type nodeWeight struct {
	name   string
	weight float64
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

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make(chan dialResult, len(proxies))

	errs := make([]error, 0, len(proxies))

	racer := func(proxyIndex int) {
		if ctx.Err() != nil {
			return
		}

		conn, connectTime, err := singleDialFunc(ctx, proxies[proxyIndex], metadata, start)

		result := dialResult{
			proxyIndex:  proxyIndex,
			conn:        conn,
			connectTime: connectTime,
			error:       err,
		}

		select {
		case results <- result:
		case <-ctx.Done():
			if conn != nil && err == nil {
				_ = conn.Close()
			}
		}
	}

	for i := 0; i < len(proxies); i++ {
		go racer(i)
	}

	completedCount := 0
	for completedCount < len(proxies) {
		select {
		case res := <-results:
			completedCount++
			if res.error == nil {
				cancel()
				return proxies[res.proxyIndex], res.conn, res.connectTime, nil
			}
			errs = append(errs, res.error)

		case <-ctx.Done():
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
		if !errors.Is(err, context.Canceled) {
			go s.recordConnectionStats("failed", metadata, proxy, connectTime, 0, 0, 0, 0, 0, 0, err)
		}
		return nil, connectTime, err
	}

	return c, connectTime, nil
}

func (s *Smart) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	availableProxies := s.GetProxies(true)
	triedProxies := make(map[string]bool)
	metadata.SmartBlock = "normal"

	getBatch := func(proxies []C.Proxy, i int) ([]C.Proxy, time.Duration) {
		const parallelDials = 3
		begin := i * parallelDials
		if begin >= len(proxies) {
			return nil, 0
		}
		end := begin + parallelDials
		if end > len(proxies) {
			end = len(proxies)
		}
		batch := proxies[begin:end:end]
		var historyConnectTime int64
		for _, p := range batch {
			triedProxies[p.Name()] = true
			hct := s.getHistoryConnectStats(metadata, p)
			if hct > historyConnectTime {
				historyConnectTime = hct
			}
		}
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
		return batch, timeout
	}

	tryDial := func(proxies []C.Proxy) (C.Conn, error) {
		var finalErr error
		for i := 0; i < maxRetries; i++ {
			if i > 0 {
				baseDelay := time.Duration(math.Pow(2, float64(i-1))) * 50 * time.Millisecond
				jitterRange := 0.2
				jitter := 1.0 + (rand.Float64()*2-1)*jitterRange
				backoffDuration := time.Duration(float64(baseDelay) * jitter)

				select {
				case <-time.After(backoffDuration):
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}

			batch, timeout := getBatch(proxies, i)
			if len(batch) == 0 {
				break
			}

			ctxDial, cancel := context.WithTimeout(ctx, timeout)
			start := time.Now()
			p, c, connectTime, err := s.ParallelDialContext(ctxDial, batch, metadata, start, s.singleDialContext)
			cancel()

			if err == nil {
				return s.WrapConnWithMetric(c, p, metadata, connectTime), nil
			}
			finalErr = err

			if s.selected != "" && len(proxies) == 1 && proxies[0].Name() == s.selected {
				break
			}
		}
		if finalErr != nil {
			s.store.MarkConnectionFailed(s.Name(), s.configName, len(proxies), triedProxies, metadata)
		}
		return nil, finalErr
	}

	if s.store.CheckNetworkFailure(s.Name(), s.configName) {
		proxies := s.selectFallbacks(metadata, availableProxies)
		return tryDial(proxies)
	}

	proxies := s.selectProxies(metadata, availableProxies)
	return tryDial(proxies)
}

func (s *Smart) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (pc C.PacketConn, err error) {
	proxies := s.GetProxies(true)
	triedProxies := make(map[string]bool)
	metadata.SmartBlock = "normal"

	fillAvailableProxies := func(initialProxies []C.Proxy, allProxies []C.Proxy) []C.Proxy {
		availableProxies := make([]C.Proxy, 0, len(initialProxies))
		seen := make(map[string]struct{})

		for _, p := range initialProxies {
			if p.SupportUDP() {
				availableProxies = append(availableProxies, p)
				seen[p.Name()] = struct{}{}
			}
		}

		if len(availableProxies) < maxRetries && len(allProxies) >= maxRetries {
			shuffledProxies := make([]C.Proxy, len(allProxies))
			copy(shuffledProxies, allProxies)
			rand.Shuffle(len(shuffledProxies), func(i, j int) {
				shuffledProxies[i], shuffledProxies[j] = shuffledProxies[j], shuffledProxies[i]
			})
			for _, fp := range shuffledProxies {
				if len(availableProxies) >= maxRetries {
					break
				}
				if _, exists := seen[fp.Name()]; !exists && fp.SupportUDP() {
					availableProxies = append(availableProxies, fp)
					seen[fp.Name()] = struct{}{}
				}
			}
		}

		if len(availableProxies) == 0 {
			shuffledProxies := make([]C.Proxy, len(allProxies))
			copy(shuffledProxies, allProxies)
			rand.Shuffle(len(shuffledProxies), func(i, j int) {
				shuffledProxies[i], shuffledProxies[j] = shuffledProxies[j], shuffledProxies[i]
			})
			for _, fp := range shuffledProxies {
				if len(availableProxies) >= maxRetries {
					break
				}
				if _, exists := seen[fp.Name()]; !exists {
					availableProxies = append(availableProxies, fp)
					seen[fp.Name()] = struct{}{}
				}
			}
		}

		return availableProxies
	}

	var availableProxies []C.Proxy

	if s.selected != "" {
		for _, p := range proxies {
			if p.Name() == s.selected {
				availableProxies = []C.Proxy{p}
				break
			}
		}
	}

	if len(availableProxies) == 0 {
		if s.store.CheckNetworkFailure(s.Name(), s.configName) {
			selectedProxies := s.selectFallbacks(metadata, proxies)
			availableProxies = fillAvailableProxies(selectedProxies, proxies)
		} else {
			selectedProxies := s.selectProxies(metadata, proxies)
			availableProxies = fillAvailableProxies(selectedProxies, proxies)
		}
	}

	var finalErr error
	var proxy C.Proxy
	for i := 0; i < maxRetries && i < len(availableProxies); i++ {
		proxy = availableProxies[i]
		triedProxies[proxy.Name()] = true
		historyConnectTime := s.getHistoryConnectStats(metadata, proxy)
		const thresholdRatio = 2.0
		var timeout time.Duration
		if historyConnectTime > 0 {
			timeout = time.Duration(float64(historyConnectTime)*thresholdRatio) * time.Millisecond
			if timeout > C.DefaultUDPTimeout {
				timeout = C.DefaultUDPTimeout
			}
		} else {
			timeout = C.DefaultUDPTimeout
		}
		ctxDial, cancel := context.WithTimeout(ctx, timeout)
		start := time.Now()
		pc, err = proxy.ListenPacketContext(ctxDial, metadata)
		cancel()
		connectTime := time.Since(start).Milliseconds()

		if err == nil {
			pc.AppendToChains(s)
			pc = s.registerPacketClosureMetricsCallback(pc, proxy, metadata, connectTime)
			return pc, nil
		}
		finalErr = err
		go s.recordConnectionStats("failed", metadata, proxy, connectTime, 0, 0, 0, 0, 0, 0, err)
		if s.selected != "" && len(availableProxies) == 1 && availableProxies[0].Name() == s.selected {
			break
		}
	}

	return nil, finalErr
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

	proxies = s.selectProxies(metadata, proxies)
	domain, _ := smart.GetEffectiveDomain(metadata.Host, metadata.DstIP.String())
	s.store.StoreUnwrapResult(s.Name(), s.configName, domain, proxies)

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

	var firstWriteErr error
	var firstReadErr error
	var firstReadLatency int64

	if N.NeedHandshake(c) {
		c = callback.NewFirstWriteCallBackConn(c, func(err error) {
			firstWriteErr = err
		})
	}

	c = callback.NewFirstReadCallBackConn(c, func(err error) {
		firstReadLatency = time.Since(start).Milliseconds()
		firstReadErr = err
	})

	return s.registerClosureMetricsCallback(c, proxy, metadata, connectTime, firstReadLatency, firstReadErr, firstWriteErr)
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

func (s *Smart) InitSmart() {
	cacheFile := cachefile.Cache()
	if cacheFile == nil || cacheFile.DB == nil {
		log.Fatalln("[Smart] DB Cache file is nil for group %s", s.Name())
	}

	smartStore := cachefile.NewSmartStore(cacheFile)
	if smartStore == nil {
		log.Fatalln("[Smart] Failed to create SmartStore for group %s", s.Name())
	}

	s.store = smartStore.GetStore()

	s.ctx, s.cancel = context.WithCancel(context.Background())

	smartInitOnce.Do(func() {
		s.startTimedTask(5*time.Minute, checkInterval, "Clean up groups", s.cleanupOrphanedGroups, true)
		s.startTimedTask(5*time.Minute, cacheParamAdjustInterval, "Cache parameter adjustment", s.store.AdjustCacheParameters, false)
		s.startTimedTask(5*time.Minute, flushQueueInterval, "Queue flush", func() {
			s.store.FlushQueue(false)
		}, false)

		// try load ASN database
		if !asnAvailable {
			if err := geodata.InitASN(); err == nil {
				asnAvailable = true
			} else {
				log.Warnln("[Smart] Failed to load ASN database: %v", err)
			}
		}
	})

	s.startTimedTask(5*time.Minute, checkInterval, "Clean up nodes", s.cleanupOrphanedNodeCache, true)
	s.startTimedTask(5*time.Second, checkInterval, "Preload frequent data", func() {
		preloadOnce.Do(func() {
			s.store.AdjustCacheParameters()
		})
		s.store.PreloadFrequentData(s.Name(), s.configName)
	}, true)
	s.startTimedTask(5*time.Minute, prefetchInterval, "prefetch", s.runPrefetch, false)
	s.startTimedTask(30*time.Second, rankingInterval, "ranking", s.updateNodeRanking, false)
	s.startTimedTask(5*time.Minute, recoveryCheckInterval, "Recovery check", s.checkAndRecoverDegradedNodes, false)
	s.startTimedTask(5*time.Minute, cleanupInterval, "Expired cleanup", func() {
		_ = s.store.CleanupExpiredStats(s.Name(), s.configName)
	}, false)
	s.startTimedTask(5*time.Minute, cleanupInterval, "OldDomains cleanup", func() {
		_ = s.store.CleanupOldDomains(s.Name(), s.configName)
	}, false)
	s.startTimedTask(5*time.Second, checkInterval, "Init LGBM Collector", func() {
		// load after tunnel.Running because size option ready later than group init
		if s.collectData {
			s.dataCollector = lightgbm.GetCollector()
		}
	}, true)

	if s.useLightGBM {
		s.weightModel = lightgbm.GetModel()
	}
}

// task run after tunnel.Running
func (s *Smart) startTimedTask(initialDelay, interval time.Duration, taskName string, task func(), runOnce bool) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		for tunnel.Status() != tunnel.Running {
			select {
			case <-time.After(100 * time.Millisecond):
			case <-s.ctx.Done():
				return
			}
		}

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
			log.Debugln("[Smart] Task %s for group [%s] set to run once, exiting",
				taskName, s.Name())
			return
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
	proxyMap := make(map[string]string)
	for _, p := range proxies {
		if p.AliveForTestUrl(s.testUrl) {
			proxyMap[p.Name()] = p.Name()
		}
	}
	s.store.RunPrefetch(s.Name(), s.configName, proxyMap)
}

func (s *Smart) updateNodeRanking() {
	log.Debugln("[Smart] Starting node ranking update for policy group [%s]", s.Name())

	proxies := s.GetProxies(true)
	proxyNames := make([]string, 0, len(proxies))
	for _, p := range proxies {
		proxyNames = append(proxyNames, p.Name())
	}
	ranking, err := s.store.GetNodeWeightRanking(s.Name(), s.configName, proxyNames)
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

func (s *Smart) selectFallbacks(metadata *C.Metadata, proxies []C.Proxy) []C.Proxy {
	if s.selected != "" {
		for _, p := range proxies {
			if p.Name() == s.selected {
				return []C.Proxy{p}
			}
		}
	}

	fallbacks := make([]C.Proxy, 0, len(proxies))
	var first C.Proxy
	if s.fallback != nil {
		first = s.fallback.Unwrap(metadata, true)
		if first != nil {
			fallbacks = append(fallbacks, first)
		}
	}
	piv := 0
	for i, p := range proxies {
		if p == first {
			piv = i
			break
		}
	}
	fallbacks = append(fallbacks, proxies[piv:]...)
	fallbacks = append(fallbacks, proxies[:piv]...)

	if len(fallbacks) > maxSelected {
		fallbacks = fallbacks[:maxSelected]
	}

	return fallbacks
}

func (s *Smart) selectProxies(metadata *C.Metadata, proxies []C.Proxy) []C.Proxy {
	if s.selected != "" {
		for _, p := range proxies {
			if p.Name() == s.selected {
				return []C.Proxy{p}
			}
		}
	}

	weightType := smart.WeightTypeTCP
	if metadata.NetWork == C.UDP {
		weightType = smart.WeightTypeUDP
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

	proxyByName := make(map[string]C.Proxy)
	for _, p := range proxies {
		proxyByName[p.Name()] = p
	}

	findProxiesByNames := func(names []string) []C.Proxy {
		proxies := make([]C.Proxy, 0, len(names))
		for _, name := range names {
			if !blockedNodes[name] {
				if p, ok := proxyByName[name]; ok && p.AliveForTestUrl(s.testUrl) {
					proxies = append(proxies, p)
				}
			}
		}
		return proxies
	}

	fillProxies := func(selected []C.Proxy, weights map[string]float64, all []C.Proxy, minCount int) []C.Proxy {
		filtered := make([]C.Proxy, 0, len(selected))
		for _, p := range selected {
			if weights == nil || weights[p.Name()] >= allowedWeight {
				filtered = append(filtered, p)
			}
		}
		selected = filtered

		if len(selected) >= minCount {
			return selected[:minCount]
		}
		if len(all) == len(selected) {
			return selected
		}

		selectedNames := make(map[string]bool, len(selected))
		for _, p := range selected {
			selectedNames[p.Name()] = true
		}

		remain := make([]C.Proxy, 0)
		fallbackProxy := s.fallback.Unwrap(metadata, true)
		for _, p := range all {
			if !blockedNodes[p.Name()] && !selectedNames[p.Name()] {
				if w, exists := weights[p.Name()]; weights == nil || (exists && w >= allowedWeight) || !exists {
					if fallbackProxy.Name() == p.Name() {
						selected = append([]C.Proxy{fallbackProxy}, selected...)
						selectedNames[fallbackProxy.Name()] = true
						continue
					}
					remain = append(remain, p)
				}
			}
		}
		rand.Shuffle(len(remain), func(i, j int) {
			remain[i], remain[j] = remain[j], remain[i]
		})
		for _, p := range remain {
			selected = append(selected, p)
			if len(selected) >= minCount {
				break
			}
		}
		return selected
	}

	trySelector := func(target string, weightType string) ([]C.Proxy, map[string]float64) {
		// 检查解析缓存
		if cachedProxies := s.store.GetUnwrapResult(s.Name(), s.configName, target); len(cachedProxies) > 0 {
            return cachedProxies, nil
        }

		// 检查预解析缓存
		if cachedProxyNames, cachedWeights := s.store.GetPrefetchResult(s.Name(), s.configName, target, weightType); len(cachedProxyNames) != 0 {
			if proxies := findProxiesByNames(cachedProxyNames); len(proxies) > 0 {
				weights := make(map[string]float64)
				for i, name := range cachedProxyNames {
					if i < len(cachedWeights) {
						weights[name] = cachedWeights[i]
					}
				}
				return proxies, weights
			}
		}

		// 实时计算最佳节点
		bestNodes, bestWeights, err := s.store.GetBestProxyForTarget(s.Name(), s.configName, target, weightType)
		if err == nil && len(bestNodes) != 0 {
			if proxies := findProxiesByNames(bestNodes); len(proxies) > 0 {
				weights := make(map[string]float64)
				for i, name := range bestNodes {
					if i < len(bestWeights) {
						weights[name] = bestWeights[i]
					}
				}
				return proxies, weights
			}
		}

		return nil, nil
	}

	// 尝试使用ASN信息选择
	asnNumber := s.getASNCode(metadata)
	if asnNumber != "" {
		if _, isCDN := cdnASNs[asnNumber]; !isCDN {
			asnWeightType := weightType
			if weightType == smart.WeightTypeTCP {
				asnWeightType = smart.WeightTypeTCPASN + ":" + asnNumber
			} else {
				asnWeightType = smart.WeightTypeUDPASN + ":" + asnNumber
			}

			if selected, weights := trySelector(asnNumber, asnWeightType); len(selected) > 0 {
				if result := fillProxies(selected, weights, proxies, maxSelected); len(result) > 0 {
					return result
				}
			}
		}
	}

	// 尝试使用域名信息选择
	domain, _ := smart.GetEffectiveDomain(metadata.Host, metadata.DstIP.String())
	if selected, weights := trySelector(domain, weightType); len(selected) > 0 {
		if result := fillProxies(selected, weights, proxies, maxSelected); len(result) > 0 {
			return result
		}
	}

	return s.selectFallbacks(metadata, proxies)
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
	domain, _ := smart.GetEffectiveDomain(metadata.Host, metadata.DstIP.String())
	cacheKey := smart.FormatCacheKey(smart.KeyTypeStats, s.configName, s.Name(), domain, proxy.Name())
	atomicRecord := s.store.GetOrCreateAtomicRecord(cacheKey, s.Name(), s.configName, domain, proxy.Name())
	historyConnectTime = atomicRecord.Get("connectTime").(int64)
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
		degradedWeight := math.Max(0.1, newWeight*0.1)
		log.Debugln("[Smart] Connection [%s] - [%s] - [%s] - [%s] detected zero-traffic, degrade weight from %.4f to %.4f",
			s.Name(), proxyName, weightType, addressDisplay, newWeight, degradedWeight)
		return degradedWeight, true
	}

	// 异常状态码检测
	if (downloadTotal < 0.03 && metadata.Host != "" && metadata.DstPort == 443 && metadata.NetWork == C.TCP) ||
		(rand.Float64() < 0.05 && metadata.Host != "" && metadata.DstPort == 443 && metadata.NetWork == C.TCP) {
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
					degradedWeight := math.Max(0.1, newWeight*0.1)
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

				log.Debugln("[Smart] Connection [%s] - [%s] - [%s] - [%s] detected node quality, degrade weight from %.4f to %.4f (%.1f%%), limited to %.4f",
					s.Name(), proxyName, weightType, addressDisplay,
					oldWeight, newWeight, weightChangeRatio*100, limitedWeight)

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

// 记录保存
func (s *Smart) saveStatsRecord(cacheKey, domain string, proxy C.Proxy, record *smart.StatsRecord, lastUsed time.Time) {
	record.LastUsed = lastUsed

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

	avgFailure := 0
	totalDomains := 0
	for _, data := range nodeStateData {
		var ns smart.NodeState
		if json.Unmarshal(data, &ns) == nil {
			for _, cnt := range ns.DomainFailureCount {
				avgFailure += cnt
				totalDomains++
			}
		}
	}
	if totalDomains > 0 {
		avgFailure /= totalDomains
	}
	domainCountThreshold := avgFailure + 15
	mildFailureCountThreshold := avgFailure + 20
	maxDomainCount := domainCountThreshold * 4

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
	maxSingleDomainFailure := mildFailureCountThreshold * 2
	for _, cnt := range nodeState.DomainFailureCount {
		cappedCnt := cnt
		if cappedCnt > maxSingleDomainFailure {
			cappedCnt = maxSingleDomainFailure
		}
		if cappedCnt >= mildFailureCountThreshold {
			failedDomainCount++
		}
	}

	// 线性降级
	if failedDomainCount >= domainCountThreshold {
		k := 0.7
		linearFactor := 1.0 - k*float64(failedDomainCount-domainCountThreshold)/float64(maxDomainCount-domainCountThreshold)
		if linearFactor < 0.2 {
			linearFactor = 0.2
		}
		nodeState.Degraded = true
		nodeState.DegradedFactor = linearFactor
		nodeState.BlockedUntil = time.Now().Add(time.Duration(30+failedDomainCount*2) * time.Minute)
		isDegraded = true

		additionalBlock := int(float64(nodeState.FailureCount) / 10)
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
func (s *Smart) logConnectionStats(status string, record *smart.StatsRecord, metadata *C.Metadata, baseWeight, priorityFactor float64,
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

	log.Debugln("[Smart] Status: [%s], Updated weights: (Model: [%s], TCP: [%.4f], UDP: [%.4f], TCP ASN: [%.4f], UDP ASN: [%.4f], Base: [%.4f], Priority: [%.2f]) "+
		"For (Group: [%s] - Node: [%s] - Network: [%s] - Address: [%s] - ASN: [%s]) "+
		"- Current: (Up: [%s], Down: [%s], Max Up Speed: [%s], Max Down Speed: [%s], Duration: [%s]) "+
		"- History: (Success: [%d], Failure: [%d], Connect: [%s], Latency: [%s], Total Up: [%s], Total Down: [%s], Max Up Speed: [%s], Max Down Speed: [%s], Avg Duration: [%s])",
		status, weightSource, record.Weights[smart.WeightTypeTCP], record.Weights[smart.WeightTypeUDP], tcpAsnWeight, udpAsnWeight, baseWeight, priorityFactor,
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
func (s *Smart) collectConnectionData(record *smart.StatsRecord, metadata *C.Metadata,
	uploadTotal, downloadTotal, maxUploadRate, maxDownloadRate, baseWeight float64, proxyName string, isModelPredicted bool) {

	// 采样率控制
	if s.sampleRate < 1.0 && rand.Float64() > s.sampleRate {
		return
	}

	input := lightgbm.CreateModelInputFromStatsRecord(
		record, metadata,
		uploadTotal, downloadTotal, maxUploadRate, maxDownloadRate,
	)

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
		newAverage = (oldValue*5 + newValue) / 6
	} else {
		newAverage = newValue
	}

	return newAverage
}

func (s *Smart) recordConnectionStats(status string, metadata *C.Metadata, proxy C.Proxy,
	connectTime int64, latency int64, uploadTotal int64, downloadTotal int64, maxUploadRate int64, maxDownloadRate int64,
	connectionDuration int64, err error) {

	var calculatedWeight float64
	var isModelPredicted bool

	domain, rawDomain := smart.GetEffectiveDomain(metadata.Host, metadata.DstIP.String())
	cacheKey := smart.FormatCacheKey(smart.KeyTypeStats, s.configName, s.Name(), domain, proxy.Name())
	asnInfo := s.getASNCode(metadata)
	priorityFactor := s.getPriorityFactor(proxy.Name())

	weightType := smart.WeightTypeTCP
	if metadata.NetWork == C.UDP {
		weightType = smart.WeightTypeUDP
	}

	lock := smart.GetDomainNodeLock(domain, s.Name(), proxy.Name())
	lock.Lock()
	defer lock.Unlock()

	atomicRecord := s.store.GetOrCreateAtomicRecord(cacheKey, s.Name(), s.configName, domain, proxy.Name())

	switch status {
	case "failed":
		atomicRecord.Add("failure", int64(1))
		go s.store.MarkConnectionFailed(s.Name(), s.configName, len(s.GetProxies(false)), map[string]bool{proxy.Name(): true}, metadata)
	case "closed":
		atomicRecord.Add("success", int64(1))
		go s.store.MarkConnectionSuccess(s.Name(), s.configName)
	}

	success := atomicRecord.Get("success").(int64)
	failure := atomicRecord.Get("failure").(int64)
	connectCount := success + failure

	if connectTime > 0 {
		oldConnectTime := atomicRecord.Get("connectTime").(int64)
		newConnectTime := updateAverageValue(oldConnectTime, connectTime, connectCount)
		atomicRecord.Set("connectTime", newConnectTime)
	}

	if latency > 0 {
		oldLatency := atomicRecord.Get("latency").(int64)
		newLatency := updateAverageValue(oldLatency, latency, connectCount)
		atomicRecord.Set("latency", newLatency)
	}

	connectTimeVal := atomicRecord.Get("connectTime").(int64)
	latencyVal := atomicRecord.Get("latency").(int64)
	lastUsedVal := atomicRecord.Get("lastUsed").(int64)
	durationVal := atomicRecord.Get("duration").(float64)

	uploadTotalMB := float64(uploadTotal) / (1024.0 * 1024.0)
	downloadTotalMB := float64(downloadTotal) / (1024.0 * 1024.0)
	maxUploadRateKB := float64(maxUploadRate) / 1024.0
	maxDownloadRateKB := float64(maxDownloadRate) / 1024.0

	atomicRecord.Add("uploadTotal", uploadTotalMB)
	atomicRecord.Add("downloadTotal", downloadTotalMB)

	if connectionDuration > 0 {
		s.updateConnectionDuration(atomicRecord, connectionDuration)
	}

	oldMaxUploadRate := atomicRecord.Get("maxUploadRate").(float64)
	if maxUploadRateKB > oldMaxUploadRate {
		atomicRecord.Set("maxUploadRate", maxUploadRateKB)
	}

	oldMaxDownloadRate := atomicRecord.Get("maxDownloadRate").(float64)
	if maxDownloadRateKB > oldMaxDownloadRate {
		atomicRecord.Set("maxDownloadRate", maxDownloadRateKB)
	}

	if s.useLightGBM {
		tempRecord := atomicRecord.CreateStatsSnapshot()
		input := lightgbm.CreateModelInputFromStatsRecord(
			tempRecord, metadata,
			uploadTotalMB, downloadTotalMB, maxUploadRateKB, maxDownloadRateKB,
		)
		if input != nil {
			calculatedWeight, isModelPredicted = s.weightModel.PredictWeight(status == "closed", input, priorityFactor)
		} else {
			calculatedWeight = smart.CalculateWeight(
				status == "closed", success, failure, connectTimeVal, latencyVal,
				metadata.NetWork == C.UDP, uploadTotalMB, downloadTotalMB, maxUploadRateKB, maxDownloadRateKB, durationVal, lastUsedVal) * priorityFactor
			isModelPredicted = false
		}
	} else {
		calculatedWeight = smart.CalculateWeight(
			status == "closed", success, failure, connectTimeVal, latencyVal,
			metadata.NetWork == C.UDP, uploadTotalMB, downloadTotalMB, maxUploadRateKB, maxDownloadRateKB, durationVal, lastUsedVal) * priorityFactor
		isModelPredicted = false
	}

	// 额外检查和权重调整
	var degradedWeight float64
	var isDegraded bool

	addressDisplay := rawDomain
	if rawDomain != "" && domain != "" && rawDomain != domain {
		addressDisplay = fmt.Sprintf("%s (Wildcard: %s)", rawDomain, domain)
	}

	if status == "failed" {
		degradedWeight, isDegraded = s.handleFailedConnection(proxy.Name(), cacheKey, domain, calculatedWeight, weightType)
	}

	if status == "closed" {
		if metadata.SmartBlock == "blocked" {
			degradedWeight = math.Max(0.1, calculatedWeight*0.1)
			isDegraded = true
			log.Debugln("[Smart] Connection [%s] - [%s] - [%s] - [%s] detected manual block, degrade weight from %.4f to %.4f",
				s.Name(), proxy.Name(), weightType, addressDisplay, calculatedWeight, degradedWeight)
		} else {
			historyMaxUploadRateKB := atomicRecord.Get("maxUploadRate").(float64)
			historyMaxDownloadRateKB := atomicRecord.Get("maxDownloadRate").(float64)
			historyUploadTotal := atomicRecord.Get("uploadTotal").(float64)
			historyDownloadTotal := atomicRecord.Get("downloadTotal").(float64)
			success := atomicRecord.Get("success").(int64)
			status := atomicRecord.Get("status").(int64)
			lastUsedVal := atomicRecord.Get("lastUsed").(int64)
			oldWeight := atomicRecord.Get("weights").(map[string]float64)[weightType]

			degradedWeight, isDegraded = s.checkNodeQualityDegradation(
				metadata, proxy, atomicRecord,
				addressDisplay, proxy.Name(), calculatedWeight, oldWeight,
				connectionDuration, uploadTotalMB, downloadTotalMB,
				maxUploadRateKB, maxDownloadRateKB, historyMaxUploadRateKB, historyMaxDownloadRateKB,
				historyUploadTotal, historyDownloadTotal, success, weightType,
				status, lastUsedVal)
		}
	}

	if isDegraded {
		calculatedWeight = degradedWeight
		go s.cleanupDegradedNodePreferenceCache(metadata, domain, addressDisplay, proxy.Name(), calculatedWeight, weightType, asnInfo)
	}

	baseWeight := calculatedWeight / priorityFactor

	atomicRecord.SetWeight(weightType, calculatedWeight)

	if asnInfo != "" {
		s.updateAsnWeights(atomicRecord, asnInfo, calculatedWeight, metadata.NetWork == C.UDP)
	}

	statsSnapshot := atomicRecord.CreateStatsSnapshot()

	// 数据收集
	if s.collectData {
		s.collectConnectionData(statsSnapshot, metadata, uploadTotalMB, downloadTotalMB, maxUploadRateKB, maxDownloadRateKB, baseWeight, proxy.Name(), isModelPredicted)
	}

	// 保存统计记录
	s.saveStatsRecord(cacheKey, domain, proxy, statsSnapshot, time.Now())

	// 日志输出
	s.logConnectionStats(status, statsSnapshot, metadata, baseWeight, priorityFactor, addressDisplay, proxy.Name(),
		uploadTotalMB, downloadTotalMB, maxUploadRateKB, maxDownloadRateKB, connectionDuration, asnInfo, isModelPredicted)
}


func (s *Smart) registerClosureMetricsCallback(c C.Conn, proxy C.Proxy, metadata *C.Metadata, connectTime int64, firstReadLatency int64, readErr error, firstWriteErr error) C.Conn {
	return callback.NewCloseCallbackConn(c, func() {
		tracker := statistic.DefaultManager.Get(metadata.UUID)
		if tracker != nil {
			info := tracker.Info()
			uploadTotal := info.UploadTotal.Load()
			downloadTotal := info.DownloadTotal.Load()
			connectionDuration := time.Since(info.Start).Milliseconds()
			maxUploadRate := info.MaxUploadRate.Load()
			maxDownloadRate := info.MaxDownloadRate.Load()

			if readErr == nil {
				go s.recordConnectionStats("closed", metadata, proxy, connectTime, firstReadLatency, uploadTotal, downloadTotal, maxUploadRate, maxDownloadRate, connectionDuration, nil)
			} else if readErr == io.EOF {
				if firstWriteErr != nil && firstWriteErr != io.EOF {
					go s.recordConnectionStats("failed", metadata, proxy, connectTime, firstReadLatency, uploadTotal, downloadTotal, maxUploadRate, maxDownloadRate, connectionDuration, readErr)
				} else {
					go s.recordConnectionStats("closed", metadata, proxy, connectTime, firstReadLatency, uploadTotal, downloadTotal, maxUploadRate, maxDownloadRate, connectionDuration, nil)
				}
			} else {
				go s.recordConnectionStats("failed", metadata, proxy, connectTime, firstReadLatency, uploadTotal, downloadTotal, maxUploadRate, maxDownloadRate, connectionDuration, readErr)
			}
			return
		}
	})
}

func (s *Smart) registerPacketClosureMetricsCallback(pc C.PacketConn, proxy C.Proxy, metadata *C.Metadata, connectTime int64) C.PacketConn {
	return callback.NewCloseCallbackPacketConn(pc, func() {
		tracker := statistic.DefaultManager.Get(metadata.UUID)
		if tracker != nil {
			info := tracker.Info()
			uploadTotal := info.UploadTotal.Load()
			downloadTotal := info.DownloadTotal.Load()
			connectionDuration := time.Since(info.Start).Milliseconds()
			maxUploadRate := info.MaxUploadRate.Load()
			maxDownloadRate := info.MaxDownloadRate.Load()

			go s.recordConnectionStats("closed", metadata, proxy, connectTime, 0,
				uploadTotal, downloadTotal, maxUploadRate, maxDownloadRate, connectionDuration, nil)
			return
		}
	})
}

func (s *Smart) cleanupDegradedNodePreferenceCache(metadata *C.Metadata, domain, addressDisplay, nodeName string, currentWeight float64, weightType, asnInfo string) {
	lock := smart.GetDomainNodeLock(domain, s.Name(), nodeName)
	lock.Lock()
	defer lock.Unlock()

	updatePrefetch := func(target, typeKey string, logPrefix string) {
		nodes, weights := s.store.GetPrefetchResult(s.Name(), s.configName, target, typeKey)
		if len(nodes) == 0 {
			bestNodes, bestWeights, err := s.store.GetBestProxyForTarget(s.Name(), s.configName, target, typeKey)
			if err == nil && len(bestNodes) > 0 {
				s.store.StorePrefetchResult(s.Name(), s.configName, target, typeKey, bestNodes, bestWeights)
				nodeWeightPairs := make([]string, len(bestNodes))
				for i := range bestNodes {
					nodeWeightPairs[i] = fmt.Sprintf("%s: %.2f", bestNodes[i], bestWeights[i])
				}
				if logPrefix == "domain" {
					log.Debugln("[Smart] Updated prefetch result for Group [%s]: %s [%s] => type [%s] => [%s]",
						s.Name(), logPrefix, addressDisplay, typeKey, strings.Join(nodeWeightPairs, ", "))
				} else {
					log.Debugln("[Smart] Updated prefetch result for Group [%s]: %s [%s] => type [%s] => [%s]",
						s.Name(), logPrefix, target, typeKey, strings.Join(nodeWeightPairs, ", "))
				}
				return
			}
			return
		}
		nodeWeightList := make([]nodeWeight, 0, len(nodes))
		for i := range nodes {
			nodeWeightList = append(nodeWeightList, nodeWeight{name: nodes[i], weight: weights[i]})
		}
		found := false
		for i := range nodeWeightList {
			if nodeWeightList[i].name == nodeName {
				nodeWeightList[i].weight = currentWeight
				found = true
				break
			}
		}
		if !found {
			nodeWeightList = append(nodeWeightList, nodeWeight{name: nodeName, weight: currentWeight})
		}
		sort.Slice(nodeWeightList, func(i, j int) bool {
			return nodeWeightList[i].weight > nodeWeightList[j].weight
		})
		sortedNodes := make([]string, 0, len(nodeWeightList))
		sortedWeights := make([]float64, 0, len(nodeWeightList))
		for _, nw := range nodeWeightList {
			sortedNodes = append(sortedNodes, nw.name)
			sortedWeights = append(sortedWeights, nw.weight)
		}
		s.store.StorePrefetchResult(s.Name(), s.configName, target, typeKey, sortedNodes, sortedWeights)
		nodeWeightPairs := make([]string, len(sortedNodes))
		for i := range sortedNodes {
			nodeWeightPairs[i] = fmt.Sprintf("%s: %.2f", sortedNodes[i], sortedWeights[i])
		}
		if logPrefix == "domain" {
			log.Debugln("[Smart] Updated prefetch result for Group [%s]: %s [%s] => type [%s] => [%s]",
				s.Name(), logPrefix, addressDisplay, typeKey, strings.Join(nodeWeightPairs, ", "))
		} else {
			log.Debugln("[Smart] Updated prefetch result for Group [%s]: %s [%s] => type [%s] => [%s]",
				s.Name(), logPrefix, target, typeKey, strings.Join(nodeWeightPairs, ", "))
		}
	}

	// 域名缓存
	updatePrefetch(domain, weightType, "domain")

	// ASN缓存
	if asnInfo != "" {
		asnWeightType := smart.WeightTypeTCPASN
		if weightType == smart.WeightTypeUDP {
			asnWeightType = smart.WeightTypeUDPASN
		}
		fullAsnWeightType := asnWeightType + ":" + asnInfo
		updatePrefetch(asnInfo, fullAsnWeightType, "ASN")
	}

	// sticky-sessions缓存
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
	if !metadata.DstIP.IsValid() || metadata.DstIPASN == "unknown" {
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

	if !flushQueueOnce.Swap(true) {
		s.store.FlushQueue(true)
	}

	lightgbm.CloseAllCollectors()

	smartInitOnce = sync.Once{}
	preloadOnce = sync.Once{}

	return nil
}
