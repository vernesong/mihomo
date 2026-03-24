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
	prefetchInterval         = 10 * time.Minute
	cleanupInterval          = 120 * time.Minute
	cacheParamAdjustInterval = 5 * time.Minute
	recoveryCheckInterval    = 5 * time.Minute
	checkInterval            = 10 * time.Minute
	flushQueueInterval       = 5 * time.Minute
	rankingInterval          = 30 * time.Minute

	maxRetries               = 4
	maxSelected              = 10

	parallelDials            = 3
	connectThreshold         = 2.0
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

func NewSmart(option *GroupCommonOption, providers []provider.ProxyProvider, options ...smartOption) (*Smart, error) {
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
	availableProxies := s.GetProxies(true)
	metadata.SmartBlock = "normal"

	getBatch := func(proxies []C.Proxy, i int) ([]C.Proxy, time.Duration) {
		var batch []C.Proxy
		var historyConnectTime int64
		var timeout time.Duration
		if i == 0 {
			batch = []C.Proxy{proxies[0]}
		} else {
			begin := (i-1)*parallelDials + 1
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
			timeout = time.Duration(float64(historyConnectTime)*connectThreshold) * time.Millisecond
		}

		if timeout > C.DefaultTCPTimeout || timeout <= 0 {
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

			if err != nil {
				if tunnel.ShouldStopRetry(err) {
					return nil, err
				}
				finalErr = err
			} else {
				return s.WrapConnWithMetric(c, p, metadata, connectTime), nil
			}
		}

		return nil, finalErr
	}

	proxies, _ := s.selectProxies(metadata, availableProxies)
	return tryDial(proxies)
}

func (s *Smart) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (pc C.PacketConn, err error) {
	var finalErr error
	var proxy C.Proxy
	var availableProxies []C.Proxy
	
	proxies := s.GetProxies(true)
	metadata.SmartBlock = "normal"

	availableProxies, _ = s.selectProxies(metadata, proxies)
	
	for i := 0; i < len(availableProxies) && i < 3; i++ {
		proxy = availableProxies[i]
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

		if err == nil {
			pc.AppendToChains(s)
			pc = s.registerPacketClosureMetricsCallback(pc, proxy, metadata, connectTime)
			return pc, nil
		}
		finalErr = err
		go s.recordConnectionStats("failed", metadata, proxy, connectTime, 0, 0, 0, 0, 0, 0, err)
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

	proxies, isUnwrapResult := s.selectProxies(metadata, proxies)

	if !isUnwrapResult {
		s.store.StoreUnwrapResult(s.Name(), s.configName, metadata.SmartTarget, s.getASNCode(metadata), metadata.NetWork == C.UDP, proxies)
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
		"hidden":          s.Hidden(),
		"icon":            s.Icon(),
		"policy-priority": policyPriorityStr,
		"useLightGBM":     s.useLightGBM,
		"collectData":     s.collectData,
		"sampleRate":      s.sampleRate,
		"preferASN":       s.preferASN,
	})
}

func (s *Smart) fillProxies(metadata *C.Metadata, names []string, weights []float64, all []C.Proxy, minCount int, isUDP bool, unwrap bool) ([]C.Proxy, bool) {
	blockedNodes, _ := s.store.GetBlockedNodes(s.Name(), s.configName)

	proxyByName := make(map[string]C.Proxy)
	for _, p := range all {
		proxyByName[p.Name()] = p
	}

	selected := []C.Proxy{}
	for i, name := range names {
		proxy := proxyByName[name]
		if proxy != nil && !blockedNodes[name] && proxy.AliveForTestUrl(s.testUrl) && (!isUDP || proxy.SupportUDP()) {
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
	if unwrap && len(selected) > 0 {
		return selected, true
	}

	if len(selected) >= len(all) {
		return selected, false
	}

	if len(selected) >= minCount {
		return selected[:minCount], false
	}

	var indexes []int
	var weightsMap map[string]float64
	var firstAppended bool
	checkWeightsMap := make(map[string]float64)
	for i, name := range names {
		if weights != nil && i < len(weights) {
			checkWeightsMap[name] = weights[i]
		}
	}

	filteredAll := []C.Proxy{}
	for _, p := range all {
		if _, exists := checkWeightsMap[p.Name()]; !exists {
			filteredAll = append(filteredAll, p)
		}
	}

	if len(s.policyPriority) > 0 {
		proxiesFactor := make(map[string]float64)
		for _, p := range filteredAll {
			proxiesFactor[p.Name()] = s.getPriorityFactor(p.Name())
		}

		sort.Slice(filteredAll, func(i, j int) bool {
			factorI := proxiesFactor[filteredAll[i].Name()]
			factorJ := proxiesFactor[filteredAll[j].Name()]
			if factorI == factorJ {
				return filteredAll[i].Name() < filteredAll[j].Name()
			}
			return factorI > factorJ
		})

		indexes = make([]int, len(filteredAll))
		for i := range indexes {
			indexes[i] = i
		}
	} else if ranking, err := s.store.GetNodeWeightRankingCache(s.Name(), s.configName); err == nil && len(ranking) > 0 {
		weightsMap = make(map[string]float64)
		for _, r := range ranking {
			weightsMap[r.Name] = float64(r.Weight)
		}
		sort.Slice(filteredAll, func(i, j int) bool {
			wi, existsi := weightsMap[filteredAll[i].Name()]
			wj, existsj := weightsMap[filteredAll[j].Name()]
			if existsi && existsj {
				if wi != wj {
					return wi > wj
				}
				return filteredAll[i].Name() < filteredAll[j].Name()
			}
			if existsi {
				return true
			}
			if existsj {
				return false
			}
			return filteredAll[i].Name() < filteredAll[j].Name()
		})
		indexes = make([]int, len(filteredAll))
		for i := range indexes {
			indexes[i] = i
		}
	} else {
		indexes = rand.Perm(len(filteredAll))
	}

	for _, idx := range indexes {
		p := filteredAll[idx]
		if !blockedNodes[p.Name()] && p.AliveForTestUrl(s.testUrl) && (!isUDP || p.SupportUDP()) {
			if !firstAppended {
				selected = append([]C.Proxy{p}, selected...)
				firstAppended = true
			} else {
				selected = append(selected, p)
			}
			if len(selected) >= minCount {
				selected = selected[:minCount]
				break
			}
		}
		if len(selected) == len(all) {
			break
		}
	}

	if len(selected) == 0 {
		indexes = rand.Perm(len(all))
		for _, idx := range indexes {
			if all[idx].AliveForTestUrl(s.testUrl) {
				selected = append(selected, all[idx])
			}
			if len(selected) >= minCount {
				break
			}
		}
		if len(selected) == 0 {
			for _, idx := range indexes {
				selected = append(selected, all[idx])
				if len(selected) >= minCount {
					break
				}
			}
		}
	}

	return selected, false
}

// 节点选择
func (s *Smart) selectProxies(metadata *C.Metadata, proxies []C.Proxy) ([]C.Proxy, bool) {
	// 添加ASN信息
	asnNumber := s.getASNCode(metadata)
	if metadata.SmartTarget == "" {
		metadata.SmartTarget = smart.GetEffectiveTarget(metadata.Host, metadata.DstIP.String())
	}

	if s.selected != "" {
		for _, p := range proxies {
			if p.Name() == s.selected {
				return []C.Proxy{p}, true
			}
		}
	}

	trySelector := func(isUDP bool) ([]string, []float64, bool) {
		// 检查匹配缓存
		if proxiesName := s.store.GetUnwrapResult(s.Name(), s.configName, metadata.SmartTarget, asnNumber, isUDP); len(proxiesName) > 0 {
			return proxiesName, nil, true
		}

		// 检查预解析缓存
		if proxiesName, weights := s.store.GetPrefetchResult(s.Name(), s.configName, metadata.SmartTarget, asnNumber, isUDP); len(proxiesName) > 0 {
			return proxiesName, weights, false
		}

		// 实时计算最佳节点
		if proxiesName, weights, err := s.store.GetBestProxyForTarget(s.Name(), s.configName, metadata.SmartTarget, asnNumber, isUDP); err == nil && len(proxiesName) > 0 {
			return proxiesName, weights, false
		}

		return []string{}, []float64{}, false
	}

	resultNames, resultWeights, unwrap := trySelector(metadata.NetWork == C.UDP)
	result, isUnwrapResult := s.fillProxies(metadata, resultNames, resultWeights, proxies, maxSelected, metadata.NetWork == C.UDP, unwrap)

	return result, isUnwrapResult
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

	s.startTimedTask(10*time.Minute, checkInterval, "Group orphaned nodes clean up", s.cleanupOrphanedNodeCache, true)
	s.startTimedTask(5*time.Minute, prefetchInterval, "Group targets prefetch", s.runPrefetch, false)
	s.startTimedTask(10*time.Minute, rankingInterval, "Group nodes Ranking", s.updateNodeRanking, false)
	s.startTimedTask(5*time.Minute, recoveryCheckInterval, "Group nodes recovery check", s.checkAndRecoverDegradedNodes, false)
	s.startTimedTask(10*time.Minute, cleanupInterval, "Group old records clean up", func() {
		_ = s.store.CleanupOldRecords(s.Name(), s.configName)
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
			log.Debugln("[Smart] Task [%s] for group [%s] set to run once, exiting", taskName, s.Name())
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
	ranking, err := s.store.GetNodeWeightRanking(s.Name(), s.configName, s.testUrl, proxies)
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
		categoryCounts[rank.Rank]++
	}

	log.Debugln("[Smart] Policy group [%s] node ranking update completed: %d nodes total (%s: %d, %s: %d, %s: %d)",
		s.Name(), len(ranking),
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
	target := metadata.SmartTarget
	cacheKey := smart.FormatDBKey(smart.KeyTypeStats, s.configName, s.Name(), target, proxy.Name())
	atomicRecord := s.store.GetOrCreateAtomicRecord(cacheKey, s.Name(), s.configName, target, proxy.Name())
	historyConnectTime = atomicRecord.Get("connectTime").(int64)
	return
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
    go func() {
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
    }()
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

		var shouldUpdate bool

		if state.BlockedUntil > 0 && state.BlockedUntil > time.Now().Unix() {
			state.BlockedUntil = 0
			shouldUpdate = true
			log.Debugln("[Smart] Node [%s] block period expired, unblocking", nodeName)
		}

		if state.Degraded {
			if state.BlockedUntil == 0 {
				recoveryFactor := math.Min(1.0, state.DegradedFactor+0.01)
				shouldUpdate = true
				state.FailureCount = int(float64(state.FailureCount) * 0.95)

				if recoveryFactor >= 0.99 {
					state.Degraded = false
					state.DegradedFactor = 1.0
				} else {
					state.DegradedFactor = recoveryFactor
				}
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
		s.store.AppendToGlobalQueue(operations...)
	}
}

// 失败连接处理
func (s *Smart) handleFailedConnection(proxyName string, oldWeight, calculatedWeight float64) (float64, bool) {
	var nodeState smart.NodeState
	var block bool
	now := time.Now().Unix()

	nodeStateData, _ := s.store.GetNodeStates(s.Name(), s.configName)

	if data, exists := nodeStateData[proxyName]; exists {
		if json.Unmarshal(data, &nodeState) != nil {
			nodeState = smart.NodeState{
				Name:           proxyName,
				FailureCount:   1,
				LastFailure:    now,
				Degraded:       false,
				DegradedFactor: 1.0,
			}
		} else {
			nodeState.FailureCount++
			nodeState.LastFailure = now
		}
	} else {
		nodeState = smart.NodeState{
			Name:           proxyName,
			FailureCount:   1,
			LastFailure:    now,
			Degraded:       false,
			DegradedFactor: 1.0,
		}
	}

	// 线性降级
	if nodeState.FailureCount > 0 {
		k := 0.01
		linearFactor := math.Max(0.1, 1.0-k*float64(nodeState.FailureCount))
		nodeState.DegradedFactor = linearFactor
		nodeState.Degraded = true
		if linearFactor <= 0.7 {
			block = true
			nodeState.BlockedUntil = time.Now().Add(time.Duration(30+nodeState.FailureCount*2) * time.Minute).Unix()
			additionalBlock := int(float64(nodeState.FailureCount) / 10)
			if nodeState.BlockedUntil > now {
				nodeState.BlockedUntil = time.Unix(nodeState.BlockedUntil, 0).Add(time.Duration(additionalBlock) * time.Minute).Unix()
			} else {
				nodeState.BlockedUntil = time.Now().Add(time.Duration(additionalBlock) * time.Minute).Unix()
			}
		}
	}

	if nodeStateBytes, err := json.Marshal(&nodeState); err == nil {
		s.store.AppendToGlobalQueue(smart.StoreOperation{
			Type:   smart.OpSaveNodeState,
			Group:  s.Name(),
			Config: s.configName,
			Node:   proxyName,
			Data:   nodeStateBytes,
		})
	}

	if block {
		smart.ClearBlockedNodesCache(s.Name(), s.configName)
	}

	return updateAverageValueFloat(oldWeight, calculatedWeight * nodeState.DegradedFactor, false), block
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
		"- Current: (Up: [%s], Down: [%s], Max Up Speed: [%s], Max Down Speed: [%s], Duration: [%s]) "+
		"- History: (Success: [%d], Failure: [%d], Connect: [%s], Latency: [%s], Total Up: [%s], Total Down: [%s], Max Up Speed: [%s], Max Down Speed: [%s], Avg Duration: [%s])",
		status, weightSource, record.Weights[smart.WeightTypeTCP], record.Weights[smart.WeightTypeUDP], tcpAsnWeight, udpAsnWeight, baseWeight, priorityFactor,
		s.Name(), proxyName, metadata.NetWork.String(), addressDisplay,
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

func updateAverageValueFloat(oldValue, newValue float64, force bool) float64 {
	if oldValue > 0 {
		if force {
			return math.Max(newValue, 0.1)
		}
		return math.Max((oldValue*4 + newValue*2) / 6, 0.1)
	}
	return math.Max(newValue, 0.1)
}

func (s *Smart) recordConnectionStats(status string, metadata *C.Metadata, proxy C.Proxy,
	connectTime int64, latency int64, uploadTotal int64, downloadTotal int64, maxUploadRate int64, maxDownloadRate int64,
	connectionDuration int64, err error) {

	var calculatedWeight float64
	var ModelPredicted bool

	target := metadata.SmartTarget
	wildcardTarget := smart.GetEffectiveTarget(metadata.Host, metadata.DstIP.String())
	cacheKey := smart.FormatDBKey(smart.KeyTypeStats, s.configName, s.Name(), target, proxy.Name())
	asnInfo := s.getASNCode(metadata)
	priorityFactor := s.getPriorityFactor(proxy.Name())

	addressDisplay := fmt.Sprintf("Host: [%s] - Target: [%s]", metadata.Host, target)	
	if metadata.Host == "" {
		addressDisplay = fmt.Sprintf("IP: [%s] - Target: [%s]", metadata.DstIP.String(), target)
	}

	weightType := smart.WeightTypeTCP
	if asnInfo != "" {
		if metadata.NetWork == C.UDP {
			weightType = smart.WeightTypeUDPASN + ":" + asnInfo
		} else {
			weightType = smart.WeightTypeTCPASN + ":" + asnInfo
		}
		addressDisplay += fmt.Sprintf(" - ASN: [%s]", asnInfo)
	} else {
		if metadata.NetWork == C.UDP {
			weightType = smart.WeightTypeUDP
		}
		addressDisplay += " - ASN: [unknown]"
	}

	lock := smart.GetTargetNodeLock(target, s.Name(), proxy.Name())
	lock.Lock()
	defer lock.Unlock()

	atomicRecord := s.store.GetOrCreateAtomicRecord(cacheKey, s.Name(), s.configName, target, proxy.Name())

	switch status {
	case "failed":
		if proxy.Type() == C.Reject || proxy.Type() == C.Pass || proxy.Type() == C.RejectDrop {
			return
		}
		s.onDialFailed(proxy.Type(), err, s.healthCheck)
		if ! proxy.AliveForTestUrl(s.testUrl) {
			return
		}
		atomicRecord.Add("failure", int64(1))
	case "closed":
		s.onDialSuccess()
		if ! proxy.AliveForTestUrl(s.testUrl) {
			return
		}
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
	wildcardTargetCount, wildcardTargetLastUsed := s.store.GetHostStatus(s.Name(), s.configName, wildcardTarget)
	wildcardTargetBlocked := wildcardTargetCount >= s.maxFailedTimes
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
		uploadTotalMB, downloadTotalMB, maxUploadRateKB, maxDownloadRateKB, wildcardTarget,
	)

	if s.useLightGBM && s.weightModel != nil {
		calculatedWeight, ModelPredicted = s.weightModel.PredictWeight(input, priorityFactor)
	} else {
		calculatedWeight, ModelPredicted = smart.CalculateWeight(input, priorityFactor)
	}

	// 额外检查和权重调整
	// 平均权重(适应 target 调整为 rule based 和 asn based 的情况，避免频繁突变)
	degradedWeight, isDegraded := s.checkNodeQualityDegradation(
		status, metadata, proxy, wildcardTargetBlocked,
		addressDisplay, proxy.Name(), calculatedWeight, oldWeight,
		connectionDuration, uploadTotalMB, downloadTotalMB,
		metadata.NetWork.String(),
		wildcardTargetLastUsed, wildcardTargetCount, asnInfo, metadata.NetWork == C.UDP)

	if isDegraded {
		s.updatePrefetchCache(metadata, target, addressDisplay, proxy.Name(), degradedWeight, asnInfo, metadata.NetWork == C.UDP)
	}

	needLastUsedUpdate := downloadTotalMB < 0.03 && metadata.Host != "" && metadata.DstPort == 443 && metadata.NetWork == C.TCP
	s.store.UpdateHostStatus(s.Name(), s.configName, wildcardTarget, isDegraded, needLastUsedUpdate)
	s.findSameConnection(metadata, proxy.Name(), target, asnInfo, metadata.NetWork == C.UDP, isDegraded)

	baseWeight := degradedWeight / priorityFactor

	// 数据收集
	if s.collectData {
		s.collectConnectionData(input, metadata, baseWeight, proxy.Name(), ModelPredicted)
	}

	// 更新记录
	atomicRecord.Set("lastUsed", time.Now().Unix())
	atomicRecord.SetWeight(weightType, degradedWeight, metadata.NetWork == C.UDP)
	statsSnapshot := atomicRecord.CreateStatsSnapshot()

	// 保存统计记录
	s.saveStatsRecord(target, proxy, statsSnapshot)

	// 日志输出
	s.logConnectionStats(status, statsSnapshot, metadata, baseWeight, priorityFactor, addressDisplay, proxy.Name(),
		uploadTotalMB, downloadTotalMB, maxUploadRateKB, maxDownloadRateKB, connectionDuration, asnInfo, ModelPredicted)
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

func (s *Smart) checkNodeQualityDegradation(
	status string, metadata *C.Metadata, proxy C.Proxy, hostBlocked bool,
	addressDisplay, proxyName string,
	newWeight, oldWeight float64,
	connectionDuration int64,
	uploadTotal, downloadTotal float64,
	networkType string,
	wildcardTargetLastUsed int64, wildcardTargetCount int,
	asnInfo string, isUDP bool) (float64, bool) {

	newWeight = updateAverageValueFloat(oldWeight, newWeight, false)

	if s.selected != "" {
		return newWeight, false
	}

	now := time.Now().Unix()
	degradedWeight := updateAverageValueFloat(oldWeight, newWeight * 0.1, metadata.SmartBlock == "blocked")

	// 用户手动/智能屏蔽
	if metadata.SmartBlock == "blocked" || metadata.SmartBlock == "degraded" {
		if metadata.SmartBlock == "degraded" {
			return oldWeight, false
		}
		log.Debugln("[Smart] Connection Group: [%s] - Node: [%s] - Network: [%s] - Address: [%s] detected manual block, degraded form [%.4f] to [%.4f] ...",
			s.Name(), proxyName, networkType, addressDisplay, oldWeight, degradedWeight)
		return degradedWeight, true
	}

	if status == "failed" {
		failedWeight, nodeBlock := s.handleFailedConnection(proxy.Name(), oldWeight, newWeight)
		if nodeBlock {
			if hostBlocked {
				return newWeight, false
			}
			log.Debugln("[Smart] Connection Group: [%s] - Node: [%s] - Network: [%s] - Address: [%s] detected failure, degraded form [%.4f] to [%.4f] ...",
				s.Name(), proxyName, networkType, addressDisplay, oldWeight, failedWeight)
		}
		return failedWeight, nodeBlock
	}

	// 零流量连接
	if connectionDuration > 100 && downloadTotal == 0 && uploadTotal == 0 && metadata.DstPort == 443 && !isUDP {
		if hostBlocked {
			return newWeight, false
		}
		log.Debugln("[Smart] Connection Group: [%s] - Node: [%s] - Network: [%s] - Address: [%s] detected zero-traffic, degraded form [%.4f] to [%.4f] ...",
			s.Name(), proxyName, networkType, addressDisplay, oldWeight, degradedWeight)
		return degradedWeight, true
	}

	// 异常状态码检测
	if downloadTotal < 0.03 && metadata.Host != "" && metadata.DstPort == 443 && !isUDP {
		if now - wildcardTargetLastUsed > 300 || wildcardTargetCount > 0 {
			ctx, cancel := context.WithTimeout(context.Background(), C.DefaultTCPTimeout)
			defer cancel()
			url := "https://" + metadata.Host + "/?z=" + strconv.FormatInt(rand.Int63(), 10)
			status, ok, err := proxy.StatusTest(ctx, url)
			if err == nil {
				if !ok {
					if hostBlocked {
						return newWeight, false
					}
					log.Debugln("[Smart] Connection Group: [%s] - Node: [%s] - Network: [%s] - Address: [%s] detected abnormal response [%d], degraded form [%.4f] to [%.4f] ...",
						s.Name(), proxyName, networkType, addressDisplay, status, oldWeight, degradedWeight)
					return degradedWeight, true
				}
			}
		}
	}

	// 权重波动
	if oldWeight > 0 && newWeight > 0 {
		weightDropRatio := (oldWeight - newWeight) / oldWeight
		if weightDropRatio > 0.3 {
			log.Debugln("[Smart] Connection Group: [%s] - Node: [%s] - Network: [%s] - Address: [%s] detected weight drop %.2f%% form [%.4f] to [%.4f], refine selected result...",
				s.Name(), proxyName, networkType, addressDisplay, weightDropRatio*100, oldWeight, newWeight)
			return newWeight, true
		}
	}

	return newWeight, false
}

func (s *Smart) findSameConnection(metadata *C.Metadata, proxyName, target, asnInfo string, isUDP, close bool) {
	allIDs := statistic.DefaultManager.GetSmartTargetIDs(target, asnInfo)
	hasOther := false

	if close {
		for id := range allIDs {
			if tracker := statistic.DefaultManager.Get(id); tracker != nil {
				if id != metadata.UUID && lo.Contains(tracker.Chains(), s.Name()) {
					if lo.Contains(tracker.Chains(), proxyName) {
						if metadata.SmartBlock == "blocked" {
							tracker.Info().Metadata.SmartBlock = "blocked"
						} else {
							tracker.Info().Metadata.SmartBlock = "degraded"
						}
					}
					_ = tracker.Close()
				}
			}
		}
	} else {
		delete(allIDs, metadata.UUID)
		if len(allIDs) > 0 {
			hasOther = true
		}
	}
	if !hasOther {
		s.store.DeleteUnwrapResult(s.Name(), s.configName, target, asnInfo, isUDP)
	}
}

func (s *Smart) updatePrefetchCache(metadata *C.Metadata, target, addressDisplay string, nodeName string, weight float64, asnInfo string, isUDP bool) {
	// Prefetch 缓存
	nodes, weights := s.store.GetPrefetchResult(s.Name(), s.configName, target, asnInfo, isUDP)
	nodeWeightList := make([]nodeWithWeight, 0, len(nodes))
	if len(nodes) != 0 {
		for i := range nodes {
			nodeWeightList = append(nodeWeightList, nodeWithWeight{node: nodes[i], weight: weights[i]})
		}
	}

	found := false
	for j := range nodeWeightList {
		if nodeWeightList[j].node == nodeName {
			nodeWeightList[j].weight = weight
			found = true
			break
		}
	}
	if !found {
		nodeWeightList = append(nodeWeightList, nodeWithWeight{node: nodeName, weight: weight})
	}

	sort.Slice(nodeWeightList, func(i, j int) bool {
		if nodeWeightList[i].weight != nodeWeightList[j].weight {
			return nodeWeightList[i].weight > nodeWeightList[j].weight
		}
		return nodeWeightList[i].node < nodeWeightList[j].node
	})
	sortedNodes := make([]string, 0, len(nodeWeightList))
	sortedWeights := make([]float64, 0, len(nodeWeightList))
	for _, nw := range nodeWeightList {
		sortedNodes = append(sortedNodes, nw.node)
		sortedWeights = append(sortedWeights, nw.weight)
	}
	s.store.StorePrefetchResult(s.Name(), s.configName, target, asnInfo, isUDP, sortedNodes, sortedWeights)
	nodeWeightPairs := make([]string, len(sortedNodes))
	for i := range sortedNodes {
		nodeWeightPairs[i] = fmt.Sprintf("%s: %.2f", sortedNodes[i], sortedWeights[i])
	}

	log.Debugln("[Smart] Updated prefetch result for Group: [%s] - Network: [%s] - Address: [%s] => [%s]", s.Name(), metadata.NetWork.String(), addressDisplay, strings.Join(nodeWeightPairs, ", "))
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

func parseSmartOption(config map[string]any) ([]smartOption) {
	opts := []smartOption{}

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

	return nil
}
