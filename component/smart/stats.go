package smart

import (
	"container/heap"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/common/lru"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
)

var (
	shardedLocks     [1024]*sync.RWMutex
	shardedLocksOnce sync.Once
)

type StatsRecord struct {
	Success            int64                   `json:"success"`
	Failure            int64                   `json:"failure"`
	ConnectTime        int64                   `json:"connect_time"`
	Latency            int64                   `json:"latency"`
	LastUsed           int64                   `json:"last_used"`
	Weights            map[string]float64      `json:"weights"`
	UploadTotal        float64                 `json:"upload_total"`
	DownloadTotal      float64                 `json:"download_total"`
	MaxUploadRate      float64                 `json:"max_upload_rate"`
	MaxDownloadRate    float64                 `json:"max_download_rate"`
	ConnectionDuration float64                 `json:"connection_duration"`
	LossRate           float64                 `json:"loss_rate,omitempty"`
}

type NodeState struct {
	Name               string         `json:"name"`
	LastChecked        int64          `json:"last_checked"`
	BlockedUntil       int64          `json:"blocked_until"`
	ThresholdGrade     int            `json:"threshold_grade,omitempty"`
}

type AtomicStatsRecord struct {
	success         atomic.Int64
	failure         atomic.Int64
	connectTime     atomic.Int64
	latency         atomic.Int64
	lastUsed        atomic.Int64

	uploadTotal     atomic.Float64
	downloadTotal   atomic.Float64
	duration        atomic.Float64
	maxUploadRate   atomic.Float64
	maxDownloadRate atomic.Float64
	lossRate        atomic.Float64

	weights         *lru.LruCache[string, float64]
}

type CodeNodeSet struct {
	Nodes      map[string]int64  `json:"nodes"`
	FailCounts map[string]int    `json:"fail_counts,omitempty"`
	NodeHosts  map[string]string `json:"node_hosts,omitempty"`
}

type HostStatus struct {
	initOnce    sync.Once            `json:"-"`
	mu          sync.RWMutex         `json:"-"`
	LastFailure int64                `json:"last_failure,omitempty"`
	LastCheck   int64                `json:"last_check,omitempty"`
	Blocked     bool                 `json:"blocked,omitempty"`
	Codes       map[int]*CodeNodeSet `json:"codes,omitempty"`
}

type ActiveTarget struct {
	Target   string
	ASN      string
	IsUDP    bool
	LastUsed int64
}

type NodeRankItem struct {
	Name   string
	Rank   string
	Weight float64
}

type NodeRank struct {
	LastUpdated int64           `json:"last_updated"`
	Result      []NodeRankItem  `json:"result"`
}

type targetMinHeap []ActiveTarget

// 域名节点锁
func initShardedLocks() {
	shardedLocksOnce.Do(func() {
		for i := range shardedLocks {
			shardedLocks[i] = &sync.RWMutex{}
		}
	})
}

func GetTargetNodeLock(target, group, proxy string) *sync.RWMutex {
	initShardedLocks()

	const fnvOffset32 uint32 = 2166136261
	const fnvPrime32 uint32 = 16777619
	h := fnvOffset32
	for i := 0; i < len(target); i++ {
		h ^= uint32(target[i])
		h *= fnvPrime32
	}
	for i := 0; i < len(group); i++ {
		h ^= uint32(group[i])
		h *= fnvPrime32
	}
	for i := 0; i < len(proxy); i++ {
		h ^= uint32(proxy[i])
		h *= fnvPrime32
	}

	return shardedLocks[h&1023]
}

// 获取或创建原子记录
func (s *Store) GetOrCreateAtomicRecord(cacheKey string, group, config, target, proxy string) *AtomicStatsRecord {
	if value, ok := recordCache.Get(cacheKey); ok {
		return value
	}

	record := &AtomicStatsRecord{
		weights:         lru.New[string, float64](lru.WithSize[string, float64](100)),
	}

	if existingData, err := s.GetStatsForTarget(group, config, target, proxy); err == nil {
		if data, exists := existingData[proxy]; exists {
			var existingRecord StatsRecord
			if json.Unmarshal(data, &existingRecord) == nil {
				record.success.Store(existingRecord.Success)
				record.failure.Store(existingRecord.Failure)
				record.connectTime.Store(existingRecord.ConnectTime)
				record.latency.Store(existingRecord.Latency)
				record.lastUsed.Store(existingRecord.LastUsed)
				record.uploadTotal.Store(existingRecord.UploadTotal)
				record.downloadTotal.Store(existingRecord.DownloadTotal)
				record.duration.Store(existingRecord.ConnectionDuration)
				record.maxUploadRate.Store(existingRecord.MaxUploadRate)
				record.maxDownloadRate.Store(existingRecord.MaxDownloadRate)
				record.lossRate.Store(existingRecord.LossRate)
				if existingRecord.Weights != nil {
					for k, v := range existingRecord.Weights {
						record.weights.Set(k, v)
					}
				}
			}
		}
	}

	recordCache.Set(cacheKey, record)
	return record
}

// 创建统计快照
func (record *AtomicStatsRecord) CreateStatsSnapshot(cacheKey string) *StatsRecord {
	if record == nil {
		return &StatsRecord{}
	}

	snapshot := &StatsRecord{
		Success:            record.success.Load(),
		Failure:            record.failure.Load(),
		ConnectTime:        record.connectTime.Load(),
		Latency:            record.latency.Load(),
		LastUsed:           record.lastUsed.Load(),
		UploadTotal:        record.uploadTotal.Load(),
		DownloadTotal:      record.downloadTotal.Load(),
		MaxUploadRate:      record.maxUploadRate.Load(),
		MaxDownloadRate:    record.maxDownloadRate.Load(),
		ConnectionDuration: record.duration.Load(),
		LossRate:           record.lossRate.Load(),
		Weights:            record.weights.FilterByKeyPrefix(""),
	}

	recordCache.Set(cacheKey, record)

	return snapshot
}

func (r *AtomicStatsRecord) Get(field string) interface{} {
	switch field {
	case "success":
		return r.success.Load()
	case "failure":
		return r.failure.Load()
	case "connectTime":
		return r.connectTime.Load()
	case "latency":
		return r.latency.Load()
	case "lastUsed":
		return r.lastUsed.Load()
	case "uploadTotal":
		return r.uploadTotal.Load()
	case "downloadTotal":
		return r.downloadTotal.Load()
	case "maxUploadRate":
		return r.maxUploadRate.Load()
	case "maxDownloadRate":
		return r.maxDownloadRate.Load()
	case "duration":
		return r.duration.Load()
	case "lossRate":
		return r.lossRate.Load()
	default:
		return nil
	}
}

func (r *AtomicStatsRecord) Set(field string, value interface{}) {
	switch field {
	case "success":
		if v, ok := value.(int64); ok {
			r.success.Store(v)
		}
	case "failure":
		if v, ok := value.(int64); ok {
			r.failure.Store(v)
		}
	case "connectTime":
		if v, ok := value.(int64); ok {
			r.connectTime.Store(v)
		}
	case "latency":
		if v, ok := value.(int64); ok {
			r.latency.Store(v)
		}
	case "lastUsed":
		if v, ok := value.(int64); ok {
			r.lastUsed.Store(v)
		}
	case "uploadTotal":
		if v, ok := value.(float64); ok {
			r.uploadTotal.Store(v)
		}
	case "downloadTotal":
		if v, ok := value.(float64); ok {
			r.downloadTotal.Store(v)
		}
	case "maxUploadRate":
		if v, ok := value.(float64); ok {
			r.maxUploadRate.Store(v)
		}
	case "maxDownloadRate":
		if v, ok := value.(float64); ok {
			r.maxDownloadRate.Store(v)
		}
	case "duration":
		if v, ok := value.(float64); ok {
			r.duration.Store(v)
		}
	case "lossRate":
		if v, ok := value.(float64); ok {
			r.lossRate.Store(v)
		}
	}
}

func (r *AtomicStatsRecord) Add(field string, value interface{}) {
	switch field {
	case "success":
		if v, ok := value.(int64); ok {
			current := r.success.Load()
			if v > 0 && current > math.MaxInt64/2-v {
				r.success.Store(math.MaxInt64 / 4)
			} else {
				r.success.Add(v)
			}
		}
	case "failure":
		if v, ok := value.(int64); ok {
			current := r.failure.Load()
			if v > 0 && current > math.MaxInt64/2-v {
				r.failure.Store(math.MaxInt64 / 4)
			} else {
				r.failure.Add(v)
			}
		}
	case "uploadTotal":
		if v, ok := value.(float64); ok {
			current := r.uploadTotal.Load()
			// 1PB (1024^5 bytes)
			const maxUpload = 1125899906842624.0
			if current+v > maxUpload {
				r.uploadTotal.Store(maxUpload / 2)
			} else {
				r.uploadTotal.Add(v)
			}
		}
	case "downloadTotal":
		if v, ok := value.(float64); ok {
			current := r.downloadTotal.Load()
			// 1PB (1024^5 bytes)
			const maxDownload = 1125899906842624.0
			if current+v > maxDownload {
				r.downloadTotal.Store(maxDownload / 2)
			} else {
				r.downloadTotal.Add(v)
			}
		}
	}
}

func (r *AtomicStatsRecord) GetWeight(weightType string) float64 {
	if value, ok := r.weights.Get(weightType); ok {
		return value
	}
	return 0
}

func (r *AtomicStatsRecord) SetWeight(weightType string, value float64, isUDP bool) {
	r.weights.Set(weightType, value)
	if weightType != WeightTypeTCP && weightType != WeightTypeUDP {
		if isUDP {
			minUDP := r.avgASNWeight(WeightTypeUDP)
			if minUDP > 0 {
				r.weights.Set(WeightTypeUDP, minUDP)
			}
		} else {
			minTCP := r.avgASNWeight(WeightTypeTCP)
			if minTCP > 0 {
				r.weights.Set(WeightTypeTCP, minTCP)
			}
		}
	}
}

func (r *AtomicStatsRecord) avgASNWeight(prefix string) float64 {
	weights := r.weights.FilterByKeyPrefix(prefix)
	var sum float64
	var count int
	for k, v := range weights {
		if k == prefix {
			continue
		}
		sum += v
		count++
	}
	if count == 0 {
		return 0.0
	}
	return sum / float64(count)
}

// 获取节点权重排名缓存
func (s *Store) GetNodeWeightRankingCache(group, config string) (NodeRank, error) {
	pathPrefix := FormatDBKey(KeyTypeRanking, config, group)
	rawResult, err := s.GetSubBytesByPath(pathPrefix)
	if err != nil {
		return NodeRank{}, err
	}

	for _, data := range rawResult {
		var wrapper NodeRank
		if err := json.Unmarshal(data, &wrapper); err == nil && len(wrapper.Result) > 0 {
			return wrapper, nil
		}
	}

	return NodeRank{}, nil
}

// 获取节点权重排名
func (s *Store) GetNodeWeightRanking(group, config, testUrl string, proxies []C.Proxy) (NodeRank, error) {
	if len(proxies) == 0 {
		return NodeRank{}, fmt.Errorf("no proxies provided")
	}

	resultItems := make([]NodeRankItem, 0, len(proxies))
	proxyNames := make([]string, 0, len(proxies))
	allNodes := make(map[string]struct{}, len(proxies))
	aliveNodes := make(map[string]bool, len(proxies))
	blockedNodes := s.GetBlockedNodes(group, config)
	for _, p := range proxies {
		name := p.Name()
		proxyNames = append(proxyNames, name)
		if p.AliveForTestUrl(testUrl) && !blockedNodes[name] {
			aliveNodes[name] = true
		}
		allNodes[name] = struct{}{}
	}

	globalCacheParams.mutex.RLock()
	prefetchLimit := globalCacheParams.MaxTargets / 2
	globalCacheParams.mutex.RUnlock()

	activeTargets := s.GetActiveTargets(group, config, prefetchLimit)

	nodeScores := make(map[string]float64, len(proxies))

	for _, ad := range activeTargets {
		nodes, weights := s.GetPrefetchResult(group, config, ad.Target, ad.ASN, ad.IsUDP)
		limit := len(nodes)
		if len(weights) < limit {
			limit = len(weights)
		}
		for i := 0; i < limit; i++ {
			node := nodes[i]
			if _, ok := allNodes[node]; !ok {
				continue
			}
			if weights[i] < AllowedWeight {
				break
			}
			score := 1.0 / float64(i+1)
			if score < 1e-4 {
				break
			}
			nodeScores[node] += score
		}
	}

	maxScore := 0.0
	for _, score := range nodeScores {
		if score > maxScore {
			maxScore = score
		}
	}

	if maxScore == 0 {
		return NodeRank{}, nil
	}

	for _, node := range proxyNames {
		score := nodeScores[node]
		percentScore := math.Round(score / maxScore * 100 * 100) / 100
		resultItems = append(resultItems, NodeRankItem{Name: node, Weight: percentScore, Rank: ""})
	}

	sort.Slice(resultItems, func(i, j int) bool {
		ai := aliveNodes[resultItems[i].Name]
		aj := aliveNodes[resultItems[j].Name]
		if ai != aj {
			return ai
		}
		if resultItems[i].Weight != resultItems[j].Weight {
			return resultItems[i].Weight > resultItems[j].Weight
		}
		return resultItems[i].Name < resultItems[j].Name
	})

	if len(resultItems) > 0 {
		aliveCount := 0
		positiveAliveCount := 0
		for _, r := range resultItems {
			if aliveNodes[r.Name] {
				aliveCount++
				if r.Weight > 0 {
					positiveAliveCount++
				}
			}
		}

		if aliveCount > 0 && positiveAliveCount > 0 {
				mostUsedBound := int(float64(positiveAliveCount) * 0.2)
				if mostUsedBound < 1 {
					mostUsedBound = 1
				}

				occasionalBound := mostUsedBound + int(float64(positiveAliveCount)*0.5)

				for i := 0; i < mostUsedBound; i++ {
					resultItems[i].Rank = RankMostUsed
				}
				for i := mostUsedBound; i < occasionalBound; i++ {
					resultItems[i].Rank = RankOccasional
				}
				for i := occasionalBound; i < aliveCount; i++ {
					resultItems[i].Rank = RankRarelyUsed
				}
		}

		for i := aliveCount; i < len(resultItems); i++ {
			resultItems[i].Rank = RankRarelyUsed
		}

		wrapper := NodeRank{LastUpdated: time.Now().Unix(), Result: resultItems}
		s.StoreNodeWeightRanking(group, config, wrapper)
		return wrapper, nil
    }

    return NodeRank{}, nil
}

// 存储节点权重排名
func (s *Store) StoreNodeWeightRanking(group, config string, ranking NodeRank) {
	data, err := json.Marshal(ranking)
	if err != nil {
		return
	}

	s.AppendToGlobalQueue(StoreOperation{
		Type:   OpSaveRanking,
		Group:  group,
		Config: config,
		Data:   data,
	})
}

// 获取目标的最佳代理
func (s *Store) GetBestProxyForTarget(group, config, target, asnNumber string, isUDP bool) ([]string, []float64, error) {
	if target == "" {
		return nil, nil, errors.New("empty target")
	}

	now := time.Now().Unix()
	minDecay := 0.4

	getTimeDecay := func(lastUsedTime int64) float64 {
		return GetTimeDecayWithCache(lastUsedTime, now, minDecay)
	}

	allStatsMap, err := s.GetAllStats(group, config)
	if err != nil {
		return nil, nil, err
	}

	weightType := WeightTypeTCP
	if isUDP {
		weightType = WeightTypeUDP
	}

	nodesWithWeight := make(map[string]float64)

	// 优先使用 ASN
	if asnNumber != "" && !CdnASNs[asnNumber] {
		asnWeightType := WeightTypeTCPASN + ":" + asnNumber
		if isUDP {
			asnWeightType = WeightTypeUDPASN + ":" + asnNumber
		}

		nodeCounts := make(map[string]int64)

		for _, mapStats := range allStatsMap {
			for nodeName, data := range mapStats {
				var record StatsRecord
				if json.Unmarshal(data, &record) != nil {
					continue
				}
				if record.Weights == nil {
					continue
				}
				
				weight, ok := record.Weights[asnWeightType]
				if !ok || weight <= 0 {
					continue
				}
				
				timeDecay := getTimeDecay(record.LastUsed)
				decayedWeight := weight * timeDecay
				nodesWithWeight[nodeName] += decayedWeight
				nodeCounts[nodeName]++
			}
		}

		for nodeName, sum := range nodesWithWeight {
			if count := nodeCounts[nodeName]; count > 0 {
				nodesWithWeight[nodeName] = sum / float64(count)
			}
		}
	} else {
		var mapStats map[string][]byte
		if stats, ok := allStatsMap[target]; ok {
			mapStats = stats
		} else {
			if stats, err := s.GetStatsForTarget(group, config, target, ""); err == nil {
				mapStats = stats
			}
		}

		for nodeName, data := range mapStats {
			var record StatsRecord
			if json.Unmarshal(data, &record) != nil {
				continue
			}
			var weight float64
			if record.Weights != nil {
				weight = record.Weights[weightType]
			}
			if weight > 0 {
				timeDecay := getTimeDecay(record.LastUsed)
				// weight maybe is all target ASNs average weight because of avgASNWeight()
				decayedWeight := weight * timeDecay
				nodesWithWeight[nodeName] = decayedWeight
			}
		}
	}

	if len(nodesWithWeight) == 0 {
		return nil, nil, errors.New("no best node with enough weight")
	}

	nodeList := make([]NodeWithWeight, 0, len(nodesWithWeight))
	for node, weight := range nodesWithWeight {
		nodeList = append(nodeList, NodeWithWeight{node, weight})
	}

	sort.Slice(nodeList, func(i, j int) bool {
		if nodeList[i].Weight != nodeList[j].Weight {
			return nodeList[i].Weight > nodeList[j].Weight
		}
		return nodeList[i].Node < nodeList[j].Node
	})

	n := len(nodeList)
	bestNodes := make([]string, n)
	bestWeights := make([]float64, n)
	for i, nw := range nodeList {
		bestNodes[i] = nw.Node
		bestWeights[i] = nw.Weight
	}

	return bestNodes, bestWeights, nil
}

// 获取活跃域名
func (h targetMinHeap) Len() int           { return len(h) }
func (h targetMinHeap) Less(i, j int) bool { return h[i].LastUsed < h[j].LastUsed }
func (h targetMinHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *targetMinHeap) Push(x interface{}) { *h = append(*h, x.(ActiveTarget)) }
func (h *targetMinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (s *Store) GetActiveTargets(group, config string, limit int) []ActiveTarget {
	allStats, err := s.GetAllStats(group, config)
	if err != nil || len(allStats) == 0 {
		return nil
	}

	h := &targetMinHeap{}
	heap.Init(h)

	type seenKey struct {
		target, asn string
		isUDP       bool
	}
	seen := make(map[seenKey]int64)

	for target, nodeStats := range allStats {
		var maxLastUsed int64
		// key: "asn:is_udp", value: lastUsed
		activeCombinations := make(map[string]int64)

		for _, data := range nodeStats {
			var record StatsRecord
			if json.Unmarshal(data, &record) != nil {
				continue
			}
			if record.LastUsed > maxLastUsed {
				maxLastUsed = record.LastUsed
			}
			if record.Weights == nil {
				continue
			}

			if w, ok := record.Weights[WeightTypeTCP]; ok && w > 0 {
				key := ":false"
				if last, exists := activeCombinations[key]; !exists || record.LastUsed > last {
					activeCombinations[key] = record.LastUsed
				}
			}
			if w, ok := record.Weights[WeightTypeUDP]; ok && w > 0 {
				key := ":true"
				if last, exists := activeCombinations[key]; !exists || record.LastUsed > last {
					activeCombinations[key] = record.LastUsed
				}
			}

			// 处理 ASN 权重
			for key, weight := range record.Weights {
				if strings.HasPrefix(key, WeightTypeTCPASN) && weight > 0 {
					if _, asn, ok := strings.Cut(key, ":"); ok {
						combKey := asn + ":false"
						if last, exists := activeCombinations[combKey]; !exists || record.LastUsed > last {
							activeCombinations[combKey] = record.LastUsed
						}
					}
				} else if strings.HasPrefix(key, WeightTypeUDPASN) && weight > 0 {
					if _, asn, ok := strings.Cut(key, ":"); ok {
						combKey := asn + ":true"
						if last, exists := activeCombinations[combKey]; !exists || record.LastUsed > last {
							activeCombinations[combKey] = record.LastUsed
						}
					}
				}
			}
		}

		if len(activeCombinations) == 0 {
			continue
		}

		hasASN := false
		for combKey := range activeCombinations {
			if asn, _, _ := strings.Cut(combKey, ":"); asn != "" {
				hasASN = true
				break
			}
		}

		for combKey, lastUsed := range activeCombinations {
			asn, udpStr, _ := strings.Cut(combKey, ":")
			isUDP := udpStr == "true"

			if asn == "" && hasASN {
				continue
			}

			sk := seenKey{target, asn, isUDP}
			if existingLast, exists := seen[sk]; !exists || lastUsed > existingLast {
				seen[sk] = lastUsed
				heap.Push(h, ActiveTarget{
					Target:   target,
					ASN:      asn,
					IsUDP:    isUDP,
					LastUsed: lastUsed,
				})
				if h.Len() > limit {
					heap.Pop(h)
				}
			}
		}
	}

	sorted := make([]ActiveTarget, 0, h.Len())
	for h.Len() > 0 {
		sorted = append(sorted, heap.Pop(h).(ActiveTarget))
	}
	for i, j := 0, len(sorted) - 1; i < j; i, j = i + 1, j - 1 {
		sorted[i], sorted[j] = sorted[j], sorted[i]
	}

	return sorted
}

// RunPrefetch 最佳节点预计算
func (s *Store) RunPrefetch(group, config string, proxyMap map[string]bool) int {
	log.Debugln("[SmartStore] Executing target and ASN pre-calculation for policy group [%s]", group)

	if len(proxyMap) == 0 {
		log.Debugln("[SmartStore] No available nodes for prefetch calculation in group [%s]", group)
		return 0
	}

	globalCacheParams.mutex.RLock()
	prefetchLimit := globalCacheParams.MaxTargets / 2
	globalCacheParams.mutex.RUnlock()

	activeTargets := s.GetActiveTargets(group, config, prefetchLimit)

	type prefetchItem struct {
		target      string
		asnNumber   string
		isUDP       bool
		bestNodes   []string
		bestWeights []float64
	}

	type asnCacheKey struct {
		asnNumber   string
		isUDP       bool
	}

	type asnCacheValue struct {
		nodes       []string
		weights     []float64
	}

	asnCache := make(map[asnCacheKey]asnCacheValue)

	items := make([]prefetchItem, 0, len(activeTargets))

	for _, active := range activeTargets {
		var bestNodes []string
		var bestWeights []float64
		var err error

		if active.ASN != "" && !CdnASNs[active.ASN] {
			key := asnCacheKey{active.ASN, active.IsUDP}
			if v, ok := asnCache[key]; ok {
				bestNodes = v.nodes
				bestWeights = v.weights
			} else {
				bestNodes, bestWeights, err = s.GetBestProxyForTarget(group, config, active.Target, active.ASN, active.IsUDP)
				asnCache[key] = asnCacheValue{
					nodes:      bestNodes,
					weights:    bestWeights,
				}
			}
		} else {
			bestNodes, bestWeights, err = s.GetBestProxyForTarget(group, config, active.Target, active.ASN, active.IsUDP)
		}

		if err != nil || len(bestNodes) == 0 {
			continue
		}

		nodes := make([]string, 0, len(bestNodes))
		weights := make([]float64, 0, len(bestWeights))
		for i := 0; i < len(bestNodes); i++ {
			if !proxyMap[bestNodes[i]] {
				continue
			}
			nodes = append(nodes, bestNodes[i])
			weights = append(weights, bestWeights[i])
		}

		if len(nodes) > 0 {
			item := prefetchItem{
				target:      active.Target,
				asnNumber:   active.ASN,
				isUDP:       active.IsUDP,
				bestNodes:   nodes,
				bestWeights: weights,
			}
			items = append(items, item)
		}
	}

	asnCache = make(map[asnCacheKey]asnCacheValue)
	targetNodeExistsCache := make(map[string]map[string]bool)

	prefetchCount := 0

	for _, item := range items {
		oldNodes, oldWeights := s.GetPrefetchResult(group, config, item.target, item.asnNumber, item.isUDP)

		target := item.target
		if item.asnNumber != "" {
			target += " (ASN: " + item.asnNumber + ")"
		}

		networkType := "tcp"
		if item.isUDP {
			networkType = "udp"
		}

		var sortedNodes []string
		var sortedWeights []float64
		var needUpdate bool
		cacheHit := false
		if item.asnNumber != "" && !CdnASNs[item.asnNumber] {
			key := asnCacheKey{item.asnNumber, item.isUDP}
			if v, ok := asnCache[key]; ok {
				sortedNodes = v.nodes
				sortedWeights = v.weights
				cacheHit = true
			}
		}

		if !cacheHit {
			if len(oldNodes) == 0 {
				needUpdate = true
				sortedNodes = item.bestNodes
				sortedWeights = item.bestWeights
			} else {
				finalNodeMap := make(map[string]float64, len(oldNodes)+len(item.bestNodes))
				for i, node := range oldNodes {
					finalNodeMap[node] = oldWeights[i]
				}

				for i, newNode := range item.bestNodes {
					newW := item.bestWeights[i]
					if oldW, exists := finalNodeMap[newNode]; exists {
						// prevent degrade recovery too fast
						if oldW <= 0 || math.Abs(newW - oldW) / oldW > 0.1 {
							finalNodeMap[newNode] = newW
							needUpdate = true
						}
					} else {
						finalNodeMap[newNode] = newW
						needUpdate = true
					}
				}

				// Clean up prefetch results if node stats expired, otherwise they may stay in prefetch results even if they are recovered.
				nodeExistsMap, ok := targetNodeExistsCache[item.target]
				if !ok {
					nodeExistsMap = make(map[string]bool)
					targetNodeExistsCache[item.target] = nodeExistsMap
				}

				for node := range finalNodeMap {
					if !proxyMap[node] && finalNodeMap[node] > AllowedWeight {
						delete(finalNodeMap, node)
						continue
					}
					exists, checked := nodeExistsMap[node]
					if !checked {
						if nodeStats, err := s.GetStatsForTarget(group, config, item.target, node); err == nil && len(nodeStats) > 0 {
							exists = true
						}
						nodeExistsMap[node] = exists
					}
					if !exists {
						delete(finalNodeMap, node)
					}
				}

				if needUpdate {
					var nodeList []NodeWithWeight
					for node, weight := range finalNodeMap {
						nodeList = append(nodeList, NodeWithWeight{node, weight})
					}
					sort.Slice(nodeList, func(i, j int) bool {
						if nodeList[i].Weight != nodeList[j].Weight {
							return nodeList[i].Weight > nodeList[j].Weight
						}
						return nodeList[i].Node < nodeList[j].Node
					})

					sortedNodes = make([]string, len(nodeList))
					sortedWeights = make([]float64, len(nodeList))
					for i, nw := range nodeList {
						sortedNodes[i] = nw.Node
						sortedWeights[i] = nw.Weight
					}
				} else {
					sortedNodes = item.bestNodes
					sortedWeights = item.bestWeights
				}
			}

			if item.asnNumber != "" && !CdnASNs[item.asnNumber] {
				key := asnCacheKey{item.asnNumber, item.isUDP}
				asnCache[key] = asnCacheValue{
					nodes:      sortedNodes,
					weights:    sortedWeights,
				}
			}
		}

		if needUpdate {
			s.StorePrefetchResult(group, config, item.target, item.asnNumber, item.isUDP, sortedNodes, sortedWeights)
		}

		prefetchCount++

		nodeWeightPairs := make([]string, len(sortedNodes))
		for i := range sortedNodes {
			nodeWeightPairs[i] = fmt.Sprintf("%s: %.2f", sortedNodes[i], sortedWeights[i])
		}

		if len(oldNodes) == 0 {
			log.Debugln("[SmartStore] Prefetching for group [%s]: network: [%s] => target: [%s] => result: [%s] (no old result)",
				group, networkType, target, strings.Join(nodeWeightPairs, ", "))
		} else if cacheHit {
			log.Debugln("[SmartStore] Prefetching for group [%s]: network: [%s] => target: [%s] => result: [%s] (from cache)",
				group, networkType, target, strings.Join(nodeWeightPairs, ", "))
		} else if needUpdate {
			oldNodeWeightPairs := make([]string, len(oldNodes))
			for i := range oldNodes {
				oldNodeWeightPairs[i] = fmt.Sprintf("%s: %.2f", oldNodes[i], oldWeights[i])
			}
			log.Debugln("[SmartStore] Prefetching for group [%s]: network: [%s] => target: [%s] => result: [%s] (updated from old: [%s])",
				group, networkType, target, strings.Join(nodeWeightPairs, ", "), strings.Join(oldNodeWeightPairs, ", "))
		}
	}

	log.Infoln("[SmartStore] Prefetch completed for group [%s]: pre-calculated [%d] targets",
		group, prefetchCount)
	return prefetchCount
}

// GetBlockedNodes 获取被屏蔽节点
func (s *Store) GetBlockedNodes(group, config string) map[string]bool {
	cacheKey := FormatDBKey(config, group)
	if cached, ok := blockedNodesCache.Get(cacheKey); ok {
		return cached
	}

	stateData, err := s.GetNodeStates(group, config)
	if err != nil {
		return nil
	}
	blockedNodes := make(map[string]bool)

	for nodeName, data := range stateData {
		var state NodeState
		if json.Unmarshal(data, &state) == nil {
			if state.BlockedUntil > 0 && state.BlockedUntil > time.Now().Unix() {
				blockedNodes[nodeName] = true
			}
		}
	}

	blockedNodesCache.Set(cacheKey, blockedNodes)
	return blockedNodes
}

// GetNodeStates 获取节点状态
func (s *Store) GetNodeStates(group, config string) (map[string][]byte, error) {
	pathPrefix := FormatDBKey(KeyTypeNode, config, group)
	result := make(map[string][]byte)

	rawResult, err := s.GetSubBytesByPath(pathPrefix)
	if err != nil {
		return nil, err
	}

	for fullPath, data := range rawResult {
		nodeName := fullPath[strings.LastIndexByte(fullPath, '/')+1:]
		result[nodeName] = data
	}

	return result, nil
}

// 获取域名的统计数据
func (s *Store) GetStatsForTarget(group, config, target, proxy string) (map[string][]byte, error) {
	result := make(map[string][]byte)

	var pathPrefix string
	if proxy != "" {
		pathPrefix = FormatDBKey(KeyTypeStats, config, group, target, proxy)
	} else {
		pathPrefix = FormatDBKey(KeyTypeStats, config, group, target)
	}

	rawResult, err := s.GetSubBytesByPath(pathPrefix)
	if err != nil {
		return nil, err
	}

	if proxy != "" {
		for _, data := range rawResult {
			result[proxy] = data
		}
	} else {
		for fullPath, data := range rawResult {
			nodeName := fullPath[strings.LastIndexByte(fullPath, '/') + 1:]
			result[nodeName] = data
		}
	}

	return result, nil
}

// 获取所有统计数据
func (s *Store) GetAllStats(group, config string) (map[string]map[string][]byte, error) {
	pathPrefix := FormatDBKey(KeyTypeStats, config, group)

	result := make(map[string]map[string][]byte)

	rawResult, err := s.GetSubBytesByPath(pathPrefix)
	if err != nil {
		return nil, err
	}

	for fullPath, data := range rawResult {
		if strings.Count(fullPath, "/") < 5 {
			continue
		}
		lastSlash := strings.LastIndexByte(fullPath, '/')
		node := fullPath[lastSlash+1:]
		prevSlash := strings.LastIndexByte(fullPath[:lastSlash], '/')
		target := fullPath[prevSlash+1 : lastSlash]

		if _, ok := result[target]; !ok {
			result[target] = make(map[string][]byte)
		}
		result[target][node] = data
	}

	return result, nil
}

// 获取缓存中的所有组名
func (s *Store) GetAllGroupsForConfig(config string) ([]string, error) {
	groupsMap := make(map[string]bool)

	statsPath := FormatDBKey(KeyTypeStats, config)
	raw, err := s.GetSubBytesByPath(statsPath)
	if err != nil {
		raw = nil
	}

	for fullPath := range raw {
		parts := strings.Split(fullPath, "/")
		if len(parts) >= 4 {
			group := parts[3]
			if group != "" {
				groupsMap[group] = true
			}
		}
	}

	if len(groupsMap) == 0 {
		scanResults, err2 := s.DBViewPrefixScan(statsPath, -1, false)
		if err2 != nil {
			return nil, err2
		}
		for path := range scanResults {
			parts := strings.Split(path, "/")
			if len(parts) >= 4 {
				group := parts[3]
				if group != "" {
					groupsMap[group] = true
				}
			}
		}
	}

	result := make([]string, 0, len(groupsMap))
	for g := range groupsMap {
		result = append(result, g)
	}

	return result, nil
}

// 通过缓存数据获取组中的节点
func (s *Store) GetAllNodesForGroup(group, config string) ([]string, error) {
	nodesMap := make(map[string]bool)

	nodesPath := FormatDBKey(KeyTypeNode, config, group)
	nodeStatesData, err := s.GetSubBytesByPath(nodesPath)
	if err == nil {
		for key := range nodeStatesData {
			nodeName := key[strings.LastIndexByte(key, '/') + 1:]
			if nodeName != "" {
				nodesMap[nodeName] = true
			}
		}
	}

	statsPath := FormatDBKey(KeyTypeStats, config, group)
	statsData, err := s.GetSubBytesByPath(statsPath)
	if err == nil {
		for key := range statsData {
			if strings.Count(key, "/") < 5 {
				continue
			}
			nodeName := key[strings.LastIndexByte(key, '/') + 1:]
			if nodeName != "" {
				nodesMap[nodeName] = true
			}
		}
	}

	result := make([]string, 0, len(nodesMap))
	for node := range nodesMap {
		result = append(result, node)
	}
	return result, nil
}

// 域名失败屏蔽
func (s *Store) GetHostStatus(group, config, wildcardTarget string) (map[string]bool, int64, int64, bool) {
	pathPrefix := FormatDBKey(KeyTypeHostFailures, config, group, wildcardTarget)

	now := time.Now().Unix()
	resultNodes := make(map[string]bool)

	hs, _ := hostStatusCache.GetOrStore(pathPrefix, func() *HostStatus { return &HostStatus{} })

	hs.initOnce.Do(func() {
		rawResult, err := s.GetSubBytesByPath(pathPrefix)
		if err == nil {
			for _, data := range rawResult {
				if err := json.Unmarshal(data, hs); err == nil {
					break
				}
			}
		}
	})

	hs.mu.RLock()
	defer hs.mu.RUnlock()

	for _, codeSet := range hs.Codes {
		if codeSet == nil {
			continue
		}
		for nodeName, nodeEntry := range codeSet.Nodes {
			if nodeEntry == 0 || nodeEntry > now {
				resultNodes[nodeName] = true
			}
		}
	}

	return resultNodes, hs.LastCheck, hs.LastFailure, hs.Blocked
}

func (s *Store) UpdateHostStatus(group, config, wildcardTarget string, metadata *C.Metadata, name string, maxFailedTimes int, failure, checked bool, statusCode int64) bool {
	if !checked {
		return false
	}

	newCode := int(statusCode)

	var host string
	if failure && newCode == 2 {
		if metadata.Host == "" {
			return false
		}
		host = metadata.Host
	}

	pathPrefix := FormatDBKey(KeyTypeHostFailures, config, group, wildcardTarget)

	var failedBlock bool
	now := time.Now().Unix()

	hs, _ := hostStatusCache.GetOrStore(pathPrefix, func() *HostStatus { return &HostStatus{} })

	hs.initOnce.Do(func() {
		rawResult, err := s.GetSubBytesByPath(pathPrefix)
		if err == nil {
			for _, data := range rawResult {
				if err := json.Unmarshal(data, hs); err == nil {
					break
				}
			}
		}
	})

	hs.mu.Lock()
	defer hs.mu.Unlock()

	if hs.Codes == nil {
		hs.Codes = make(map[int]*CodeNodeSet)
	}

	for code, codeSet := range hs.Codes {
		if codeSet == nil {
			delete(hs.Codes, code)
			continue
		}
		if code == 1 {
			continue
		}
		if code == 2 {
			if hs.Blocked {
				for nodeName, nodeEntry := range codeSet.Nodes {
					if nodeEntry != 0 && nodeEntry <= now {
						delete(codeSet.Nodes, nodeName)
						if codeSet.NodeHosts != nil {
							delete(codeSet.NodeHosts, nodeName)
						}
					}
				}
				if len(codeSet.Nodes) == 0 {
					delete(hs.Codes, code)
				}
			}
			continue
		}
		for nodeName, nodeEntry := range codeSet.Nodes {
			if nodeEntry != 0 && nodeEntry <= now {
				delete(codeSet.Nodes, nodeName)
				if codeSet.FailCounts != nil {
					delete(codeSet.FailCounts, nodeName)
				}
			}
		}
		if len(codeSet.Nodes) == 0 && len(codeSet.FailCounts) == 0 {
			delete(hs.Codes, code)
		}
	}

	oldLastFailure := hs.LastFailure
	currentCode := -1

	for code, codeSet := range hs.Codes {
		if codeSet == nil {
			continue
		}
		if _, ok := codeSet.Nodes[name]; ok {
			if currentCode == -1 || code < currentCode {
				currentCode = code
			}
		}
		if codeSet.FailCounts != nil {
			if _, ok := codeSet.FailCounts[name]; ok {
				if currentCode == -1 || code < currentCode {
					currentCode = code
				}
			}
		}
	}

	if !failure && newCode == 0 {
		for code, codeSet := range hs.Codes {
			if code == 1 || codeSet == nil {
				continue
			}
			delete(codeSet.Nodes, name)
			if codeSet.FailCounts != nil {
				delete(codeSet.FailCounts, name)
			}
			if codeSet.NodeHosts != nil {
				delete(codeSet.NodeHosts, name)
			}
		}
		goto saveAndReturn
	}

	if currentCode != -1 && currentCode != newCode {
		if newCode > currentCode {
			goto saveAndReturn
		}
		for code, codeSet := range hs.Codes {
			if code == newCode || codeSet == nil {
				continue
			}
			delete(codeSet.Nodes, name)
			if codeSet.FailCounts != nil {
				delete(codeSet.FailCounts, name)
			}
			if codeSet.NodeHosts != nil {
				delete(codeSet.NodeHosts, name)
			}
		}
	}

	if failure || newCode == 3 {
		hs.LastFailure = now

		if hs.Codes[newCode] == nil {
			hs.Codes[newCode] = &CodeNodeSet{
				Nodes: make(map[string]int64),
			}
		}
		codeSet := hs.Codes[newCode]
		if codeSet.Nodes == nil {
			codeSet.Nodes = make(map[string]int64)
		}

		switch newCode {
		case 1:
			codeSet.Nodes[name] = 0 // TTL=0 means permanent
		case 2:
			codeSet.Nodes[name] = time.Now().Add(HostFailureNodeTTL).Unix()
			if codeSet.NodeHosts == nil {
				codeSet.NodeHosts = make(map[string]string)
			}
			codeSet.NodeHosts[name] = host
		case 3:
			if codeSet.FailCounts == nil {
				codeSet.FailCounts = make(map[string]int)
			}
			count := codeSet.FailCounts[name]
			if oldLastFailure == 0 || now-oldLastFailure > 300 {
				count = 1
			} else {
				count++
			}
			if count >= maxFailedTimes {
				codeSet.Nodes[name] = time.Now().Add(HostFailureNodeTTL).Unix()
				delete(codeSet.FailCounts, name)
				failedBlock = true
			} else {
				codeSet.FailCounts[name] = count
			}
		default:
			codeSet.Nodes[name] = time.Now().Add(HostFailureNodeTTL).Unix()
		}
	}

saveAndReturn:
	hs.LastCheck = now
	hostBlockingCount := 0

	for code, cs := range hs.Codes {
		if code != 1 && cs != nil {
			hostBlockingCount += len(cs.Nodes)
		}
	}
	if hostBlockingCount > maxFailedTimes {
		hs.Blocked = true
		failedBlock = false
	} else {
		hs.Blocked = false
	}

	for code, codeSet := range hs.Codes {
		if codeSet == nil || (len(codeSet.Nodes) == 0 && len(codeSet.FailCounts) == 0) {
			delete(hs.Codes, code)
		}
	}

	data, err := json.Marshal(hs)
	if err != nil {
		return failedBlock
	}

	s.AppendToGlobalQueue(StoreOperation{
		Type:   OpSaveHostFailures,
		Group:  group,
		Config: config,
		Target: wildcardTarget,
		Data:   data,
	})

	return failedBlock
}

func (s *Store) CheckHostStatus(group, config string) (map[string]map[string]string, error) {
	pathPrefix := FormatDBKey(KeyTypeHostFailures, config, group)
	dataMap, err := s.GetSubBytesByPath(pathPrefix)
	if err != nil {
		return nil, err
	}

	// result[wildcardTarget][nodeName] = host
	result := make(map[string]map[string]string)
	now := time.Now().Unix()

	for fullPath, data := range dataMap {
		var hs HostStatus
		if err := json.Unmarshal(data, &hs); err != nil {
			continue
		}

		lastSlash := strings.LastIndexByte(fullPath, '/')
		if lastSlash < 0 {
			continue
		}
		wildcardTarget := fullPath[lastSlash+1:]

		if hs.Blocked {
			continue
		}

		codeSet, ok := hs.Codes[2]
		if !ok || codeSet == nil || codeSet.NodeHosts == nil {
			continue
		}

		for nodeName, nodeEntry := range codeSet.Nodes {
			if nodeEntry != 0 && nodeEntry <= now {
				continue
			}
			h, ok2 := codeSet.NodeHosts[nodeName]
			if !ok2 || h == "" {
				continue
			}
			if _, ok := result[wildcardTarget]; !ok {
				result[wildcardTarget] = make(map[string]string)
			}
			result[wildcardTarget][nodeName] = h
		}
	}

	return result, nil
}

// 移除节点数据
func (s *Store) RemoveNodesData(group, config string, nodes []string) error {
	if len(nodes) == 0 {
		return nil
	}

	removeNodesFromQueue(group, config, nodes)

	nodeSet := make(map[string]struct{}, len(nodes))
	for _, n := range nodes {
		nodeSet[n] = struct{}{}
	}

	var firstErr error

	// 清理 stats
	statsPrefix := FormatDBKey(KeyTypeStats, config, group)
	statsResults, err := s.DBViewPrefixScan(statsPrefix, -1, false)
	if err != nil {
		return err
	}
	var statsToDelete []string
	for path := range statsResults {
		if strings.Count(path, "/") < 5 {
			continue
		}
		node := path[strings.LastIndexByte(path, '/')+1:]
		if _, ok := nodeSet[node]; ok {
			statsToDelete = append(statsToDelete, path)
		}
	}
	if len(statsToDelete) > 0 {
		if delErr := s.DBBatchDeletePrefix(statsToDelete, true); delErr != nil && firstErr == nil {
			firstErr = delErr
		}
	}

	// 清理 prefetch
	prefetchPrefix := FormatDBKey(KeyTypePrefetch, config, group)
	prefetchResults, err := s.DBViewPrefixScan(prefetchPrefix, -1, false)
	if err != nil {
		if firstErr == nil {
			firstErr = err
		}
		return firstErr
	}
	var prefetchToDelete []string
	for path, data := range prefetchResults {
		var pm PrefetchMap
		if err := json.Unmarshal(data, &pm); err != nil {
			prefetchToDelete = append(prefetchToDelete, path)
			continue
		}

		changed := false
		newTCPNodes := make([]string, 0, len(pm.TCP.Nodes))
		newTCPWeights := make([]float64, 0, len(pm.TCP.Weights))
		for i, node := range pm.TCP.Nodes {
			if _, toRemove := nodeSet[node]; !toRemove {
				newTCPNodes = append(newTCPNodes, node)
				if i < len(pm.TCP.Weights) {
					newTCPWeights = append(newTCPWeights, pm.TCP.Weights[i])
				}
			} else {
				changed = true
			}
		}
		pm.TCP.Nodes = newTCPNodes
		pm.TCP.Weights = newTCPWeights

		newUDPNodes := make([]string, 0, len(pm.UDP.Nodes))
		newUDPWeights := make([]float64, 0, len(pm.UDP.Weights))
		for i, node := range pm.UDP.Nodes {
			if _, toRemove := nodeSet[node]; !toRemove {
				newUDPNodes = append(newUDPNodes, node)
				if i < len(pm.UDP.Weights) {
					newUDPWeights = append(newUDPWeights, pm.UDP.Weights[i])
				}
			} else {
				changed = true
			}
		}
		pm.UDP.Nodes = newUDPNodes
		pm.UDP.Weights = newUDPWeights

		if changed {
			if len(pm.TCP.Nodes) == 0 && len(pm.UDP.Nodes) == 0 && pm.RefTCP == "" && pm.RefUDP == "" {
				prefetchToDelete = append(prefetchToDelete, path)
			} else {
				newData, merr := json.Marshal(pm)
				if merr != nil {
					if firstErr == nil {
						firstErr = merr
					}
					continue
				}
				if perr := s.DBBatchPutItem(path, newData); perr != nil && firstErr == nil {
					firstErr = perr
				}
			}
		}
	}
	if len(prefetchToDelete) > 0 {
		if delErr := s.DBBatchDeletePrefix(prefetchToDelete, true); delErr != nil && firstErr == nil {
			firstErr = delErr
		}
	}

	// 清理 ranking
	rankingPrefix := FormatDBKey(KeyTypeRanking, config, group)
	rankingResults, err := s.DBViewPrefixScan(rankingPrefix, -1, true)
	if err != nil {
		if firstErr == nil {
			firstErr = err
		}
		return firstErr
	}
	var rankingToDelete []string
	for path, data := range rankingResults {
		var wrapper NodeRank
		if err := json.Unmarshal(data, &wrapper); err != nil {
			rankingToDelete = append(rankingToDelete, path)
			continue
		}

		changed := false
		newResult := make([]NodeRankItem, 0, len(wrapper.Result))
		for _, rank := range wrapper.Result {
			if _, toRemove := nodeSet[rank.Name]; toRemove {
				changed = true
			} else {
				newResult = append(newResult, rank)
			}
		}

		if changed {
			if len(newResult) == 0 {
				rankingToDelete = append(rankingToDelete, path)
			} else {
				newWrapper := NodeRank{LastUpdated: wrapper.LastUpdated, Result: newResult}
				newData, merr := json.Marshal(newWrapper)
				if merr != nil {
					if firstErr == nil {
						firstErr = merr
					}
					continue
				}
				if perr := s.DBBatchPutItem(path, newData); perr != nil && firstErr == nil {
					firstErr = perr
				}
			}
		}
	}
	if len(rankingToDelete) > 0 {
		if delErr := s.DBBatchDeletePrefix(rankingToDelete, true); delErr != nil && firstErr == nil {
			firstErr = delErr
		}
	}

	var failuresToDelete []string
	failuresPrefix := FormatDBKey(KeyTypeHostFailures, config, group)
	failuresResults, err := s.DBViewPrefixScan(failuresPrefix, -1, false)
	if err != nil {
		if firstErr == nil {
			firstErr = err
		}
	} else {
		for path, data := range failuresResults {
			var hs HostStatus
			if err := json.Unmarshal(data, &hs); err != nil {
				failuresToDelete = append(failuresToDelete, path)
				continue
			}
			if hs.Codes == nil {
				continue
			}
			changed := false
			for code, codeSet := range hs.Codes {
				if codeSet == nil {
					continue
				}
				for nodeName := range codeSet.Nodes {
					if _, toRemove := nodeSet[nodeName]; toRemove {
						delete(codeSet.Nodes, nodeName)
						if codeSet.NodeHosts != nil {
							delete(codeSet.NodeHosts, nodeName)
						}
						changed = true
					}
				}
				if codeSet.FailCounts != nil {
					for nodeName := range codeSet.FailCounts {
						if _, toRemove := nodeSet[nodeName]; toRemove {
							delete(codeSet.FailCounts, nodeName)
							changed = true
						}
					}
				}
				if len(codeSet.Nodes) == 0 && len(codeSet.FailCounts) == 0 {
					delete(hs.Codes, code)
				}
			}
			if changed {
				if lastSlash := strings.LastIndexByte(path, '/'); lastSlash >= 0 {
					wTarget := path[lastSlash+1:]
					cachePath := FormatDBKey(KeyTypeHostFailures, config, group, wTarget)
					hostStatusCache.Delete(cachePath)
				}
				if len(hs.Codes) == 0 {
					failuresToDelete = append(failuresToDelete, path)
				} else {
					newData, merr := json.Marshal(hs)
					if merr != nil {
						if firstErr == nil {
							firstErr = merr
						}
						continue
					}
					if perr := s.DBBatchPutItem(path, newData); perr != nil && firstErr == nil {
						firstErr = perr
					}
				}
			}
		}
	}
	if len(failuresToDelete) > 0 {
		if delErr := s.DBBatchDeletePrefix(failuresToDelete, true); delErr != nil && firstErr == nil {
			firstErr = delErr
		}
	}

	// 删除节点状态
	nodeStateKeys := make([]string, len(nodes))
	for i, nodeName := range nodes {
		nodeStateKeys[i] = FormatDBKey(KeyTypeNode, config, group, nodeName)
	}
	if len(nodeStateKeys) > 0 {
		s.DBBatchDeletePrefix(nodeStateKeys, true)
	}

	return firstErr
}

// 清理旧的记录
func (s *Store) CleanupOldRecords(group, config string) {
	keyTypes := []string{KeyTypeStats, KeyTypePrefetch, KeyTypeHostFailures}

	globalCacheParams.mutex.RLock()
	maxTargets := globalCacheParams.MaxTargets
	globalCacheParams.mutex.RUnlock()

	for _, keyType := range keyTypes {
		pathPrefix := FormatDBKey(keyType, config, group)

		rawData, err := s.DBViewPrefixScan(pathPrefix, -1, false)
		if err != nil {
			continue
		}

		type targetInfo struct {
			time    time.Time
			value   float64
			target  string
		}
		targetMap := make(map[string]*targetInfo)
		var toDelete []string

		for path, data := range rawData {
			parts := strings.Split(path, "/")
			if len(parts) < 5 {
				continue
			}
			target := parts[4]

			// 优先保留使用频率高和最近使用的记录
			var lastTime int64
			var value float64
			switch keyType {
			case KeyTypeStats:
				if len(parts) < 6 {
					continue
				}
				var record StatsRecord
				if err := json.Unmarshal(data, &record); err != nil {
					toDelete = append(toDelete, path)
					continue
				}
				lastTime = record.LastUsed
				value = float64(record.Success + record.Failure)
			case KeyTypePrefetch:
				var pm PrefetchMap
				if err := json.Unmarshal(data, &pm); err != nil {
					toDelete = append(toDelete, path)
					continue
				}
				lastTime = pm.UpdatedTime
				value = float64(len(pm.TCP.Nodes) + len(pm.UDP.Nodes))
			case KeyTypeHostFailures:
				var stats HostStatus
				if err := json.Unmarshal(data, &stats); err != nil {
					toDelete = append(toDelete, path)
					continue
				}
				if stats.LastFailure > lastTime {
					lastTime = stats.LastFailure
				}
				totalNodes := 0
				for _, codeSet := range stats.Codes {
					if codeSet != nil {
						totalNodes += len(codeSet.Nodes)
					}
				}
				value = float64(totalNodes)
			default:
				continue
			}

			targetMap[path] = &targetInfo{
				time:    time.Unix(lastTime, 0),
				value:   value,
				target:  target,
			}
		}

		var invalidTargets []string
		var validTargets []string
		for path, info := range targetMap {
			if info.time.Unix() <= 0 {
				invalidTargets = append(invalidTargets, path)
			} else {
				validTargets = append(validTargets, path)
			}
		}

		totalRecords := len(targetMap)
		toDeleteCount := totalRecords - maxTargets
		if toDeleteCount < 0 {
			toDeleteCount = 0
		}
		deleted := 0

		sort.Slice(validTargets, func(i, j int) bool {
			infoI := targetMap[validTargets[i]]
			infoJ := targetMap[validTargets[j]]
			if infoI.value != infoJ.value {
				return infoI.value < infoJ.value
			}
			return infoI.time.Before(infoJ.time)
		})

		for i := 0; i < len(validTargets); i++ {
			path := validTargets[i]
			info := targetMap[path]
			shouldDeleteByCount := deleted < toDeleteCount && totalRecords > maxTargets*2
			if shouldDeleteByCount || time.Since(info.time) > RecordExpiredTime {
				toDelete = append(toDelete, path)
				deleted++
			}
		}

		for _, path := range invalidTargets {
			toDelete = append(toDelete, path)
			deleted++
		}
		if len(toDelete) > 0 {
			if delErr := s.DBBatchDeletePrefix(toDelete, true); delErr != nil {
				log.Debugln("[SmartStore] Failed to batch clean records for keyType [%s], group [%s]: %v", keyType, group, delErr)
				deleted = 0
			}
		}

		if deleted > 0 {
			if keyType == KeyTypeStats {
				recordCache.RemoveByKeyPrefix(pathPrefix)
			} else if keyType == KeyTypePrefetch {
				dbResultCache.RemoveByKeyPrefix(pathPrefix)
			} else if keyType == KeyTypeHostFailures {
				hostStatusCache.RemoveByKeyPrefix(pathPrefix)
			}
			log.Debugln("[SmartStore] Cleaned up [%d] old [%s] records, group [%s] keeping [%d] valuable and recent data...",
				deleted, keyType, group, totalRecords - deleted)
		}
	}

	return
}