package smart

import (
	"container/heap"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
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

type AtomicStatsRecord struct {
	success         atomic.Int64
	failure         atomic.Int64
	connectTime     atomic.Int64
	latency         atomic.Int64
	lastUsed        atomic.Int64
	status          atomic.Int64

	uploadTotal     atomic.Float64
	downloadTotal   atomic.Float64
	duration        atomic.Float64
	maxUploadRate   atomic.Float64
	maxDownloadRate atomic.Float64

	weights *lru.LruCache[string, float64]
}

type ActiveTarget struct {
	Target   string
	ASN      string
	IsUDP    bool
	LastUsed int64
}

type NodeRank struct {
	Name   string
	Rank   string
	Weight int
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

	h := fnv.New32a()
	h.Write([]byte(target))
	h.Write([]byte(group))
	h.Write([]byte(proxy))
	hash := h.Sum32()

	return shardedLocks[hash&1023]
}

// 获取或创建原子记录
func (s *Store) GetOrCreateAtomicRecord(cacheKey string, group, config, target, proxy string) *AtomicStatsRecord {
	if value, ok := recordCache.Get(cacheKey); ok {
		return value
	}

	record := &AtomicStatsRecord{
		uploadTotal:     atomic.NewFloat64(0),
		downloadTotal:   atomic.NewFloat64(0),
		duration:        atomic.NewFloat64(0),
		maxUploadRate:   atomic.NewFloat64(0),
		maxDownloadRate: atomic.NewFloat64(0),
		weights:         lru.New[string, float64](lru.WithSize[string, float64](100)),
	}
	record.lastUsed.Store(time.Now().Unix())

	if existingData, err := s.GetStatsForTarget(group, config, target); err == nil {
		if data, exists := existingData[proxy]; exists {
			var existingRecord StatsRecord
			if json.Unmarshal(data, &existingRecord) == nil {
				record.success.Store(existingRecord.Success)
				record.failure.Store(existingRecord.Failure)
				record.connectTime.Store(existingRecord.ConnectTime)
				record.latency.Store(existingRecord.Latency)
				record.lastUsed.Store(existingRecord.LastUsed)
				if existingRecord.Weights != nil {
					for k, v := range existingRecord.Weights {
						record.weights.Set(k, v)
					}
				}
				record.uploadTotal.Store(existingRecord.UploadTotal)
				record.downloadTotal.Store(existingRecord.DownloadTotal)
				record.duration.Store(existingRecord.ConnectionDuration)
				record.maxUploadRate.Store(existingRecord.MaxUploadRate)
				record.maxDownloadRate.Store(existingRecord.MaxDownloadRate)
				record.status.Store(existingRecord.Status)
			}
		}
	}

	recordCache.Set(cacheKey, record)
	return record
}

// 创建统计快照
func (record *AtomicStatsRecord) CreateStatsSnapshot() *StatsRecord {
	if record == nil {
		return &StatsRecord{Weights: make(map[string]float64)}
	}

	weights := record.weights.FilterByKeyPrefix("")

	return &StatsRecord{
		Success:            record.success.Load(),
		Failure:            record.failure.Load(),
		ConnectTime:        record.connectTime.Load(),
		Latency:            record.latency.Load(),
		LastUsed:           record.lastUsed.Load(),
		Weights:            weights,
		UploadTotal:        record.uploadTotal.Load(),
		DownloadTotal:      record.downloadTotal.Load(),
		MaxUploadRate:      record.maxUploadRate.Load(),
		MaxDownloadRate:    record.maxDownloadRate.Load(),
		ConnectionDuration: record.duration.Load(),
		Status:             record.status.Load(),
	}
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
	case "weights":
    	return r.weights.FilterByKeyPrefix("")
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
	case "status":
		return r.status.Load()
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
	case "status":
		if v, ok := value.(int64); ok {
			r.status.Store(v)
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
	case "weights":
		if v, ok := value.(map[string]float64); ok {
			for k, val := range v {
				r.weights.Set(k, val)
			}
		}
	}
}

func (r *AtomicStatsRecord) Add(field string, value interface{}) {
	switch field {
	case "success":
		if v, ok := value.(int64); ok {
			r.success.Add(v)
		}
	case "failure":
		if v, ok := value.(int64); ok {
			r.failure.Add(v)
		}
	case "uploadTotal":
		if v, ok := value.(float64); ok {
			r.uploadTotal.Add(v)
		}
	case "downloadTotal":
		if v, ok := value.(float64); ok {
			r.downloadTotal.Add(v)
		}
	}
}

func (r *AtomicStatsRecord) GetWeight(weightType string) float64 {
	if value, ok := r.weights.Get(weightType); ok {
		return value
	}
	return 0
}

func (r *AtomicStatsRecord) SetWeight(weightType string, value float64) {
	r.weights.Set(weightType, value)
}

// 获取节点权重排名缓存
func (s *Store) GetNodeWeightRankingCache(group, config string) ([]NodeRank, error) {
	ops := getGlobalQueueSnapshot()
	for _, op := range ops {
		if op.Type == OpSaveRanking && op.Group == group && op.Config == config {
			var ranking []NodeRank
			if err := json.Unmarshal(op.Data, &ranking); err == nil && len(ranking) > 0 {
				return ranking, nil
			}
		}
	}

	pathPrefix := FormatDBKey("smart", KeyTypeRanking, config, group, "")
	rawResult, err := s.GetSubBytesByPath(pathPrefix)
	if err != nil {
		return nil, err
	}

	for _, data := range rawResult {
		var ranking []NodeRank
		if err := json.Unmarshal(data, &ranking); err == nil && len(ranking) > 0 {
			return ranking, nil
		}
	}

	return []NodeRank{}, nil
}

// 获取节点权重排名
func (s *Store) GetNodeWeightRanking(group, config, testUrl string, proxies []C.Proxy) ([]NodeRank, error) {
	var result []NodeRank
	if len(proxies) == 0 {
		return result, fmt.Errorf("no proxies provided")
	}

	allNodes := make(map[string]bool, len(proxies))
	aliveNodes := make(map[string]bool, len(proxies))
	for _, p := range proxies {
		if p.AliveForTestUrl(testUrl) {
			aliveNodes[p.Name()] = true
		}
		allNodes[p.Name()] = true
	}

	globalCacheParams.mutex.RLock()
	prefetchLimit := globalCacheParams.PrefetchLimit
	globalCacheParams.mutex.RUnlock()

	activeTargets := s.GetActiveTargets(group, config, prefetchLimit)

	nodeCounts := make(map[string]int)
	for _, ad := range activeTargets {
		nodes, _ := s.GetPrefetchResult(group, config, ad.Target, ad.ASN, ad.IsUDP)
		if len(nodes) > 0 {
			node := nodes[0]
			if allNodes[node] {
				nodeCounts[node]++
			}
		}
	}

	for node := range allNodes {
		weight := nodeCounts[node]
		result = append(result, NodeRank{Name: node, Weight: weight, Rank: ""})
	}

	sort.Slice(result, func(i, j int) bool {
		ai := aliveNodes[result[i].Name]
		aj := aliveNodes[result[j].Name]
		if ai != aj {
			return ai
		}
		return result[i].Weight > result[j].Weight
	})

	if len(result) > 0 {
		aliveCount := 0
		for _, r := range result {
			if aliveNodes[r.Name] {
				aliveCount++
			}
		}
		if aliveCount > 0 {
			result[0].Rank = RankMostUsed
			if aliveCount == 2 {
				if result[1].Weight > 0 {
					result[1].Rank = RankOccasional
				} else {
					result[1].Rank = RankRarelyUsed
				}
			} else if aliveCount >= 3 {
				mostUsedBound := int(float64(aliveCount) * 0.2)
				if mostUsedBound < 1 {
					mostUsedBound = 1
				}
				occasionalBound := mostUsedBound + int(float64(aliveCount)*0.5)
				for i := 1; i < mostUsedBound && i < aliveCount; i++ {
					if result[i].Weight > 0 {
						result[i].Rank = RankMostUsed
					} else {
						result[i].Rank = RankRarelyUsed
					}
				}
				for i := mostUsedBound; i < occasionalBound && i < aliveCount; i++ {
					if result[i].Weight > 0 {
						result[i].Rank = RankOccasional
					} else {
						result[i].Rank = RankRarelyUsed
					}
				}
				for i := occasionalBound; i < aliveCount; i++ {
					result[i].Rank = RankRarelyUsed
				}
			}
			for i := 0; i < aliveCount; i++ {
				if result[i].Rank == "" {
					result[i].Rank = RankRarelyUsed
				}
			}
		}
		for i := aliveCount; i < len(result); i++ {
			result[i].Rank = RankRarelyUsed
		}
	}

	s.StoreNodeWeightRanking(group, config, result)
	return result, nil
}

// 存储节点权重排名
func (s *Store) StoreNodeWeightRanking(group, config string, ranking []NodeRank) error {
	data, err := json.Marshal(ranking)
	if err != nil {
		return fmt.Errorf("failed to serialize ranking data: %w", err)
	}

	appendToGlobalQueue(StoreOperation{
		Type:   OpSaveRanking,
		Group:  group,
		Config: config,
		Data:   data,
	})

	needFlush := len(getGlobalQueueSnapshot()) >= GetBatchSaveThreshold()

	if needFlush {
		go s.FlushQueue(true)
	}

	return nil
}

// 获取目标的最佳代理
func (s *Store) GetBestProxyForTarget(group, config, target, asnNumber string, isUDP bool) ([]string, []float64, error) {
	if target == "" {
		return nil, nil, errors.New("empty target")
	}

	now := time.Now().Unix()
	minDecay := math.Max(0.1, 0.4)
	decayCache := make(map[int64]float64, 72)

	getTimeDecay := func(lastUsedTime int64) float64 {
		return GetTimeDecayWithCache(lastUsedTime, now, minDecay, decayCache)
	}

	allStatsMap, err := s.GetAllStats(group, config)
	if err != nil {
		return nil, nil, err
	}

	weightType := WeightTypeTCP
	if isUDP {
		weightType = WeightTypeUDP
	}

	stateData, _ := s.GetNodeStates(group, config)
	for _, data := range stateData {
		var state NodeState
		if err := json.Unmarshal(data, &state); err == nil {
			if state.BlockedUntil > 0 && state.BlockedUntil > now {
				continue
			}
		}
	}

	nodesWithWeight := make(map[string]float64)

	// 优先使用 ASN，取 75% 分位数权重排序并使用target结果进行修正
	if asnNumber != "" {
		asnWeightType := WeightTypeTCPASN + ":" + asnNumber
		if isUDP {
			asnWeightType = WeightTypeUDPASN + ":" + asnNumber
		}

		nodeWeights := make(map[string][]float64)

		for mapTarget, mapStats := range allStatsMap {
			for nodeName, data := range mapStats {
				var record StatsRecord
				if json.Unmarshal(data, &record) != nil {
					continue
				}
				if record.Weights != nil {
					if mapTarget == target {
						for k, weight := range record.Weights {
							if !isUDP && strings.HasPrefix(k, WeightTypeTCPASN) && weight > 0 {
								timeDecay := getTimeDecay(record.LastUsed)
								decayedWeight := weight * timeDecay
								nodeWeights[nodeName] = append(nodeWeights[nodeName], decayedWeight)
							}
							if isUDP && strings.HasPrefix(k, WeightTypeUDPASN) && weight > 0 {
								timeDecay := getTimeDecay(record.LastUsed)
								decayedWeight := weight * timeDecay
								nodeWeights[nodeName] = append(nodeWeights[nodeName], decayedWeight)
							}
						}
					} else {
						if weight, ok := record.Weights[asnWeightType]; ok && weight > 0 {
							timeDecay := getTimeDecay(record.LastUsed)
							decayedWeight := weight * timeDecay
							nodeWeights[nodeName] = append(nodeWeights[nodeName], decayedWeight)
						}
					}
				}
			}
		}

		// 75%分位数
		calculatePercentile75 := func(weights []float64) float64 {
			if len(weights) == 0 {
				return 0.0
			}
			sorted := make([]float64, len(weights))
			copy(sorted, weights)
			sort.Float64s(sorted)
			n := len(sorted)
			index := int(float64(n-1) * 0.75)
			return sorted[index]
		}

		for nodeName, weights := range nodeWeights {
			if len(weights) >= DefaultMinSampleCount {
				hasLowWeight := false
				minWeight := math.MaxFloat64
				for _, w := range weights {
					if w < AllowedWeight {
						hasLowWeight = true
					}
					if w < minWeight {
						minWeight = w
					}
				}
				var finalWeight float64
				if hasLowWeight {
					finalWeight = minWeight
				} else {
					finalWeight = calculatePercentile75(weights)
				}
				nodesWithWeight[nodeName] = finalWeight
			}
		}
	}

	var mapStats map[string][]byte
	if stats, ok := allStatsMap[target]; ok {
		mapStats = stats
	} else {
		if stats, err := s.GetStatsForTarget(group, config, target); err == nil {
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
			decayedWeight := weight * timeDecay
			if _, exists := nodesWithWeight[nodeName]; !exists {
				if asnNumber == "" || CdnASNs[asnNumber] {
					nodesWithWeight[nodeName] = decayedWeight
				}
			} else {
				if decayedWeight < AllowedWeight && nodesWithWeight[nodeName] > AllowedWeight {
					nodesWithWeight[nodeName] = decayedWeight
				}
			}
		}
	}

	var nodeList []NodeWithWeight
	for node, weight := range nodesWithWeight {
		nodeList = append(nodeList, NodeWithWeight{node, weight})
	}

	if len(nodeList) == 0 {
		return nil, nil, errors.New("no best node with enough weight")
	}

	sort.Slice(nodeList, func(i, j int) bool {
		if nodeList[i].Weight != nodeList[j].Weight {
			return nodeList[i].Weight > nodeList[j].Weight
		}
		return nodeList[i].Node < nodeList[j].Node
	})

	var bestNodes []string
	var bestWeights []float64
	for i := 0; i < len(nodeList); i++ {
		bestNodes = append(bestNodes, nodeList[i].Node)
		bestWeights = append(bestWeights, nodeList[i].Weight)
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

	// key: "target:asn:is_udp"
	seen := make(map[string]int64)

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
					parts := strings.Split(key, ":")
					if len(parts) >= 2 {
						asn := parts[1]
						combKey := asn + ":false"
						if last, exists := activeCombinations[combKey]; !exists || record.LastUsed > last {
							activeCombinations[combKey] = record.LastUsed
						}
					}
				} else if strings.HasPrefix(key, WeightTypeUDPASN) && weight > 0 {
					parts := strings.Split(key, ":")
					if len(parts) >= 2 {
						asn := parts[1]
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
			parts := strings.Split(combKey, ":")
			if len(parts) >= 2 && parts[0] != "" {
				hasASN = true
				break
			}
		}

		for combKey, lastUsed := range activeCombinations {
			parts := strings.Split(combKey, ":")
			asn := ""
			isUDP := false
			if len(parts) >= 2 {
				asn = parts[0]
				if parts[1] == "true" {
					isUDP = true
				}
			}

			if asn == "" && hasASN {
				continue
			}

			recordKey := fmt.Sprintf("%s:%s:%t", target, asn, isUDP)
			if existingLast, exists := seen[recordKey]; !exists || lastUsed > existingLast {
				seen[recordKey] = lastUsed
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

	result := make([]ActiveTarget, 0, h.Len())
	var sorted []ActiveTarget
	for h.Len() > 0 {
		sorted = append(sorted, heap.Pop(h).(ActiveTarget))
	}

	for i := len(sorted) - 1; i >= 0; i-- {
		result = append(result, sorted[i])
	}

	return result
}

// RunPrefetch 最佳节点预计算
func (s *Store) RunPrefetch(group, config string, proxyMap map[string]string) int {
	log.Debugln("[SmartStore] Executing target and ASN pre-calculation for policy group [%s]", group)

	blockedNodes := make(map[string]bool)
	stateData, _ := s.GetNodeStates(group, config)
	for nodeName, data := range stateData {
		var state NodeState
		if json.Unmarshal(data, &state) == nil {
			if state.BlockedUntil > 0 && state.BlockedUntil > time.Now().Unix() {
				blockedNodes[nodeName] = true
			}
		}
	}

	availableProxyMap := make(map[string]string)
	for name, value := range proxyMap {
		if !blockedNodes[name] {
			availableProxyMap[name] = value
		}
	}

	if len(availableProxyMap) == 0 {
		log.Debugln("[SmartStore] No available nodes for prefetch calculation in group [%s]", group)
		return 0
	}

	globalCacheParams.mutex.RLock()
	prefetchLimit := globalCacheParams.PrefetchLimit
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

	var items []prefetchItem

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
			if _, exists := availableProxyMap[bestNodes[i]]; exists {
				nodes = append(nodes, bestNodes[i])
				weights = append(weights, bestWeights[i])
			}
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
				finalNodeMap := make(map[string]float64)
				for i, node := range oldNodes {
					finalNodeMap[node] = oldWeights[i]
				}

				needUpdate = false

				for i, newNode := range item.bestNodes {
					newW := item.bestWeights[i]
					if oldW, exists := finalNodeMap[newNode]; exists {
						// prevent degrade recovery too fast
						if math.Abs(newW - oldW) / oldW > 0.1 && (oldW > AllowedWeight || newW < AllowedWeight) {
							finalNodeMap[newNode] = newW
							needUpdate = true
						}
					} else {
						finalNodeMap[newNode] = newW
						needUpdate = true
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
			s.StorePrefetchResult(group, config, item.target, item.asnNumber, item.isUDP, sortedNodes, sortedWeights, len(oldNodes))
		}

		nodeWeightPairs := make([]string, len(sortedNodes))
		for i := range sortedNodes {
			nodeWeightPairs[i] = fmt.Sprintf("%s: %.2f", sortedNodes[i], sortedWeights[i])
		}

		oldNodeWeightPairs := make([]string, len(oldNodes))
		for i := range oldNodes {
			oldNodeWeightPairs[i] = fmt.Sprintf("%s: %.2f", oldNodes[i], oldWeights[i])
		}

		prefetchCount++
		if len(oldNodes) == 0 {
			log.Debugln("[SmartStore] Prefetching for group [%s]: network: [%s] => target: [%s] => result: [%s] (no old result)",
				group, networkType, target, strings.Join(nodeWeightPairs, ", "))
		} else if cacheHit {
			log.Debugln("[SmartStore] Prefetching for group [%s]: network: [%s] => target: [%s] => result: [%s] (from cache)",
				group, networkType, target, strings.Join(nodeWeightPairs, ", "))
		} else if needUpdate {
			log.Debugln("[SmartStore] Prefetching for group [%s]: network: [%s] => target: [%s] => result: [%s] (updated from old: [%s])",
				group, networkType, target, strings.Join(nodeWeightPairs, ", "), strings.Join(oldNodeWeightPairs, ", "))
		}
	}

	log.Infoln("[SmartStore] Prefetch completed for group [%s]: pre-calculated [%d] targets",
		group, prefetchCount)
	return prefetchCount
}

// GetNodeStates 获取节点状态
func (s *Store) GetNodeStates(group, config string) (map[string][]byte, error) {
	pathPrefix := FormatDBKey("smart", KeyTypeNode, config, group, "")
	result := make(map[string][]byte)

	rawResult, err := s.GetSubBytesByPath(pathPrefix)
	if err != nil {
		return nil, err
	}

	for fullPath, data := range rawResult {
		parts := strings.Split(fullPath, "/")
		if len(parts) > 0 {
			nodeName := parts[len(parts)-1]
			result[nodeName] = data
		}
	}

	ops := getGlobalQueueSnapshot()
	for _, op := range ops {
		if op.Type == OpSaveNodeState && op.Group == group && op.Config == config {
			result[op.Node] = op.Data
		}
	}

	return result, nil
}

// 获取域名的统计数据
func (s *Store) GetStatsForTarget(group, config, target string) (map[string][]byte, error) {
	result := make(map[string][]byte)

	ops := getGlobalQueueSnapshot()
	for _, op := range ops {
		if op.Type == OpSaveStats && op.Group == group && op.Config == config && op.Target == target {
			result[op.Node] = op.Data
		}
	}

	pathPrefix := FormatDBKey("smart", KeyTypeStats, config, group, target, "")
	rawResult, err := s.GetSubBytesByPath(pathPrefix)
	if err != nil {
		return nil, err
	}

	for fullPath, data := range rawResult {
		parts := strings.Split(fullPath, "/")
		if len(parts) > 0 {
			nodeName := parts[len(parts)-1]
			result[nodeName] = data
		}
	}

	return result, nil
}

// 获取所有统计数据
func (s *Store) GetAllStats(group, config string) (map[string]map[string][]byte, error) {
	pathPrefix := FormatDBKey("smart", KeyTypeStats, config, group, "")

	result := make(map[string]map[string][]byte)

	rawResult, err := s.GetSubBytesByPath(pathPrefix)
	if err != nil {
		return nil, err
	}

	for fullPath, data := range rawResult {
		parts := strings.Split(fullPath, "/")
		if len(parts) < 6 {
			continue
		}
		target := parts[len(parts)-2]
		node := parts[len(parts)-1]

		if _, ok := result[target]; !ok {
			result[target] = make(map[string][]byte)
		}
		result[target][node] = data
	}

	ops := getGlobalQueueSnapshot()
	for _, op := range ops {
		if op.Type == OpSaveStats && op.Group == group && op.Config == config {
			target := op.Target
			nodeName := op.Node
			if _, exists := result[target]; !exists {
				result[target] = make(map[string][]byte)
			}
			result[target][nodeName] = op.Data
		}
	}

	return result, nil
}

// 删除域名记录
func (s *Store) DeleteTargetRecords(group, config, target string) {
	key := FormatDBKey("smart", KeyTypeStats, config, group, target, "")
	if err := s.DeleteByPath(key); err != nil {
		return
	}

	statsCachePrefix := FormatCacheKey(KeyTypeStats, config, group, target)
	RemoveCacheValuesByPrefix(statsCachePrefix)
}

// 获取缓存中的所有组名
func (s *Store) GetAllGroupsForConfig(config string) ([]string, error) {
	groupsMap := make(map[string]bool)

	statsPath := FormatDBKey("smart", KeyTypeStats, config)
	raw, err := s.GetSubBytesByPath(statsPath)
	if err == nil {
		for fullPath := range raw {
			parts := strings.Split(fullPath, "/")
			if len(parts) >= 4 {
				group := parts[3]
				if group != "" {
					groupsMap[group] = true
				}
			}
		}
	} else {
		prefix := statsPath + "/"
		scanResults, err2 := s.DBViewPrefixScan(prefix, -1)
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

	ops := getGlobalQueueSnapshot()
	for _, op := range ops {
		if op.Config == config && op.Group != "" {
			groupsMap[op.Group] = true
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

	nodesPath := FormatDBKey("smart", KeyTypeNode, config, group, "")
	nodeStatesData, err := s.GetSubBytesByPath(nodesPath)
	if err == nil {
		for key := range nodeStatesData {
			parts := strings.Split(key, "/")
			if len(parts) > 0 {
				nodeName := parts[len(parts)-1]
				if nodeName != "" {
					nodesMap[nodeName] = true
				}
			}
		}
	}

	statsPath := FormatDBKey("smart", KeyTypeStats, config, group, "")
	statsData, err := s.GetSubBytesByPath(statsPath)
	if err == nil {
		for key := range statsData {
			parts := strings.Split(key, "/")
			if len(parts) >= 6 {
				nodeName := parts[len(parts)-1]
				if nodeName != "" {
					nodesMap[nodeName] = true
				}
			}
		}
	}

	ops := getGlobalQueueSnapshot()
	for _, op := range ops {
		if op.Group == group && op.Config == config {
			if op.Type == OpSaveNodeState || op.Type == OpSaveStats {
				if op.Node != "" {
					nodesMap[op.Node] = true
				}
			}
		}
	}

	result := make([]string, 0, len(nodesMap))
	for node := range nodesMap {
		result = append(result, node)
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

	targetNodePairs := make(map[string][]string)
	var firstErr error

	// 清理 stats
	statsPrefix := FormatDBKey("smart", KeyTypeStats, config, group, "")
	statsResults, err := s.DBViewPrefixScan(statsPrefix, -1)
	if err != nil {
		return err
	}
	for path := range statsResults {
		parts := strings.Split(path, "/")
		if len(parts) >= 6 {
			target := parts[len(parts)-2]
			node := parts[len(parts)-1]
			if _, ok := nodeSet[node]; ok {
				targetNodePairs[target] = append(targetNodePairs[target], node)
				s.DeleteCacheResult(KeyTypeStats, config, group, target, node)
			}
		}
	}

	// 清理 prefetch
	prefetchPrefix := FormatDBKey("smart", KeyTypePrefetch, config, group, "")
	prefetchResults, err := s.DBViewPrefixScan(prefetchPrefix, -1)
	if err != nil {
		if firstErr == nil {
			firstErr = err
		}
		return firstErr
	}
	for path := range prefetchResults {
		parts := strings.Split(path, "/")
		if len(parts) < 5 {
			continue
		}
		target := parts[4]
		if target == "" {
			continue
		}

		cacheKey := FormatCacheKey(KeyTypePrefetch, config, group, target)
		if value, found := GetCacheValue(cacheKey); found {
			if bv, ok := value.([]byte); ok {
				var pm PrefetchMap
				if json.Unmarshal(bv, &pm) == nil {
					changed := false
					if pm.RefTCP != "" {
						if _, refFound := GetCacheValue(pm.RefTCP); !refFound {
							pm.RefTCP = ""
							changed = true
						}
					}
					if pm.RefUDP != "" {
						if _, refFound := GetCacheValue(pm.RefUDP); !refFound {
							pm.RefUDP = ""
							changed = true
						}
					}
					if changed {
						if pm.RefTCP == "" && pm.RefUDP == "" && len(pm.TCP.Nodes) == 0 && len(pm.UDP.Nodes) == 0 {
							s.DeleteCacheResult(KeyTypePrefetch, config, group, target, "")
						} else {
							newData, merr := json.Marshal(pm)
							if merr != nil {
								if firstErr == nil {
									firstErr = merr
								}
								continue
							}
							SetCacheValue(cacheKey, newData)
							dbKey := FormatDBKey("smart", KeyTypePrefetch, config, group, target)
							if perr := s.DBBatchPutItem(dbKey, newData); perr != nil && firstErr == nil {
								firstErr = perr
							}
						}
					}
				}
			}
		}
	}

	// 清理 ranking
	rankingPrefix := FormatDBKey("smart", KeyTypeRanking, config, group, "")
	rankingResults, err := s.DBViewPrefixScan(rankingPrefix, -1)
	if err != nil {
		if firstErr == nil {
			firstErr = err
		}
		return firstErr
	}
	for path, data := range rankingResults {
		var ranking []NodeRank
		if err := json.Unmarshal(data, &ranking); err != nil {
			continue
		}

		changed := false
		newRanking := make([]NodeRank, 0, len(ranking))
		for _, rank := range ranking {
			toRemove := false
			for _, node := range nodes {
				if rank.Name == node {
					toRemove = true
					changed = true
					break
				}
			}
			if !toRemove {
				newRanking = append(newRanking, rank)
			}
		}

		dbKey := path
		cacheKey := FormatCacheKey(KeyTypeRanking, config, group, "")

		if changed {
			if len(newRanking) == 0 {
				s.DeleteCacheResult(KeyTypeRanking, config, group, "", "")
			} else {
				newData, merr := json.Marshal(newRanking)
				if merr != nil {
					if firstErr == nil {
						firstErr = merr
					}
					continue
				}
				if perr := s.DBBatchPutItem(dbKey, newData); perr != nil && firstErr == nil {
					firstErr = perr
				}
				SetCacheValue(cacheKey, newData)
			}
		}
	}

	// 删除节点状态
	for _, nodeName := range nodes {
		s.DeleteCacheResult(KeyTypeNode, config, group, nodeName, "")
	}

	return firstErr
}

// 清理旧的域名记录
func (s *Store) CleanupOldTargets(group, config string) error {
	statsPrefix := FormatDBKey("smart", KeyTypeStats, config, group, "")

	globalCacheParams.mutex.RLock()
	maxTargets := globalCacheParams.MaxTargets * 2
	globalCacheParams.mutex.RUnlock()

	statsData, err := s.DBViewPrefixScan(statsPrefix, -1)
	if err != nil {
		return err
	}

	targetLastUsed := make(map[string]time.Time)
	for path, data := range statsData {
		parts := strings.Split(path, "/")
		if len(parts) < 6 {
			continue
		}
		target := parts[len(parts)-2]
		var statsRecord StatsRecord
		if err := json.Unmarshal(data, &statsRecord); err != nil {
			continue
		}
		lastUsedTime := time.Unix(statsRecord.LastUsed, 0)
		if last, ok := targetLastUsed[target]; !ok || lastUsedTime.After(last) {
			targetLastUsed[target] = lastUsedTime
		}
	}

	type targetInfo struct {
		target   string
		lastUsed time.Time
	}
	var targetList []targetInfo
	for target, lastUsed := range targetLastUsed {
		targetList = append(targetList, targetInfo{target, lastUsed})
	}
	sort.Slice(targetList, func(i, j int) bool {
		return targetList[i].lastUsed.Before(targetList[j].lastUsed)
	})

	if len(targetList) <= maxTargets {
		return nil
	}
	toDelete := targetList[:len(targetList)-maxTargets]
	for _, info := range toDelete {
		s.DeleteTargetRecords(group, config, info.target)
		s.DeleteCacheResult(KeyTypePrefetch, config, group, info.target, "")
	}

	log.Debugln("[SmartStore] Cleaned up [%d] old target records, keeping the latest [%d] (group %s)",
		len(toDelete), maxTargets, group)
	return nil
}

// 清理过期统计数据
func (s *Store) CleanupExpiredStats(group, config string) error {
	statsPrefix := FormatDBKey("smart", KeyTypeStats, config, group, "")
	statsData, err := s.DBViewPrefixScan(statsPrefix, -1)
	if err != nil {
		return err
	}

	threshold := time.Now().Add(-RetentionPeriod)
	var expiredTargets []string
	targetLastUsed := make(map[string]time.Time)

	for path, data := range statsData {
		parts := strings.Split(path, "/")
		if len(parts) < 6 {
			continue
		}
		target := parts[len(parts)-2]
		var statsRecord StatsRecord
		if err := json.Unmarshal(data, &statsRecord); err != nil {
			continue
		}
		lastUsedTime := time.Unix(statsRecord.LastUsed, 0)
		if last, ok := targetLastUsed[target]; !ok || lastUsedTime.After(last) {
			targetLastUsed[target] = lastUsedTime
		}
	}

	for target, lastUsed := range targetLastUsed {
		if lastUsed.Before(threshold) {
			expiredTargets = append(expiredTargets, target)
			s.DeleteTargetRecords(group, config, target)
			s.DeleteCacheResult(KeyTypePrefetch, config, group, target, "")
		}
	}

	if len(expiredTargets) > 0 {
		log.Debugln("[SmartStore] Deleted [%d] expired targets for group [%s]", len(expiredTargets), group)
	}

	return nil
}