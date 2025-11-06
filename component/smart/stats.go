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
	degraded        atomic.Bool

	uploadTotal     *atomic.Float64
	downloadTotal   *atomic.Float64
	duration        *atomic.Float64
	maxUploadRate   *atomic.Float64
	maxDownloadRate *atomic.Float64

	weights         atomic.TypedValue[map[string]float64]
}

type ActiveDomain struct {
	Domain   string
	ASN      string
	IsUDP    bool
	LastUsed time.Time
}

type NodeRank struct {
	Name   string
	Rank   string
	Weight int
}

type domainMinHeap []ActiveDomain

// 域名节点锁
func initShardedLocks() {
	shardedLocksOnce.Do(func() {
		for i := range shardedLocks {
			shardedLocks[i] = &sync.RWMutex{}
		}
	})
}

func GetDomainNodeLock(domain, group, proxyName string) *sync.RWMutex {
	initShardedLocks()

	h := fnv.New32a()
	h.Write([]byte(domain))
	h.Write([]byte(group))
	h.Write([]byte(proxyName))
	hash := h.Sum32()

	return shardedLocks[hash&1023]
}

// 获取或创建原子记录
func (s *Store) GetOrCreateAtomicRecord(cacheKey string, groupName, configName, domain, proxyName string) *AtomicStatsRecord {
	if value, ok := recordCache.Get(cacheKey); ok {
		return value
	}

	record := &AtomicStatsRecord{
		uploadTotal:     new(atomic.Float64),
		downloadTotal:   new(atomic.Float64),
		duration:        new(atomic.Float64),
		maxUploadRate:   new(atomic.Float64),
		maxDownloadRate: new(atomic.Float64),
	}
	record.weights.Store(make(map[string]float64))
	record.lastUsed.Store(time.Now().Unix())
	record.status.Store(0)

	if existingData, err := s.GetStatsForDomain(groupName, configName, domain, proxyName); err == nil {
		if data, exists := existingData[proxyName]; exists {
			var existingRecord StatsRecord
			if json.Unmarshal(data, &existingRecord) == nil {
				record.success.Store(existingRecord.Success)
				record.failure.Store(existingRecord.Failure)
				record.connectTime.Store(existingRecord.ConnectTime)
				record.latency.Store(existingRecord.Latency)
				record.lastUsed.Store(existingRecord.LastUsed.Unix())
				record.weights.Store(atomic.CloneMap(existingRecord.Weights))
				record.uploadTotal.Store(existingRecord.UploadTotal)
				record.downloadTotal.Store(existingRecord.DownloadTotal)
				record.duration.Store(existingRecord.ConnectionDuration)
				record.maxUploadRate.Store(existingRecord.MaxUploadRate)
				record.maxDownloadRate.Store(existingRecord.MaxDownloadRate)
			}
		}
	}

	recordCache.Set(cacheKey, record)
	return record
}

// 创建统计快照
func (record *AtomicStatsRecord) CreateStatsSnapshot() *StatsRecord {
	if record == nil {
		return &StatsRecord{
			Weights: make(map[string]float64),
		}
	}

	return &StatsRecord{
		Success:            record.success.Load(),
		Failure:            record.failure.Load(),
		ConnectTime:        record.connectTime.Load(),
		Latency:            record.latency.Load(),
		LastUsed:           time.Unix(record.lastUsed.Load(), 0),
		Weights:            atomic.CloneMap(record.weights.Load()),
		UploadTotal:        record.uploadTotal.Load(),
		DownloadTotal:      record.downloadTotal.Load(),
		MaxUploadRate:      record.maxUploadRate.Load(),
		MaxDownloadRate:    record.maxDownloadRate.Load(),
		ConnectionDuration: record.duration.Load(),
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
		return atomic.CloneMap(r.weights.Load())
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
	case "degraded":
		return r.degraded.Load()
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
			r.weights.Store(atomic.CloneMap(v))
		}
	case "degraded":
		if v, ok := value.(bool); ok {
			r.degraded.Store(v)
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
	weights := r.weights.Load()
	if weights == nil {
		return 0
	}
	return weights[weightType]
}

func (r *AtomicStatsRecord) SetWeight(weightType string, value float64) {
	r.weights.Update(func(old map[string]float64) map[string]float64 {
		newMap := atomic.CloneMap(old)
		newMap[weightType] = value
		return newMap
	})
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

	activeDomains := s.GetActiveDomains(group, config, prefetchLimit)

	nodeCounts := make(map[string]int)
	for _, ad := range activeDomains {
		nodes, _ := s.GetPrefetchResult(group, config, ad.Domain, ad.ASN, ad.IsUDP)
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

	nodeStatesMap := make(map[string]NodeState)
	stateData, _ := s.GetNodeStates(group, config)
	for nodeName, data := range stateData {
		var state NodeState
		if err := json.Unmarshal(data, &state); err == nil {
			if !state.BlockedUntil.IsZero() && state.BlockedUntil.After(time.Now()) {
				continue
			}
			nodeStatesMap[nodeName] = state
		}
	}

	nodesWithWeight := make(map[string]float64)

	// 优先使用ASN，取最小权重排序并对cdnASNs进行修正
	if asnNumber != "" {
		asnWeightType := WeightTypeTCPASN + ":" + asnNumber
		if isUDP {
			asnWeightType = WeightTypeUDPASN + ":" + asnNumber
		}

		nodeWeights := make(map[string][]float64)

		for domainTarget, domainStats := range allStatsMap {
			for nodeName, data := range domainStats {
				var record StatsRecord
				if json.Unmarshal(data, &record) != nil {
					continue
				}
				if record.Weights != nil {
					if domainTarget == target {
						for k, weight := range record.Weights {
							if !isUDP && strings.HasPrefix(k, WeightTypeTCPASN) && weight > 0 {
								timeDecay := getTimeDecay(record.LastUsed.Unix())
								decayedWeight := weight * timeDecay
								nodeWeights[nodeName] = append(nodeWeights[nodeName], decayedWeight)
							}
							if isUDP && strings.HasPrefix(k, WeightTypeUDPASN) && weight > 0 {
								timeDecay := getTimeDecay(record.LastUsed.Unix())
								decayedWeight := weight * timeDecay
								nodeWeights[nodeName] = append(nodeWeights[nodeName], decayedWeight)
							}
						}
					} else {
						if weight, ok := record.Weights[asnWeightType]; ok && weight > 0 {
							timeDecay := getTimeDecay(record.LastUsed.Unix())
							decayedWeight := weight * timeDecay
							nodeWeights[nodeName] = append(nodeWeights[nodeName], decayedWeight)
						}
					}
				}
			}
		}

		for nodeName, weights := range nodeWeights {
			if len(weights) >= DefaultMinSampleCount {
				minWeight := math.MaxFloat64
				for _, w := range weights {
					if w < minWeight {
						minWeight = w
					}
				}
				nodesWithWeight[nodeName] = minWeight
			}
		}

		for nodeName, weight := range nodesWithWeight {
			if state, ok := nodeStatesMap[nodeName]; ok && state.Degraded {
				nodesWithWeight[nodeName] = weight * state.DegradedFactor
			}
		}
	}

	var domainStats map[string][]byte
	if stats, ok := allStatsMap[target]; ok {
		domainStats = stats
	}

	for nodeName, data := range domainStats {
		var record StatsRecord
		if json.Unmarshal(data, &record) != nil {
			continue
		}
		var weight float64
		if record.Weights != nil {
			weight = record.Weights[weightType]
		}
		if weight > 0 {
			timeDecay := getTimeDecay(record.LastUsed.Unix())
			decayedWeight := weight * timeDecay
			if state, exists := nodeStatesMap[nodeName]; exists && state.Degraded {
				decayedWeight *= state.DegradedFactor
			}
			if _, exists := nodesWithWeight[nodeName]; !exists {
				if asnNumber == "" || cdnASNs[asnNumber] {
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
func (h domainMinHeap) Len() int            { return len(h) }
func (h domainMinHeap) Less(i, j int) bool  { return h[i].LastUsed.Before(h[j].LastUsed) }
func (h domainMinHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *domainMinHeap) Push(x interface{}) { *h = append(*h, x.(ActiveDomain)) }
func (h *domainMinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (s *Store) GetActiveDomains(group, config string, limit int) []ActiveDomain {
	allStats, err := s.GetAllStats(group, config)
	if err != nil || len(allStats) == 0 {
		return nil
	}

	h := &domainMinHeap{}
	heap.Init(h)

	// key: "domain:asn:is_udp"
	seen := make(map[string]time.Time)

	for domain, nodeStats := range allStats {
		var maxLastUsed time.Time
		// key: "asn:is_udp", value: lastUsed
		activeCombinations := make(map[string]time.Time)

		for _, data := range nodeStats {
			var record StatsRecord
			if json.Unmarshal(data, &record) != nil {
				continue
			}
			if maxLastUsed.IsZero() || record.LastUsed.After(maxLastUsed) {
				maxLastUsed = record.LastUsed
			}
			if record.Weights == nil {
				continue
			}

			if w, ok := record.Weights[WeightTypeTCP]; ok && w > 0 {
				key := ":false"
				if last, exists := activeCombinations[key]; !exists || record.LastUsed.After(last) {
					activeCombinations[key] = record.LastUsed
				}
			}
			if w, ok := record.Weights[WeightTypeUDP]; ok && w > 0 {
				key := ":true"
				if last, exists := activeCombinations[key]; !exists || record.LastUsed.After(last) {
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
						if last, exists := activeCombinations[combKey]; !exists || record.LastUsed.After(last) {
							activeCombinations[combKey] = record.LastUsed
						}
					}
				} else if strings.HasPrefix(key, WeightTypeUDPASN) && weight > 0 {
					parts := strings.Split(key, ":")
					if len(parts) >= 2 {
						asn := parts[1]
						combKey := asn + ":true"
						if last, exists := activeCombinations[combKey]; !exists || record.LastUsed.After(last) {
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

			recordKey := fmt.Sprintf("%s:%s:%t", domain, asn, isUDP)
			if existingLast, exists := seen[recordKey]; !exists || lastUsed.After(existingLast) {
				seen[recordKey] = lastUsed
				heap.Push(h, ActiveDomain{
					Domain:   domain,
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

	result := make([]ActiveDomain, 0, h.Len())
	var sorted []ActiveDomain
	for h.Len() > 0 {
		sorted = append(sorted, heap.Pop(h).(ActiveDomain))
	}

	for i := len(sorted) - 1; i >= 0; i-- {
		result = append(result, sorted[i])
	}

	return result
}

// RunPrefetch 最佳节点预计算
func (s *Store) RunPrefetch(group, config string, proxyMap map[string]string) int {
	log.Debugln("[SmartStore] Executing domain and ASN pre-calculation for policy group [%s]", group)

	blockedNodes := make(map[string]bool)
	stateData, _ := s.GetNodeStates(group, config)
	for nodeName, data := range stateData {
		var state NodeState
		if json.Unmarshal(data, &state) == nil {
			if !state.BlockedUntil.IsZero() && state.BlockedUntil.After(time.Now()) {
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

	activeDomains := s.GetActiveDomains(group, config, prefetchLimit)

	type prefetchItem struct {
		domain      string
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

	for _, active := range activeDomains {
		var bestNodes []string
		var bestWeights []float64
		var err error

		if active.ASN != "" && !cdnASNs[active.ASN] {
			key := asnCacheKey{active.ASN, active.IsUDP}
			if v, ok := asnCache[key]; ok {
				bestNodes = v.nodes
				bestWeights = v.weights
			} else {
				bestNodes, bestWeights, err = s.GetBestProxyForTarget(group, config, active.Domain, active.ASN, active.IsUDP)
				asnCache[key] = asnCacheValue{
					nodes:      bestNodes,
					weights:    bestWeights,
				}
			}
		} else {
			bestNodes, bestWeights, err = s.GetBestProxyForTarget(group, config, active.Domain, active.ASN, active.IsUDP)
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
				domain:      active.Domain,
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
		oldNodes, oldWeights := s.GetPrefetchResult(group, config, item.domain, item.asnNumber, item.isUDP)

		target := item.domain
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
		if item.asnNumber != "" && !cdnASNs[item.asnNumber] {
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
						if math.Abs(newW-oldW)/oldW > 0.2 || newW < AllowedWeight {
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

			if item.asnNumber != "" && !cdnASNs[item.asnNumber] {
				key := asnCacheKey{item.asnNumber, item.isUDP}
				asnCache[key] = asnCacheValue{
					nodes:      sortedNodes,
					weights:    sortedWeights,
				}
			}
		}

		if needUpdate {
			s.StorePrefetchResult(group, config, item.domain, item.asnNumber, item.isUDP, sortedNodes, sortedWeights)
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
			log.Debugln("[SmartStore] Prefetching for group [%s]: network [%s] => target [%s] => [%s] (no old result)",
				group, networkType, target, strings.Join(nodeWeightPairs, ", "))
		} else if cacheHit {
			log.Debugln("[SmartStore] Prefetching for group [%s]: network [%s] => target [%s] => [%s] (from cache)",
				group, networkType, target, strings.Join(nodeWeightPairs, ", "))
		} else if needUpdate {
			log.Debugln("[SmartStore] Prefetching for group [%s]: network [%s] => target [%s] => [%s] (updated from old: [%s])",
				group, networkType, target, strings.Join(nodeWeightPairs, ", "), strings.Join(oldNodeWeightPairs, ", "))
		}
	}

	log.Infoln("[SmartStore] Prefetch completed for group [%s]: pre-calculated [%d] targets",
		group, prefetchCount)
	return prefetchCount
}

// GetNodeStates 获取节点状态
func (s *Store) GetNodeStates(group, config string) (map[string][]byte, error) {
	cacheKey := fmt.Sprintf("%s:%s", group, config)
	if cached, ok := nodeStatesCache.Get(cacheKey); ok {
		return cached, nil
	}

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

	nodeStatesCache.Set(cacheKey, result)

	return result, nil
}

// 获取域名的统计数据
func (s *Store) GetStatsForDomain(group, config, domain, proxyName string) (map[string][]byte, error) {
    result := make(map[string][]byte)

    ops := getGlobalQueueSnapshot()
    for _, op := range ops {
        if op.Type == OpSaveStats && op.Group == group && op.Config == config && op.Target == domain && op.Node == proxyName {
            result[proxyName] = op.Data
            return result, nil
        }
    }

    pathKey := FormatDBKey("smart", KeyTypeStats, config, group, domain, proxyName)
    rawResult, err := s.GetSubBytesByPath(pathKey)
    if err != nil {
        return nil, err
    }

    result[proxyName] = rawResult[pathKey]

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
		domain := parts[len(parts)-2]
		node := parts[len(parts)-1]

		if _, ok := result[domain]; !ok {
			result[domain] = make(map[string][]byte)
		}
		result[domain][node] = data
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
func (s *Store) DeleteDomainRecords(group, config, domain string) error {
	key := FormatDBKey("smart", KeyTypeStats, config, group, domain, "")
	if err := s.DeleteByPath(key); err != nil {
		return err
	}

	statsCachePrefix := FormatCacheKey(KeyTypeStats, config, group, domain)
	RemoveCacheValuesByPrefix(statsCachePrefix)

	return nil
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

	domainNodePairs := make(map[string][]string)
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
			domain := parts[len(parts)-2]
			node := parts[len(parts)-1]
			if _, ok := nodeSet[node]; ok {
				domainNodePairs[domain] = append(domainNodePairs[domain], node)
				s.DeleteCacheResult(KeyTypeStats, config, group, domain, node)
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
func (s *Store) CleanupOldDomains(group, config string) error {
	statsPrefix := FormatDBKey("smart", KeyTypeStats, config, group, "")

	globalCacheParams.mutex.RLock()
	maxDomains := globalCacheParams.MaxDomains * 2
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
		domain := parts[len(parts)-2]
		var statsRecord StatsRecord
		if err := json.Unmarshal(data, &statsRecord); err != nil {
			continue
		}
		if last, ok := targetLastUsed[domain]; !ok || statsRecord.LastUsed.After(last) {
			targetLastUsed[domain] = statsRecord.LastUsed
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

	if len(targetList) <= maxDomains {
		return nil
	}
	toDelete := targetList[:len(targetList)-maxDomains]
	for _, info := range toDelete {
		err := s.DeleteDomainRecords(group, config, info.target)
		if err != nil {
			log.Warnln("[SmartStore] Failed to delete domain [%s]: %v", info.target, err)
		}
		s.DeleteCacheResult(KeyTypePrefetch, config, group, info.target, "")
	}

	log.Debugln("[SmartStore] Cleaned up [%d] old domain records, keeping the latest [%d] (group %s)",
		len(toDelete), maxDomains, group)
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
	var expiredDomains []string
	domainLastUsed := make(map[string]time.Time)

	for path, data := range statsData {
		parts := strings.Split(path, "/")
		if len(parts) < 6 {
			continue
		}
		domain := parts[len(parts)-2]
		var statsRecord StatsRecord
		if err := json.Unmarshal(data, &statsRecord); err != nil {
			continue
		}
		if last, ok := domainLastUsed[domain]; !ok || statsRecord.LastUsed.After(last) {
			domainLastUsed[domain] = statsRecord.LastUsed
		}
	}

	for domain, lastUsed := range domainLastUsed {
		if lastUsed.Before(threshold) {
			expiredDomains = append(expiredDomains, domain)
			err := s.DeleteDomainRecords(group, config, domain)
			if err != nil {
				log.Warnln("[SmartStore] Failed to delete expired domain [%s]: %v", domain, err)
			}
		}
	}

	if len(expiredDomains) > 0 {
		log.Debugln("[SmartStore] Deleted [%d] expired domains for group [%s]", len(expiredDomains), group)
	}

	return nil
}

// 标记连接失败
func (s *Store) MarkConnectionFailed(group, config, triedProxies, domain string, proxiesCount int) {
	networkStatus := s.CheckNetworkFailure(group, config)
	if networkStatus {
		return
	}
	failedKey := FormatCacheKey(KeyTypeFailed, config, group, triedProxies)
	SetCacheValue(failedKey, domain)

	failedPrefix := FormatCacheKey(KeyTypeFailed, config, group, "")
	failedCache := GetCacheValuesByPrefix(failedPrefix)
	failedNodeCount := len(failedCache)
	nodeThreshold := int(math.Min(float64(proxiesCount), math.Max(float64(proxiesCount)/1.5, 3)))

	if failedNodeCount >= nodeThreshold {
		networkKey := FormatCacheKey(keyTypeNetwork, config, group)
		SetCacheValue(networkKey, true)
		unwrapCache.Clear()
		log.Warnln("[SmartStore] Network failure detected for group [%s:%s], failed nodes: %d", group, config, failedNodeCount)
	}
}

// 标记连接成功
func (s *Store) MarkConnectionSuccess(group, config string) {
	networkStatus := s.CheckNetworkFailure(group, config)
	if !networkStatus {
		return
	}
	networkKey := FormatCacheKey(keyTypeNetwork, config, group)
	failedKey := FormatCacheKey(KeyTypeFailed, config, group, "")
	RemoveCacheValuesByPrefix(failedKey)
	SetCacheValue(networkKey, false)
	log.Infoln("[SmartStore] Network recovered for group [%s:%s]", group, config)
}

// 检查网络故障状态
func (s *Store) CheckNetworkFailure(group, config string) bool {
	networkKey := FormatCacheKey(keyTypeNetwork, config, group)
	val, ok := GetCacheValue(networkKey)
	if ok {
		if b, ok := val.(bool); ok {
			return b
		}
	}
	return false
}