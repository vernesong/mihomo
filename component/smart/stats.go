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
	"github.com/metacubex/mihomo/log"
)

var (
	shardedLocks     [1024]*sync.RWMutex
	shardedLocksOnce sync.Once
)

var (
	globalAtomicManager *AtomicRecordManager
	atomicManagerOnce   sync.Once
)

type AtomicStatsRecord struct {
	success     atomic.Int64
	failure     atomic.Int64
	connectTime atomic.Int64
	latency     atomic.Int64
	lastUsed    atomic.Int64
	status      atomic.Int64

	weights         atomic.TypedValue[map[string]float64]
	uploadTotal     *atomic.Float64
	downloadTotal   *atomic.Float64
	duration        *atomic.Float64
	maxUploadRate   *atomic.Float64
	maxDownloadRate *atomic.Float64
}

type AtomicRecordManager struct {
	records sync.Map
}

type domainLastUsed struct {
	domain   string
	lastUsed time.Time
	types    []string
}

type domainMinHeap []domainLastUsed

type asnLastUsed struct {
	asn      string
	lastUsed time.Time
	types    []string
}

type asnMinHeap []asnLastUsed

func initShardedLocks() {
	shardedLocksOnce.Do(func() {
		for i := range shardedLocks {
			shardedLocks[i] = &sync.RWMutex{}
		}
	})
}

// 域名节点锁
func GetDomainNodeLock(domain, group, proxyName string) *sync.RWMutex {
	initShardedLocks()

	h := fnv.New32a()
	h.Write([]byte(domain))
	h.Write([]byte(group))
	h.Write([]byte(proxyName))
	hash := h.Sum32()

	return shardedLocks[hash&1023]
}

func GetAtomicManager() *AtomicRecordManager {
	atomicManagerOnce.Do(func() {
		globalAtomicManager = &AtomicRecordManager{}
	})
	return globalAtomicManager
}

// 获取或创建原子记录
func (m *AtomicRecordManager) GetOrCreateAtomicRecord(cacheKey string, store *Store, groupName, configName, domain, proxyName string) *AtomicStatsRecord {
	if value, ok := m.records.Load(cacheKey); ok {
		return value.(*AtomicStatsRecord)
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

	if store != nil {
		if existingData, err := store.GetStatsForDomain(groupName, configName, domain); err == nil {
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
	}

	actual, loaded := m.records.LoadOrStore(cacheKey, record)
	if loaded {
		return actual.(*AtomicStatsRecord)
	}

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

// 获取节点权重排名
func (s *Store) GetNodeWeightRanking(group, config string, onlyCache bool, proxies []string) (map[string]string, error) {
	if onlyCache {
		cacheKey := FormatCacheKey(KeyTypeRanking, config, group, "")
		cachedData, ok := GetCacheValue(cacheKey)
		if ok {
			if rankingData, isRanking := cachedData.(RankingData); isRanking && len(rankingData.Ranking) > 0 {
				return rankingData.Ranking, nil
			} else if rankingMap, isMap := cachedData.(map[string]string); isMap && len(rankingMap) > 0 {
				return rankingMap, nil
			}
		}

		dbKey := FormatDBKey("smart", KeyTypeRanking, config, group, "")
		data, err := s.DBViewGetItem(dbKey)
		if err == nil && data != nil {
			var rankingData RankingData
			if json.Unmarshal(data, &rankingData) == nil && len(rankingData.Ranking) > 0 {
				SetCacheValue(cacheKey, rankingData)
				return rankingData.Ranking, nil
			}
		}

		return make(map[string]string), nil
	}

	var allNodes []string
	if len(proxies) > 0 {
		allNodes = proxies
	} else {
		allNodes, _ = s.GetAllNodesForGroup(group, config)
	}

	nodeDataMap := make(map[string]*struct {
		tcpWeights    float64
		tcpSamples    int
		udpWeights    float64
		udpSamples    int
		asnSamples    int
		finalWeight   float64
		degradeFactor float64
	}, len(allNodes))

	nodeStatesMap := make(map[string]NodeState)
	stateData, _ := s.GetNodeStates(group, config)

	for nodeName, data := range stateData {
		var state NodeState
		if json.Unmarshal(data, &state) == nil {
			if !state.BlockedUntil.IsZero() && state.BlockedUntil.After(time.Now()) {
				continue
			}

			nodeStatesMap[nodeName] = state

			nodeDataMap[nodeName] = &struct {
				tcpWeights    float64
				tcpSamples    int
				udpWeights    float64
				udpSamples    int
				asnSamples    int
				finalWeight   float64
				degradeFactor float64
			}{
				degradeFactor: 1.0,
			}

			if state.Degraded {
				nodeDataMap[nodeName].degradeFactor = state.DegradedFactor
			}
		}
	}

	for _, nodeName := range allNodes {
		if _, exists := nodeDataMap[nodeName]; !exists {
			nodeDataMap[nodeName] = &struct {
				tcpWeights    float64
				tcpSamples    int
				udpWeights    float64
				udpSamples    int
				asnSamples    int
				finalWeight   float64
				degradeFactor float64
			}{
				degradeFactor: 1.0,
			}
		}
	}

	now := time.Now().Unix()
	decayCache := make(map[int64]float64, 72)

	totalNodes := len(allNodes)
	minDecay := math.Max(0.1, 0.4-float64(totalNodes)*0.005)

	getTimeDecay := func(lastUsedTime int64) float64 {
		return GetTimeDecayWithCache(lastUsedTime, now, minDecay, decayCache)
	}

	allStats, err := s.GetAllStats(group, config, true)
	if err != nil {
		return nil, err
	}

	for _, nodeStats := range allStats {
		for nodeName, data := range nodeStats {
			nodeData, ok := nodeDataMap[nodeName]
			if !ok {
				continue
			}

			var record StatsRecord
			if json.Unmarshal(data, &record) != nil {
				continue
			}

			samples := record.Success + record.Failure
			if samples < DefaultMinSampleCount {
				continue
			}

			timeDecay := getTimeDecay(record.LastUsed.Unix())
			timeDecayedSamples := float64(samples) * timeDecay

			if record.Weights == nil {
				continue
			}

			// 处理TCP权重
			if tcpWeight, ok := record.Weights[WeightTypeTCP]; ok && tcpWeight > 0 {
				nodeData.tcpWeights += tcpWeight * timeDecay * float64(samples)
				nodeData.tcpSamples += int(timeDecayedSamples)
			}

			// 处理UDP权重
			if udpWeight, ok := record.Weights[WeightTypeUDP]; ok && udpWeight > 0 {
				nodeData.udpWeights += udpWeight * timeDecay * float64(samples)
				nodeData.udpSamples += int(timeDecayedSamples)
			}

			// 处理ASN权重 - 只统计数量，具体权重在第二次遍历中处理
			for key := range record.Weights {
				if strings.HasPrefix(key, WeightTypeTCPASN) || strings.HasPrefix(key, WeightTypeUDPASN) {
					nodeData.asnSamples++
				}
			}
		}
	}

	for _, nodeStats := range allStats {
		for nodeName, data := range nodeStats {
			nodeData, ok := nodeDataMap[nodeName]
			if !ok || nodeData.asnSamples == 0 {
				continue
			}

			var record StatsRecord
			if json.Unmarshal(data, &record) != nil {
				continue
			}

			timeDecay := getTimeDecay(record.LastUsed.Unix())

			// 处理ASN权重 - 现在已经确定此节点有ASN样本
			for key, weight := range record.Weights {
				if strings.HasPrefix(key, WeightTypeTCPASN) && weight > 0 {
					// ASN权重贡献限制为25%
					asnBonus := weight * timeDecay * 0.25
					nodeData.tcpWeights += asnBonus
					nodeData.tcpSamples++
				} else if strings.HasPrefix(key, WeightTypeUDPASN) && weight > 0 {
					// ASN权重贡献限制为25%
					asnBonus := weight * timeDecay * 0.25
					nodeData.udpWeights += asnBonus
					nodeData.udpSamples++
				}
			}
		}
	}

	nodeWeights := make(map[string]float64, len(nodeDataMap))

	for nodeName, data := range nodeDataMap {
		if data.tcpSamples > 0 {
			tcpAvgWeight := data.tcpWeights / float64(data.tcpSamples)
			tcpFinalWeight := tcpAvgWeight * data.degradeFactor
			data.finalWeight = tcpFinalWeight
		}

		if data.udpSamples > 0 {
			udpAvgWeight := data.udpWeights / float64(data.udpSamples)
			udpFinalWeight := udpAvgWeight * data.degradeFactor

			// 如果已经有TCP权重，则取平均值
			if data.finalWeight > 0 {
				data.finalWeight = (data.finalWeight + udpFinalWeight) / 2
			} else {
				data.finalWeight = udpFinalWeight
			}
		}

		if data.finalWeight > 0 {
			nodeWeights[nodeName] = data.finalWeight
		}
	}

	type nodeWeight struct {
		name   string
		weight float64
	}

	var nodesList []nodeWeight
	for name, weight := range nodeWeights {
		nodesList = append(nodesList, nodeWeight{name, weight})
	}

	sort.Slice(nodesList, func(i, j int) bool {
		return nodesList[i].weight > nodesList[j].weight
	})

	result := make(map[string]string)

	for _, node := range nodesList {
		result[node.name] = RankOccasional
	}

	if len(nodesList) > 0 {
		result[nodesList[0].name] = RankMostUsed

		if len(nodesList) == 2 {
			result[nodesList[1].name] = RankOccasional
		} else if len(nodesList) >= 3 {
			mostUsedBound := int(float64(len(nodesList)) * 0.2)
			if mostUsedBound < 1 {
				mostUsedBound = 1
			}

			occasionalBound := mostUsedBound + int(float64(len(nodesList))*0.5)

			for i := 1; i < mostUsedBound; i++ {
				result[nodesList[i].name] = RankMostUsed
			}

			for i := mostUsedBound; i < occasionalBound; i++ {
				result[nodesList[i].name] = RankOccasional
			}

			for i := occasionalBound; i < len(nodesList); i++ {
				result[nodesList[i].name] = RankRarelyUsed
			}
		}
	}

	if len(nodeWeights) > 0 {
		for _, nodeName := range allNodes {
			if _, exists := nodeWeights[nodeName]; !exists {
				result[nodeName] = RankRarelyUsed
			}
		}
	}

	s.StoreNodeWeightRanking(group, config, result)

	return result, nil
}

// 存储节点权重排名
func (s *Store) StoreNodeWeightRanking(group, config string, ranking map[string]string) error {
	rankingData := RankingData{
		Ranking:     ranking,
		LastUpdated: time.Now(),
	}

	cacheKey := FormatCacheKey(KeyTypeRanking, config, group, "")
	dbKey := FormatDBKey("smart", KeyTypeRanking, config, group, "")

	SetCacheValue(cacheKey, rankingData)

	data, err := json.Marshal(rankingData)
	if err != nil {
		return fmt.Errorf("failed to serialize ranking data: %w", err)
	}

	err = s.DBBatchPutItem(dbKey, data)

	return err
}

// 获取目标的最佳代理
func (s *Store) GetBestProxyForTarget(group, config string, target string, weightType string, allStats bool) ([]string, []float64, error) {
	if target == "" {
		return nil, nil, errors.New("empty target")
	}

	now := time.Now().Unix()
	minDecay := math.Max(0.1, 0.4)
	decayCache := make(map[int64]float64, 72)

	getTimeDecay := func(lastUsedTime int64) float64 {
		return GetTimeDecayWithCache(lastUsedTime, now, minDecay, decayCache)
	}

	allStatsMap, err := s.GetAllStats(group, config, allStats)
	if err != nil {
		return nil, nil, err
	}

	nodeStatesMap := make(map[string]NodeState)
	allAvailableNodes := make([]string, 0)
	stateData, _ := s.GetNodeStates(group, config)
	for nodeName, data := range stateData {
		var state NodeState
		if err := json.Unmarshal(data, &state); err == nil {
			if !state.BlockedUntil.IsZero() && state.BlockedUntil.After(time.Now()) {
				continue
			}
			nodeStatesMap[nodeName] = state
			allAvailableNodes = append(allAvailableNodes, nodeName)
		}
	}
	availableNodesCount := len(allAvailableNodes)

	nodesWithWeight := make(map[string]float64)
	asnMode := strings.HasPrefix(weightType, WeightTypeTCPASN) || strings.HasPrefix(weightType, WeightTypeUDPASN)
	nodeSamples := make(map[string]int)

	if asnMode {
		for _, domainStats := range allStatsMap {
			for nodeName, data := range domainStats {
				var record StatsRecord
				if json.Unmarshal(data, &record) != nil {
					continue
				}
				if record.Weights != nil {
					if weight, ok := record.Weights[weightType]; ok && weight > 0 {
						timeDecay := getTimeDecay(record.LastUsed.Unix())
						nodesWithWeight[nodeName] += weight * timeDecay
						nodeSamples[nodeName]++
					}
				}
			}
		}

		for nodeName, totalWeight := range nodesWithWeight {
			samples := nodeSamples[nodeName]
			if samples >= DefaultMinSampleCount {
				avgWeight := totalWeight / float64(samples)
				nodesWithWeight[nodeName] = avgWeight
			} else {
				delete(nodesWithWeight, nodeName)
			}
		}

		for nodeName, weight := range nodesWithWeight {
			if state, ok := nodeStatesMap[nodeName]; ok && state.Degraded {
				nodesWithWeight[nodeName] = weight * state.DegradedFactor
			}
		}
	} else {
		var domainStats map[string][]byte
		if stats, ok := allStatsMap[target]; ok {
			domainStats = stats
		} else {
			return nil, nil, errors.New("empty stats")
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
				nodesWithWeight[nodeName] = decayedWeight
			}
		}
	}

	var requiredNodeCount int

	baseCount := func() int {
		switch {
		case availableNodesCount <= 5:
			return 2
		case availableNodesCount <= 10:
			return 4
		case availableNodesCount <= 20:
			return 6
		case availableNodesCount <= 50:
			return 8
		default:
			return 10
		}
	}()

	coverageRatio := 0.0

	if availableNodesCount > 0 {
		coverageRatio = float64(len(nodesWithWeight)) / float64(availableNodesCount)
	}

	switch {
	case coverageRatio >= 0.6:
		requiredNodeCount = baseCount
		if requiredNodeCount > 2 {
			requiredNodeCount = (requiredNodeCount * 3) / 4 // 适当减少
		}
	case coverageRatio >= 0.3:
		requiredNodeCount = baseCount
	case coverageRatio >= 0.1:
		requiredNodeCount = baseCount + 1
		if requiredNodeCount < 2 {
			requiredNodeCount = 2
		}
	default:
		requiredNodeCount = baseCount + 2
		if requiredNodeCount < 3 {
			requiredNodeCount = 3
		}
		if requiredNodeCount > availableNodesCount/2 {
			requiredNodeCount = availableNodesCount / 2
			if requiredNodeCount < 1 {
				requiredNodeCount = 1
			}
		}
	}

	if len(nodesWithWeight) >= 3 {
		var maxWeight, minWeight float64
		first := true
		for _, weight := range nodesWithWeight {
			if first {
				maxWeight = weight
				minWeight = weight
				first = false
			} else {
				if weight > maxWeight {
					maxWeight = weight
				}
				if weight < minWeight {
					minWeight = weight
				}
			}
		}

		if maxWeight > 0 && minWeight > 0 {
			ratio := maxWeight / minWeight
			switch {
			case ratio >= 4.0:
				requiredNodeCount = (requiredNodeCount * 2) / 3
				if requiredNodeCount < 1 {
					requiredNodeCount = 1
				}
			case ratio >= 2.0:
				requiredNodeCount = (requiredNodeCount * 4) / 5
				if requiredNodeCount < 1 {
					requiredNodeCount = 1
				}
			case ratio >= 1.5:
				requiredNodeCount = requiredNodeCount
			case ratio < 1.3:
				requiredNodeCount = requiredNodeCount + 1
			}
			if maxWeight < 0.8 {
				requiredNodeCount = (requiredNodeCount * 3) / 4
				if requiredNodeCount < 1 {
					requiredNodeCount = 1
				}
			}
			if maxWeight > 2.5 && ratio >= 1.8 {
				requiredNodeCount = (requiredNodeCount * 3) / 4
				if requiredNodeCount < 1 {
					requiredNodeCount = 1
				}
			}
		}
	}

	if requiredNodeCount > availableNodesCount/2 {
		requiredNodeCount = availableNodesCount / 2
		if requiredNodeCount < 1 {
			requiredNodeCount = 1
		}
	}

	if availableNodesCount > 1 && requiredNodeCount < 2 {
		requiredNodeCount = 2
	}

	type nodeWeight struct {
		name   string
		weight float64
	}
	var nodeList []nodeWeight
	for node, weight := range nodesWithWeight {
		nodeList = append(nodeList, nodeWeight{node, weight})
	}
	sort.Slice(nodeList, func(i, j int) bool {
		return nodeList[i].weight > nodeList[j].weight
	})

	if len(nodeList) < requiredNodeCount {
		return nil, nil, errors.New("not enough nodes with valid weights")
	}

	var bestNodes []string
	var bestWeights []float64
	for i := 0; i < len(nodeList); i++ {
		bestNodes = append(bestNodes, nodeList[i].name)
		bestWeights = append(bestWeights, nodeList[i].weight)
	}

	// force retry exploring if best weight was degraded
	if len(bestWeights) > 0 && bestWeights[0] < 0.4 && len(nodesWithWeight) < availableNodesCount {
		return nil, nil, errors.New("no suitable node")
	}

	return bestNodes, bestWeights, nil
}

// 获取活跃域名
func (h domainMinHeap) Len() int            { return len(h) }
func (h domainMinHeap) Less(i, j int) bool  { return h[i].lastUsed.Before(h[j].lastUsed) }
func (h domainMinHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *domainMinHeap) Push(x interface{}) { *h = append(*h, x.(domainLastUsed)) }
func (h *domainMinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (s *Store) GetActiveDomains(group, config string, limit int, all bool) map[string][]string {
	allStats, err := s.GetAllStats(group, config, all)
	if err != nil || len(allStats) == 0 {
		return nil
	}

	h := &domainMinHeap{}
	heap.Init(h)

	for domain, nodeStats := range allStats {
		var maxLastUsed time.Time
		activeTypeSet := make(map[string]struct{})
		for _, data := range nodeStats {
			var record StatsRecord
			if json.Unmarshal(data, &record) != nil {
				continue
			}
			if maxLastUsed.IsZero() || record.LastUsed.After(maxLastUsed) {
				maxLastUsed = record.LastUsed
			}
			if record.Weights != nil {
				if w, ok := record.Weights[WeightTypeTCP]; ok && w > 0 {
					activeTypeSet[WeightTypeTCP] = struct{}{}
				}
				if w, ok := record.Weights[WeightTypeUDP]; ok && w > 0 {
					activeTypeSet[WeightTypeUDP] = struct{}{}
				}
			}
		}
		if maxLastUsed.IsZero() || len(activeTypeSet) == 0 {
			continue
		}
		types := make([]string, 0, len(activeTypeSet))
		for t := range activeTypeSet {
			types = append(types, t)
		}
		heap.Push(h, domainLastUsed{
			domain:   domain,
			lastUsed: maxLastUsed,
			types:    types,
		})
		if h.Len() > limit {
			heap.Pop(h)
		}
	}

	result := make(map[string][]string)
	var sorted []domainLastUsed
	for h.Len() > 0 {
		sorted = append(sorted, heap.Pop(h).(domainLastUsed))
	}
	for i := len(sorted) - 1; i >= 0; i-- {
		result[sorted[i].domain] = sorted[i].types
	}
	return result
}

// 获取活跃的ASN
func (h asnMinHeap) Len() int            { return len(h) }
func (h asnMinHeap) Less(i, j int) bool  { return h[i].lastUsed.Before(h[j].lastUsed) }
func (h asnMinHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *asnMinHeap) Push(x interface{}) { *h = append(*h, x.(asnLastUsed)) }
func (h *asnMinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (s *Store) GetActiveASNs(group, config string, limit int, all bool) map[string][]string {
	asnLastUsedMap := make(map[string]time.Time)
	asnTypeSet := make(map[string]map[string]struct{})
	asnFrequency := make(map[string]int)

	allStats, err := s.GetAllStats(group, config, all)
	if err != nil {
		return nil
	}

	for _, nodeStats := range allStats {
		for _, data := range nodeStats {
			var record StatsRecord
			if json.Unmarshal(data, &record) != nil {
				continue
			}
			if record.Weights == nil {
				continue
			}
			for weightType, weight := range record.Weights {
				if (strings.HasPrefix(weightType, WeightTypeTCPASN) || strings.HasPrefix(weightType, WeightTypeUDPASN)) && weight > 0 {
					parts := strings.Split(weightType, ":")
					if len(parts) >= 2 {
						asn := parts[1]
						asnFrequency[asn]++
						if lastUsed, exists := asnLastUsedMap[asn]; !exists || record.LastUsed.After(lastUsed) {
							asnLastUsedMap[asn] = record.LastUsed
						}
						if asnTypeSet[asn] == nil {
							asnTypeSet[asn] = make(map[string]struct{})
						}
						if strings.HasPrefix(weightType, WeightTypeTCPASN) {
							asnTypeSet[asn][WeightTypeTCP] = struct{}{}
						}
						if strings.HasPrefix(weightType, WeightTypeUDPASN) {
							asnTypeSet[asn][WeightTypeUDP] = struct{}{}
						}
					}
				}
			}
		}
	}

	h := &asnMinHeap{}
	heap.Init(h)
	for asn, lastUsed := range asnLastUsedMap {
		if asnFrequency[asn] < DefaultMinSampleCount {
			continue
		}
		typeSet := asnTypeSet[asn]
		types := make([]string, 0, len(typeSet))
		for t := range typeSet {
			types = append(types, t)
		}
		heap.Push(h, asnLastUsed{
			asn:      asn,
			lastUsed: lastUsed,
			types:    types,
		})
		if h.Len() > limit {
			heap.Pop(h)
		}
	}

	result := make(map[string][]string)
	var sorted []asnLastUsed
	for h.Len() > 0 {
		sorted = append(sorted, heap.Pop(h).(asnLastUsed))
	}
	for i := len(sorted) - 1; i >= 0; i-- {
		result[sorted[i].asn] = sorted[i].types
	}
	return result
}

// RunPrefetch 最佳节点预先获取
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

	if prefetchLimit <= 0 {
		prefetchLimit = MinPrefetchDomainsLimit
	}

	domains := s.GetActiveDomains(group, config, prefetchLimit*3/5, true)
	asns := s.GetActiveASNs(group, config, prefetchLimit*2/5, true)

	prefetchDomains := 0
	prefetchASNs := 0

	type prefetchItem struct {
		target     string
		weightType string
		bestNode   string
		bestWeight float64
	}

	var domainItems []prefetchItem
	var asnItems []prefetchItem
	var bestNode string
	var bestWeight float64

	// 域名
	for domain, activeTypes := range domains {
		for _, weightType := range activeTypes {
			bestNodes, bestWeights, err := s.GetBestProxyForTarget(group, config, domain, weightType, true)
			if err != nil || len(bestNodes) == 0 {
				continue
			}
			found := false
			for i, node := range bestNodes {
				if node != "" && bestWeights[i] > 0 {
					if _, exists := availableProxyMap[node]; exists {
						bestNode = node
						bestWeight = bestWeights[i]
						found = true
						break
					}
				}
			}
			if !found {
				continue
			}

			item := prefetchItem{
				target:     domain,
				weightType: weightType,
				bestNode:   bestNode,
				bestWeight: bestWeight,
			}
			domainItems = append(domainItems, item)
		}
	}

	// ASN
	for asn, activeTypes := range asns {
		for _, baseType := range activeTypes {
			var weightType string
			if baseType == WeightTypeTCP {
				weightType = WeightTypeTCPASN + ":" + asn
			} else if baseType == WeightTypeUDP {
				weightType = WeightTypeUDPASN + ":" + asn
			} else {
				continue
			}
			bestNodes, bestWeights, err := s.GetBestProxyForTarget(group, config, asn, weightType, true)
			if err != nil || len(bestNodes) == 0 {
				continue
			}
			var bestNode string
			var bestWeight float64
			found := false
			for i, node := range bestNodes {
				if node != "" && bestWeights[i] > 0 {
					if _, exists := availableProxyMap[node]; exists {
						bestNode = node
						bestWeight = bestWeights[i]
						found = true
						break
					}
				}
			}
			if !found {
				continue
			}
			item := prefetchItem{
				target:     asn,
				weightType: weightType,
				bestNode:   bestNode,
				bestWeight: bestWeight,
			}
			asnItems = append(asnItems, item)
		}
	}

	// 域名
	for _, item := range domainItems {
		oldNode, oldWeight := s.GetPrefetchResult(group, config, item.target, item.weightType)
		newWeight := math.Round(item.bestWeight*100) / 100
		oldWeightRounded := math.Round(oldWeight*100) / 100

		if oldNode == "" {
			s.StorePrefetchResult(group, config, item.target, item.weightType, item.bestNode, item.bestWeight)
			prefetchDomains++
			log.Debugln("[SmartStore] Prefetching domain [%s] with best node [%s] for group [%s], weight type [%s], weight: %.2f (no old result)",
				item.target, item.bestNode, group, item.weightType, item.bestWeight)
			continue
		}

		if oldNode == item.bestNode {
			if newWeight != oldWeightRounded {
				s.StorePrefetchResult(group, config, item.target, item.weightType, item.bestNode, item.bestWeight)
				prefetchDomains++
				log.Debugln("[SmartStore] Prefetching domain [%s] with best node [%s] for group [%s], weight type [%s], weight: %.2f (old: %.2f, same node, weight changed)",
					item.target, item.bestNode, group, item.weightType, item.bestWeight, oldWeight)
			}
		} else if newWeight > oldWeightRounded {
			s.StorePrefetchResult(group, config, item.target, item.weightType, item.bestNode, item.bestWeight)
			prefetchDomains++
			log.Debugln("[SmartStore] Prefetching domain [%s] with best node [%s] for group [%s], weight type [%s], weight: %.2f (old: %.2f, upgraded)",
				item.target, item.bestNode, group, item.weightType, item.bestWeight, oldWeight)
		}
	}

	// ASN
	for _, item := range asnItems {
		oldNode, oldWeight := s.GetPrefetchResult(group, config, item.target, item.weightType)
		newWeight := math.Round(item.bestWeight*100) / 100
		oldWeightRounded := math.Round(oldWeight*100) / 100
		if oldNode == "" {
			s.StorePrefetchResult(group, config, item.target, item.weightType, item.bestNode, item.bestWeight)
			prefetchASNs++
			log.Debugln("[SmartStore] Prefetching ASN [%s] with best node [%s] for group [%s], weight type [%s], weight: %.2f (no old result)",
				item.target, item.bestNode, group, item.weightType, item.bestWeight)
			continue
		}

		if oldNode == item.bestNode {
			if newWeight != oldWeightRounded {
				s.StorePrefetchResult(group, config, item.target, item.weightType, item.bestNode, item.bestWeight)
				prefetchASNs++
				log.Debugln("[SmartStore] Prefetching ASN [%s] with best node [%s] for group [%s], weight type [%s], weight: %.2f (old: %.2f, same node, weight changed)",
					item.target, item.bestNode, group, item.weightType, item.bestWeight, oldWeight)
			}
		} else if newWeight > oldWeightRounded {
			s.StorePrefetchResult(group, config, item.target, item.weightType, item.bestNode, item.bestWeight)
			prefetchASNs++
			log.Debugln("[SmartStore] Prefetching ASN [%s] with best node [%s] for group [%s], weight type [%s], weight: %.2f (old: %.2f, upgraded)",
				item.target, item.bestNode, group, item.weightType, item.bestWeight, oldWeight)
		}
	}

	log.Infoln("[SmartStore] Prefetch completed for group [%s]: pre-calculated [%d] domains, [%d] ASNs",
		group, prefetchDomains, prefetchASNs)
	return prefetchDomains + prefetchASNs
}

// GetNodeStates 获取节点状态
func (s *Store) GetNodeStates(group, config string) (map[string][]byte, error) {
	pathPrefix := FormatDBKey("smart", KeyTypeNode, config, group, "")

	cacheKeyPrefix := FormatCacheKey(KeyTypeNode, config, group, "")
	cacheResults := GetCacheValuesByPrefix(cacheKeyPrefix)

	if len(cacheResults) > 0 {
		result := make(map[string][]byte, len(cacheResults))
		allFromCache := true

		for key, value := range cacheResults {
			parts := strings.Split(key, ":")
			if len(parts) > 0 {
				nodeName := parts[len(parts)-1]
				var data []byte
				var err error

				switch v := value.(type) {
				case []byte:
					data = make([]byte, len(v))
					copy(data, v)
				case NodeState:
					data, err = json.Marshal(v)
				default:
					allFromCache = false
					continue
				}

				if err == nil && data != nil {
					result[nodeName] = data
				} else {
					allFromCache = false
				}
			}
		}

		if allFromCache {
			globalQueueMutex.RLock()
			for _, op := range globalOperationQueue {
				if op.Type == OpSaveNodeState && op.Group == group && op.Config == config {
					result[op.Node] = op.Data
				}
			}
			globalQueueMutex.RUnlock()

			return result, nil
		}
	}

	rawResult, err := s.GetSubBytesByPath(pathPrefix, true)
	if err != nil {
		return nil, err
	}

	result := make(map[string][]byte)

	for fullPath, data := range rawResult {
		parts := strings.Split(fullPath, "/")
		if len(parts) > 0 {
			nodeName := parts[len(parts)-1]
			result[nodeName] = data
		}
	}

	for nodeName, data := range result {
		cacheKey := FormatCacheKey(KeyTypeNode, config, group, nodeName)
		var nodeState NodeState
		if json.Unmarshal(data, &nodeState) == nil {
			SetCacheValue(cacheKey, nodeState)
		} else {
			SetCacheValue(cacheKey, data)
		}
	}

	globalQueueMutex.RLock()
	for _, op := range globalOperationQueue {
		if op.Type == OpSaveNodeState && op.Group == group && op.Config == config {
			result[op.Node] = op.Data
		}
	}
	globalQueueMutex.RUnlock()

	return result, nil
}

// 获取域名的统计数据
func (s *Store) GetStatsForDomain(group, config, domain string) (map[string][]byte, error) {
	cacheKeyPrefix := FormatCacheKey(KeyTypeStats, config, group, domain)

	cacheResults := GetCacheValuesByPrefix(cacheKeyPrefix)

	if len(cacheResults) > 0 {
		result := make(map[string][]byte, len(cacheResults))
		allFromCache := true

		for key, value := range cacheResults {
			parts := strings.Split(key, ":")
			if len(parts) >= 5 {
				nodeName := parts[len(parts)-1]
				var data []byte
				var err error

				switch v := value.(type) {
				case []byte:
					data = make([]byte, len(v))
					copy(data, v)
				case StatsRecord:
					data, err = json.Marshal(v)
				default:
					allFromCache = false
					continue
				}

				if err == nil && data != nil {
					result[nodeName] = data
				} else {
					allFromCache = false
				}
			}
		}

		if allFromCache && len(result) > 0 {
			globalQueueMutex.RLock()
			for _, op := range globalOperationQueue {
				if op.Type == OpSaveStats && op.Group == group && op.Config == config && op.Domain == domain {
					result[op.Node] = op.Data
				}
			}
			globalQueueMutex.RUnlock()

			return result, nil
		}
	}

	pathPrefix := FormatDBKey("smart", KeyTypeStats, config, group, domain, "")
	rawResult, err := s.GetSubBytesByPath(pathPrefix, false)
	if err != nil {
		return nil, err
	}

	result := make(map[string][]byte)

	for fullPath, data := range rawResult {
		parts := strings.Split(fullPath, "/")
		if len(parts) > 0 {
			nodeName := parts[len(parts)-1]
			result[nodeName] = data

			cacheKey := FormatCacheKey(KeyTypeStats, config, group, domain, nodeName)
			var record StatsRecord
			if json.Unmarshal(data, &record) == nil {
				SetCacheValue(cacheKey, record)
			} else {
				SetCacheValue(cacheKey, data)
			}
		}
	}

	globalQueueMutex.RLock()
	for _, op := range globalOperationQueue {
		if op.Type == OpSaveStats && op.Group == group && op.Config == config && op.Domain == domain {
			result[op.Node] = op.Data
		}
	}
	globalQueueMutex.RUnlock()

	return result, nil
}

// 获取所有统计数据
func (s *Store) GetAllStats(group, config string, all bool) (map[string]map[string][]byte, error) {
	cacheKeyPrefix := FormatCacheKey(KeyTypeStats, config, group, "")
	cacheResults := GetCacheValuesByPrefix(cacheKeyPrefix)

	globalCacheParams.mutex.RLock()
	configMaxDomains := globalCacheParams.MaxDomains
	globalCacheParams.mutex.RUnlock()

	maxDomainsLimit := 1000
	if all {
		maxDomainsLimit = configMaxDomains
	} else if configMaxDomains < 1000 {
		maxDomainsLimit = configMaxDomains
	}

	result := make(map[string]map[string][]byte)
	domainsCount := 0

	for key, value := range cacheResults {
		if domainsCount >= maxDomainsLimit {
			break
		}
		parts := strings.Split(key, ":")
		if len(parts) >= 5 {
			domain := parts[len(parts)-2]
			nodeName := parts[len(parts)-1]
			if _, exists := result[domain]; !exists {
				if domainsCount >= maxDomainsLimit {
					break
				}
				result[domain] = make(map[string][]byte)
				domainsCount++
			}
			var data []byte
			var err error
			switch v := value.(type) {
			case []byte:
				data = make([]byte, len(v))
				copy(data, v)
			case StatsRecord:
				data, err = json.Marshal(v)
			default:
				continue
			}
			if err == nil && data != nil {
				result[domain][nodeName] = data
			}
		}
	}

	globalQueueMutex.RLock()
	for _, op := range globalOperationQueue {
		if op.Type == OpSaveStats && op.Group == group && op.Config == config {
			domain := op.Domain
			nodeName := op.Node
			if _, exists := result[domain]; !exists {
				if domainsCount >= maxDomainsLimit {
					continue
				}
				result[domain] = make(map[string][]byte)
				domainsCount++
			}
			result[domain][nodeName] = op.Data
		}
	}
	globalQueueMutex.RUnlock()

	if len(result) < maxDomainsLimit {
		pathPrefix := FormatDBKey("smart", KeyTypeStats, config, group, "")
		rawResult, err := s.DBViewPrefixScan(pathPrefix, maxDomainsLimit)
		if err != nil {
			return nil, err
		}
		for path, data := range rawResult {
			if len(result) >= maxDomainsLimit {
				break
			}
			parts := strings.Split(path, "/")
			if len(parts) < 6 {
				continue
			}
			domain := parts[len(parts)-2]
			node := parts[len(parts)-1]
			if _, exists := result[domain]; !exists {
				result[domain] = make(map[string][]byte)
			}
			if _, exists := result[domain][node]; !exists {
				result[domain][node] = data
				cacheKey := FormatCacheKey(KeyTypeStats, config, group, domain, node)
				var record StatsRecord
				if json.Unmarshal(data, &record) == nil {
					SetCacheValue(cacheKey, record)
				} else {
					SetCacheValue(cacheKey, data)
				}
			}
		}
	}

	return result, nil
}

// 获取所有域名记录
func (s *Store) GetAllDomainRecords(group, config string) ([]DomainRecord, error) {
	allStats, err := s.GetAllStats(group, config, true)
	if err != nil {
		return nil, err
	}

	var records []DomainRecord
	for domain, nodeStats := range allStats {
		for nodeName, data := range nodeStats {
			var statsRecord StatsRecord
			if err := json.Unmarshal(data, &statsRecord); err != nil {
				continue
			}

			records = append(records, DomainRecord{
				Key:      fmt.Sprintf("%s:%s:%s:%s", config, group, nodeName, domain),
				Domain:   domain,
				NodeName: nodeName,
				LastUsed: statsRecord.LastUsed,
			})
		}
	}

	sort.Slice(records, func(i, j int) bool {
		return records[i].LastUsed.After(records[j].LastUsed)
	})

	return records, nil
}

// 删除域名记录
func (s *Store) DeleteDomainRecords(group, config, domain string) error {
	key := FormatDBKey("smart", KeyTypeStats, config, group, domain, "")
	return s.DeleteByPath(key)
}

// 获取缓存中的所有组名
func (s *Store) GetAllGroupsForConfig(config string) ([]string, error) {
	groupsMap := make(map[string]bool)

	statsPath := FormatDBKey("smart", KeyTypeStats, config)
	prefix := statsPath + "/"

	scanResults, err := s.DBViewPrefixScan(prefix, 1000)
	if err != nil {
		return nil, err
	}

	for path := range scanResults {
		parts := strings.Split(path, "/")
		if len(parts) >= 4 {
			group := parts[3]
			groupsMap[group] = true
		}
	}

	result := make([]string, 0, len(groupsMap))
	for group := range groupsMap {
		result = append(result, group)
	}

	if len(result) == 0 {
		return []string{}, nil
	}

	return result, nil
}

// 通过缓存数据获取组中的节点
func (s *Store) GetAllNodesForGroup(group, config string) ([]string, error) {
	nodesMap := make(map[string]bool)
	nodesPath := FormatDBKey("smart", KeyTypeNode, config, group, "")
	nodeStatesData, err := s.GetSubBytesByPath(nodesPath, true)
	if err == nil {
		for key := range nodeStatesData {
			parts := strings.Split(key, "/")
			if len(parts) >= 5 {
				nodeName := parts[4]
				nodesMap[nodeName] = true
			}
		}
	}

	allStats, err := s.GetAllStats(group, config, true)
	if err == nil {
		for _, domainStats := range allStats {
			for nodeName := range domainStats {
				nodesMap[nodeName] = true
			}
		}
	}

	globalQueueMutex.RLock()
	for _, op := range globalOperationQueue {
		if op.Group == group && op.Config == config {
			if op.Type == OpSaveNodeState {
				nodesMap[op.Node] = true
			} else if op.Type == OpSaveStats {
				nodesMap[op.Node] = true
			}
		}
	}
	globalQueueMutex.RUnlock()

	var result []string
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

	globalQueueMutex.Lock()
	newQueue := make([]StoreOperation, 0, len(globalOperationQueue))
	for _, op := range globalOperationQueue {
		if op.Group == group && op.Config == config {
			nodeMatches := false
			for _, node := range nodes {
				if op.Node == node {
					nodeMatches = true
					break
				}
			}
			if !nodeMatches {
				newQueue = append(newQueue, op)
			}
		} else {
			newQueue = append(newQueue, op)
		}
	}
	globalOperationQueue = newQueue
	globalQueueMutex.Unlock()

	allStats, err := s.GetAllStats(group, config, true)
	if err != nil {
		return err
	}

	domainNodePairs := make(map[string][]string)
	for domain, nodeStats := range allStats {
		for _, nodeName := range nodes {
			if _, exists := nodeStats[nodeName]; exists {
				domainNodePairs[domain] = append(domainNodePairs[domain], nodeName)
			}
		}
	}

	for _, nodeName := range nodes {
		nodePath := FormatDBKey("smart", KeyTypeNode, config, group, nodeName)
		if err := s.DeleteByPath(nodePath); err != nil {
			log.Warnln("[SmartStore] Failed to delete node state for [%s]: %v", nodeName, err)
		}

		cacheKey := FormatCacheKey(KeyTypeNode, config, group, nodeName)
		DeleteCacheValue(cacheKey)
	}

	for domain, nodeNames := range domainNodePairs {
		for _, nodeName := range nodeNames {
			statsCacheKey := FormatCacheKey(KeyTypeStats, config, group, domain, nodeName)
			DeleteCacheValue(statsCacheKey)

			statsPath := FormatDBKey("smart", KeyTypeStats, config, group, domain, nodeName)
			if err := s.DeleteByPath(statsPath); err != nil {
				log.Warnln("[SmartStore] Failed to delete stats for [%s], domain [%s]: %v", nodeName, domain, err)
			}
		}
	}

	return nil
}

// 清理旧的域名记录
func (s *Store) CleanupOldDomains(group, config string) error {
	domains := make(map[string]time.Time)
	domainRecords, err := s.GetAllDomainRecords(group, config)
	if err != nil {
		return err
	}
	for _, record := range domainRecords {
		if lastUsed, exists := domains[record.Domain]; !exists || record.LastUsed.After(lastUsed) {
			domains[record.Domain] = record.LastUsed
		}
	}

	type domainInfo struct {
		domain   string
		lastUsed time.Time
	}
	var domainList []domainInfo
	for domain, lastUsed := range domains {
		domainList = append(domainList, domainInfo{
			domain:   domain,
			lastUsed: lastUsed,
		})
	}
	sort.Slice(domainList, func(i, j int) bool {
		return domainList[i].lastUsed.Before(domainList[j].lastUsed)
	})

	globalCacheParams.mutex.RLock()
	maxDomains := globalCacheParams.MaxDomains
	globalCacheParams.mutex.RUnlock()
	if maxDomains <= 0 {
		maxDomains = MinDomainsLimit
	}

	if len(domainList) > maxDomains {
		toDelete := domainList[:len(domainList)-maxDomains]

		for _, info := range toDelete {
			// 删除域名统计数据（缓存和DB）
			err := s.DeleteDomainRecords(group, config, info.domain)
			if err != nil {
				log.Warnln("[SmartStore] Failed to delete domain [%s]: %v", info.domain, err)
			}
			// 同时清理预取结果（缓存和DB）
			s.DeleteCacheResult(KeyTypePrefetch, group, config, info.domain)
			prefetchDBKey := FormatDBKey("smart", KeyTypePrefetch, config, group, info.domain)
			_ = s.DeleteByPath(prefetchDBKey)
		}

		log.Debugln("[SmartStore] Cleaned up [%d] old domain records, keeping the latest [%d] (group %s)",
			len(toDelete), maxDomains, group)
	}

	RemoveCacheValuesByPrefix(FormatCacheKey(KeyTypeStats, config, group, ""))

	return nil
}

// 清理过期统计数据
func (s *Store) CleanupExpiredStats(group, config string) error {
	records, err := s.GetAllDomainRecords(group, config)
	if err != nil {
		return err
	}

	threshold := time.Now().Add(-RetentionPeriod)
	var expiredDomains []string

	domainLastUsed := make(map[string]time.Time)
	for _, record := range records {
		lastUsed, exists := domainLastUsed[record.Domain]
		if !exists || record.LastUsed.After(lastUsed) {
			domainLastUsed[record.Domain] = record.LastUsed
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

func (s *Store) clearThrottlePrefix(prefix string) {
	n := &s.networkFailureManager
	n.cacheThrottle.mutex.Lock()
	for k := range n.cacheThrottle.lastSet {
		if strings.HasPrefix(k, prefix) {
			delete(n.cacheThrottle.lastSet, k)
		}
	}
	for k := range n.cacheThrottle.lastClear {
		if strings.HasPrefix(k, prefix) {
			delete(n.cacheThrottle.lastClear, k)
		}
	}
	n.cacheThrottle.mutex.Unlock()
}

// 标记连接失败
func (s *Store) MarkConnectionFailed(group, config string, proxiesCount int, triedProxies map[string]bool) {
	n := &s.networkFailureManager
	groupKey := fmt.Sprintf("%s:%s", group, config)
	now := time.Now()

	for proxy := range triedProxies {
		key := FormatCacheKey(KeyTypeFailed, config, group, proxy)
		n.cacheThrottle.mutex.Lock()
		last := n.cacheThrottle.lastSet[key]
		if now.Sub(last) >= n.writeInterval {
			n.cacheThrottle.lastSet[key] = now
			n.cacheThrottle.mutex.Unlock()
			SetCacheValue(key, now)
		} else {
			n.cacheThrottle.mutex.Unlock()
		}
	}

	failedPrefix := FormatCacheKey(KeyTypeFailed, config, group, "")
	failedCount := len(GetCacheValuesByPrefix(failedPrefix))
	threshold := int(math.Min(float64(proxiesCount), math.Max(float64(proxiesCount)/1.5, 3)))

	n.lock.Lock()
	defer n.lock.Unlock()
	if failedCount >= threshold && !n.status[groupKey] {
		n.status[groupKey] = true
		n.successCount[groupKey] = 0
		n.lastFailure[groupKey] = now
		log.Warnln("[SmartStore] Network failure detected for group [%s:%s] after [%d] consecutive failures", group, config, failedCount)
	}
}

// 标记连接成功
func (s *Store) MarkConnectionSuccess(group, config string) {
	n := &s.networkFailureManager
	groupKey := fmt.Sprintf("%s:%s", group, config)
	n.lock.Lock()
	defer n.lock.Unlock()

	failedPrefix := FormatCacheKey(KeyTypeFailed, config, group, "")
	now := time.Now()

	if n.status[groupKey] {
		n.successCount[groupKey]++
		if n.successCount[groupKey] >= 3 || now.Sub(n.lastFailure[groupKey]) > 30*time.Second {
			n.status[groupKey] = false
			n.successCount[groupKey] = 0
			log.Infoln("[SmartStore] Network recovered for group [%s:%s]", group, config)
			RemoveCacheValuesByPrefix(failedPrefix)
			s.clearThrottlePrefix(failedPrefix)
		}
	} else {
		n.cacheThrottle.mutex.Lock()
		lastClear := n.cacheThrottle.lastClear[failedPrefix]
		if now.Sub(lastClear) >= n.clearInterval {
			n.cacheThrottle.lastClear[failedPrefix] = now
			n.cacheThrottle.mutex.Unlock()
			RemoveCacheValuesByPrefix(failedPrefix)
			s.clearThrottlePrefix(failedPrefix)
		} else {
			n.cacheThrottle.mutex.Unlock()
		}
	}
}

// 检查网络故障状态
func (s *Store) CheckNetworkFailure(group, config string) bool {
	n := &s.networkFailureManager
	groupKey := fmt.Sprintf("%s:%s", group, config)
	n.lock.RLock()
	defer n.lock.RUnlock()
	return n.status[groupKey]
}

// 清理故障缓存
func (s *Store) ClearFailureCache(level, config, group string) {
	n := &s.networkFailureManager
	n.lock.Lock()
	defer n.lock.Unlock()
	if level == "all" {
		n.status = make(map[string]bool)
		n.successCount = make(map[string]int)
		n.lastFailure = make(map[string]time.Time)
		n.cacheThrottle.mutex.Lock()
		n.cacheThrottle.lastSet = make(map[string]time.Time)
		n.cacheThrottle.lastClear = make(map[string]time.Time)
		n.cacheThrottle.mutex.Unlock()
	} else if level == "group" {
		groupKey := fmt.Sprintf("%s:%s", group, config)
		delete(n.status, groupKey)
		delete(n.successCount, groupKey)
		delete(n.lastFailure, groupKey)
		failedPrefix := FormatCacheKey(KeyTypeFailed, config, group, "")
		n.cacheThrottle.mutex.Lock()
		for k := range n.cacheThrottle.lastSet {
			if strings.HasPrefix(k, failedPrefix) {
				delete(n.cacheThrottle.lastSet, k)
			}
		}
		for k := range n.cacheThrottle.lastClear {
			if strings.HasPrefix(k, failedPrefix) {
				delete(n.cacheThrottle.lastClear, k)
			}
		}
		n.cacheThrottle.mutex.Unlock()
	} else if level == "config" {
		for key := range n.status {
			if strings.Contains(key, ":"+config) {
				delete(n.status, key)
				delete(n.successCount, key)
				delete(n.lastFailure, key)
			}
		}
		failedPrefix := FormatCacheKey(KeyTypeFailed, config, "", "")
		n.cacheThrottle.mutex.Lock()
		for k := range n.cacheThrottle.lastSet {
			if strings.HasPrefix(k, failedPrefix) {
				delete(n.cacheThrottle.lastSet, k)
			}
		}
		for k := range n.cacheThrottle.lastClear {
			if strings.HasPrefix(k, failedPrefix) {
				delete(n.cacheThrottle.lastClear, k)
			}
		}
		n.cacheThrottle.mutex.Unlock()
	}
}
