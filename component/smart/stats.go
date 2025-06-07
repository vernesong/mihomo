package smart

import (
    "encoding/json"
    "math"
    "sort"
    "strings"
    "time"
    "errors"
    "fmt"
    "math/rand"
    "hash/fnv"
    "sync"
    "sync/atomic"

    "github.com/metacubex/mihomo/log"
)

var (
    shardedLocks [1024]*sync.RWMutex
    shardedLocksOnce sync.Once
)

type AtomicStatsRecord struct {
    success     int64
    failure     int64
    connectTime int64
    latency     int64
    lastUsed    int64
    
    mu            sync.RWMutex
    weights       map[string]float64
    uploadTotal   float64
    downloadTotal float64
    duration      float64
}

type AtomicRecordManager struct {
    records sync.Map
}

var (
    globalAtomicManager *AtomicRecordManager
    atomicManagerOnce   sync.Once
)

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
        weights: make(map[string]float64),
    }
    atomic.StoreInt64(&record.lastUsed, time.Now().Unix())
    
    if store != nil {
        if existingData, err := store.GetStatsForDomain(groupName, configName, domain); err == nil {
            if data, exists := existingData[proxyName]; exists {
                var existingRecord StatsRecord
                if json.Unmarshal(data, &existingRecord) == nil {
                    atomic.StoreInt64(&record.success, existingRecord.Success)
                    atomic.StoreInt64(&record.failure, existingRecord.Failure)
                    atomic.StoreInt64(&record.connectTime, existingRecord.ConnectTime)
                    atomic.StoreInt64(&record.latency, existingRecord.Latency)
                    atomic.StoreInt64(&record.lastUsed, existingRecord.LastUsed.Unix())
                    
                    record.mu.Lock()
                    for k, v := range existingRecord.Weights {
                        record.weights[k] = v
                    }
                    record.uploadTotal = existingRecord.UploadTotal
                    record.downloadTotal = existingRecord.DownloadTotal
                    record.duration = existingRecord.ConnectionDuration
                    record.mu.Unlock()
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
    success := record.Get("success").(int64)
    failure := record.Get("failure").(int64)
    connectTime := record.Get("connectTime").(int64)
    latency := record.Get("latency").(int64)
    lastUsed := record.Get("lastUsed").(int64)
    
    weights := record.Get("weights")
    var weightsMap map[string]float64
    if weights != nil {
        weightsMap = weights.(map[string]float64)
    }
    
    uploadTotal := record.Get("uploadTotal").(float64)
    downloadTotal := record.Get("downloadTotal").(float64)
    duration := record.Get("duration").(float64)
    
    return &StatsRecord{
        Success:            success,
        Failure:            failure,
        ConnectTime:        connectTime,
        Latency:            latency,
        LastUsed:           time.Unix(lastUsed, 0),
        Weights:            weightsMap,
        UploadTotal:        uploadTotal,
        DownloadTotal:      downloadTotal,
        ConnectionDuration: duration,
    }
}

func (r *AtomicStatsRecord) Get(field string) interface{} {
    switch field {
    case "success":
        return atomic.LoadInt64(&r.success)
    case "failure":
        return atomic.LoadInt64(&r.failure)
    case "connectTime":
        return atomic.LoadInt64(&r.connectTime)
    case "latency":
        return atomic.LoadInt64(&r.latency)
    case "lastUsed":
        return atomic.LoadInt64(&r.lastUsed)
    case "weights":
        r.mu.RLock()
        defer r.mu.RUnlock()
        if r.weights == nil {
            return nil
        }
        result := make(map[string]float64, len(r.weights))
        for k, v := range r.weights {
            result[k] = v
        }
        return result
    case "uploadTotal":
        r.mu.RLock()
        defer r.mu.RUnlock()
        return r.uploadTotal
    case "downloadTotal":
        r.mu.RLock()
        defer r.mu.RUnlock()
        return r.downloadTotal
    case "duration":
        r.mu.RLock()
        defer r.mu.RUnlock()
        return r.duration
    default:
        return nil
    }
}

func (r *AtomicStatsRecord) Set(field string, value interface{}) {
    switch field {
    case "success":
        if v, ok := value.(int64); ok {
            atomic.StoreInt64(&r.success, v)
        }
    case "failure":
        if v, ok := value.(int64); ok {
            atomic.StoreInt64(&r.failure, v)
        }
    case "connectTime":
        if v, ok := value.(int64); ok {
            atomic.StoreInt64(&r.connectTime, v)
        }
    case "latency":
        if v, ok := value.(int64); ok {
            atomic.StoreInt64(&r.latency, v)
        }
    case "lastUsed":
        if v, ok := value.(int64); ok {
            atomic.StoreInt64(&r.lastUsed, v)
        }
    case "uploadTotal":
        if v, ok := value.(float64); ok {
            r.mu.Lock()
            defer r.mu.Unlock()
            r.uploadTotal = v
        }
    case "downloadTotal":
        if v, ok := value.(float64); ok {
            r.mu.Lock()
            defer r.mu.Unlock()
            r.downloadTotal = v
        }
    case "duration":
        if v, ok := value.(float64); ok {
            r.mu.Lock()
            defer r.mu.Unlock()
            r.duration = v
        }
    }
}

func (r *AtomicStatsRecord) Add(field string, value interface{}) {
    switch field {
    case "success":
        if v, ok := value.(int64); ok {
            atomic.AddInt64(&r.success, v)
        }
    case "failure":
        if v, ok := value.(int64); ok {
            atomic.AddInt64(&r.failure, v)
        }
    case "uploadTotal":
        if v, ok := value.(float64); ok {
            r.mu.Lock()
            defer r.mu.Unlock()
            r.uploadTotal += v
        }
    case "downloadTotal":
        if v, ok := value.(float64); ok {
            r.mu.Lock()
            defer r.mu.Unlock()
            r.downloadTotal += v
        }
    }
}

// 权重相关的特殊方法
func (r *AtomicStatsRecord) GetWeight(weightType string) float64 {
    r.mu.RLock()
    defer r.mu.RUnlock()
    
    if r.weights == nil {
        return 0
    }
    return r.weights[weightType]
}

func (r *AtomicStatsRecord) SetWeight(weightType string, value float64) {
    r.mu.Lock()
    defer r.mu.Unlock()
    
    if r.weights == nil {
        r.weights = make(map[string]float64)
    }
    r.weights[weightType] = value
}

// 获取节点权重排名
func (s *Store) GetNodeWeightRanking(group, config string, onlyCache bool, proxies []string) (map[string]string, error) {
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
    var data []byte
    
    data, err := s.DBViewGetItem(dbKey)
    if err == nil && data != nil {
        var rankingData RankingData
        if json.Unmarshal(data, &rankingData) == nil && len(rankingData.Ranking) > 0 {
            SetCacheValue(cacheKey, rankingData)
            return rankingData.Ranking, nil
        }
    }

    if onlyCache {
        return make(map[string]string), nil
    }
    
    var allNodes []string
    if len(proxies) > 0 {
        allNodes = proxies
    } else {
        allNodes, _ = s.GetAllNodesForGroup(group, config)
    }
        
    nodeDataMap := make(map[string]*struct{
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
            
            nodeDataMap[nodeName] = &struct{
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
            nodeDataMap[nodeName] = &struct{
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
    decayCache := make(map[int64]float64, 24)
    
    totalNodes := len(allNodes)
    minDecay := math.Max(0.1, 0.4 - float64(totalNodes)*0.005)
    
    getTimeDecay := func(lastUsedTime int64) float64 {
        if decay, ok := decayCache[lastUsedTime]; ok {
            return decay
        }
        
        hoursSinceLastConn := float64(now - lastUsedTime) / 3600.0
        var decay float64
        
        switch {
        case hoursSinceLastConn <= 72:
            // 0-72小时内线性衰减到1.0
            decay = 1.0 - (hoursSinceLastConn / 72.0) * 0.3
        case hoursSinceLastConn <= 120: 
            // 72-120小时内线性衰减到0.8
            decay = 0.7 - (hoursSinceLastConn - 72.0) * 0.2 / 48.0
        case hoursSinceLastConn <= 192:
            // 120-192小时内线性衰减到0.5
            decay = 0.5 - (hoursSinceLastConn - 120.0) * 0.2 / 72.0
        case hoursSinceLastConn <= 360:
            // 192-360小时内线性衰减到0.3
            decay = 0.3 - (hoursSinceLastConn - 192.0) * 0.1 / 168.0
        default:
            decay = 0.3
        }
        
        decay = math.Max(minDecay, decay)
        decayCache[lastUsedTime] = decay
        return decay
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
            
            occasionalBound := mostUsedBound + int(float64(len(nodesList)) * 0.5)
            
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
func (s *Store) GetBestProxyForTarget(group, config string, target string, weightType string) (string, float64, map[string]float64, error) {
    if target == "" {
        return "", 0, nil, errors.New("empty target")
    }

    if strings.HasPrefix(weightType, WeightTypeTCPASN) || strings.HasPrefix(weightType, WeightTypeUDPASN) {
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

        allDomainsStats, err := s.GetAllStats(group, config, false)
        if err != nil {
            return "", 0, nil, err
        }

        expectedNodes := len(allDomainsStats) / 4
        if expectedNodes < 8 {
            expectedNodes = 8
        }
        
        asnNodeScores := make(map[string]float64, expectedNodes)
        asnNodeSamples := make(map[string]int, expectedNodes)

        for _, domainStats := range allDomainsStats {
            for nodeName, data := range domainStats {
                if _, blocked := nodeStatesMap[nodeName]; !blocked {
                    continue
                }
                
                var record StatsRecord
                if json.Unmarshal(data, &record) != nil {
                    continue
                }

                if record.Weights != nil {
                    if weight, ok := record.Weights[weightType]; ok && weight > 0 {
                        asnNodeScores[nodeName] += weight
                        asnNodeSamples[nodeName]++
                    }
                }
            }
        }

        nodesWithWeight := make(map[string]float64, len(asnNodeScores))
        for nodeName, totalWeight := range asnNodeScores {
            samples := asnNodeSamples[nodeName]
            if samples >= DefaultMinSampleCount {
                avgWeight := totalWeight / float64(samples)
                var nameHash uint32
                if len(nodeName) >= 3 {
                    h := uint32(0)
                    for i := 0; i < 3; i++ {
                        h = h*31 + uint32(nodeName[i])
                    }
                    nameHash = h
                } else {
                    nameHash = uint32(len(nodeName)) * 31
                }
                jitter := 0.97 + (float64(nameHash % 100) / 100.0 * 0.06)
                nodesWithWeight[nodeName] = avgWeight * jitter
            }
        }

        for nodeName, weight := range nodesWithWeight {
            if state, ok := nodeStatesMap[nodeName]; ok && state.Degraded {
                nodesWithWeight[nodeName] = weight * state.DegradedFactor
            }
        }

        if len(nodesWithWeight) == 0 {
            return "", 0, nil, errors.New("no valid nodes for ASN")
        }

        var requiredNodeCount int
        switch {
        case availableNodesCount < 10:
            requiredNodeCount = availableNodesCount / 2
            if requiredNodeCount < 1 {
                requiredNodeCount = 1
            }
        case availableNodesCount < 30:
            requiredNodeCount = availableNodesCount / 4
        case availableNodesCount > 50 && len(nodesWithWeight) > 0 && float64(len(nodesWithWeight))/float64(availableNodesCount) < 0.1:
            requiredNodeCount = 4
        case availableNodesCount > 100 && len(nodesWithWeight) > 0 && float64(len(nodesWithWeight))/float64(availableNodesCount) < 0.05:
            requiredNodeCount = 2
        default:
            requiredNodeCount = 5
        }

        if len(nodesWithWeight) >= requiredNodeCount {
            var bestNode string
            var bestWeight float64
            
            for node, weight := range nodesWithWeight {
                if weight > bestWeight {
                    bestWeight = weight
                    bestNode = node
                }
            }
            
            return bestNode, bestWeight, nodesWithWeight, nil
        } else {
            if len(allAvailableNodes) == 0 {
                return "", 0, nil, errors.New("no available nodes")
            }
            
            randomIndex := int(time.Now().UnixNano() % int64(len(allAvailableNodes)))
            randomNode := allAvailableNodes[randomIndex]
                
            return randomNode, 0.0, nodesWithWeight, nil
        }
    } else {
        stats, err := s.GetStatsForDomain(group, config, target)
        if err != nil || len(stats) == 0 {
            return "", 0, nil, err
        }
        
        nodeStatesMap := make(map[string]NodeState)
        allAvailableNodes := make([]string, 0)
        stateData, _ := s.GetNodeStates(group, config)
        for nodeName, data := range stateData {
            var state NodeState
            if err := json.Unmarshal(data, &state); err == nil {
                // 检查节点是否在屏蔽期
                if !state.BlockedUntil.IsZero() && state.BlockedUntil.After(time.Now()) {
                    continue
                }
                nodeStatesMap[nodeName] = state
                allAvailableNodes = append(allAvailableNodes, nodeName)
            }
        }
        
        availableNodesCount := len(allAvailableNodes)
        
        // 处理有权重的节点
        nodesWithWeight := make(map[string]float64, len(stats))
        for nodeName, data := range stats {
            var record StatsRecord
            if json.Unmarshal(data, &record) != nil {
                continue
            }
            
            var weight float64
            if record.Weights != nil {
                weight = record.Weights[weightType]
            }
            
            if weight > 0 {
                // 检查节点是否被降级
                if state, exists := nodeStatesMap[nodeName]; exists && state.Degraded {
                    weight *= state.DegradedFactor
                }
                
                nodesWithWeight[nodeName] = weight
            }
        }
        
        var requiredNodeCount int
        switch {
        case availableNodesCount < 10:
            requiredNodeCount = availableNodesCount / 2
            if requiredNodeCount < 1 {
                requiredNodeCount = 1
            }
        case availableNodesCount < 30:
            requiredNodeCount = availableNodesCount / 4
        case availableNodesCount > 50 && len(nodesWithWeight) > 0 && float64(len(nodesWithWeight))/float64(availableNodesCount) < 0.1:
            requiredNodeCount = 4
        case availableNodesCount > 100 && len(nodesWithWeight) > 0 && float64(len(nodesWithWeight))/float64(availableNodesCount) < 0.05:
            requiredNodeCount = 2
        default:
            requiredNodeCount = 5
        }
        
        if len(nodesWithWeight) >= requiredNodeCount {
            var bestNode string
            var bestWeight float64
            
            for node, weight := range nodesWithWeight {
                if weight > bestWeight {
                    bestWeight = weight
                    bestNode = node
                }
            }
            
            return bestNode, bestWeight, nodesWithWeight, nil
        } else {
            if len(allAvailableNodes) == 0 {
                return "", 0, nil, errors.New("no available nodes")
            }
            
            randomIndex := int(time.Now().UnixNano() % int64(len(allAvailableNodes)))
            randomNode := allAvailableNodes[randomIndex]
                
            return randomNode, 0.0, nodesWithWeight, nil
        }
    }
}

// 获取活跃域名
func (s *Store) GetActiveDomains(group, config string, limit int) ([]string, error) {
    allStats, err := s.GetAllStats(group, config, false)
    if err != nil {
        return nil, err
    }

    if len(allStats) == 0 {
        return nil, nil
    }

    type domainActivity struct {
        domain     string
        lastUsed   time.Time
        totalHits  int64
        sampleSize int
        avgWeight  float64
        score      float64
    }

    var domains []domainActivity

    for domain, nodeStats := range allStats {
        var lastUsed time.Time
        var totalSuccess, totalFailure int64
        var totalWeight float64
        validSamples := 0
        var timeDecayedSamples float64 = 0

        for _, data := range nodeStats {
            var record StatsRecord
            if json.Unmarshal(data, &record) != nil {
                continue
            }

            if lastUsed.IsZero() || record.LastUsed.After(lastUsed) {
                lastUsed = record.LastUsed
            }

            now := time.Now().Unix()
            hoursSinceLastConn := float64(now-record.LastUsed.Unix()) / 3600.0

            timeDecay := math.Exp(-hoursSinceLastConn / 24.0)

            // 防止太久远的连接完全被忽略
            timeDecay = math.Max(0.3, timeDecay)

            decayedSuccess := float64(record.Success) * timeDecay
            decayedFailure := float64(record.Failure) * timeDecay

            totalSuccess += int64(decayedSuccess)
            totalFailure += int64(decayedFailure)

            var weight float64
            if record.Weights != nil {
                if w, ok := record.Weights[WeightTypeTCP]; ok {
                    weight += w
                }
                if w, ok := record.Weights[WeightTypeUDP]; ok {
                    weight += w
                }
            }

            if weight <= 0 && record.Success+record.Failure >= DefaultMinSampleCount {
                weight = CalculateWeight(
                    record.Success, record.Failure, 
                    record.ConnectTime, record.Latency,
                    false,
                    record.UploadTotal,
                    record.DownloadTotal,
                    record.ConnectionDuration,
                    record.LastUsed.Unix(),
                )
            }

            decayedWeight := weight * timeDecay
            totalWeight += decayedWeight
            timeDecayedSamples += float64(record.Success+record.Failure) * timeDecay
            validSamples++
        }

        if validSamples > 0 {
            avgWeight := totalWeight / float64(validSamples)
            domains = append(domains, domainActivity{
                domain:     domain,
                lastUsed:   lastUsed,
                totalHits:  totalSuccess + totalFailure,
                sampleSize: int(timeDecayedSamples),
                avgWeight:  avgWeight,
            })
        }
    }

    if len(domains) == 0 {
        return nil, nil
    }

    now := time.Now()
    for i := range domains {
        timeDiff := now.Sub(domains[i].lastUsed)
        timeScore := 1.0 / (1.0 + float64(timeDiff.Hours())/24.0)
        sampleScore := float64(domains[i].totalHits) * domains[i].avgWeight
        domains[i].score = timeScore*0.8 + (sampleScore * 0.2)
    }

    sort.Slice(domains, func(i, j int) bool {
        return domains[i].score > domains[j].score
    })

    if len(domains) > limit {
        domains = domains[:limit]
    }

    result := make([]string, len(domains))
    for i, d := range domains {
        result[i] = d.domain
    }
    return result, nil
}

// 获取活跃的ASN
func (s *Store) GetActiveASNs(group, config string, limit int) []string {
    asnFrequency := make(map[string]int)
    asnLastUsed := make(map[string]time.Time)

    allStats, err := s.GetAllStats(group, config, false)
    if err != nil {
        return nil
    }

    for _, nodeStats := range allStats {
        for _, data := range nodeStats {
            var record StatsRecord
            if json.Unmarshal(data, &record) != nil {
                continue
            }

            for weightType := range record.Weights {
                if strings.HasPrefix(weightType, WeightTypeTCPASN) || strings.HasPrefix(weightType, WeightTypeUDPASN) {
                    parts := strings.Split(weightType, ":")
                    if len(parts) >= 2 {
                        asn := parts[1]
                        asnFrequency[asn]++

                        if lastUsed, exists := asnLastUsed[asn]; !exists || record.LastUsed.After(lastUsed) {
                            asnLastUsed[asn] = record.LastUsed
                        }
                    }
                }
            }
        }
    }

    type asnInfo struct {
        asn       string
        frequency int
        lastUsed  time.Time
    }
    var asnList []asnInfo

    for asn, freq := range asnFrequency {
        if freq >= DefaultMinSampleCount {
            asnList = append(asnList, asnInfo{
                asn:       asn,
                frequency: freq,
                lastUsed:  asnLastUsed[asn],
            })
        }
    }

    // 排序逻辑: 频率相差两倍以上按频率排序，否则按最近使用时间排序
    sort.Slice(asnList, func(i, j int) bool {
        if asnList[i].frequency > asnList[j].frequency*2 {
            return true
        }
        if asnList[j].frequency > asnList[i].frequency*2 {
            return false
        }

        return asnList[i].lastUsed.After(asnList[j].lastUsed)
    })

    result := make([]string, 0, limit)
    for i, info := range asnList {
        if i >= limit {
            break
        }
        result = append(result, info.asn)
    }

    return result
}

// RunPrefetch 运行预取
func (s *Store) RunPrefetch(group, config string, proxyMap map[string]string) int {
    log.Debugln("[SmartStore] Executing domain and ASN pre-calculation for policy group [%s]", group)

    randomExplorationRate := 0.05

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

    domains, _ := s.GetActiveDomains(group, config, prefetchLimit)
    asns := s.GetActiveASNs(group, config, prefetchLimit/2)

    prefetchCount := 0
    randomExplorationCount := 0
    algorithmRandomCount := 0
    weightTypes := []string{WeightTypeTCP, WeightTypeUDP}

    availableNodes := make([]string, 0, len(availableProxyMap))
    for name := range availableProxyMap {
        availableNodes = append(availableNodes, name)
    }

    type prefetchItem struct {
        target     string
        weightType string
        bestNode   string
        bestWeight float64
        isAlgorithmRandom bool
    }
    
    var domainItems []prefetchItem
    var asnItems []prefetchItem

    for _, domain := range domains {
        for _, weightType := range weightTypes {
            bestNode, bestWeight, _, err := s.GetBestProxyForTarget(group, config, domain, weightType)
            if err != nil || bestNode == "" {
                continue
            }

            if _, exists := availableProxyMap[bestNode]; exists {
                item := prefetchItem{
                    target:            domain,
                    weightType:        weightType,
                    bestNode:          bestNode,
                    bestWeight:        bestWeight,
                    isAlgorithmRandom: bestWeight <= 0,
                }
                domainItems = append(domainItems, item)
                
                if item.isAlgorithmRandom {
                    algorithmRandomCount++
                }
            }
        }
    }

    for _, asn := range asns {
        for _, baseType := range []string{WeightTypeTCPASN, WeightTypeUDPASN} {
            weightType := baseType + ":" + asn
            bestNode, bestWeight, _, err := s.GetBestProxyForTarget(group, config, asn, weightType)
            if err != nil || bestNode == "" {
                continue
            }

            if _, exists := availableProxyMap[bestNode]; exists {
                item := prefetchItem{
                    target:            asn,
                    weightType:        weightType,
                    bestNode:          bestNode,
                    bestWeight:        bestWeight,
                    isAlgorithmRandom: bestWeight <= 0,
                }
                asnItems = append(asnItems, item)
                
                if item.isAlgorithmRandom {
                    algorithmRandomCount++
                }
            }
        }
    }

    totalItems := len(domainItems) + len(asnItems)
    algorithmRandomRatio := 0.0
    if totalItems > 0 {
        algorithmRandomRatio = float64(algorithmRandomCount) / float64(totalItems)
    }

    // 如果随机比例小于5%，则进行额外随机探索
    shouldDoExtraRandomization := algorithmRandomRatio < 0.05

    for _, item := range domainItems {
        if item.isAlgorithmRandom {
            s.StorePrefetchResult(group, config, item.target, item.weightType, item.bestNode)
            prefetchCount++
        } else if shouldDoExtraRandomization && rand.Float64() < randomExplorationRate && len(availableNodes) > 0 {
            var randomNode string
            maxAttempts := 5
            for i := 0; i < maxAttempts; i++ {
                randomIndex := rand.Intn(len(availableNodes))
                candidate := availableNodes[randomIndex]
                if candidate != item.bestNode {
                    randomNode = candidate
                    break
                }
            }
            
            if randomNode != "" {
                s.StorePrefetchResult(group, config, item.target, item.weightType, randomNode)
                randomExplorationCount++
                log.Debugln("[SmartStore] Random exploration: domain [%s] -> node [%s] (type: %s, replaced best: %s)", 
                    item.target, randomNode, item.weightType, item.bestNode)
            } else {
                s.StorePrefetchResult(group, config, item.target, item.weightType, item.bestNode)
            }
            prefetchCount++
        } else {
            s.StorePrefetchResult(group, config, item.target, item.weightType, item.bestNode)
            prefetchCount++
        }
    }

    for _, item := range asnItems {
        if item.isAlgorithmRandom {
            s.StorePrefetchResult(group, config, item.target, item.weightType, item.bestNode)
            prefetchCount++
        } else if shouldDoExtraRandomization && rand.Float64() < randomExplorationRate && len(availableNodes) > 0 {
            var randomNode string
            maxAttempts := 5
            for i := 0; i < maxAttempts; i++ {
                randomIndex := rand.Intn(len(availableNodes))
                candidate := availableNodes[randomIndex]
                if candidate != item.bestNode {
                    randomNode = candidate
                    break
                }
            }
            
            if randomNode != "" {
                s.StorePrefetchResult(group, config, item.target, item.weightType, randomNode)
                randomExplorationCount++
                log.Debugln("[SmartStore] Random exploration: ASN [%s] -> node [%s] (type: %s, replaced best: %s)", 
                    item.target, randomNode, item.weightType, item.bestNode)
            } else {
                s.StorePrefetchResult(group, config, item.target, item.weightType, item.bestNode)
            }
            prefetchCount++
        } else {
            s.StorePrefetchResult(group, config, item.target, item.weightType, item.bestNode)
            prefetchCount++
        }
    }

    log.Infoln("[SmartStore] Prefetch completed for group [%s]: pre-calculated %d domain/ASN mappings (%d algorithm randoms, %d extra explorations, ratio: %.1f%%)",
        group, prefetchCount, algorithmRandomCount, randomExplorationCount, algorithmRandomRatio*100)
    return prefetchCount
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
            globalQueueMutex.Lock()
            for _, op := range globalOperationQueue {
                if op.Type == OpSaveNodeState && op.Group == group && op.Config == config {
                    result[op.Node] = op.Data
                }
            }
            globalQueueMutex.Unlock()
            
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
    
    globalCacheLock.Lock()
    for nodeName, data := range result {
        cacheKey := FormatCacheKey(KeyTypeNode, config, group, nodeName)
        var nodeState NodeState
        if json.Unmarshal(data, &nodeState) == nil {
            dataCache.Set(cacheKey, nodeState)
        } else {
            dataCache.Set(cacheKey, data)
        }
    }
    globalCacheLock.Unlock()
    
    globalQueueMutex.Lock()
    for _, op := range globalOperationQueue {
        if op.Type == OpSaveNodeState && op.Group == group && op.Config == config {
            result[op.Node] = op.Data
        }
    }
    globalQueueMutex.Unlock()
    
    return result, nil
}

// 获取域名的统计数据
func (s *Store) GetStatsForDomain(group, config, domain string) (map[string][]byte, error) {
    cacheKeyPrefix := FormatCacheKey(KeyTypeStats, config, group, domain)
    
    globalCacheLock.RLock()
    cacheResults := dataCache.FilterByKeyPrefix(cacheKeyPrefix)
    globalCacheLock.RUnlock()
    
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
            globalQueueMutex.Lock()
            for _, op := range globalOperationQueue {
                if op.Type == OpSaveStats && op.Group == group && op.Config == config && op.Domain == domain {
                    result[op.Node] = op.Data
                }
            }
            globalQueueMutex.Unlock()
            
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
            globalCacheLock.Lock()
            if json.Unmarshal(data, &record) == nil {
                dataCache.Set(cacheKey, record)
            } else {
                dataCache.Set(cacheKey, data)
            }
            globalCacheLock.Unlock()
        }
    }
    
    globalQueueMutex.Lock()
    for _, op := range globalOperationQueue {
        if op.Type == OpSaveStats && op.Group == group && op.Config == config && op.Domain == domain {
            result[op.Node] = op.Data
        }
    }
    globalQueueMutex.Unlock()
    
    return result, nil
}

// 获取所有统计数据
func (s *Store) GetAllStats(group, config string, all bool) (map[string]map[string][]byte, error) {
    cacheKeyPrefix := FormatCacheKey(KeyTypeStats, config, group, "")
    
    globalCacheLock.RLock()
    cacheResults := dataCache.FilterByKeyPrefix(cacheKeyPrefix)
    globalCacheLock.RUnlock()

    globalCacheParams.mutex.RLock()
    configMaxDomains := globalCacheParams.MaxDomains
    globalCacheParams.mutex.RUnlock()

    maxDomainsLimit := 500
    if all {
        maxDomainsLimit = configMaxDomains
    } else {
        if configMaxDomains < 500 {
            maxDomainsLimit = configMaxDomains
        }
    }
    
    if len(cacheResults) > int(float64(maxDomainsLimit) * 0.6) && rand.Float64() > 0.15 {
        result := make(map[string]map[string][]byte)
        domainsCount := 0

        keys := make([]string, 0, len(cacheResults))
        for key := range cacheResults {
            keys = append(keys, key)
        }

        rand.Shuffle(len(keys), func(i, j int) {
            keys[i], keys[j] = keys[j], keys[i]
        })
        
        for _, key := range keys {
            if domainsCount >= maxDomainsLimit {
                break
            }
            
            value := cacheResults[key]
            parts := strings.Split(key, ":")
            if len(parts) >= 5 {
                domain := parts[len(parts)-2]
                nodeName := parts[len(parts)-1]
                
                if _, exists := result[domain]; !exists {
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
        
        globalQueueMutex.Lock()
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
        globalQueueMutex.Unlock()
        
        if len(result) > 0 {
            return result, nil
        }
    }
    
    pathPrefix := FormatDBKey("smart", KeyTypeStats, config, group, "")
    result := make(map[string]map[string][]byte)
    
    skipOffset := 0
    if maxDomainsLimit > 300 {
        skipOffset = rand.Intn(maxDomainsLimit / 3)
    }

    rawResult, err := s.DBViewPrefixScan(pathPrefix, skipOffset, maxDomainsLimit)
    if err != nil {
        return nil, err
    }
    
    for path, data := range rawResult {
        parts := strings.Split(path, "/")
        if len(parts) < 6 {
            continue
        }

        domain := parts[len(parts)-2]
        node := parts[len(parts)-1]

        if _, exists := result[domain]; !exists {
            result[domain] = make(map[string][]byte)
        }

        result[domain][node] = data
        
        cacheKey := FormatCacheKey(KeyTypeStats, config, group, domain, node)
        var record StatsRecord
        globalCacheLock.Lock()
        if json.Unmarshal(data, &record) == nil {
            dataCache.Set(cacheKey, record)
        } else {
            dataCache.Set(cacheKey, data)
        }
        globalCacheLock.Unlock()
    }

    globalQueueMutex.Lock()
    for _, op := range globalOperationQueue {
        if op.Type == OpSaveStats && op.Group == group && op.Config == config {
            domain := op.Domain
            nodeName := op.Node
            
            if _, exists := result[domain]; !exists {
                result[domain] = make(map[string][]byte)
            }
            
            result[domain][nodeName] = op.Data
        }
    }
    globalQueueMutex.Unlock()
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

// 获取配置中的所有组
func (s *Store) GetAllGroupsForConfig(config string) ([]string, error) {
    groupsMap := make(map[string]bool)
    
    statsPath := FormatDBKey("smart", KeyTypeStats, config)
    prefix := statsPath + "/"
    
    scanResults, err := s.DBViewPrefixScan(prefix, 0, 1000)
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

// 获取组中的所有节点
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
    
    globalQueueMutex.Lock()
    for _, op := range globalOperationQueue {
        if (op.Group == group && op.Config == config) {
            if op.Type == OpSaveNodeState {
                nodesMap[op.Node] = true
            } else if op.Type == OpSaveStats {
                nodesMap[op.Node] = true
            }
        }
    }
    globalQueueMutex.Unlock()
    
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
            log.Warnln("[SmartStore] Failed to delete node state for %s: %v", nodeName, err)
        }
        
        globalCacheLock.Lock()
        cacheKey := FormatCacheKey(KeyTypeNode, config, group, nodeName)
        dataCache.Delete(cacheKey)
        globalCacheLock.Unlock()
    }
    
    for domain, nodeNames := range domainNodePairs {
        for _, nodeName := range nodeNames {
            globalCacheLock.Lock()
            statsCacheKey := FormatCacheKey(KeyTypeStats, config, group, domain, nodeName)
            dataCache.Delete(statsCacheKey)
            globalCacheLock.Unlock()
            
            statsPath := FormatDBKey("smart", KeyTypeStats, config, group, domain, nodeName)
            if err := s.DeleteByPath(statsPath); err != nil {
                log.Warnln("[SmartStore] Failed to delete stats for %s, domain %s: %v", nodeName, domain, err)
            }
        }
    }

    return nil
}

// 标记连接失败
func (s *Store) MarkConnectionFailed(group, config, host string) {
    if s == nil {
        return
    }

    domain := GetEffectiveDomain(host, "")
    if domain == "" {
        return
    }

    groupKey := fmt.Sprintf("%s:%s", group, config)
    
    globalCacheLock.Lock()
    key := FormatCacheKey(KeyTypeFailed, config, group, domain)
    dataCache.Set(key, time.Now())
    failedCount := 0
    failedPrefix := FormatCacheKey(KeyTypeFailed, config, group, "")
    failedDomains := dataCache.FilterByKeyPrefix(failedPrefix)
    failedCount = len(failedDomains)
    globalCacheLock.Unlock()

    if failedCount >= NetworkFailureThreshold {
        s.failureStatusLock.Lock()
        wasFailure := s.networkFailureStatus[groupKey]
        s.networkFailureStatus[groupKey] = true
        s.successCount[groupKey] = 0
        if !wasFailure {
            log.Warnln("[SmartStore] Network failure detected for group [%s:%s] after %d consecutive failures",
                group, config, failedCount)
            s.lastNetworkFailure[groupKey] = time.Now()
        }
        s.failureStatusLock.Unlock()
    }
}

// 标记连接成功
func (s *Store) MarkConnectionSuccess(group, config string) {
    if s == nil {
        return
    }

    groupKey := fmt.Sprintf("%s:%s", group, config)
    s.failureStatusLock.Lock()
    defer s.failureStatusLock.Unlock()

    if s.networkFailureStatus[groupKey] {
        if s.successCount == nil {
            s.successCount = make(map[string]int)
        }

        s.successCount[groupKey]++

        if s.successCount[groupKey] >= 3 || time.Since(s.lastNetworkFailure[groupKey]) > 30*time.Second {
            s.networkFailureStatus[groupKey] = false
            log.Infoln("[SmartStore] Network recovered for group [%s:%s] after %d successful connections",
                group, config, s.successCount[groupKey])
            s.successCount[groupKey] = 0
            
            globalCacheLock.Lock()
            failedPrefix := FormatCacheKey(KeyTypeFailed, config, group, "")
            dataCache.RemoveByKeyPrefix(failedPrefix)
            globalCacheLock.Unlock()
        }
    }
}

// 检查网络故障状态
func (s *Store) CheckNetworkFailure(group, config string) bool {
    if s == nil {
        return false
    }

    groupKey := fmt.Sprintf("%s:%s", group, config)
    s.failureStatusLock.RLock()
    defer s.failureStatusLock.RUnlock()

    return s.networkFailureStatus[groupKey]
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
                log.Warnln("[SmartStore] Failed to delete domain %s: %v", info.domain, err)
            }
            // 同时清理预取结果（缓存和DB）
            s.DeleteCacheResult(KeyTypePrefetch, group, config, info.domain)
            prefetchDBKey := FormatDBKey("smart", KeyTypePrefetch, config, group, info.domain)
            _ = s.DeleteByPath(prefetchDBKey)
        }

        log.Debugln("[SmartStore] Cleaned up %d old domain records, keeping the latest %d (group %s)",
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
                log.Warnln("[SmartStore] Failed to delete expired domain %s: %v", domain, err)
            }
        }
    }

    if len(expiredDomains) > 0 {
        log.Debugln("[SmartStore] Deleted %d expired domains for group %s", len(expiredDomains), group)
    }

    return nil
}