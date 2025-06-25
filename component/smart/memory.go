package smart

import (
    "encoding/json"
    "math"
    "strings"
    "time"

    "github.com/metacubex/mihomo/common/lru"
    "github.com/metacubex/mihomo/log"
)

func InitializeCache() {
    globalCacheParams.mutex.Lock()
    defer globalCacheParams.mutex.Unlock()
    
    if dataCache != nil && domainResultCache != nil && StatsCache != nil {
        return
    }
    
    globalCacheParams.BatchSaveThreshold = MinBatchThreshLimit
    globalCacheParams.MaxDomains = MinDomainsLimit
    globalCacheParams.PrefetchLimit = MinPrefetchDomainsLimit
    globalCacheParams.CacheMaxSize = MinCacheSizeLimit
    globalCacheParams.MemoryLimit = getSystemMemoryLimit()

    dataCache = lru.New[string, interface{}](
        lru.WithSize[string, interface{}](globalCacheParams.CacheMaxSize),
        lru.WithAge[string, interface{}](CacheMaxAge),
    )
    
    domainResultCache = lru.New[string, string](
        lru.WithSize[string, string](globalCacheParams.CacheMaxSize),
        lru.WithAge[string, string](CacheMaxAge),
    )

    StatsCache = lru.New[string, *StatsRecord](
        lru.WithSize[string, *StatsRecord](globalCacheParams.CacheMaxSize),
        lru.WithAge[string, *StatsRecord](CacheMaxAge),
    )
}

// 从全局缓存获取值
func GetCacheValue(cacheKey string) (interface{}, bool) {
    globalCacheLock.RLock()
    defer globalCacheLock.RUnlock()
    return dataCache.Get(cacheKey)
}

// 设置全局缓存值
func SetCacheValue(cacheKey string, value interface{}) {
    globalCacheLock.Lock()
    defer globalCacheLock.Unlock()
    dataCache.Set(cacheKey, value)
}

// 删除全局缓存值
func DeleteCacheValue(cacheKey string) {
    globalCacheLock.Lock()
    defer globalCacheLock.Unlock()
    dataCache.Delete(cacheKey)
}

// 按前缀获取缓存值
func GetCacheValuesByPrefix(prefix string) map[string]interface{} {
    globalCacheLock.RLock()
    defer globalCacheLock.RUnlock()
    return dataCache.FilterByKeyPrefix(prefix)
}

// 按前缀移除缓存值
func RemoveCacheValuesByPrefix(prefix string) {
    globalCacheLock.Lock()
    defer globalCacheLock.Unlock()
    dataCache.RemoveByKeyPrefix(prefix)
}

// 存储预取结果
func (s *Store) StorePrefetchResult(group, config string, target string, weightType string, proxyName string) {
    if target == "" || proxyName == "" {
        return
    }
    
    cacheKey := FormatCacheKey(KeyTypePrefetch, config, group, target)
    
    var prefetchMap map[string]string
    existingValue, found := GetCacheValue(cacheKey)
    if found {
        switch v := existingValue.(type) {
        case map[string]string:
            prefetchMap = make(map[string]string, len(v)+1)
            for k, v := range v {
                prefetchMap[k] = v
            }
        default:
            prefetchMap = make(map[string]string)
        }
    } else {
        prefetchMap = make(map[string]string)
    }
    
    prefetchMap[weightType] = proxyName
    SetCacheValue(cacheKey, prefetchMap)

    data, err := json.Marshal(prefetchMap)
    if err != nil {
        return
    }
    
    globalQueueMutex.Lock()
    globalOperationQueue = append(globalOperationQueue, StoreOperation{
        Type:   OpSavePrefetch,
        Group:  group,
        Config: config,
        Domain: target,
        Data:   data,
    })
    
    globalCacheParams.mutex.RLock()
    needFlush := len(globalOperationQueue) >= globalCacheParams.BatchSaveThreshold
    globalCacheParams.mutex.RUnlock()
    globalQueueMutex.Unlock()
    
    if needFlush {
        go s.FlushQueue(true)
    }
}

// 获取预取结果
func (s *Store) GetPrefetchResult(group, config string, target string, weightType string) string {
    if target == "" {
        return ""
    }
    
    cacheKey := FormatCacheKey(KeyTypePrefetch, config, group, target)
    
    var proxyName string
    
    if value, ok := GetCacheValue(cacheKey); ok {
        switch v := value.(type) {
        case map[string]string:
            if name, exists := v[weightType]; exists {
                proxyName = name
            }
        default:
        }
    }
    
    if proxyName != "" {
        return proxyName
    }
    
    dbKey := FormatDBKey("smart", KeyTypePrefetch, config, group, target)
    
    data, err := s.DBViewGetItem(dbKey)
    if err == nil && data != nil {
        var prefetchMap map[string]string
        if err = json.Unmarshal(data, &prefetchMap); err != nil {
            return ""
        }
    
        proxyName = prefetchMap[weightType]
        if proxyName == "" {
            return ""
        }
    
        nodeStateData, _ := s.GetNodeStates(group, config)
        if data, exists := nodeStateData[proxyName]; exists {
            var state NodeState
            if json.Unmarshal(data, &state) == nil && 
            !state.BlockedUntil.IsZero() && 
            state.BlockedUntil.After(time.Now()) {
                return ""
            }
        }
        
        SetCacheValue(cacheKey, prefetchMap)
        
        return proxyName
    }

    return ""
}

// 预加载所有预计算结果
func (s *Store) LoadAllPrefetchResults(group, config string, limit int) int {
    var (
        loadCount     int
        parseFailures int
    )
    
    if group == "" || config == "" {
        return 0
    }
    
    prefetchPrefix := FormatDBKey("smart", KeyTypePrefetch, config, group)
    results, err := s.DBViewPrefixScan(prefetchPrefix, 0, limit*2)
    if err != nil {
        log.Warnln("[SmartStore] Failed to load prefetch results: %v", err)
        return 0
    }

    for path, v := range results {
        parts := strings.Split(path, "/")
        if len(parts) < 5 {
            continue
        }
        
        domain := parts[4]
        if domain == "" {
            continue
        }
        
        var prefetchMap map[string]string
        if err := json.Unmarshal(v, &prefetchMap); err != nil {
            parseFailures++
            continue
        }
        
        cacheKey := FormatCacheKey(KeyTypePrefetch, config, group, domain)
        SetCacheValue(cacheKey, prefetchMap)
        
        loadCount++
        
        if loadCount >= limit {
            break
        }
    }

    if err != nil {
        log.Warnln("[SmartStore] Failed to load prefetch results: %v", err)
    }
    
    return loadCount
}

// 加载域名到缓存
func (s *Store) LoadDomainsToCache(group, config string, domains []string, maxDomains int) int {
    if len(domains) == 0 {
        return 0
    }
    
    if maxDomains > 0 && len(domains) > maxDomains {
        domains = domains[:maxDomains]
    }
    
    var loadedCount int
    var batchSize = 20
    weightTypes := []string{WeightTypeTCP, WeightTypeUDP}
    
    for i := 0; i < len(domains); i += batchSize {
        end := i + batchSize
        if end > len(domains) {
            end = len(domains)
        }
        
        batch := domains[i:end]
        
        for _, domain := range batch {
            for _, weightType := range weightTypes {
                stats, err := s.GetStatsForDomain(group, config, domain)
                if err != nil || len(stats) == 0 {
                    continue
                }
                
                loadedCount++
                
                bestNode, _, _, _ := s.GetBestProxyForTarget(group, config, domain, weightType)
                if bestNode != "" {
                    s.StorePrefetchResult(group, config, domain, weightType, bestNode)
                }
            }
        }
    }
    
    return loadedCount
}

// 加载ASN到缓存
func (s *Store) LoadASNsToCache(group, config string, asns []string) int {
    if len(asns) == 0 {
        return 0
    }
    
    var loadedCount int
    var batchSize = 20

    weightTypes := []string{WeightTypeTCPASN, WeightTypeUDPASN}
    
    for i := 0; i < len(asns); i += batchSize {
        end := i + batchSize
        if end > len(asns) {
            end = len(asns)
        }
        
        batch := asns[i:end]
        
        for _, asn := range batch {
            for _, baseType := range weightTypes {
                weightType := baseType + ":" + asn
                
                bestNode, _, _, _ := s.GetBestProxyForTarget(group, config, asn, weightType)
                if bestNode != "" {
                    s.StorePrefetchResult(group, config, asn, weightType, bestNode)
                    loadedCount++
                }
            }
        }
    }
    
    return loadedCount
}

func (s *Store) StoreUnwrapResult(group, config string, target string, proxyName string) {
    if target == "" || proxyName == "" {
        return
    }
    
    key := FormatCacheKey(KeyTypeUnwrap, config, group, target)
    SetCacheValue(key, proxyName)
}

func (s *Store) GetUnwrapResult(group, config string, target string) string {
    if target == "" {
        return ""
    }
    
    key := FormatCacheKey(KeyTypeUnwrap, config, group, target)
    if value, ok := GetCacheValue(key); ok {
        if proxyName, isString := value.(string); isString {
            return proxyName
        }
    }
    
    return ""
}

// 删除缓存结果
func (s *Store) DeleteCacheResult(keyType, group, config, key string) {
    if key == "" {
        return
    }

    cachekey := FormatCacheKey(keyType, config, group, key)
    DeleteCacheValue(cachekey)

    dbkey := FormatDBKey("smart", keyType, config, group, key)
    s.DeleteByPath(dbkey)
}

// 调整缓存参数
func (s *Store) AdjustCacheParameters() {
    memoryUsagePercent := GetSystemMemoryUsage()
    memoryUsage := memoryUsagePercent / 100.0
    
    globalCacheParams.mutex.Lock()
    
    isFirstRun := globalCacheParams.LastMemoryUsage == 0
    needAdjust := isFirstRun

    if !isFirstRun {
        memoryChanged := math.Abs(memoryUsage-globalCacheParams.LastMemoryUsage) > 0.1
        needAdjust = memoryChanged || memoryUsage > 0.7
    }
    
    globalCacheParams.LastMemoryUsage = memoryUsage
    
    if !needAdjust && !isFirstRun {
        globalCacheParams.mutex.Unlock()
        return
    }
    
    var newCacheSize int
    var cacheMaxAge int64 = CacheMaxAge
    
    if memoryUsage > 0.9 {
        log.Warnln("[SmartStore] Critical memory pressure detected (%.1f%%), taking emergency measures", memoryUsage*100)
        
        globalCacheParams.MaxDomains = MinDomainsLimit
        globalCacheParams.CacheMaxSize = MinCacheSizeLimit
        globalCacheParams.BatchSaveThreshold = MinBatchThreshLimit
        globalCacheParams.PrefetchLimit = MinPrefetchDomainsLimit
        
        newCacheSize = MinCacheSizeLimit/2
        cacheMaxAge = CacheMaxAge/2
    } else {
        adjustFactor := 4 * memoryUsage * (1 - memoryUsage)
        
        if memoryUsage > 0.85 {
            globalCacheParams.MaxDomains = MinDomainsLimit
            globalCacheParams.CacheMaxSize = MinCacheSizeLimit
            globalCacheParams.BatchSaveThreshold = MinBatchThreshLimit
            globalCacheParams.PrefetchLimit = MinPrefetchDomainsLimit
        } else {
            value := MinDomainsLimit + int(float64(MaxDomainsLimit-MinDomainsLimit)*adjustFactor*MemoryDomainsFactor)
            globalCacheParams.MaxDomains = ClampValue(value, MinDomainsLimit, MaxDomainsLimit)
                
            value = MinCacheSizeLimit + int(float64(MaxCacheSizeLimit-MinCacheSizeLimit)*adjustFactor*MemoryCacheSizeFactor)
            globalCacheParams.CacheMaxSize = ClampValue(value, MinCacheSizeLimit, MaxCacheSizeLimit)
                
            value = MinBatchThreshLimit + int(float64(MaxBatchThreshLimit-MinBatchThreshLimit)*adjustFactor*MemoryBatchFactor)
            globalCacheParams.BatchSaveThreshold = ClampValue(value, MinBatchThreshLimit, MaxBatchThreshLimit)
                
            value = MinPrefetchDomainsLimit + int(float64(MaxPrefetchDomainsLimit-MinPrefetchDomainsLimit)*adjustFactor*MemoryPrefetchFactor)
            globalCacheParams.PrefetchLimit = ClampValue(value, MinPrefetchDomainsLimit, MaxPrefetchDomainsLimit)
        }

        log.Infoln("[SmartStore] Parameters adjusted: MaxDomains=%d, CacheSize=%d, BatchThreshold=%d, PrefetchLimit=%d", 
            globalCacheParams.MaxDomains, globalCacheParams.CacheMaxSize, 
            globalCacheParams.BatchSaveThreshold, globalCacheParams.PrefetchLimit)
        
        newCacheSize = globalCacheParams.CacheMaxSize
    }
    
    globalCacheParams.mutex.Unlock()

    newDataCache := lru.New[string, interface{}](
        lru.WithSize[string, interface{}](newCacheSize),
        lru.WithAge[string, interface{}](cacheMaxAge),
    )
    
    newDomainResultCache := lru.New[string, string](
        lru.WithSize[string, string](newCacheSize),
        lru.WithAge[string, string](cacheMaxAge),
    )
    
    newStatsCache := lru.New[string, *StatsRecord](
        lru.WithSize[string, *StatsRecord](newCacheSize),
        lru.WithAge[string, *StatsRecord](cacheMaxAge),
    )

    var entries map[string]interface{}
    var preserveRatio float64
    
    if dataCache != nil {
        switch {
        case memoryUsage > 0.9: 
            preserveRatio = 0.2
            entries = GetCacheValuesByPrefix(KeyTypeNode + ":")
            prefetchEntries := GetCacheValuesByPrefix(KeyTypePrefetch + ":")
            for k, v := range prefetchEntries {
                entries[k] = v
            }
        case memoryUsage > 0.8:
            preserveRatio = 0.4
            entries = GetCacheValuesByPrefix(KeyTypeNode + ":")
            prefetchEntries := GetCacheValuesByPrefix(KeyTypePrefetch + ":")
            for k, v := range prefetchEntries {
                entries[k] = v
            }
        case memoryUsage > 0.7:
            preserveRatio = 0.6
            entries = GetCacheValuesByPrefix("")
        default:
            preserveRatio = 0.8
            entries = GetCacheValuesByPrefix("")
        }
        
        dataCount := 0
        maxItems := int(float64(len(entries)) * preserveRatio)
        
        // 优先级顺序: 节点状态 > 预计算结果 > 统计数据 > 其他
        for k, v := range entries {
            if dataCount >= maxItems {
                break
            }
            
            // 节点状态数据总是保留
            if strings.HasPrefix(k, KeyTypeNode) || 
               strings.HasPrefix(k, KeyTypePrefetch) {
                newDataCache.Set(k, v)
                dataCount++
                continue
            }
            
            // 其他数据根据容量限制保留
            if dataCount < maxItems {
                newDataCache.Set(k, v)
                dataCount++
            }
        }
        
        log.Infoln("[SmartStore] Cache adjusted: preserved %d/%d items (%.1f%%) under memory pressure %.1f%%", 
                   dataCount, len(entries), float64(dataCount)/float64(len(entries))*100, memoryUsagePercent)
    }
    
    globalCacheLock.Lock()
    dataCache = newDataCache
    domainResultCache = newDomainResultCache
    StatsCache = newStatsCache
    globalCacheLock.Unlock()
    
    globalQueueMutex.Lock()
    queueLength := len(globalOperationQueue)
    globalQueueMutex.Unlock()
    
    if (memoryUsage > 0.8 && queueLength > 0) || 
       (memoryUsage > 0.6 && queueLength > globalCacheParams.BatchSaveThreshold/2) {
        go s.FlushQueue(true)
    }
}

// 预加载频繁使用的数据
func (s *Store) PreloadFrequentData(group, config string, proxies []string) {
    log.Infoln("[SmartStore] Starting data preloading for group [%s], config [%s]", group, config)
    
    memUsage := GetSystemMemoryUsage()

    domainLimit := 50
    asnLimit := 20
    prefetchLoadLimit := 50
    if memUsage < 40.0 {
        domainLimit = 100
        prefetchLoadLimit = 100
        asnLimit = 40
    } else if memUsage > 70.0 {
        domainLimit = 25
        asnLimit = 10
        prefetchLoadLimit = 50
    }

    start := time.Now()
    
    s.LoadAllPrefetchResults(group, config, prefetchLoadLimit)
    
    stateData, err := s.GetNodeStates(group, config)
    nodeStatesCount := 0
    if err == nil {
        nodeStatesCount = len(stateData)
    }
    
    ranking, _ := s.GetNodeWeightRanking(group, config, true, proxies)
    
    domains, err := s.GetActiveDomains(group, config, domainLimit)
    if err == nil && len(domains) > 0 {
        s.LoadDomainsToCache(group, config, domains, domainLimit)
    }
    
    asns := s.GetActiveASNs(group, config, asnLimit)
    if len(asns) > 0 {
        s.LoadASNsToCache(group, config, asns)
    }
    
    log.Infoln("[SmartStore] Preloaded data for group [%s]: %d domains, %d ASNs, %d node stats, %d node rankings, completed in %.2f seconds", 
        group, len(domains), len(asns), nodeStatesCount, len(ranking), time.Since(start).Seconds())
}

// 按级别清理内存缓存
func ClearCacheByLevel(level string, config string, group string) {
    if level == "all" {
        RemoveCacheValuesByPrefix("")
        globalCacheLock.Lock()
        if StatsCache != nil {
            StatsCache.Clear()
        }
        if domainResultCache != nil {
            domainResultCache.Clear()
        }
        globalCacheLock.Unlock()
    } else if level == "config" {
        RemoveCacheValuesByPrefix(FormatCacheKey(KeyTypeUnwrap, config, "", ""))
        RemoveCacheValuesByPrefix(FormatCacheKey(KeyTypeFailed, config, "", ""))
        RemoveCacheValuesByPrefix(FormatCacheKey(KeyTypeNode, config, "", ""))
        RemoveCacheValuesByPrefix(FormatCacheKey(KeyTypeStats, config, "", ""))
        RemoveCacheValuesByPrefix(FormatCacheKey(KeyTypeRanking, config, "", ""))
        RemoveCacheValuesByPrefix(FormatCacheKey(KeyTypePrefetch, config, "", ""))
        
        globalCacheLock.Lock()
        if StatsCache != nil {
            StatsCache.RemoveByKeyPrefix(":" + config + ":")
        }
        globalCacheLock.Unlock()
    } else if level == "group" {
        RemoveCacheValuesByPrefix(FormatCacheKey(KeyTypeUnwrap, config, group, ""))
        RemoveCacheValuesByPrefix(FormatCacheKey(KeyTypeFailed, config, group, ""))
        RemoveCacheValuesByPrefix(FormatCacheKey(KeyTypeNode, config, group, ""))
        RemoveCacheValuesByPrefix(FormatCacheKey(KeyTypeStats, config, group, ""))
        RemoveCacheValuesByPrefix(FormatCacheKey(KeyTypeRanking, config, group, ""))
        RemoveCacheValuesByPrefix(FormatCacheKey(KeyTypePrefetch, config, group, ""))
        
        globalCacheLock.Lock()
        if StatsCache != nil {
            StatsCache.RemoveByKeyPrefix(":" + config + ":" + group + ":")
        }
        globalCacheLock.Unlock()
    }
}

// 从数据库结果更新缓存
func UpdateCacheFromDBResult(fullPath string, data []byte) {
    if data == nil || len(data) == 0 || fullPath == "" {
        return
    }

    pathParts := strings.Split(fullPath, "/")
    if len(pathParts) >= 3 && pathParts[0] == "smart" {
        keyType := pathParts[1]
        config := pathParts[2]
        group := ""
        if len(pathParts) >= 4 {
            group = pathParts[3]
        }
        
        var cacheKey string
        
        if len(pathParts) >= 6 && keyType == KeyTypeStats {
            // smart/stats/config/group/domain/node
            cacheKey = FormatCacheKey(keyType, config, group, pathParts[4], pathParts[5])
        } else if len(pathParts) >= 5 && keyType != KeyTypeStats {
            // smart/keytype/config/group/target
            cacheKey = FormatCacheKey(keyType, config, group, pathParts[4])
        }
        
        if cacheKey != "" {
            var cacheValue interface{}
            
            switch keyType {
            case KeyTypeStats:
                var record StatsRecord
                if json.Unmarshal(data, &record) == nil {
                    cacheValue = record
                } else {
                    log.Debugln("[SmartStore] Failed to unmarshal stats record for key %s", cacheKey)
                    cacheValue = data
                }
            case KeyTypeNode:
                var state NodeState
                if json.Unmarshal(data, &state) == nil {
                    cacheValue = state
                } else {
                    log.Debugln("[SmartStore] Failed to unmarshal node state for key %s", cacheKey)
                    cacheValue = data
                }
            case KeyTypePrefetch:
                var prefetchMap map[string]string
                if json.Unmarshal(data, &prefetchMap) == nil {
                    cacheValue = prefetchMap
                } else {
                    log.Debugln("[SmartStore] Failed to unmarshal prefetch map for key %s", cacheKey)
                    cacheValue = data
                }
            case KeyTypeRanking:
                var rankingData RankingData
                if json.Unmarshal(data, &rankingData) == nil {
                    cacheValue = rankingData
                } else {
                    log.Debugln("[SmartStore] Failed to unmarshal ranking data for key %s", cacheKey)
                    cacheValue = data
                }
            default:
                cacheValue = data
            }
            
            SetCacheValue(cacheKey, cacheValue)
        }
    }
}

// 从数据库路径提取缓存键
func ExtractCachePrefixFromPath(pathStr string) string {
    pathParts := strings.Split(pathStr, "/")
    
    if len(pathParts) >= 3 && pathParts[0] == "smart" {
        keyType := pathParts[1]
        config := pathParts[2]
        group := ""
        if len(pathParts) >= 4 {
            group = pathParts[3]
        }
        
        if len(pathParts) >= 6 && keyType == KeyTypeStats {
            // smart/stats/config/group/domain/node
            return FormatCacheKey(keyType, config, group, pathParts[4], pathParts[5])
        } else if len(pathParts) >= 5 && keyType != KeyTypeStats {
            // smart/keytype/config/group/target
            return FormatCacheKey(keyType, config, group, pathParts[4])
        } else if len(pathParts) == 4 {
            // smart/keytype/config/group
            return FormatCacheKey(keyType, config, group)
        } else if len(pathParts) == 3 {
            // smart/keytype/config
            return FormatCacheKey(keyType, config, "")
        }
    }
    
    return ""
}