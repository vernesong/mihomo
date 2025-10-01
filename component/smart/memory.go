package smart

import (
	"encoding/json"
	"math"
	"strings"
	"time"

	"github.com/metacubex/mihomo/common/lru"
	"github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
	"github.com/metacubex/mihomo/tunnel"
)

func InitCache() {
	globalCacheParams.mutex.Lock()
	defer globalCacheParams.mutex.Unlock()

	if dataCache != nil {
		return
	}

	globalCacheParams.BatchSaveThreshold = MinBatchThreshLimit
	globalCacheParams.MaxDomains = MinDomainsLimit
	globalCacheParams.PrefetchLimit = MinPrefetchDomainsLimit
	globalCacheParams.CacheMaxSize = MinDomainsLimit + MinPrefetchDomainsLimit
	globalCacheParams.MemoryLimit = getSystemMemoryLimit()

	dataCache = lru.New[string, interface{}](
		lru.WithSize[string, interface{}](globalCacheParams.CacheMaxSize),
		lru.WithAge[string, interface{}](CacheMaxAge),
	)

	domainCache = lru.New[string, string](
		lru.WithSize[string, string](globalCacheParams.MaxDomains),
		lru.WithAge[string, string](CacheMaxAge),
	)

	prefixCountCache = lru.New[string, int](
		lru.WithSize[string, int](2000),
		lru.WithAge[string, int](int64(300)),
	)

	nodeStatesCache = lru.New[string, map[string][]byte](
		lru.WithSize[string, map[string][]byte](1000),
		lru.WithAge[string, map[string][]byte](int64(300)),
	)
}

// 从全局缓存获取值
func GetCacheValue(cacheKey string) (interface{}, bool) {
	return dataCache.Get(cacheKey)
}

// 设置全局缓存值
func SetCacheValue(cacheKey string, value interface{}) {
	dataCache.Set(cacheKey, value)
}

// 删除全局缓存值
func DeleteCacheValue(cacheKey string) {
	dataCache.Delete(cacheKey)
}

// 按前缀获取缓存值
func GetCacheValuesByPrefix(prefix string) map[string]interface{} {
	return dataCache.FilterByKeyPrefix(prefix)
}

// 按前缀移除缓存值
func RemoveCacheValuesByPrefix(prefix string) {
	dataCache.RemoveByKeyPrefix(prefix)
}

// 存储预取结果
func (s *Store) StorePrefetchResult(group, config string, target string, weightType string, proxyNames []string, weights []float64) {
	if target == "" || len(proxyNames) == 0 {
		return
	}

	cacheKey := FormatCacheKey(KeyTypePrefetch, config, group, target)

	var prefetchMap PrefetchMap
	existingValue, found := GetCacheValue(cacheKey)
	if found {
		switch existingMap := existingValue.(type) {
		case PrefetchMap:
			prefetchMap = make(PrefetchMap, len(existingMap)+1)
			for wt, nodeWeight := range existingMap {
				prefetchMap[wt] = nodeWeight
			}
		case []byte:
			var pm PrefetchMap
			if json.Unmarshal(existingMap, &pm) == nil {
				prefetchMap = make(PrefetchMap, len(pm)+1)
				for wt, nodeWeight := range pm {
					prefetchMap[wt] = nodeWeight
				}
			} else {
				prefetchMap = make(PrefetchMap)
			}
		default:
			prefetchMap = make(PrefetchMap)
		}
	} else {
		prefetchMap = make(PrefetchMap)
	}

	prefetchMap[weightType] = NodeWithWeight{
		Nodes:   proxyNames,
		Weights: weights,
	}

	data, err := json.Marshal(prefetchMap)
	if err != nil {
		return
	}

	appendToGlobalQueue(StoreOperation{
		Type:   OpSavePrefetch,
		Group:  group,
		Config: config,
		Domain: target,
		Data:   data,
	})

	globalCacheParams.mutex.RLock()
	needFlush := len(getGlobalQueueSnapshot()) >= globalCacheParams.BatchSaveThreshold
	globalCacheParams.mutex.RUnlock()

	if needFlush {
		go s.FlushQueue(true)
	}
}

// 获取预取结果
func (s *Store) GetPrefetchResult(group, config string, target string, weightType string) ([]string, []float64) {
	if target == "" {
		return nil, nil
	}

	ops := getGlobalQueueSnapshot()
	for _, op := range ops {
		if op.Type == OpSavePrefetch && op.Group == group && op.Config == config && op.Domain == target {
			var prefetchMap PrefetchMap
			if err := json.Unmarshal(op.Data, &prefetchMap); err == nil {
				if res, exists := prefetchMap[weightType]; exists {
					if len(res.Nodes) > 0 && len(res.Weights) == len(res.Nodes) {
						return res.Nodes, res.Weights
					}
				}
			}
		}
	}

	cacheKey := FormatCacheKey(KeyTypePrefetch, config, group, target)
	if value, ok := GetCacheValue(cacheKey); ok {
		switch v := value.(type) {
		case PrefetchMap:
			if res, exists := v[weightType]; exists {
				if len(res.Nodes) > 0 && len(res.Weights) == len(res.Nodes) {
					return res.Nodes, res.Weights
				}
			}
		case []byte:
			var pm PrefetchMap
			if json.Unmarshal(v, &pm) == nil {
				if res, exists := pm[weightType]; exists {
					if len(res.Nodes) > 0 && len(res.Weights) == len(res.Nodes) {
						return res.Nodes, res.Weights
					}
				}
			}
		}
	}

	dbKey := FormatDBKey("smart", KeyTypePrefetch, config, group, target)
	data, err := s.DBViewGetItem(dbKey)
	if err == nil && data != nil {
		var prefetchMap PrefetchMap
		if err = json.Unmarshal(data, &prefetchMap); err == nil {
			SetCacheValue(cacheKey, data)
			if res, exists := prefetchMap[weightType]; exists {
				if len(res.Nodes) > 0 && len(res.Weights) == len(res.Nodes) {
					return res.Nodes, res.Weights
				}
			}
		}
	}

	return nil, nil
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
	results, err := s.DBViewPrefixScan(prefetchPrefix, limit)
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

		var prefetchMap PrefetchMap
		if err := json.Unmarshal(v, &prefetchMap); err != nil {
			parseFailures++
			continue
		}

		cacheKey := FormatCacheKey(KeyTypePrefetch, config, group, domain)
		SetCacheValue(cacheKey, v)

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

func (s *Store) StoreUnwrapResult(group, config string, target string, proxyNames []string) {
	if target == "" || len(proxyNames) == 0 {
		return
	}

	key := FormatCacheKey(KeyTypeUnwrap, config, group, target)
	SetCacheValue(key, proxyNames)
}

func (s *Store) GetUnwrapResult(group, config string, target string) []string {
	if target == "" {
		return nil
	}

	key := FormatCacheKey(KeyTypeUnwrap, config, group, target)
	if value, ok := GetCacheValue(key); ok {
		if proxyNames, ok := value.([]string); ok {
			return proxyNames
		}
	}

	return nil
}

// 删除缓存结果
func (s *Store) DeleteCacheResult(keyType, config, group, key1, key2 string) {
	var cachePrefix string
	var dbPrefix string

	if key1 == "" && key2 == "" {
		cachePrefix = FormatCacheKey(keyType, config, group, "")
		dbPrefix = FormatDBKey("smart", keyType, config, group, "")
		RemoveCacheValuesByPrefix(cachePrefix)
		s.DeleteByPath(dbPrefix)
		return
	}

	if key2 != "" {
		cachePrefix = FormatCacheKey(keyType, config, group, key1, key2)
		dbPrefix = FormatDBKey("smart", keyType, config, group, key1, key2)
		DeleteCacheValue(cachePrefix)
		s.DeleteByPath(dbPrefix)
		return
	}

	cachePrefix = FormatCacheKey(keyType, config, group, key1)
	dbPrefix = FormatDBKey("smart", keyType, config, group, key1)
	RemoveCacheValuesByPrefix(cachePrefix)
	s.DeleteByPath(dbPrefix)
}

// 调整缓存参数
func (s *Store) AdjustCacheParameters() {
	memoryUsagePercent := GetSystemMemoryUsage()
	memoryUsage := memoryUsagePercent / 100.0

	smartGroupCount := 0
	for _, proxy := range tunnel.Proxies() {
		if proxy.Type() == constant.Smart {
			smartGroupCount++
		}
	}

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
		globalCacheParams.MaxDomains = MinDomainsLimit
		globalCacheParams.BatchSaveThreshold = MinBatchThreshLimit
		globalCacheParams.PrefetchLimit = MinPrefetchDomainsLimit
		globalCacheParams.CacheMaxSize = (globalCacheParams.MaxDomains + globalCacheParams.PrefetchLimit) * smartGroupCount
		newCacheSize = globalCacheParams.CacheMaxSize / 2
		cacheMaxAge = CacheMaxAge / 2
	} else {
		adjustFactor := 4 * memoryUsage * (1 - memoryUsage)

		if memoryUsage > 0.85 {
			globalCacheParams.MaxDomains = MinDomainsLimit
			globalCacheParams.BatchSaveThreshold = MinBatchThreshLimit
			globalCacheParams.PrefetchLimit = MinPrefetchDomainsLimit
			globalCacheParams.CacheMaxSize = (globalCacheParams.MaxDomains + globalCacheParams.PrefetchLimit) * smartGroupCount
		} else {
			value := MinDomainsLimit + int(float64(MaxDomainsLimit-MinDomainsLimit)*adjustFactor*MemoryDomainsFactor)
			globalCacheParams.MaxDomains = ClampValue(value, MinDomainsLimit, MaxDomainsLimit)

			value = MinBatchThreshLimit + int(float64(MaxBatchThreshLimit-MinBatchThreshLimit)*adjustFactor*MemoryBatchFactor)
			globalCacheParams.BatchSaveThreshold = ClampValue(value, MinBatchThreshLimit, MaxBatchThreshLimit)

			value = MinPrefetchDomainsLimit + int(float64(MaxPrefetchDomainsLimit-MinPrefetchDomainsLimit)*adjustFactor*MemoryPrefetchFactor)
			globalCacheParams.PrefetchLimit = ClampValue(value, MinPrefetchDomainsLimit, MaxPrefetchDomainsLimit)

			globalCacheParams.CacheMaxSize = (globalCacheParams.MaxDomains + globalCacheParams.PrefetchLimit) * smartGroupCount
		}

		newCacheSize = globalCacheParams.CacheMaxSize
	}

	log.Infoln("[SmartStore] Parameters adjusted: MaxDomains=%d, CacheSize=%d, BatchThreshold=%d, PrefetchLimit=%d",
		globalCacheParams.MaxDomains, globalCacheParams.CacheMaxSize,
		globalCacheParams.BatchSaveThreshold, globalCacheParams.PrefetchLimit)

	globalCacheParams.mutex.Unlock()

	newDataCache := lru.New[string, interface{}](
		lru.WithSize[string, interface{}](newCacheSize),
		lru.WithAge[string, interface{}](cacheMaxAge),
	)

	domainCache = lru.New[string, string](
		lru.WithSize[string, string](globalCacheParams.MaxDomains),
		lru.WithAge[string, string](cacheMaxAge),
	)

	prefixCountCache = lru.New[string, int](
		lru.WithSize[string, int](1000),
		lru.WithAge[string, int](int64(300)),
	)

	nodeStatesCache = lru.New[string, map[string][]byte](
		lru.WithSize[string, map[string][]byte](1000),
		lru.WithAge[string, map[string][]byte](int64(300)),
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

			if strings.HasPrefix(k, KeyTypeNode) ||
				strings.HasPrefix(k, KeyTypePrefetch) ||
				strings.HasPrefix(k, KeyTypeStats) ||
				strings.HasPrefix(k, KeyTypeRanking) {

				if bv, ok := v.([]byte); ok && len(bv) > 0 {
					newDataCache.Set(k, bv)
					dataCount++
				}
				continue
			}

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
	globalCacheLock.Unlock()

	queueLength := len(getGlobalQueueSnapshot())

	if (memoryUsage > 0.8 && queueLength > 0) ||
		(memoryUsage > 0.6 && queueLength > globalCacheParams.BatchSaveThreshold/2) {
		go s.FlushQueue(true)
	}
}

// 预加载数据
func (s *Store) PreloadFrequentData(group, config string, proxies []string) {
	log.Infoln("[SmartStore] Starting data preloading for group [%s], config [%s]", group, config)

	globalCacheParams.mutex.RLock()
	domainLimit := globalCacheParams.MaxDomains / 2
	prefetchLoadLimit := globalCacheParams.PrefetchLimit
	globalCacheParams.mutex.RUnlock()

	start := time.Now()

	prefetchCount := s.LoadAllPrefetchResults(group, config, prefetchLoadLimit)

	stateData, err := s.GetNodeStates(group, config)
	nodeStatesCount := 0
	if err == nil {
		nodeStatesCount = len(stateData)
	}

	ranking, _ := s.GetNodeWeightRanking(group, config, true, proxies)

	domains := s.GetActiveDomains(group, config, domainLimit)

	log.Infoln("[SmartStore] Preloaded data for group [%s]: %d domains, %d node stats, %d prefetch results, %d node rankings, completed in %.2f seconds",
		group, len(domains), nodeStatesCount, prefetchCount, len(ranking), time.Since(start).Seconds())
}

// 按级别清理内存缓存
func ClearCacheByLevel(level string, config string, group string) {
	if level == "all" {
		RemoveCacheValuesByPrefix("")
	} else if level == "config" {
		RemoveCacheValuesByPrefix(FormatCacheKey(KeyTypeUnwrap, config, ""))
		RemoveCacheValuesByPrefix(FormatCacheKey(KeyTypeFailed, config, ""))
		RemoveCacheValuesByPrefix(FormatCacheKey(KeyTypeNode, config, ""))
		RemoveCacheValuesByPrefix(FormatCacheKey(KeyTypeStats, config, ""))
		RemoveCacheValuesByPrefix(FormatCacheKey(KeyTypeRanking, config, ""))
		RemoveCacheValuesByPrefix(FormatCacheKey(KeyTypePrefetch, config, ""))
	} else if level == "group" {
		RemoveCacheValuesByPrefix(FormatCacheKey(KeyTypeUnwrap, config, group, ""))
		RemoveCacheValuesByPrefix(FormatCacheKey(KeyTypeFailed, config, group, ""))
		RemoveCacheValuesByPrefix(FormatCacheKey(KeyTypeNode, config, group, ""))
		RemoveCacheValuesByPrefix(FormatCacheKey(KeyTypeStats, config, group, ""))
		RemoveCacheValuesByPrefix(FormatCacheKey(KeyTypeRanking, config, group, ""))
		RemoveCacheValuesByPrefix(FormatCacheKey(KeyTypePrefetch, config, group, ""))
	}

	domainCache.Clear()

	prefixCountCache.Clear()

	nodeStatesCache.Clear()
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
