package smart

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/metacubex/mihomo/common/lru"
	C "github.com/metacubex/mihomo/constant"
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
	globalCacheParams.MaxTargets = MinTargetsLimit
	globalCacheParams.PrefetchLimit = MinPrefetchTargetsLimit
	globalCacheParams.CacheMaxSize = MinTargetsLimit + MinPrefetchTargetsLimit
	globalCacheParams.MemoryLimit = getSystemMemoryLimit()

	dataCache = lru.New[string, interface{}](
		lru.WithSize[string, interface{}](globalCacheParams.CacheMaxSize),
		lru.WithAge[string, interface{}](CacheMaxAge),
	)

	targetCache = lru.New[string, string](
		lru.WithSize[string, string](globalCacheParams.MaxTargets / 2),
		lru.WithAge[string, string](CacheMaxAge),
	)

	prefixCountCache = lru.New[string, int](
		lru.WithSize[string, int](globalCacheParams.MaxTargets / 2),
		lru.WithAge[string, int](300),
	)

	nodeStatesCache = lru.New[string, map[string][]byte](
		lru.WithSize[string, map[string][]byte](2000),
		lru.WithAge[string, map[string][]byte](120),
	)

	unwrapCache = lru.New[string, UnwrapMap](
		lru.WithSize[string, UnwrapMap](globalCacheParams.PrefetchLimit / 2),
	)

	recordCache = lru.New[string, *AtomicStatsRecord](
		lru.WithSize[string, *AtomicStatsRecord](globalCacheParams.MaxTargets / 2),
		lru.WithAge[string, *AtomicStatsRecord](CacheMaxAge),
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
func (s *Store) StorePrefetchResult(group, config string, target string, asnNumber string, isUDP bool, proxyNames []string, weights []float64, oldNodesCount int) {
	if target == "" || len(proxyNames) == 0 {
		return
	}

	targetCacheKey := FormatCacheKey(KeyTypePrefetch, config, group, target)

	var pm PrefetchMap
	if value, found := GetCacheValue(targetCacheKey); found {
		if bv, ok := value.([]byte); ok {
			json.Unmarshal(bv, &pm)
		}
	}
	nodeWeight := NodesWithWeights{Nodes: proxyNames, Weights: weights}
	if isUDP {
		pm.UDP = nodeWeight
	} else {
		pm.TCP = nodeWeight
	}
	if oldNodesCount == 0 {
		pm.UpdatedTime = time.Now().Unix()
	}
	data, err := json.Marshal(pm)
	if err != nil {
		return
	}
	SetCacheValue(targetCacheKey, data)

	appendToGlobalQueue(StoreOperation{
		Type:   OpSavePrefetch,
		Group:  group,
		Config: config,
		Target: target,
		Data:   data,
	})

	if asnNumber != "" && !CdnASNs[asnNumber] {
		asnCacheKey := FormatCacheKey(KeyTypePrefetch, config, group, asnNumber)
		var asnPm PrefetchMap
		if asnValue, asnFound := GetCacheValue(asnCacheKey); asnFound {
			if asnBv, asnOk := asnValue.([]byte); asnOk {
				json.Unmarshal(asnBv, &asnPm)
			}
		}
		if isUDP {
			asnPm.RefUDP = targetCacheKey
		} else {
			asnPm.RefTCP = targetCacheKey
		}
		if oldNodesCount == 0 {
			asnPm.UpdatedTime = time.Now().Unix()
		}
		asnData, asnErr := json.Marshal(asnPm)
		if asnErr != nil {
			return
		}
		SetCacheValue(asnCacheKey, asnData)

		appendToGlobalQueue(StoreOperation{
			Type:   OpSavePrefetch,
			Group:  group,
			Config: config,
			Target: asnNumber,
			Data:   asnData,
		})
	}

	needFlush := len(getGlobalQueueSnapshot()) >= GetBatchSaveThreshold()
	if needFlush {
		go s.FlushQueue(true)
	}
}

// 获取预取结果
func (s *Store) GetPrefetchResult(group, config string, target string, asnNumber string, isUDP bool) ([]string, []float64) {
	if target == "" {
		return nil, nil
	}

	findResult := func(pm PrefetchMap) ([]string, []float64) {
		if pm.UpdatedTime > 0 && time.Now().Unix() - pm.UpdatedTime > PrefetchCacheMaxAge {
			return nil, nil
		}
		var res NodesWithWeights
		if isUDP {
			res = pm.UDP
		} else {
			res = pm.TCP
		}
		if len(res.Nodes) > 0 && len(res.Weights) == len(res.Nodes) {
			return res.Nodes, res.Weights
		}
		return nil, nil
	}

	getFromCache := func(key string) (PrefetchMap, bool) {
		if value, found := GetCacheValue(key); found {
			if bv, ok := value.([]byte); ok {
				var pm PrefetchMap
				if json.Unmarshal(bv, &pm) == nil {
					return pm, true
				}
			}
		}
		return PrefetchMap{}, false
	}

	getFromQueue := func(ops []StoreOperation, group, config, target string) (PrefetchMap, bool) {
		for _, op := range ops {
			if op.Type == OpSavePrefetch && op.Group == group && op.Config == config && op.Target == target {
				var pm PrefetchMap
				if json.Unmarshal(op.Data, &pm) == nil {
					return pm, true
				}
			}
		}
		return PrefetchMap{}, false
	}

	getFromDB := func(pathPrefix string) (PrefetchMap, bool) {
		rawResult, err := s.GetSubBytesByPath(pathPrefix)
		if err != nil {
			return PrefetchMap{}, false
		}
		for _, data := range rawResult {
			var pm PrefetchMap
			if json.Unmarshal(data, &pm) == nil {
				return pm, true
			}
		}
		return PrefetchMap{}, false
	}

	getRefKey := func(pm PrefetchMap, isUDP bool) string {
		if isUDP {
			return pm.RefUDP
		}
		return pm.RefTCP
	}

	ops := getGlobalQueueSnapshot()

	// ASN
	if asnNumber != "" && !CdnASNs[asnNumber] {
		asnKey := FormatCacheKey(KeyTypePrefetch, config, group, asnNumber)
		asnPathPrefix := FormatDBKey("smart", KeyTypePrefetch, config, group, asnNumber)

		if pm, ok := getFromCache(asnKey); ok {
			if refKey := getRefKey(pm, isUDP); refKey != "" {
				if refPm, ok := getFromCache(refKey); ok {
					if nodes, weights := findResult(refPm); nodes != nil {
						return nodes, weights
					}
				}
			}
		}

		if pm, ok := getFromQueue(ops, group, config, asnNumber); ok {
			if refKey := getRefKey(pm, isUDP); refKey != "" {
				parts := strings.Split(refKey, ":")
				if len(parts) >= 4 {
					parsedTarget := strings.Join(parts[3:], ":")
					if refPm, ok := getFromQueue(ops, group, config, parsedTarget); ok {
						if nodes, weights := findResult(refPm); nodes != nil {
							return nodes, weights
						}
					}
				}
			}
		}

		if pm, ok := getFromDB(asnPathPrefix); ok {
			if refKey := getRefKey(pm, isUDP); refKey != "" {
				parts := strings.Split(refKey, ":")
				if len(parts) >= 4 {
					parsedTarget := strings.Join(parts[3:], ":")
					targetPathPrefix := FormatDBKey("smart", KeyTypePrefetch, config, group, parsedTarget)
					if refPm, ok := getFromDB(targetPathPrefix); ok {
						if nodes, weights := findResult(refPm); nodes != nil {
							return nodes, weights
						}
					}
				}
			}
		}
	}

	// target
	targetKey := FormatCacheKey(KeyTypePrefetch, config, group, target)
	pathPrefix := FormatDBKey("smart", KeyTypePrefetch, config, group, target)

	if pm, ok := getFromCache(targetKey); ok {
		if nodes, weights := findResult(pm); nodes != nil {
			return nodes, weights
		}
	}

	if pm, ok := getFromQueue(ops, group, config, target); ok {
		if nodes, weights := findResult(pm); nodes != nil {
			return nodes, weights
		}
	}

	if pm, ok := getFromDB(pathPrefix); ok {
		if nodes, weights := findResult(pm); nodes != nil {
			return nodes, weights
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

		target := strings.Join(parts[4:], "/")
		if target == "" {
			continue
		}

		var pm PrefetchMap
		if err := json.Unmarshal(v, &pm); err != nil {
			parseFailures++
			continue
		}

		cacheKey := FormatCacheKey(KeyTypePrefetch, config, group, target)
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

func (s *Store) StoreUnwrapResult(group, config string, target string, asnNumber string, isUDP bool, proxies []C.Proxy) {
	if target == "" || len(proxies) == 0 {
		return
	}

	targetKey := fmt.Sprintf("%s:%s:%s", config, group, target)

	if asnNumber != "" && !CdnASNs[asnNumber] {
		asnKey := fmt.Sprintf("%s:%s:%s", config, group, asnNumber)
		if value, found := unwrapCache.Get(asnKey); found {
			um := value
			if isUDP {
				if len(um.UDP) == 0 {
					um.UDP = proxies
					unwrapCache.Set(asnKey, um)
				}
			} else {
				if len(um.TCP) == 0 {
					um.TCP = proxies
					unwrapCache.Set(asnKey, um)
				}
			}
		} else {
			um := UnwrapMap{}
			if isUDP {
				um.UDP = proxies
			} else {
				um.TCP = proxies
			}
			unwrapCache.Set(asnKey, um)
		}

		if value, found := unwrapCache.Get(targetKey); found {
			um := value
			if isUDP {
				if um.RefUDP == "" {
					um.RefUDP = asnKey
					unwrapCache.Set(targetKey, um)
				}
			} else {
				if um.RefTCP == "" {
					um.RefTCP = asnKey
					unwrapCache.Set(targetKey, um)
				}
			}
		} else {
			um := UnwrapMap{}
			if isUDP {
				um.RefUDP = asnKey
			} else {
				um.RefTCP = asnKey
			}
			unwrapCache.Set(targetKey, um)
		}
	} else {
		if value, found := unwrapCache.Get(targetKey); found {
			um := value
			if isUDP {
				um.UDP = proxies
			} else {
				um.TCP = proxies
			}
			unwrapCache.Set(targetKey, um)
		} else {
			um := UnwrapMap{}
			if isUDP {
				um.UDP = proxies
			} else {
				um.TCP = proxies
			}
			unwrapCache.Set(targetKey, um)
		}
	}
}

func (s *Store) GetUnwrapResult(group, config, target, asnNumber string, isUDP bool) []C.Proxy {
	if target == "" {
		return nil
	}

	targetKey := fmt.Sprintf("%s:%s:%s", config, group, target)

	if value, found := unwrapCache.Get(targetKey); found {
		um := value
		var refKey string
		if isUDP {
			refKey = um.RefUDP
		} else {
			refKey = um.RefTCP
		}
		if refKey != "" {
			if refValue, found := unwrapCache.Get(refKey); found {
				refUm := refValue
				if isUDP {
					return refUm.UDP
				} else {
					return refUm.TCP
				}
			}
		} else {
			if isUDP {
				return um.UDP
			} else {
				return um.TCP
			}
		}
	}

	if asnNumber != "" && !CdnASNs[asnNumber] {
		asnKey := fmt.Sprintf("%s:%s:%s", config, group, asnNumber)
		if value, found := unwrapCache.Get(asnKey); found {
			um := value
			if isUDP {
				return um.UDP
			} else {
				return um.TCP
			}
		}
	}

	return nil
}

func (s *Store) DeleteUnwrapResult(group, config string, target string, asnNumber string, isUDP bool) {
	if target == "" {
		return
	}

	targetKey := fmt.Sprintf("%s:%s:%s", config, group, target)

	if value, found := unwrapCache.Get(targetKey); found {
		um := value
		if isUDP {
			um.UDP = nil
			um.RefUDP = ""
		} else {
			um.TCP = nil
			um.RefTCP = ""
		}
		if len(um.TCP) == 0 && len(um.UDP) == 0 && um.RefTCP == "" && um.RefUDP == "" {
			unwrapCache.Delete(targetKey)
		} else {
			unwrapCache.Set(targetKey, um)
		}
	}

	if asnNumber != "" && !CdnASNs[asnNumber] {
		asnKey := fmt.Sprintf("%s:%s:%s", config, group, asnNumber)
		if value, found := unwrapCache.Get(asnKey); found {
			um := value
			if isUDP {
				um.UDP = nil
			} else {
				um.TCP = nil
			}
			if len(um.TCP) == 0 && len(um.UDP) == 0 {
				unwrapCache.Delete(asnKey)
			} else {
				unwrapCache.Set(asnKey, um)
			}
		}
	}
}

func (s * Store) ClearUnwrapResult(group, config string) {
	cachePrefix := fmt.Sprintf("%s:%s:%s", config, group, "")
	unwrapCache.RemoveByKeyPrefix(cachePrefix)
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
		if proxy.Type() == C.Smart {
			smartGroupCount++
		}
	}

	globalCacheParams.mutex.Lock()
	defer globalCacheParams.mutex.Unlock()

	isFirstRun := globalCacheParams.LastMemoryUsage == 0
	needAdjust := isFirstRun

	if !isFirstRun {
		memoryChanged := math.Abs(memoryUsage-globalCacheParams.LastMemoryUsage) > 0.1
		needAdjust = memoryChanged || memoryUsage > 0.7
	}

	globalCacheParams.LastMemoryUsage = memoryUsage

	if !needAdjust && !isFirstRun {
		return
	}

	var newCacheSize int
	var cacheMaxAge int64 = CacheMaxAge

	if memoryUsage > 0.9 {
		globalCacheParams.MaxTargets = MinTargetsLimit
		globalCacheParams.BatchSaveThreshold = MinBatchThreshLimit
		globalCacheParams.PrefetchLimit = MinPrefetchTargetsLimit
		globalCacheParams.CacheMaxSize = (globalCacheParams.MaxTargets + globalCacheParams.PrefetchLimit) * smartGroupCount
		newCacheSize = globalCacheParams.CacheMaxSize / 2
		cacheMaxAge = CacheMaxAge / 2
	} else {
		adjustFactor := 4 * memoryUsage * (1 - memoryUsage)

		if memoryUsage > 0.85 {
			globalCacheParams.MaxTargets = MinTargetsLimit
			globalCacheParams.BatchSaveThreshold = MinBatchThreshLimit
			globalCacheParams.PrefetchLimit = MinPrefetchTargetsLimit
			globalCacheParams.CacheMaxSize = (globalCacheParams.MaxTargets + globalCacheParams.PrefetchLimit) * smartGroupCount
		} else {
			value := MinTargetsLimit + int(float64(MaxTargetsLimit-MinTargetsLimit)*adjustFactor*MemoryTargetsFactor)
			globalCacheParams.MaxTargets = ClampValue(value, MinTargetsLimit, MaxTargetsLimit)

			value = MinBatchThreshLimit + int(float64(MaxBatchThreshLimit-MinBatchThreshLimit)*adjustFactor*MemoryBatchFactor)
			globalCacheParams.BatchSaveThreshold = ClampValue(value, MinBatchThreshLimit, MaxBatchThreshLimit)

			value = MinPrefetchTargetsLimit + int(float64(MaxPrefetchTargetsLimit-MinPrefetchTargetsLimit)*adjustFactor*MemoryPrefetchFactor)
			globalCacheParams.PrefetchLimit = ClampValue(value, MinPrefetchTargetsLimit, MaxPrefetchTargetsLimit)

			globalCacheParams.CacheMaxSize = (globalCacheParams.MaxTargets + globalCacheParams.PrefetchLimit) * smartGroupCount
		}

		newCacheSize = globalCacheParams.CacheMaxSize
	}

	log.Infoln("[SmartStore] Parameters adjusted: MaxTargets=%d, CacheSize=%d, BatchThreshold=%d, PrefetchLimit=%d",
		globalCacheParams.MaxTargets, globalCacheParams.CacheMaxSize,
		globalCacheParams.BatchSaveThreshold, globalCacheParams.PrefetchLimit)

	newDataCache := lru.New[string, interface{}](
		lru.WithSize[string, interface{}](newCacheSize),
		lru.WithAge[string, interface{}](cacheMaxAge),
	)

	targetCache = lru.New[string, string](
		lru.WithSize[string, string](globalCacheParams.MaxTargets),
		lru.WithAge[string, string](cacheMaxAge),
	)

	prefixCountCache = lru.New[string, int](
		lru.WithSize[string, int](globalCacheParams.MaxTargets / 2),
		lru.WithAge[string, int](300),
	)

	nodeStatesCache = lru.New[string, map[string][]byte](
		lru.WithSize[string, map[string][]byte](2000),
		lru.WithAge[string, map[string][]byte](120),
	)

	unwrapCache = lru.New[string, UnwrapMap](
		lru.WithSize[string, UnwrapMap](globalCacheParams.PrefetchLimit / 2),
	)

	recordCache = lru.New[string, *AtomicStatsRecord](
		lru.WithSize[string, *AtomicStatsRecord](globalCacheParams.MaxTargets / 2),
		lru.WithAge[string, *AtomicStatsRecord](CacheMaxAge),
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
func (s *Store) PreloadFrequentData(group, config string) {
	log.Infoln("[SmartStore] Starting data preloading for group [%s], config [%s]", group, config)

	globalCacheParams.mutex.RLock()
	targetLimit := globalCacheParams.MaxTargets / 2
	prefetchLoadLimit := globalCacheParams.PrefetchLimit
	globalCacheParams.mutex.RUnlock()

	start := time.Now()

	prefetchCount := s.LoadAllPrefetchResults(group, config, prefetchLoadLimit)

	stateData, err := s.GetNodeStates(group, config)
	nodeStatesCount := 0
	if err == nil {
		nodeStatesCount = len(stateData)
	}

	ranking, _ := s.GetNodeWeightRankingCache(group, config)

	targets := s.GetActiveTargets(group, config, targetLimit)

	log.Infoln("[SmartStore] Preloaded data for group [%s]: %d targets, %d node stats, %d prefetch results, %d node rankings, completed in %.2f seconds",
		group, len(targets), nodeStatesCount, prefetchCount, len(ranking), time.Since(start).Seconds())
}

// 按级别清理内存缓存
func ClearCacheByLevel(level string, config string, group string) {
	if level == "all" {
		RemoveCacheValuesByPrefix("")
	} else if level == "config" {
		RemoveCacheValuesByPrefix(FormatCacheKey(KeyTypeNode, config, ""))
		RemoveCacheValuesByPrefix(FormatCacheKey(KeyTypeStats, config, ""))
		RemoveCacheValuesByPrefix(FormatCacheKey(KeyTypeRanking, config, ""))
		RemoveCacheValuesByPrefix(FormatCacheKey(KeyTypePrefetch, config, ""))
	} else if level == "group" {
		RemoveCacheValuesByPrefix(FormatCacheKey(KeyTypeNode, config, group, ""))
		RemoveCacheValuesByPrefix(FormatCacheKey(KeyTypeStats, config, group, ""))
		RemoveCacheValuesByPrefix(FormatCacheKey(KeyTypeRanking, config, group, ""))
		RemoveCacheValuesByPrefix(FormatCacheKey(KeyTypePrefetch, config, group, ""))
	}

	targetCache.Clear()

	prefixCountCache.Clear()

	nodeStatesCache.Clear()

	unwrapCache.Clear()

	recordCache.Clear()
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
			// smart/stats/config/group/target/node
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
