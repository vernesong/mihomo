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
)

func InitCache() {
	globalCacheParams.mutex.Lock()
	defer globalCacheParams.mutex.Unlock()

	if unwrapCache != nil {
		return
	}

	globalCacheParams.BatchSaveThreshold = MinBatchThreshLimit
	globalCacheParams.MaxTargets = MinTargetsLimit
	globalCacheParams.MemoryLimit = getSystemMemoryLimit()

	targetCache = lru.New[string, string](
		lru.WithSize[string, string](globalCacheParams.MaxTargets / 4),
	)

	unwrapCache = lru.New[string, UnwrapMap](
		lru.WithSize[string, UnwrapMap](globalCacheParams.MaxTargets / 4),
	)

	recordCache = lru.New[string, *AtomicStatsRecord](
		lru.WithSize[string, *AtomicStatsRecord](globalCacheParams.MaxTargets / 4),
	)

	dbResultCache = lru.New[string, map[string][]byte](
		lru.WithSize[string, map[string][]byte](globalCacheParams.MaxTargets / 4),
		lru.WithAge[string, map[string][]byte](300),
	)
}

// 存储预取结果
func (s *Store) StorePrefetchResult(group, config string, target string, asnNumber string, isUDP bool, proxyNames []string, weights []float64) {
	if target == "" || len(proxyNames) == 0 {
		return
	}

	targetCacheKey := FormatDBKey(KeyTypePrefetch, config, group, target)

	var pm PrefetchMap

	nodeWeight := NodesWithWeights{Nodes: proxyNames, Weights: weights}
	if isUDP {
		pm.UDP = nodeWeight
	} else {
		pm.TCP = nodeWeight
	}
	pm.UpdatedTime = time.Now().Unix()
	data, err := json.Marshal(pm)
	if err != nil {
		return
	}

	appendToGlobalQueue(StoreOperation{
		Type:   OpSavePrefetch,
		Group:  group,
		Config: config,
		Target: target,
		Data:   data,
	})

	if asnNumber != "" && !CdnASNs[asnNumber] {
		var asnPm PrefetchMap
		if isUDP {
			asnPm.RefUDP = targetCacheKey
		} else {
			asnPm.RefTCP = targetCacheKey
		}
		asnPm.UpdatedTime = time.Now().Unix()
		asnData, asnErr := json.Marshal(asnPm)
		if asnErr != nil {
			return
		}

		appendToGlobalQueue(StoreOperation{
			Type:   OpSavePrefetch,
			Group:  group,
			Config: config,
			Target: asnNumber,
			Data:   asnData,
		})
	}

	go s.FlushQueue(false)
}

// 获取预取结果
func (s *Store) GetPrefetchResult(group, config string, target string, asnNumber string, isUDP bool) ([]string, []float64) {
	if target == "" {
		return nil, nil
	}

	findResult := func(pm PrefetchMap) ([]string, []float64) {
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

	getPrefetchMap := func(pathPrefix string) (PrefetchMap, bool) {
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

	// ASN
	if asnNumber != "" && !CdnASNs[asnNumber] {
		asnPathPrefix := FormatDBKey(KeyTypePrefetch, config, group, asnNumber)
		if pm, ok := getPrefetchMap(asnPathPrefix); ok {
			if refKey := getRefKey(pm, isUDP); refKey != "" {
				parts := strings.Split(refKey, "/")
				if len(parts) >= 5 {
					parsedTarget := strings.Join(parts[4:], "/")
					targetPathPrefix := FormatDBKey(KeyTypePrefetch, config, group, parsedTarget)
					if refPm, ok := getPrefetchMap(targetPathPrefix); ok {
						if nodes, weights := findResult(refPm); nodes != nil {
							return nodes, weights
						}
					}
				}
			}
		}
	}

	// target
	pathPrefix := FormatDBKey(KeyTypePrefetch, config, group, target)
	if pm, ok := getPrefetchMap(pathPrefix); ok {
		if nodes, weights := findResult(pm); nodes != nil {
			return nodes, weights
		}
	}

	return nil, nil
}

func (s *Store) StoreUnwrapResult(group, config string, target string, asnNumber string, isUDP bool, proxies []C.Proxy) {
	if target == "" || len(proxies) == 0 {
		return
	}

	names := make([]string, len(proxies))
	for i, p := range proxies {
		names[i] = p.Name()
	}

	targetKey := fmt.Sprintf("%s:%s:%s", config, group, target)

	if asnNumber != "" && !CdnASNs[asnNumber] {
		asnKey := fmt.Sprintf("%s:%s:%s", config, group, asnNumber)
		if value, found := unwrapCache.Get(asnKey); found {
			um := value
			if isUDP {
				if len(um.UDP) == 0 {
					um.UDP = names
					unwrapCache.Set(asnKey, um)
				}
			} else {
				if len(um.TCP) == 0 {
					um.TCP = names
					unwrapCache.Set(asnKey, um)
				}
			}
		} else {
			um := UnwrapMap{}
			if isUDP {
				um.UDP = names
			} else {
				um.TCP = names
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
				um.UDP = names
			} else {
				um.TCP = names
			}
			unwrapCache.Set(targetKey, um)
		} else {
			um := UnwrapMap{}
			if isUDP {
				um.UDP = names
			} else {
				um.TCP = names
			}
			unwrapCache.Set(targetKey, um)
		}
	}
}

func (s *Store) GetUnwrapResult(group, config, target, asnNumber string, isUDP bool) []string {
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

// 调整缓存参数
func (s *Store) AdjustCacheParameters() {
	memoryUsage := GetSystemMemoryUsage()

	globalCacheParams.mutex.Lock()
	defer globalCacheParams.mutex.Unlock()

	isFirstRun := globalCacheParams.LastMemoryUsage == 0
	needAdjust := isFirstRun

	if !isFirstRun {
		memoryChanged := math.Abs(memoryUsage - globalCacheParams.LastMemoryUsage) * globalCacheParams.MemoryLimit > 20
		needAdjust = memoryChanged || memoryUsage > 0.7
	}

	globalCacheParams.LastMemoryUsage = memoryUsage

	if !needAdjust && !isFirstRun {
		return
	}

	if memoryUsage > 0.9 {
		globalCacheParams.MaxTargets = MinTargetsLimit
		globalCacheParams.BatchSaveThreshold = MinBatchThreshLimit
	} else {
		adjustFactor := (1 - memoryUsage) * 0.5
		globalCacheParams.MaxTargets = MinTargetsLimit + int(float64(MaxTargetsLimit-MinTargetsLimit)*adjustFactor)
		globalCacheParams.BatchSaveThreshold = MinBatchThreshLimit + int(float64(MaxBatchThreshLimit-MinBatchThreshLimit)*adjustFactor)
	}

	log.Infoln("[SmartStore] Parameters adjusted: MaxTargets=%d, BatchThreshold=%d",
		globalCacheParams.MaxTargets,
		globalCacheParams.BatchSaveThreshold)

	targetCache = lru.ResetLRU(targetCache, globalCacheParams.MaxTargets / 4)
	unwrapCache = lru.ResetLRU(unwrapCache, globalCacheParams.MaxTargets / 4)
	recordCache = lru.ResetLRU(recordCache, globalCacheParams.MaxTargets / 4)
	dbResultCache = lru.ResetLRU(dbResultCache, globalCacheParams.MaxTargets / 4, lru.WithAge[string, map[string][]byte](300))

	if (memoryUsage > 0.8) {
		go s.FlushQueue(true)
	}
}

// 按级别清理内存缓存
func (s *Store) clearCache(level string, config string, group string) {
	targetCache.Clear()

	unwrapCache.Clear()

	recordCache.Clear()

	dbResultCache.Clear()

	s.FlushQueue(true)
}