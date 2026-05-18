package smart

import (
	"encoding/json"
	"math"
	"strings"
	"time"

	"github.com/metacubex/mihomo/common/lru"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
)

var (
	targetCache *lru.LruCache[string, string]

	unwrapCache *lru.LruCache[string, UnwrapMap]

	recordCache *lru.LruCache[string, *AtomicStatsRecord]

	dbResultCache *lru.LruCache[string, map[string][]byte]

	blockedNodesCache *lru.LruCache[string, map[string]bool]

	hostStatusCache *lru.LruCache[string, HostStatus]
)

type (
	UnwrapMap struct {
		TCP    []string  `json:"tcp,omitempty"`
		UDP    []string  `json:"udp,omitempty"`
		RefTCP string    `json:"ref_tcp,omitempty"`
		RefUDP string    `json:"ref_udp,omitempty"`
	}

	NodesWithWeights struct {
		Nodes   []string  `json:"nodes"`
		Weights []float64 `json:"weights"`
	}

	NodeWithWeight struct {
		Node   string
		Weight float64
	}

	PrefetchMap struct {
		TCP         NodesWithWeights `json:"tcp,omitempty"`
		UDP         NodesWithWeights `json:"udp,omitempty"`
		RefTCP      string           `json:"ref_tcp,omitempty"`
		RefUDP      string           `json:"ref_udp,omitempty"`
		UpdatedTime int64            `json:"updated_time,omitempty"`
	}
)

func InitCache() {
	globalCacheParams.mutex.Lock()
	defer globalCacheParams.mutex.Unlock()

	if unwrapCache != nil {
		return
	}

	globalCacheParams.BatchSaveThreshold = MinBatchThreshLimit
	globalCacheParams.MaxTargets = MinTargetsLimit

	targetCache = lru.New[string, string](
		lru.WithSize[string, string](globalCacheParams.MaxTargets / 4),
		lru.WithAge[string, string](300),
	)

	unwrapCache = lru.New[string, UnwrapMap](
		lru.WithSize[string, UnwrapMap](globalCacheParams.MaxTargets / 4),
		lru.WithAge[string, UnwrapMap](600),
	)

	recordCache = lru.New[string, *AtomicStatsRecord](
		lru.WithSize[string, *AtomicStatsRecord](globalCacheParams.MaxTargets / 4),
		lru.WithAge[string, *AtomicStatsRecord](300),
	)

	dbResultCache = lru.New[string, map[string][]byte](
		lru.WithSize[string, map[string][]byte](globalCacheParams.MaxTargets / 4),
		lru.WithAge[string, map[string][]byte](300),
	)

	blockedNodesCache = lru.New[string, map[string]bool](
		lru.WithSize[string, map[string]bool](globalCacheParams.MaxTargets / 4),
		lru.WithAge[string, map[string]bool](300),
	)

	hostStatusCache = lru.New[string, HostStatus](
		lru.WithSize[string, HostStatus](globalCacheParams.MaxTargets / 4),
		lru.WithAge[string, HostStatus](300),
	)
}

// 存储预取结果
func (s *Store) StorePrefetchResult(group, config string, target string, asnNumber string, isUDP bool, proxyNames []string, weights []float64) {
	if target == "" || len(proxyNames) == 0 {
		return
	}

	targetCacheKey := FormatDBKey(KeyTypePrefetch, config, group, target)

	var pm PrefetchMap
	operations := make([]StoreOperation, 0, 2)
	nodeWeight := NodesWithWeights{Nodes: proxyNames, Weights: weights}

	if isUDP {
		pm.UDP = nodeWeight
	} else {
		pm.TCP = nodeWeight
	}
	pm.UpdatedTime = time.Now().Unix()

	data, err := json.Marshal(pm)
	if err == nil {
		operations = append(operations, StoreOperation{
			Type:   OpSavePrefetch,
			Group:  group,
			Config: config,
			Target: target,
			Data:   data,
		})
	}

	if asnNumber != "" && !CdnASNs[asnNumber] {
		var asnPm PrefetchMap
		if isUDP {
			asnPm.RefUDP = targetCacheKey
		} else {
			asnPm.RefTCP = targetCacheKey
		}
		asnPm.UpdatedTime = time.Now().Unix()
		
		asnData, asnErr := json.Marshal(asnPm)
		if asnErr == nil {
			operations = append(operations, StoreOperation{
				Type:   OpSavePrefetch,
				Group:  group,
				Config: config,
				Target: asnNumber,
				Data:   asnData,
			})
		}
	}

	if len(operations) > 0 {
		s.AppendToGlobalQueue(operations...)
	}
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

	targetKey := FormatDBKey(config, group, target)

	if asnNumber != "" && !CdnASNs[asnNumber] {
		asnKey := FormatDBKey(config, group, asnNumber)
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
			var um UnwrapMap
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
			var um UnwrapMap
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
			var um UnwrapMap
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

	targetKey := FormatDBKey(config, group, target)

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
		asnKey := FormatDBKey(config, group, asnNumber)
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

	targetKey := FormatDBKey(config, group, target)

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
		asnKey := FormatDBKey(config, group, asnNumber)
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

func (s *Store) UpdateBlockedNodesCache(group, config string, updates map[string]*NodeState) {
	cacheKey := FormatDBKey(config, group)
	blocked := s.GetBlockedNodes(group, config)
	now := time.Now().Unix()

	for node, state := range updates {
		if state == nil {
			continue
		}
		if state.BlockedUntil > 0 && state.BlockedUntil > now {
			blocked[node] = true
		} else {
			delete(blocked, node)
		}
	}

	blockedNodesCache.Set(cacheKey, blocked)
}

// 调整缓存参数
func (s *Store) AdjustCacheParameters() {
	memoryUsage := GetSystemMemoryUsage()

	globalCacheParams.mutex.Lock()
	defer globalCacheParams.mutex.Unlock()

	isFirstRun := globalCacheParams.LastMemoryUsage == 0
	needAdjust := isFirstRun

	if !isFirstRun {
		memoryChanged := math.Abs(memoryUsage - globalCacheParams.LastMemoryUsage) > 0.05
		needAdjust = memoryChanged
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

	targetCache = lru.ResetLRU(targetCache, globalCacheParams.MaxTargets / 4, lru.WithAge[string, string](300))
	unwrapCache = lru.ResetLRU(unwrapCache, globalCacheParams.MaxTargets / 4, lru.WithAge[string, UnwrapMap](600))
	recordCache = lru.ResetLRU(recordCache, globalCacheParams.MaxTargets / 4, lru.WithAge[string, *AtomicStatsRecord](300))
	dbResultCache = lru.ResetLRU(dbResultCache, globalCacheParams.MaxTargets / 4, lru.WithAge[string, map[string][]byte](300))
	blockedNodesCache = lru.ResetLRU(blockedNodesCache, globalCacheParams.MaxTargets / 4, lru.WithAge[string, map[string]bool](300))
	hostStatusCache = lru.ResetLRU(hostStatusCache, globalCacheParams.MaxTargets / 4, lru.WithAge[string, HostStatus](300))
	go s.FlushQueue(true)
}

// 按级别清理内存缓存
func (s *Store) clearCache(level string, config string, group string) {
	targetCache.Clear()

	unwrapCache.Clear()

	recordCache.Clear()

	dbResultCache.Clear()

	blockedNodesCache.Clear()

	hostStatusCache.Clear()

	s.FlushQueue(true)
}