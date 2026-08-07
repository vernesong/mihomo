package smart

import (
	"encoding/json"
	"math"
	"time"

	"github.com/metacubex/mihomo/common/lru"
	"github.com/metacubex/mihomo/common/xsync"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
)

var (
	targetCache *lru.LruCache[string, string]

	unwrapCache *lru.LruCache[string, UnwrapMap]

	recordCache *lru.LruCache[string, *AtomicStatsRecord]

	dbResultCache *lru.LruCache[string, map[string][]byte]

	blockedNodesCache *lru.LruCache[string, map[string]bool]

	hostStatusCache *lru.LruCache[string, *HostStatus]
)

var (
	targetCacheRefreshFlags    xsync.Map[string, bool]
	dbResultRefreshFlags       xsync.Map[string, bool]
	blockedNodesRefreshFlags   xsync.Map[string, bool]
)

type (
	UnwrapMap struct {
		Proxies []string `json:"proxies,omitempty"`
		Ref     string   `json:"ref,omitempty"`
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
		lru.WithSize[string, string](globalCacheParams.MaxTargets / 3),
		lru.WithAge[string, string](300),
		lru.WithStale[string, string](true),
	)

	unwrapCache = lru.New[string, UnwrapMap](
		lru.WithSize[string, UnwrapMap](globalCacheParams.MaxTargets / 3),
		lru.WithAge[string, UnwrapMap](600),
		lru.WithStale[string, UnwrapMap](true),
	)

	recordCache = lru.New[string, *AtomicStatsRecord](
		lru.WithSize[string, *AtomicStatsRecord](globalCacheParams.MaxTargets / 3),
		lru.WithAge[string, *AtomicStatsRecord](300),
		lru.WithStale[string, *AtomicStatsRecord](true),
	)

	dbResultCache = lru.New[string, map[string][]byte](
		lru.WithSize[string, map[string][]byte](globalCacheParams.MaxTargets / 3),
		lru.WithAge[string, map[string][]byte](300),
		lru.WithStale[string, map[string][]byte](true),
	)

	blockedNodesCache = lru.New[string, map[string]bool](
		lru.WithSize[string, map[string]bool](globalCacheParams.MaxTargets / 3),
		lru.WithAge[string, map[string]bool](300),
		lru.WithStale[string, map[string]bool](true),
	)

	hostStatusCache = lru.New[string, *HostStatus](
		lru.WithSize[string, *HostStatus](globalCacheParams.MaxTargets / 3),
		lru.WithAge[string, *HostStatus](300),
		lru.WithStale[string, *HostStatus](true),
	)
}

// 存储预取结果
func (s *Store) StorePrefetchResult(group, config string, target string, asnNumber string, isUDP bool, proxyNames []string, weights []float64) {
	if target == "" || len(proxyNames) == 0 {
		return
	}

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
		targetCacheKey := FormatDBKey(KeyTypePrefetch, config, group, target)
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

	loadPM := func(pathPrefix string) (PrefetchMap, bool) {
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

	pick := func(pm PrefetchMap) ([]string, []float64) {
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

	// ASN
	if asnNumber != "" && !CdnASNs[asnNumber] {
		if pm, ok := loadPM(FormatDBKey(KeyTypePrefetch, config, group, asnNumber)); ok {
			if nodes, weights := pick(pm); nodes != nil {
				return nodes, weights
			}
			var refKey string
			if isUDP {
				refKey = pm.RefUDP
			} else {
				refKey = pm.RefTCP
			}
			if refKey != "" {
				if refPm, ok := loadPM(refKey); ok {
					if nodes, weights := pick(refPm); nodes != nil {
						return nodes, weights
					}
				}
			}
		}
	}

	// target
	if pm, ok := loadPM(FormatDBKey(KeyTypePrefetch, config, group, target)); ok {
		if nodes, weights := pick(pm); nodes != nil {
			return nodes, weights
		}
	}

	return nil, nil
}

func (s *Store) StoreUnwrapResult(group, config string, target string, asnNumber string, wildcardTarget string, proxies []C.Proxy) {
	if target == "" || len(proxies) == 0 {
		return
	}

	names := make([]string, len(proxies))
	for i, p := range proxies {
		names[i] = p.Name()
	}

	// SmartTarget (same ruleset = same node)
	targetKey := FormatDBKey(config, group, target)
	unwrapCache.Set(targetKey, UnwrapMap{Proxies: names})

	// ASN sharing (CDN excluded): first-writer-wins
	if asnNumber != "" && !CdnASNs[asnNumber] {
		asnKey := FormatDBKey(config, group, asnNumber)
		if existing, _, found := unwrapCache.GetWithExpire(asnKey); !found || len(existing.Proxies) == 0 {
			unwrapCache.Set(asnKey, UnwrapMap{Proxies: names})
		}
	}
}

func (s *Store) GetUnwrapResult(group, config, target, asnNumber string, wildcardTarget string) (proxies []string, expired bool) {
	if target == "" {
		return nil, false
	}

	targetKey := FormatDBKey(config, group, target)
	if value, expireTime, found := unwrapCache.GetWithExpire(targetKey); found {
		if len(value.Proxies) > 0 {
			return value.Proxies, expireTime.Before(time.Now())
		}
	}

	if asnNumber != "" && !CdnASNs[asnNumber] {
		asnKey := FormatDBKey(config, group, asnNumber)
		if value, expireTime, found := unwrapCache.GetWithExpire(asnKey); found {
			if len(value.Proxies) > 0 {
				unwrapCache.Set(targetKey, UnwrapMap{Proxies: value.Proxies})
				return value.Proxies, expireTime.Before(time.Now())
			}
		}
	}

	return nil, false
}

func (s *Store) DeleteUnwrapResult(group, config string, target string, asnNumber string, wildcardTarget string) {
	if target == "" {
		return
	}

	targetKey := FormatDBKey(config, group, target)
	unwrapCache.Delete(targetKey)

	if asnNumber != "" && !CdnASNs[asnNumber] {
		asnKey := FormatDBKey(config, group, asnNumber)
		unwrapCache.Delete(asnKey)
	}
}

func (s *Store) UpdateBlockedNodesCache(group, config string, updates map[string]*NodeState) {
	cacheKey := FormatDBKey(config, group)
	blocked, _, _ := blockedNodesCache.GetWithExpire(cacheKey)

	newBlocked := make(map[string]bool, len(blocked)+len(updates))
	for k, v := range blocked {
		newBlocked[k] = v
	}

	now := time.Now().Unix()

	for node, state := range updates {
		if state == nil {
			continue
		}
		if state.BlockedUntil > 0 && state.BlockedUntil > now {
			newBlocked[node] = true
		} else {
			delete(newBlocked, node)
		}
	}

	blockedNodesCache.Set(cacheKey, newBlocked)
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

	cacheSize := globalCacheParams.MaxTargets / 4
	targetCache = lru.ResetLRU(targetCache, cacheSize, lru.WithAge[string, string](300), lru.WithStale[string, string](true))
	unwrapCache = lru.ResetLRU(unwrapCache, cacheSize, lru.WithAge[string, UnwrapMap](600), lru.WithStale[string, UnwrapMap](true))
	recordCache = lru.ResetLRU(recordCache, cacheSize, lru.WithAge[string, *AtomicStatsRecord](300), lru.WithStale[string, *AtomicStatsRecord](true))
	dbResultCache = lru.ResetLRU(dbResultCache, cacheSize, lru.WithAge[string, map[string][]byte](300), lru.WithStale[string, map[string][]byte](true))
	blockedNodesCache = lru.ResetLRU(blockedNodesCache, cacheSize, lru.WithAge[string, map[string]bool](300), lru.WithStale[string, map[string]bool](true))
	hostStatusCache = lru.ResetLRU(hostStatusCache, cacheSize, lru.WithAge[string, *HostStatus](300), lru.WithStale[string, *HostStatus](true))
	go s.FlushQueue(true)
}

// 按级别清理内存缓存
func (s *Store) clearCache(level string, config string, group string) {
	s.FlushQueue(true)

	if level == "all" {
		targetCache.Clear()
		unwrapCache.Clear()
		recordCache.Clear()
		dbResultCache.Clear()
		blockedNodesCache.Clear()
		hostStatusCache.Clear()
		return
	}

	targetCache.Clear()

	if level == "config" {
		unwrapCache.RemoveByKeyPrefix(FormatDBKey(config) + "/")
		recordCache.RemoveByKeyPrefix(FormatDBKey(KeyTypeStats, config) + "/")
		for _, kt := range []string{KeyTypeStats, KeyTypeNode, KeyTypePrefetch, KeyTypeRanking, KeyTypeHostFailures} {
			dbResultCache.RemoveByKeyPrefix(FormatDBKey(kt, config) + "/")
		}
		blockedNodesCache.RemoveByKeyPrefix(FormatDBKey(config) + "/")
		hostStatusCache.RemoveByKeyPrefix(FormatDBKey(KeyTypeHostFailures, config) + "/")
	} else if level == "group" {
		groupKey := FormatDBKey(config, group) // "smart/{config}/{group}"
		unwrapCache.RemoveByKeyPrefix(groupKey + "/")
		recordCache.RemoveByKeyPrefix(FormatDBKey(KeyTypeStats, config, group) + "/")
		for _, kt := range []string{KeyTypeStats, KeyTypeNode, KeyTypePrefetch, KeyTypeRanking, KeyTypeHostFailures} {
			dbResultCache.Delete(FormatDBKey(kt, config, group))
		}
		blockedNodesCache.Delete(groupKey)
		hostStatusCache.RemoveByKeyPrefix(FormatDBKey(KeyTypeHostFailures, config, group) + "/")
	}
}