package smart

import (
	"errors"
	"fmt"
	"math"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/common/cmd"
	"github.com/metacubex/mihomo/common/lru"
	"github.com/metacubex/mihomo/log"

	"golang.org/x/net/publicsuffix"
)

const (
	OpSaveNodeState StoreOperationType = iota
	OpSaveStats
	OpSavePrefetch
	OpSaveRanking
)

const (
	KeyTypePrefetch = "prefetch"
	KeyTypeUnwrap   = "unwrap"
	KeyTypeFailed   = "failed"
	KeyTypeNode     = "node"
	KeyTypeStats    = "stats"
	KeyTypeRanking  = "ranking"

	WeightTypeTCP    = "tcp"
	WeightTypeUDP    = "udp"
	WeightTypeTCPASN = "tcp_asn"
	WeightTypeUDPASN = "udp_asn"
)

const (
	DefaultMinSampleCount = 2
	RetentionPeriod       = 14 * 24 * time.Hour
	CacheMaxAge           = 21600

	MaxDomainsLimit         = 2000
	MinDomainsLimit         = 300
	MaxBatchThreshLimit     = 500
	MinBatchThreshLimit     = 100
	MaxPrefetchDomainsLimit = 1000
	MinPrefetchDomainsLimit = 100

	MemoryDomainsFactor   = 0.8
	MemoryCacheSizeFactor = 0.7
	MemoryBatchFactor     = 0.7
	MemoryPrefetchFactor  = 0.7

	RankMostUsed   = "MostUsed"
	RankOccasional = "OccasionalUsed"
	RankRarelyUsed = "RarelyUsed"
)

type StoreOperationType int

var bucketSmartStats = []byte("smart_stats")

var (
	globalInitInstances = make(map[string]bool)
	globalInitLock      sync.Mutex

	globalOperationQueue atomic.TypedValue[[]StoreOperation]

	globalCacheParams struct {
		BatchSaveThreshold int
		MaxDomains         int
		PrefetchLimit      int
		CacheMaxSize       int
		MemoryLimit        float64
		LastMemoryUsage    float64
		mutex              sync.RWMutex
	}

	dataCache       *lru.LruCache[string, interface{}]
	globalCacheLock sync.RWMutex

	cachedMemoryLimit float64
	memoryLimitOnce   sync.Once

	domainCache *lru.LruCache[string, string]
)

type (
	StoreOperation struct {
		Type   StoreOperationType
		Group  string
		Config string
		Domain string
		Node   string
		Data   []byte
	}

	DomainRecord struct {
		Key      string    `json:"key"`
		NodeName string    `json:"node_name"`
		Domain   string    `json:"domain"`
		LastUsed time.Time `json:"last_used"`
	}

	StatsRecord struct {
		Success            int64              `json:"success"`
		Failure            int64              `json:"failure"`
		ConnectTime        int64              `json:"connect_time"`
		Latency            int64              `json:"latency"`
		LastUsed           time.Time          `json:"last_used"`
		Weights            map[string]float64 `json:"weights"`
		UploadTotal        float64            `json:"upload_total"`
		DownloadTotal      float64            `json:"download_total"`
		MaxUploadRate      float64            `json:"max_upload_rate"`
		MaxDownloadRate    float64            `json:"max_download_rate"`
		ConnectionDuration float64            `json:"connection_duration"`
	}

	NodeState struct {
		Name               string         `json:"name"`
		FailureCount       int            `json:"failure_count"`
		LastFailure        time.Time      `json:"last_failure"`
		BlockedUntil       time.Time      `json:"blocked_until"`
		Degraded           bool           `json:"degraded"`
		DegradedFactor     float64        `json:"degraded_factor"`
		DomainFailureCount map[string]int `json:"domain_failure_count"`
	}

	RankingData struct {
		Ranking     map[string]string `json:"ranking"`
		LastUpdated time.Time         `json:"last_updated"`
	}

	NodeWithWeight struct {
		Nodes   []string  `json:"nodes"`
		Weights []float64 `json:"weights"`
	}

	PrefetchMap map[string]NodeWithWeight
)

func InitializeGlobalParams() {
	InitializeCache()
}

// 格式化缓存键
func FormatCacheKey(keyType, config, group string, parts ...string) string {
	elements := []string{keyType, config, group}
	elements = append(elements, parts...)
	return strings.Join(elements, ":")
}

// 格式化数据库键
func FormatDBKey(first string, parts ...string) string {
	elements := make([]string, 0, len(parts)+1)
	elements = append(elements, first)

	for _, part := range parts {
		if part != "" {
			elements = append(elements, part)
		}
	}

	return strings.Join(elements, "/")
}

// 获取有效顶级域名加一二级域名并使用通配符处理
func GetEffectiveDomain(host string, dstIP string) (string, string) {
	rawHost := host

	if host == "" {
		if dstIP != "" {
			return dstIP, dstIP
		}
		return "", ""
	}

	h := strings.ToLower(host)

	validLabel := regexp.MustCompile(`^[a-z0-9-]+$`)
	hexRandom := regexp.MustCompile(`^[0-9a-f]{8,}$`)

	compute := func() string {
		parts := strings.Split(h, ".")
		reg, err := publicsuffix.EffectiveTLDPlusOne(h)
		if err != nil || reg == "" || reg == h || !(h == reg || strings.HasSuffix(h, "."+reg)) {
			if len(parts) >= 2 {
				reg = strings.Join(parts[len(parts)-2:], ".")
			} else {
				return h
			}
		}

		var sub string
		if h == reg {
			sub = ""
		} else {
			sub = strings.TrimSuffix(h, "."+reg)
		}

		if sub == "" {
			return reg
		}

		labels := strings.Split(sub, ".")
		last := labels[len(labels)-1]

		if strings.Contains(last, "-") {
			last = "*"
		} else if hexRandom.MatchString(last) {
			last = "*"
		} else {
			letters := 0
			digits := 0
			for _, r := range last {
				if r >= 'a' && r <= 'z' {
					letters++
				} else if r >= '0' && r <= '9' {
					digits++
				}
			}
			if letters > 0 && digits > 0 {
				if len(last) > 10 || (digits > 0 && float64(digits)/float64(len(last)) > 0.6) {
					last = "*"
				}
			}
		}

		if !validLabel.MatchString(last) || strings.HasPrefix(last, "-") || strings.HasSuffix(last, "-") {
			last = "*"
		}

		var normalizedSub string
		if len(labels) == 1 {
			normalizedSub = last
		} else {
			normalizedSub = "*." + last
		}

		if normalizedSub == "" || normalizedSub == "*" || normalizedSub == "*.*" {
			return "*." + reg
		}

		return normalizedSub + "." + reg
	}

	if domainCache != nil {
		if result, _ := domainCache.GetOrStore(h, func() string {
			return compute()
		}); result != "" {
			if strings.HasPrefix(result, "*.") {
				domainCache.Set(result, result)
				domainCache.Set(h, result)
				return result, rawHost
			}

			if result == h {
				parts := strings.Split(h, ".")
				if len(parts) == 2 {
					wildcard := "*." + h
					domainCache.Set(h, wildcard)
					domainCache.Set(wildcard, wildcard)
					return wildcard, rawHost
				}
				if len(parts) > 2 {
					wildcard := "*." + parts[len(parts)-2] + "." + parts[len(parts)-1]
					if cachedVal, ok := domainCache.Get(wildcard); ok && cachedVal != "" {
						domainCache.Set(h, cachedVal)
						return cachedVal, rawHost
					}
				}
			}
		}
	}

	return compute(), rawHost
}

// 限制值在指定范围内
func ClampValue(value, min, max int) int {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}

// 时间衰减
func GetTimeDecayWithCache(lastUsedTime int64, now int64, minDecay float64, decayCache map[int64]float64) float64 {
	fuzzyLastUsedTime := (lastUsedTime / 3600) * 3600

	if decay, ok := decayCache[fuzzyLastUsedTime]; ok {
		return decay
	}

	hoursSinceLastConn := float64(now-fuzzyLastUsedTime) / 3600.0
	var decay float64

	switch {
	case hoursSinceLastConn <= 24:
		// 0-24小时：保持高权重
		decay = 1.0
	case hoursSinceLastConn <= 72:
		// 24-72小时：线性衰减到0.8
		decay = 1.0 - (hoursSinceLastConn-24.0)/48.0*0.2
	case hoursSinceLastConn <= 168: // 7天
		// 72-168小时：线性衰减到0.5
		decay = 0.8 - (hoursSinceLastConn-72.0)/96.0*0.3
	case hoursSinceLastConn <= 720: // 30天
		// 168-720小时：线性衰减到0.3
		decay = 0.5 - (hoursSinceLastConn-168.0)/552.0*0.2
	default:
		decay = 0.3
	}

	decay = math.Max(minDecay, decay)
	decayCache[fuzzyLastUsedTime] = decay
	return decay
}

// 根据系统内存计算限制
func CalculateMemoryBasedLimit(memUsage float64, min, max int, factor float64) int {
	if memUsage < 0 {
		memUsage = 0
	} else if memUsage > 100 {
		memUsage = 100
	}

	availFactor := 1.0 - (memUsage / 100.0)

	value := min + int(float64(max-min)*availFactor*factor)

	return ClampValue(value, min, max)
}

// 获取批量保存阈值
func GetBatchSaveThreshold() int {
	globalCacheParams.mutex.RLock()
	defer globalCacheParams.mutex.RUnlock()

	if globalCacheParams.BatchSaveThreshold <= 0 {
		return MinBatchThreshLimit
	}

	return globalCacheParams.BatchSaveThreshold
}

// 获取系统内存使用情况
func GetSystemMemoryUsage() float64 {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	inuse := float64(memStats.Alloc) / (1024 * 1024)

	globalCacheParams.mutex.RLock()
	memLimit := globalCacheParams.MemoryLimit
	globalCacheParams.mutex.RUnlock()

	if memLimit <= 0 {
		memLimit = 100
	}

	usagePercent := math.Min(inuse/memLimit*100.0, 100.0)
	return usagePercent
}

// 检查当前实例是否是特定配置的第一个实例
func IsFirstInstanceForConfig(config string) bool {
	globalInitLock.Lock()
	defer globalInitLock.Unlock()

	key := fmt.Sprintf("%s", config)
	if globalInitInstances[key] {
		return false
	}

	globalInitInstances[key] = true
	return true
}

func getSystemMemoryLimit() float64 {
	memoryLimitOnce.Do(func() {
		var memTotal float64 = 100.0
		var output string
		var err error

		if runtime.GOOS == "windows" {
			output, err = cmd.ExecCmd("wmic OS get TotalVisibleMemorySize")
			if err == nil {
				lines := strings.Split(output, "\n")
				if len(lines) >= 2 {
					memStr := strings.TrimSpace(lines[1])
					memKB, parseErr := strconv.ParseFloat(memStr, 64)
					if parseErr == nil {
						memTotal = memKB / 1024.0
					}
				}
			}
		} else if runtime.GOOS == "linux" || runtime.GOOS == "android" || runtime.GOOS == "darwin" || runtime.GOOS == "freebsd" {
			output, err = cmd.ExecCmd("grep MemTotal /proc/meminfo")
			if err == nil {
				parts := strings.Fields(output)
				if len(parts) >= 2 {
					memStr := strings.TrimSuffix(parts[1], "kB")
					memStr = strings.TrimSpace(memStr)
					memKB, parseErr := strconv.ParseFloat(memStr, 64)
					if parseErr == nil {
						memTotal = memKB / 1024.0
					}
				}
			}
		}

		memTotal = memTotal / 4.0

		if memTotal < 100.0 {
			cachedMemoryLimit = 100.0
		} else if memTotal > 512.0 {
			cachedMemoryLimit = 512.0
		} else {
			cachedMemoryLimit = memTotal
		}
	})

	return cachedMemoryLimit
}

// 按级别刷新缓存
func (s *Store) FlushByLevel(level string, config string, group string) error {
	if level == "" {
		return errors.New("flush level cannot be empty")
	}

	if level == "all" {
		emptyQueue := make([]StoreOperation, 0, MinBatchThreshLimit)
		replaceGlobalQueue(emptyQueue)
	} else if level == "config" {
		filterQueueByConfig(config)
	} else if level == "group" {
		filterQueueByGroup(group, config)
	}

	ClearCacheByLevel(level, config, group)

	if level == "all" {
		s.ClearFailureCache(level, "", "")
	} else if level == "config" {
		s.ClearFailureCache(level, config, "")
	} else if level == "group" {
		s.ClearFailureCache(level, config, group)
	}

	if level == "all" {
		s.DeleteByPath("smart")
	} else if level == "config" {
		s.DeleteByPath(FormatDBKey("smart", KeyTypeStats, config))
		s.DeleteByPath(FormatDBKey("smart", KeyTypeNode, config))
		s.DeleteByPath(FormatDBKey("smart", KeyTypeRanking, config))
		s.DeleteByPath(FormatDBKey("smart", KeyTypePrefetch, config))
	} else if level == "group" {
		s.DeleteByPath(FormatDBKey("smart", KeyTypeStats, config, group, ""))
		s.DeleteByPath(FormatDBKey("smart", KeyTypeNode, config, group, ""))
		s.DeleteByPath(FormatDBKey("smart", KeyTypeRanking, config, group, ""))
		s.DeleteByPath(FormatDBKey("smart", KeyTypePrefetch, config, group, ""))
	}

	return nil
}

// 清空所有缓存
func (s *Store) FlushAll() error {
	log.Debugln("[SmartStore] Starting FlushAll, current queue length: %d", len(getGlobalQueueSnapshot()))
	err := s.FlushByLevel("all", "", "")
	if err == nil {
		log.Debugln("[SmartStore] All Smart data cleared")
	}
	return err
}

// 按配置清空缓存
func (s *Store) FlushByConfig(config string) error {
	err := s.FlushByLevel("config", config, "")
	if err == nil {
		log.Debugln("[SmartStore] All data for config [%s] cleared", config)
	}
	return err
}

func (s *Store) FlushByGroup(group, config string) error {
	err := s.FlushByLevel("group", config, group)
	if err == nil {
		log.Debugln("[SmartStore] All data for group [%s] config [%s] cleared", group, config)
	}
	return err
}
