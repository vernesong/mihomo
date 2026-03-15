package smart

import (
	"errors"
	"math"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/metacubex/bbolt"
	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/common/cmd"
	"github.com/metacubex/mihomo/common/lru"
	"github.com/metacubex/mihomo/log"

	"golang.org/x/net/publicsuffix"
)

const (
	OpSaveNodeState         = iota
	OpSaveStats
	OpSavePrefetch
	OpSaveRanking
	OpSaveHostFailures
)

const (
	KeyTypePrefetch         = "prefetch"
	KeyTypeNode             = "node"
	KeyTypeStats            = "stats"
	KeyTypeRanking          = "ranking"
	KeyTypeHostFailures     = "failures"

	WeightTypeTCP           = "tcp"
	WeightTypeUDP           = "udp"
	WeightTypeTCPASN        = "tcp_asn"
	WeightTypeUDPASN        = "udp_asn"
)

const (
	DefaultMinSampleCount   = 2

	MaxTargetsLimit         = 4000
	MinTargetsLimit         = 500
	MaxBatchThreshLimit     = 300
	MinBatchThreshLimit     = 50

	AllowedWeight           = 0.4

	RankMostUsed            = "MostUsed"
	RankOccasional          = "OccasionalUsed"
	RankRarelyUsed          = "RarelyUsed"
)

var (
	db *bbolt.DB
	bucketSmartStats = []byte("smart_stats")

	globalOperationQueue atomic.TypedValue[[]StoreOperation]

	globalCacheParams struct {
		BatchSaveThreshold int
		MaxTargets         int
		LastMemoryUsage    float64
		mutex              sync.RWMutex
	}

	targetCache *lru.LruCache[string, string]

	unwrapCache *lru.LruCache[string, UnwrapMap]

	recordCache *lru.LruCache[string, *AtomicStatsRecord]

	dbResultCache *lru.LruCache[string, map[string][]byte]

	blockedNodesCache *lru.LruCache[string, map[string]bool]
)

var CdnASNs = map[string]bool{
	"13335":  true, // Cloudflare
	"12222":  true, // Akamai
	"16625":  true, // Akamai
	"20940":  true, // Akamai
	"31110":  true, // Akamai
	"35994":  true, // Akamai
	"54113":  true, // Fastly
	"22822":  true, // Limelight Networks
	"15133":  true, // EdgeCast (Verizon)
	"19551":  true, // Incapsula (Imperva)
	"20446":  true, // StackPath / Bunny
	"60068":  true, // CDN77
	"16509":  true, // Amazon CloudFront
	"36408":  true, // CDNetworks
	"4809":   true, // ChinaCache
	"199524": true, // Gcore
	"212238": true, // BelugaCDN
	"55933":  true, // QUANTIL
	"43260":  true, // Medianova
	"43317":  true, // CDNvideo
	"43996":  true, // CDNsun
	"52320":  true, // GlobeNet
	"396982": true, // Leaseweb CDN
	"16276":  true, // OVH CDN
	"30081":  true, // CacheFly
	"12389":  true, // Zenlayer (跨境CDN)
	"37888":  true, // Alibaba CDN
	"45090":  true, // Tencent CDN
	"174":    true, // Cogent Communications (CDN)
	"3356":   true, // Level 3 Communications (CDN)
	"3209":   true, // Vodafone (CDN服务)
	"14061":  true, // DigitalOcean
	"8452":   true, // Infospace
}

type (
	Store struct {}

	StoreOperation struct {
		Type   int
		Group  string
		Config string
		Target string
		Node   string
		Data   []byte
	}

	StatsRecord struct {
		Success            int64                   `json:"success"`
		Failure            int64                   `json:"failure"`
		ConnectTime        int64                   `json:"connect_time"`
		Latency            int64                   `json:"latency"`
		LastUsed           int64                   `json:"last_used"`
		Weights            map[string]float64      `json:"weights"`
		UploadTotal        float64                 `json:"upload_total"`
		DownloadTotal      float64                 `json:"download_total"`
		MaxUploadRate      float64                 `json:"max_upload_rate"`
		MaxDownloadRate    float64                 `json:"max_download_rate"`
		ConnectionDuration float64                 `json:"connection_duration"`
	}

	ModelInput struct {
		// 节点历史性能指标
		Success     int64 // 成功次数
		Failure     int64 // 失败次数
		ConnectTime int64 // 连接时间(毫秒)
		Latency     int64 // 延迟(毫秒)

		// 上传相关特征
		UploadTotal          float64 // 上传流量(字节)
		HistoryUploadTotal   float64 // 历史上传流量(字节)
		MaxuploadRate        float64 // 最大上传速率(字节/秒)
		HistoryMaxUploadRate float64 // 历史最大上传速率(字节/秒)

		// 下载相关特征
		DownloadTotal          float64 // 下载流量(字节)
		HistoryDownloadTotal   float64 // 历史下载流量(字节)
		MaxdownloadRate        float64 // 最大下载速率(字节/秒)
		HistoryMaxDownloadRate float64 // 历史最大下载速率(字节/秒)

		ConnectionDuration float64 // 连接持续时间(毫秒)
		LastUsed           int64   // 上次使用时间

		// 连接特征
		IsUDP bool // 是否UDP连接
		IsTCP bool // 是否TCP连接

		// 元数据特征
		DestIPASN string   // 目标IP的ASN信息
		Host      string   // 域名信息
		DestIP    string   // 目标IP地址
		DestPort  uint16   // 目标端口
		DestGeoIP []string // 目标IP的地理位置信息

		GroupName string // 策略组名称
		NodeName  string // 节点名称
	}

	NodeState struct {
		Name               string         `json:"name"`
		FailureCount       int            `json:"failure_count"`
		LastFailure        int64          `json:"last_failure"`
		BlockedUntil       int64          `json:"blocked_until"`
		Degraded           bool           `json:"degraded"`
		DegradedFactor     float64        `json:"degraded_factor"`
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

	UnwrapMap struct {
		TCP    []string  `json:"tcp,omitempty"`
		UDP    []string  `json:"udp,omitempty"`
		RefTCP string    `json:"ref_tcp,omitempty"`
		RefUDP string    `json:"ref_udp,omitempty"`
	}
)

func NewStore(newdb *bbolt.DB) *Store {
	db = newdb
	InitCache()
	InitQueue()
	return &Store{}
}

// 格式化数据库键
func FormatDBKey(parts ...string) string {
	elements := make([]string, 0, len(parts)+1)
	elements = append(elements, "smart")

	for _, part := range parts {
		if part != "" {
			elements = append(elements, part)
		}
	}

	return strings.Join(elements, "/")
}

func formatOperationKey(op *StoreOperation) string {
	switch op.Type {
	case OpSaveNodeState:
		return FormatDBKey(KeyTypeNode, op.Config, op.Group, op.Node)
	case OpSaveStats:
		return FormatDBKey(KeyTypeStats, op.Config, op.Group, op.Target, op.Node)
	case OpSavePrefetch:
		return FormatDBKey(KeyTypePrefetch, op.Config, op.Group, op.Target)
	case OpSaveRanking:
		return FormatDBKey(KeyTypeRanking, op.Config, op.Group)
	case OpSaveHostFailures:
		return FormatDBKey(KeyTypeHostFailures, op.Config, op.Group, op.Target)
	default:
		return ""
	}
}

// 获取有效顶级域名加一二级域名并使用通配符处理
func GetEffectiveTarget(host string, dstIP string) (string) {
	if host == "" {
		return dstIP
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

	if targetCache != nil {
		if result, _ := targetCache.GetOrStore(h, func() string {
			return compute()
		}); result != "" {
			if strings.HasPrefix(result, "*.") {
				targetCache.Set(result, result)
				targetCache.Set(h, result)
				return result
			}

			if result == h {
				parts := strings.Split(h, ".")
				if len(parts) == 2 {
					wildcard := "*." + h
					targetCache.Set(h, wildcard)
					targetCache.Set(wildcard, wildcard)
					return wildcard
				}
				if len(parts) > 2 {
					wildcard := "*." + parts[len(parts)-2] + "." + parts[len(parts)-1]
					if cachedVal, ok := targetCache.Get(wildcard); ok && cachedVal != "" {
						targetCache.Set(h, cachedVal)
						return cachedVal
					}
				}
			}
		}
	}

	return compute()
}

// 时间衰减
func GetTimeDecayWithCache(lastUsedTime int64, now int64, minDecay float64) float64 {
	fuzzyLastUsedTime := (lastUsedTime / 3600) * 3600

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
		decay = 0.1
	}

	decay = math.Max(minDecay, decay)
	return decay
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
	var total float64 = 0.0
	var available float64 = 0.0
	var output string
	var err error

	// 获取总内存
	if runtime.GOOS == "windows" {
		output, err = cmd.ExecCmd("wmic OS get TotalVisibleMemorySize")
		if err == nil {
			lines := strings.Split(output, "\n")
			if len(lines) >= 2 {
				memStr := strings.TrimSpace(lines[1])
				memKB, parseErr := strconv.ParseFloat(memStr, 64)
				if parseErr == nil {
					total = memKB / 1024.0
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
					total = memKB / 1024.0
				}
			}
		}
	}

	// 获取可用内存
	if runtime.GOOS == "windows" {
		output, err = cmd.ExecCmd("wmic OS get FreePhysicalMemory")
		if err == nil {
			lines := strings.Split(output, "\n")
			if len(lines) >= 2 {
				memStr := strings.TrimSpace(lines[1])
				memKB, parseErr := strconv.ParseFloat(memStr, 64)
				if parseErr == nil {
					available = memKB / 1024.0
				}
			}
		}
	} else if runtime.GOOS == "linux" || runtime.GOOS == "android" || runtime.GOOS == "darwin" || runtime.GOOS == "freebsd" {
		output, err = cmd.ExecCmd("grep MemAvailable /proc/meminfo")
		if err == nil {
			parts := strings.Fields(output)
			if len(parts) >= 2 {
				memStr := strings.TrimSuffix(parts[1], "kB")
				memStr = strings.TrimSpace(memStr)
				memKB, parseErr := strconv.ParseFloat(memStr, 64)
				if parseErr == nil {
					available = memKB / 1024.0
				}
			}
		}
	}

	if total > 0 {
		used := total - available
		return math.Min(used/total, 1.0)
	}
	return 0.5
}

func InitQueue()  {
	threshold := GetBatchSaveThreshold()
	emptyQueue := make([]StoreOperation, 0, threshold)
	replaceGlobalQueue(emptyQueue)
}

func (s *Store) AppendToGlobalQueue(operations ...StoreOperation) {
	if len(operations) == 0 {
		return
	}

	shouldFlush := false
	var snapshot []StoreOperation

	globalOperationQueue.Update(func(old []StoreOperation) []StoreOperation {
		opMap := make(map[string]*StoreOperation)

		for i := range old {
			key := formatOperationKey(&old[i])
			if key != "" {
				opMap[key] = &old[i]
			}
		}

		for i := range operations {
			key := formatOperationKey(&operations[i])
			if key != "" {
				opMap[key] = &operations[i]
			}
		}

		newQueue := make([]StoreOperation, 0, len(opMap))
		for _, op := range opMap {
			newQueue = append(newQueue, *op)
		}

		threshold := GetBatchSaveThreshold()
		if len(newQueue) >= threshold {
			shouldFlush = true
			snapshot = make([]StoreOperation, len(newQueue))
			copy(snapshot, newQueue)
			newQueue = make([]StoreOperation, 0, threshold)
		}

		return newQueue
	})

	if shouldFlush && len(snapshot) > 0 {
		go func() {
			if err := s.BatchSave(snapshot); err == nil {
				log.Debugln("[SmartStore] Queue datas saved, operations: [%d]", len(snapshot))
			}
		}()
	}
}

func replaceGlobalQueue(newQueue []StoreOperation) {
	globalOperationQueue.Store(newQueue)
}

func getGlobalQueueSnapshot() []StoreOperation {
	return globalOperationQueue.Load()
}

func updateGlobalQueue(updateFunc func([]StoreOperation) []StoreOperation) {
	globalOperationQueue.Update(updateFunc)
}

func removeFromGlobalQueue(shouldRemove func(StoreOperation) bool) {
	updateGlobalQueue(func(currentQueue []StoreOperation) []StoreOperation {
		newQueue := make([]StoreOperation, 0, len(currentQueue))
		for _, op := range currentQueue {
			if !shouldRemove(op) {
				newQueue = append(newQueue, op)
			}
		}
		return newQueue
	})
}

func filterQueueByConfig(config string) {
	updateGlobalQueue(func(currentQueue []StoreOperation) []StoreOperation {
		newQueue := make([]StoreOperation, 0, len(currentQueue))
		for _, op := range currentQueue {
			if op.Config != config {
				newQueue = append(newQueue, op)
			}
		}
		return newQueue
	})
}

func filterQueueByGroup(group, config string) {
	updateGlobalQueue(func(currentQueue []StoreOperation) []StoreOperation {
		newQueue := make([]StoreOperation, 0, len(currentQueue))
		for _, op := range currentQueue {
			if !(op.Group == group && op.Config == config) {
				newQueue = append(newQueue, op)
			}
		}
		return newQueue
	})
}

func removeNodesFromQueue(group, config string, nodes []string) {
	removeFromGlobalQueue(func(op StoreOperation) bool {
		if op.Group == group && op.Config == config {
			for _, node := range nodes {
				if op.Node == node {
					return true
				}
			}
		}
		return false
	})
}

// 按级别刷新缓存
func (s *Store) FlushByLevel(level string, config string, group string) error {
	if level == "" {
		return errors.New("flush level cannot be empty")
	}

	if level == "all" {
		InitQueue()
	} else if level == "config" {
		filterQueueByConfig(config)
	} else if level == "group" {
		filterQueueByGroup(group, config)
	}

	s.clearCache(level, config, group)

	if level == "all" {
		s.DBBatchDeletePrefix("smart", false)
	} else if level == "config" {
		s.DBBatchDeletePrefix(FormatDBKey(KeyTypeStats, config), false)
		s.DBBatchDeletePrefix(FormatDBKey(KeyTypeNode, config), false)
		s.DBBatchDeletePrefix(FormatDBKey(KeyTypeRanking, config), false)
		s.DBBatchDeletePrefix(FormatDBKey(KeyTypePrefetch, config), false)
		s.DBBatchDeletePrefix(FormatDBKey(KeyTypeHostFailures, config), false)
	} else if level == "group" {
		s.DBBatchDeletePrefix(FormatDBKey(KeyTypeStats, config, group), false)
		s.DBBatchDeletePrefix(FormatDBKey(KeyTypeNode, config, group), false)
		s.DBBatchDeletePrefix(FormatDBKey(KeyTypeRanking, config, group), false)
		s.DBBatchDeletePrefix(FormatDBKey(KeyTypePrefetch, config, group), false)
		s.DBBatchDeletePrefix(FormatDBKey(KeyTypeHostFailures, config, group), false)
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
