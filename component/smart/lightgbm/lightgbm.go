package lightgbm

import (
    "context"
    "fmt"
    "io"
    "math"
    "net/http"
    "net/netip"
    "os"
    "regexp"
    "strconv"
    "strings"
    "sync"
    "time"

    C "github.com/metacubex/mihomo/constant"
    mihomoHttp "github.com/metacubex/mihomo/component/http"
    "github.com/metacubex/mihomo/component/smart"
    "github.com/metacubex/mihomo/log"
    "github.com/dmitryikh/leaves"
)

const (
    MaxFeatureSize = 21  // 特征数量
)

var (
    globalModel       *WeightModel
    modelInitOnce     sync.Once
    
    asnNumberRegex    = regexp.MustCompile(`^(\d+)`)
    domainRegex       = regexp.MustCompile(`([a-zA-Z0-9-]+)(\.[a-zA-Z0-9-]+)+$`)
    ipv4Regex         = regexp.MustCompile(`^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$`)
    
    // 常见ASN提供商类型分类
    asnCategories = map[string]int{
        // 全球科技
        "google":      1,
        "amazon":      2,
        "microsoft":   3,
        "facebook":    4,
        "apple":       5,
        "cloudflare":  6,
        "akamai":      7,
        "fastly":      8,
        "netflix":     9,
        "alibaba":     10,
        "tencent":     11,
        "baidu":       12,
        // 中国运营商
        "chinatelecom": 13,
        "chinaunicom":  14,
        "chinamobile":  15,
        "chinaedu":     16,
        "cstnet":       17,
        // 全球CDN/云服务
        "cdn77":        20,
        "limelight":    21,
        "edgecast":     22,
        "stackpath":    23,
        "imperva":      24,
        "oracle":       25,
        "ibm":          26,
        "digitalocean": 27,
        "linode":       28,
        "ovh":          29,
        "hetzner":      30,
        "vultr":        31,
        "cogent":       32,
        "leaseweb":     33,
        "upyun":        34,
        "qingcloud":    35,
        "ucloud":       36,
        // 国际主要运营商
        "verizon":      40,
        "comcast":      41,
        "att":          42,
        "sprint":       43,
        "tmobile":      44,
        "level3":       45,
        "ntt":          46,
        "kddi":         47,
        "softbank":     48,
        "telstra":      49,
        "singtel":      50,
        "starhub":      51,
        "m1":           52,
        "pccw":         53,
        "hkbn":         54,
        "smartone":     55,
        "hgc":          56,
        "cht":          57,
        "fetnet":       58,
        "twm":          59,
        // 内容提供商
        "twitter":      70,
        "twitch":       71,
        "discord":      72,
        "spotify":      73,
        "github":       74,
        "steam":        75,
        "blizzard":     76,
        "riotgames":    77,
        "epicgames":    78,
        "ea":           79,
        "bytedance":    80,
        "bilibili":     81,
        "netactuate":   82,
        // 主要交换中心
        "hkix":         90,
        "linx":         91,
        "jpix":         92,
        "equinix":      93,
        "sgix":         94,
        "de-cix":       95,
        "ams-ix":       96,
        // 教育科研
        "cern":         100,
        "mit":          101,
        "stanford":     102,
        "tsinghua":     103,
        "pku":          104,
        // 金融行业
        "visa":         110,
        "mastercard":   111,
        "paypal":       112,
        "stripe":       113,
        "alipay":       114,
        "wechatpay":    115,
    }
    
    // 主要国家/地区代码映射
    geoCategories = map[string]int{
        "CN": 1,  // 中国
        "HK": 2,  // 香港
        "TW": 3,  // 台湾
        "JP": 4,  // 日本
        "KR": 5,  // 韩国
        "SG": 6,  // 新加坡
        "US": 7,  // 美国
        "CA": 8,  // 加拿大
        "GB": 9,  // 英国
        "DE": 10, // 德国
        "FR": 11, // 法国
        "RU": 12, // 俄罗斯
        "AU": 13, // 澳大利亚
        "IN": 14, // 印度
        "BR": 15, // 巴西
        "IT": 16, // 意大利
        "ES": 17, // 西班牙
        "NL": 18, // 荷兰
        "SE": 19, // 瑞典
        "CH": 20, // 瑞士
        "PL": 21, // 波兰
        "TR": 22, // 土耳其
        "MX": 23, // 墨西哥
        "ZA": 24, // 南非
        "AR": 25, // 阿根廷
        "ID": 26, // 印度尼西亚
        "TH": 27, // 泰国
        "VN": 28, // 越南
        "PH": 29, // 菲律宾
        "MY": 30, // 马来西亚
        "MO": 31, // 澳门
    }
    
    // 端口服务分类
    wellKnownPorts = map[uint16]int{
        22:   1,  // SSH
        25:   2,  // SMTP
        53:   3,  // DNS
        80:   4,  // HTTP
        110:  5,  // POP3
        143:  6,  // IMAP
        443:  7,  // HTTPS
        465:  8,  // SMTPS
        993:  9,  // IMAPS
        995:  10, // POP3S
        1194: 11, // OpenVPN
        1812: 12, // RADIUS
        3306: 13, // MySQL
        5432: 14, // PostgreSQL
        6379: 15, // Redis
        27017:16, // MongoDB
        6660: 17, // IRC
        6665: 17, // IRC
        6666: 17, // IRC
        6667: 17, // IRC
        6668: 17, // IRC
        6669: 17, // IRC
        8000: 18, // 常见HTTP替代端口
        8008: 18, // 常见HTTP替代端口
        8080: 18, // 常见HTTP替代端口
        8443: 19, // 常见HTTPS替代端口
        8883: 20, // MQTT over TLS
    }
    
    // 端口范围分类
    portRanges = []struct {
        min, max uint16
        category int
    }{
        {0, 1023, 20},       // 系统端口
        {1024, 49151, 21},   // 注册端口
        {49152, 65535, 22},  // 动态端口
    }

    gameSpecificPorts = map[uint16]bool{
        25565: true,            // Minecraft
        27015: true, 27016: true, 27017: true, 27018: true, 27019: true, 27020: true, // Steam/Counter-Strike
        27031: true, 27036: true, // Steam In-Home Streaming
        3074:  true,            // Xbox Live
        3478:  true, 3479: true, // PlayStation Network / Nintendo Switch Online
        3659:  true,            // 腾讯游戏
        6250:  true,            // 网易游戏
        7000:  true, 7001: true, 7002: true, 7003: true, 7004: true, // 多种游戏服务
        8393:  true, 8394: true, // Origin
        9000:  true, 9001: true, // QQ游戏
        9330:  true, 9331: true, // 多种游戏服务
        9339:  true,            // 多种游戏服务
        14000: true, 14001: true, 14002: true, 14003: true, 14004: true, 14008: true, // Battlefield
        16000: true,            // Battlefield
        18000: true, 18060: true, 18120: true, 18180: true, 18240: true, 18300: true, // Fortnite
        19000: true, 19132: true, // Minecraft PE
        20000: true, 20001: true, 20002: true, // Garena
        22100: true, 22101: true, 22102: true, // Valorant
        30000: true, 30001: true, 30002: true, 30003: true, 30004: true, // Call of Duty
        35000: true, 35001: true, 35002: true, // PUBG/和平精英
        40000: true, 40001: true, 40002: true, // 多种游戏服务
        50000: true, 50001: true, 50002: true, // League of Legends（外服）
        50505: true,            // Arena of Valor / 王者荣耀
        65010: true, 65050: true, // LOL手游
        3724:  true, // World of Warcraft
        6112:  true, // Warcraft III/Battle.net
        6881:  true, // BitTorrent
    }

    // 通信服务专用端口
    communicationPorts = map[uint16]bool{
        5060:  true, 5061: true,  // SIP
        1720:  true,              // H.323
        1080:  true, 1443: true,  // 多种代理和通信服务
        3478:  true, 3479: true,  // STUN/TURN
        5349:  true, 5350: true,  // STUN/TURN over TLS
        5222:  true, 5269: true,  // XMPP
        5938:  true,              // TeamViewer
        6881:  true, 6882: true, 6883: true, 6884: true, 6885: true, 
        6886:  true, 6887: true, 6888: true, 6889: true, // BT
        8801:  true, 8802: true,  // 多种 P2P 通信
        8443:  true,              // 常见 WebRTC/视频会议
        10000: true, 10001: true, // WebRTC Media
        19302: true, 19303: true, // Google STUN
        50000: true, 50001: true, 50002: true, // 常见 RTP 媒体端口
        50003: true, 50004: true, 50005: true, // 常见 RTP 媒体端口
        55000: true, 55001: true, // 多种通信应用
        1863:  true, // MSN Messenger
        5228:  true, // Google GCM/FCM
        34784: true, // Zoom
    }

    // 端口范围分类 - 游戏和通信相关
    gameCommRanges = []struct {
        min, max uint16
        category int // 1=游戏, 2=通信, 3=混合
    }{
        {3000, 3999, 3},   // 混合范围，包含多种游戏和通信应用
        {5000, 5999, 2},   // 通信应用范围
        {6000, 7000, 3},   // 混合范围，包含P2P和游戏
        {8000, 9000, 3},   // 混合范围，包含游戏和通信
        {10000, 20000, 3}, // 混合范围，包含WebRTC和多种游戏服务
        {27000, 28000, 1}, // Steam和相关游戏端口
        {30000, 32000, 1}, // 游戏服务常见端口
        {49000, 50000, 2}, // 多种RTP/通信使用的高位端口
        {50000, 55000, 3}, // 混合范围，包含通信和游戏
        {55000, 60000, 2}, // 多种通信使用的高位端口
    }
    
    // 常见流媒体/游戏域名关键字
    streamingKeywords = []string{
        "youtube", "netflix", "hulu", "spotify", "tiktok", "douyin", "youku", "iqiyi",
        "bilibili", "twitch", "hbo", "disney", "vimeo", "vod", "stream", "video", 
        "media", "movie", "tv", "music", "audio", "cdm", "cdn", "content",
        "live", "livestream", "replay", "shorts", "kuaishou", "huya", "douyu",
    }
    
    gameKeywords = []string{
        "game", "play", "steam", "xbox", "playstation", "nintendo", "ea.com", "riot",
        "blizzard", "ubisoft", "epic", "cod", "minecraft", "roblox", "pubg", "fortnite",
        "valorant", "riotgames", "leagueoflegends", "warzone",
        "apex", "apexlegends", "overwatch", "dota", "csgo",
        "counterstrike", "hearthstone", "battlenet", "battle.net",
        "genshin", "mihoyo", "hoyoverse", "lol", "arenaofvalor", "honorofkings",
    }
    
    communicationKeywords = []string{
        "meet", "zoom", "teams", "voip", "sip", "call", "chat", "conference", "webex",
        "discord", "slack", "telegram", "signal", "whatsapp", "skype", "wechat",
        "voicechat", "videocall", "rtc", "webrtc", "jitsi",
        "mumble", "ventrilo", "teamspeak", "discord.gg",
        "meeting", "conference", "huddle", "gather",
        "qq", "msn", "icq", "line", "kakao", "viber", "imo", "element",
    }
    
    privateIPNetworks = []struct {
        prefix   netip.Prefix
        category int
    }{
        // IPv4私有地址范围
        {netip.MustParsePrefix("10.0.0.0/8"), 1},
        {netip.MustParsePrefix("172.16.0.0/12"), 1},
        {netip.MustParsePrefix("192.168.0.0/16"), 1},
        {netip.MustParsePrefix("127.0.0.0/8"), 2},
        {netip.MustParsePrefix("169.254.0.0/16"), 3},
        
        // IPv6私有地址范围
        {netip.MustParsePrefix("::1/128"), 2},
        {netip.MustParsePrefix("fe80::/10"), 3},
        {netip.MustParsePrefix("fc00::/7"), 1},
        {netip.MustParsePrefix("2001:db8::/32"), 4},
    }
)

type WeightModel struct {
    model       *leaves.Ensemble
    transforms  *FeatureTransforms
    lastUpdate  time.Time
    mutex       sync.RWMutex
}

type ModelInput struct {
    // 节点历史性能指标
    Success         int64   // 成功次数
    Failure         int64   // 失败次数
    ConnectTime     int64   // 连接时间(毫秒)
    Latency         int64   // 延迟(毫秒)
    UploadTotal     float64 // 上传流量(字节)
    DownloadTotal   float64 // 下载流量(字节)
    ConnectionDuration float64 // 连接持续时间(毫秒)
    LastUsed        int64   // 上次使用时间
    
    // 连接特征
    IsUDP           bool    // 是否UDP连接
    IsTCP           bool    // 是否TCP连接
    
    // 元数据特征
    DestIPASN       string  // 目标IP的ASN信息
    Host            string  // 域名信息
    DestIP          string  // 目标IP地址
    DestPort        uint16  // 目标端口
    DestGeoIP       []string // 目标IP的地理位置信息

    GroupName       string  // 策略组名称
    NodeName        string  // 节点名称
}

func GetModel() *WeightModel {
    modelInitOnce.Do(func() {
        globalModel = &WeightModel{}
        
        modelPath := C.Path.SmartModel()
        
        if _, err := os.Stat(modelPath); err == nil {
            if err := globalModel.loadModel(modelPath); err != nil {
                log.Warnln("[Smart] Model.bin invalid, remove and download: %v", err)
                
                if rmErr := os.Remove(modelPath); rmErr != nil {
                    log.Errorln("[Smart] Failed to remove invalid Model.bin: %v", rmErr)
                    globalModel = nil
                    return
                }
                
                if downloadErr := downloadModel(modelPath); downloadErr != nil {
                    log.Errorln("[Smart] Failed to download Model.bin: %v", downloadErr)
                    globalModel = nil
                    return
                }
                
                if reloadErr := globalModel.loadModel(modelPath); reloadErr != nil {
                    log.Errorln("[Smart] Failed to load downloaded Model.bin: %v", reloadErr)
                    globalModel = nil
                    return
                }
                
                log.Infoln("[Smart] Model.bin downloaded and loaded successfully")
            } else {
                log.Infoln("[Smart] Model file loaded successfully")
            }
        } else {
            log.Infoln("[Smart] Can't find Model.bin, start download")
            if downloadErr := downloadModel(modelPath); downloadErr != nil {
                log.Errorln("[Smart] Can't download Model.bin: %v", downloadErr)
                globalModel = nil
                return
            }
            
            if loadErr := globalModel.loadModel(modelPath); loadErr != nil {
                log.Errorln("[Smart] Failed to load downloaded Model.bin: %v", loadErr)
                globalModel = nil
                return
            }
            
            log.Infoln("[Smart] Download Model.bin finish")
        }
    })
    
    return globalModel
}

func (m *WeightModel) loadModel(path string) error {
    m.mutex.Lock()
    defer m.mutex.Unlock()
    
    model, err := leaves.LGEnsembleFromFile(path, false)
    if err != nil {
        return fmt.Errorf("failed to load binary model: %v", err)
    }

    // 加载transforms参数
    transforms, err := LoadTransformsFromModel(path)
    if err != nil {
        log.Warnln("[Smart] Failed to load transforms parameters: %v, using default config", err)
        transforms = &FeatureTransforms{
            TransformsEnabled: false,
            FeatureOrder:  getDefaultFeatureOrder(),
            Transforms:    []TransformParams{},
        }
    } else {
        if transforms.TransformsEnabled {
            if err := transforms.ValidateTransforms(MaxFeatureSize); err != nil {
                log.Warnln("[Smart] ValidateTransforms failed: %v", err)
                transforms.TransformsEnabled = false
            } else {
                transforms.DebugTransforms()
            }
        }
    }
    
    m.transforms = transforms
    m.model = model
    m.lastUpdate = time.Now()
    return nil
}

func downloadModel(path string) (err error) {
    modelUrl := getModelDownloadURL()
    
    ctx, cancel := context.WithTimeout(context.Background(), time.Second*90)
    defer cancel()
    
    resp, err := mihomoHttp.HttpRequest(ctx, modelUrl, http.MethodGet, nil, nil)
	if err != nil {
		return
	}
    defer resp.Body.Close()
    
    f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, resp.Body)

	return err
}

func getModelDownloadURL() string {
    return "https://github.com/vernesong/mihomo/releases/download/LightGBM-Model/Model.bin"
}

func (m *WeightModel) PredictWeight(input *ModelInput, priorityFactor float64) (float64, bool) {
    if m == nil {
        return m.fallbackPrediction(input, priorityFactor), false
    }

    total := input.Success + input.Failure
    if total < smart.DefaultMinSampleCount {
        return 0, false
    }

    m.mutex.RLock()
    model := m.model
    transforms := m.transforms
    m.mutex.RUnlock()
    
    if model == nil {
        return m.fallbackPrediction(input, priorityFactor), false
    }
    
    // 准备原始特征
    features := prepareFeatures(input)
    if len(features) == 0 {
        return m.fallbackPrediction(input, priorityFactor), false
    }
    
    // 应用特征变换
    if transforms != nil && transforms.TransformsEnabled {
        features = transforms.ApplyTransforms(features)
    }
    
    var prediction float64
    
    defer func() {
        if r := recover(); r != nil {
            log.Errorln("[Smart] Model prediction panic: %v", r)
            prediction = m.fallbackPrediction(input, priorityFactor)
        }
    }()
    
    prediction = model.PredictSingle(features, 0)
    
    if math.IsNaN(prediction) || prediction <= 0 {
        return m.fallbackPrediction(input, priorityFactor), false
    }
    
    return prediction * priorityFactor, true
}

func (m *WeightModel) fallbackPrediction(input *ModelInput, priorityFactor float64) float64 {
    return smart.CalculateWeight(
        input.Success,
        input.Failure,
        input.ConnectTime,
        input.Latency,
        input.IsUDP,
        input.UploadTotal,
        input.DownloadTotal,
        input.ConnectionDuration,
        input.LastUsed,
    ) * priorityFactor
}

func min(a, b int) int {
    if a < b {
        return a
    }
    return b
}

func hashStringToFloat(s string, buckets int) float64 {
    if s == "" || buckets <= 0 {
        return 0.0
    }
    
    // FNV-1a
    const (
        fnvOffsetBasis uint32 = 2166136261
        fnvPrime       uint32 = 16777619
    )
    
    hash := fnvOffsetBasis
    for i := 0; i < len(s); i++ {
        hash = hash ^ uint32(s[i])
        hash = hash * fnvPrime
    }
    
    return float64((hash % uint32(buckets)) + 1)
}

func prepareFeatures(input *ModelInput) []float64 {
    if input == nil {
        return []float64{}
    }

    features := make([]float64, 0, MaxFeatureSize)
    
    // 1. 节点性能指标 - 基础特征
    uploadMB := input.UploadTotal / (1024.0 * 1024.0)
    downloadMB := input.DownloadTotal / (1024.0 * 1024.0)
    durationMinutes := input.ConnectionDuration / 60000.0
    lastUsedSeconds := 0.0
    if input.LastUsed > 0 {
        lastUsedSeconds = float64(time.Now().Unix() - input.LastUsed)
    }
    
    // 核心性能指标
    features = append(features, float64(input.Success))                  // 成功次数
    features = append(features, float64(input.Failure))                  // 失败次数
    features = append(features, math.Log1p(float64(input.ConnectTime)))  // 连接时间（对数变换）
    features = append(features, math.Log1p(float64(input.Latency)))      // 延迟（对数变换）
    features = append(features, math.Log1p(uploadMB))                    // 上传流量MB（对数变换）
    features = append(features, math.Log1p(downloadMB))                  // 下载流量MB（对数变换）
    features = append(features, math.Log1p(durationMinutes))             // 连接持续时间分钟（对数变换）
    features = append(features, math.Log1p(lastUsedSeconds))             // 上次使用至今秒数（对数变换）
    
    // 网络协议特征
    features = append(features, boolToFloat(input.IsUDP))                // 是否UDP协议
    features = append(features, boolToFloat(input.IsTCP))                // 是否TCP协议
    
    // 2. ASN特征提取
    asnFeature := extractASNFeature(input.DestIPASN)
    features = append(features, float64(asnFeature))                     // ASN类别特征
    
    // 3. GeoIP特征提取
    countryFeature := extractGeoIPFeature(input.DestGeoIP)
    features = append(features, float64(countryFeature))                 // 国家/地区特征              // 大洲特征
    
    // 4. 目标地址特征处理
    var addressFeature int
    if input.Host != "" {
        // 优先使用域名特征
        addressFeature = extractDomainTypeFeature(input.Host)
    } else if input.DestIP != "" {
        // 备用IP特征
        addressFeature = extractIPFeature(input.DestIP)
    }
    features = append(features, float64(addressFeature))
    
    // 5. 端口特征提取
    portFeature := extractPortFeature(input.DestPort)
    features = append(features, float64(portFeature))
    
    // 6. 流量比例特征 - 帮助区分不同应用类型
    trafficRatio := 0.0
    if uploadMB > 0 && downloadMB > 0 {
        if uploadMB > downloadMB {
            trafficRatio = downloadMB / uploadMB  // 0-1之间，上传为主
        } else {
            trafficRatio = -uploadMB / downloadMB // -1-0之间，下载为主
        }
    }
    features = append(features, trafficRatio)
    
    // 7. 流量时间密度 - 单位时间内的流量
    trafficDensity := 0.0
    if durationMinutes > 0 {
        trafficDensity = math.Log1p((uploadMB + downloadMB) / durationMinutes) 
    }
    features = append(features, trafficDensity) 
    
    // 8. 连接类型特征 - 基于端口和地址类型的综合特征
    connectionTypeFeature := deriveConnectionType(input.DestPort, addressFeature, portFeature)
    features = append(features, float64(connectionTypeFeature))
    
    // 9. 元数据哈希特征
    features = append(features, hashStringToFloat(input.DestIPASN, 500))
    features = append(features, hashStringToFloat(input.Host, 1000))
    features = append(features, hashStringToFloat(input.DestIP, 10000))
    geoHash := 0.0
    if len(input.DestGeoIP) > 0 {
        geoHash = hashStringToFloat(input.DestGeoIP[0], 200)
    }
    features = append(features, geoHash)

    // 确保特征向量大小不超过模型预期
    if len(features) > MaxFeatureSize {
        features = features[:MaxFeatureSize]
    }
    
    return features
}

func extractASNFeature(asnInfo string) int {
    if asnInfo == "" {
        return 0
    }
    
    asnInfo = strings.ToLower(asnInfo)
    
    // 1. 检查是否匹配已知ASN类别
    for keyword, category := range asnCategories {
        if strings.Contains(asnInfo, keyword) {
            return category
        }
    }
    
    // 2. 尝试提取ASN号码并进行简单分类
    if matches := asnNumberRegex.FindStringSubmatch(asnInfo); len(matches) > 1 {
        if asnNum, err := strconv.Atoi(matches[1]); err == nil {
            // 粗略分类ASN号码范围
            switch {
            case asnNum < 1000:
                return 50  // 早期分配的ASN
            case asnNum < 10000:
                return 51  // 较早分配的ASN
            case asnNum < 50000:
                return 52  // 中期分配的ASN
            case asnNum < 150000:
                return 53  // 较新分配的ASN
            default:
                return 54  // 最新分配的ASN
            }
        }
    }
    
    return 0
}

func extractGeoIPFeature(geoIPInfo []string) int {
    if geoIPInfo == nil || len(geoIPInfo) == 0 {
        return 0
    }
    
    // 1. 尝试提取国家/地区代码
    countryCode := ""
    if len(geoIPInfo) > 0 {
        countryCode = geoIPInfo[0]
    }
    
    // 2. 使用预定义类别
    if category, exists := geoCategories[countryCode]; exists {
        return category
    }
    
    // 3. 其他地区使用简单哈希分类
    if countryCode != "" {
        hashValue := 0
        for _, r := range countryCode {
            hashValue = hashValue*31 + int(r)
        }
        return 30 + (hashValue % 20)
    }
    
    return 0  // 默认未知
}

func extractDomainTypeFeature(host string) int {
    if host == "" {
        return 0
    }
    
    host = strings.ToLower(host)
    
    // 1. 检查是否为IP地址形式
    if strings.Contains(host, "[") || (strings.Count(host, ".") == 3 && 
        ipv4Regex.MatchString(host)) {
        return 1  // IP地址
    }
    
    // 2. 检查流媒体/视频相关域名
    for _, keyword := range streamingKeywords {
        if strings.Contains(host, keyword) {
            return 2  // 流媒体服务
        }
    }
    
    // 3. 检查游戏相关域名
    for _, keyword := range gameKeywords {
        if strings.Contains(host, keyword) {
            return 3  // 游戏服务
        }
    }
    
    // 4. 检查通讯/会议相关域名
    for _, keyword := range communicationKeywords {
        if strings.Contains(host, keyword) {
            return 4  // 通讯服务
        }
    }
    
    // 5. 检查顶级域名类型
    if strings.HasSuffix(host, ".cn") {
        return 10  // 中国顶级域名
    } else if strings.HasSuffix(host, ".com") {
        return 11  // 商业域名
    } else if strings.HasSuffix(host, ".net") {
        return 12  // 网络服务域名
    } else if strings.HasSuffix(host, ".org") {
        return 13  // 组织域名
    } else if strings.HasSuffix(host, ".gov") {
        return 14  // 政府域名
    } else if strings.HasSuffix(host, ".edu") {
        return 15  // 教育域名
    }
    
    // 6. 分析域名长度和结构
    if matches := domainRegex.FindStringSubmatch(host); len(matches) > 1 {
        domainParts := strings.Split(host, ".")
        if len(domainParts) >= 3 {
            return 30  // 三级及以上域名
        } else {
            return 31  // 二级域名
        }
    }
    
    return 0
}

func extractIPFeature(ipAddr string) int {
    if ipAddr == "" {
        return 0
    }

    addr, err := netip.ParseAddr(ipAddr)
    if err != nil {
        return 0
    }

    for _, network := range privateIPNetworks {
        if network.prefix.Contains(addr) {
            return network.category + 100  // 101-104为不同私有IP类型
        }
    }
    
    if addr.Is4() {
        return 110  // IPv4公网地址
    } else {
        return 111  // IPv6公网地址
    }
}

func extractPortFeature(port uint16) int {
    // 1. 检查是否为已知端口
    if category, exists := wellKnownPorts[port]; exists {
        return category
    }
    
    // 2. 检查是否为游戏或通信专用端口
    if _, isGame := gameSpecificPorts[port]; isGame {
        return 30  // 游戏端口
    }
    
    if _, isComm := communicationPorts[port]; isComm {
        return 31  // 通信端口
    }
    
    // 3. 检查是否在游戏/通信端口范围内
    for _, r := range gameCommRanges {
        if port >= r.min && port <= r.max {
            switch r.category {
            case 1:
                return 32  // 游戏端口范围
            case 2:
                return 33  // 通信端口范围
            case 3:
                return 34  // 混合端口范围
            }
        }
    }
    
    // 4. 检查端口范围
    for _, r := range portRanges {
        if port >= r.min && port <= r.max {
            return r.category
        }
    }
    
    return 0  // 未知端口类型
}

func deriveConnectionType(port uint16, addressFeature, portFeature int) int {
    // 网页浏览特征
    if port == 80 || port == 443 || portFeature == 4 || portFeature == 5 || 
       portFeature == 11 || portFeature == 12 {
        return 1
    }
    
    // 流媒体特征
    if addressFeature == 2 {
        return 2
    }
    
    // 游戏/通讯特征 - 使用专用端口映射
    if addressFeature == 3 || addressFeature == 4 {
        return 3
    }
    
    // 查找是否为游戏专用端口
    if _, isGamePort := gameSpecificPorts[port]; isGamePort {
        return 3
    }
    
    // 查找是否为通信专用端口
    if _, isCommPort := communicationPorts[port]; isCommPort {
        return 3
    }
    
    // 检查端口范围
    for _, r := range gameCommRanges {
        if port >= r.min && port <= r.max {
            return 3  // 游戏/通信特征
        }
    }
    
    // 系统服务和应用的常见端口 - 常见的高端口应用通常是游戏或通信应用
    if port > 10000 && port < 65000 {
        return 3  // 大多数高位端口用于游戏或通信
    }
    
    // 数据库访问特征
    if portFeature == 8 || portFeature == 9 {
        return 4
    }
    
    // 文件传输特征
    if port == 20 || port == 21 || port == 22 || port == 989 || port == 990 {
        return 5
    }
    
    return 0
}

func boolToFloat(b bool) float64 {
    if b {
        return 1.0
    }
    return 0.0
}

func CreateModelInputFromStats(success, failure, connectTime, latency int64, 
    isUDP bool, isTCP bool, uploadTotal, downloadTotal float64, 
    connectionDuration float64, lastUsed int64, metadata *C.Metadata) *ModelInput {
    
    var input = &ModelInput{
        Success:           success,
        Failure:           failure,
        ConnectTime:       connectTime,
        Latency:           latency,
        UploadTotal:       uploadTotal,
        DownloadTotal:     downloadTotal,
        ConnectionDuration: connectionDuration,
        LastUsed:          lastUsed,
        IsUDP:             isUDP,
        IsTCP:             isTCP,
    }

    if metadata != nil {
        input.DestIPASN = metadata.DstIPASN
        input.Host = metadata.Host
        if metadata.DstIP.IsValid() {
            input.DestIP = metadata.DstIP.String()
        }
        input.DestPort = metadata.DstPort
        input.DestGeoIP = metadata.DstGeoIP
    }
    
    return input
}

func CreateModelInputFromStatsRecord(record *smart.StatsRecord, metadata *C.Metadata, 
    uploadTotal, downloadTotal int64, connectionDuration int64) *ModelInput {
    
    if record == nil || metadata == nil {
        return nil
    }
    
    return CreateModelInputFromStats(
        int64(record.Success),
        int64(record.Failure),
        record.ConnectTime,
        record.Latency,
        metadata.NetWork == C.UDP,
        metadata.NetWork == C.TCP,
        float64(uploadTotal),
        float64(downloadTotal),
        float64(connectionDuration),
        record.LastUsed.Unix(),
        metadata,
    )
}