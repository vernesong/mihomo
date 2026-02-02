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

	"github.com/vernesong/leaves"
	"github.com/metacubex/mihomo/common/singleflight"
	mihomoHttp "github.com/metacubex/mihomo/component/http"
	"github.com/metacubex/mihomo/component/smart"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
)

const (
	MaxFeatureSize = 27
)

var (
	smartModel   *WeightModel
	reloadModel  = singleflight.Group[bool]{StoreResult: false}
	modelOnce    sync.Once

	asnNumberRegex = regexp.MustCompile(`^(\d+)`)
	domainRegex    = regexp.MustCompile(`([a-zA-Z0-9-]+)(\.[a-zA-Z0-9-]+)+$`)
	ipv4Regex      = regexp.MustCompile(`^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$`)

	// 常见ASN提供商类型分类
	asnCategories = map[string]int{
		// 全球科技
		"google":     1,
		"amazon":     2,
		"microsoft":  3,
		"facebook":   4,
		"apple":      5,
		"cloudflare": 6,
		"akamai":     7,
		"fastly":     8,
		"netflix":    9,
		"alibaba":    10,
		"tencent":    11,
		"baidu":      12,
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
		"verizon":  40,
		"comcast":  41,
		"att":      42,
		"sprint":   43,
		"tmobile":  44,
		"level3":   45,
		"ntt":      46,
		"kddi":     47,
		"softbank": 48,
		"telstra":  49,
		"singtel":  50,
		"starhub":  51,
		"m1":       52,
		"pccw":     53,
		"hkbn":     54,
		"smartone": 55,
		"hgc":      56,
		"cht":      57,
		"fetnet":   58,
		"twm":      59,
		// 内容提供商
		"twitter":    70,
		"twitch":     71,
		"discord":    72,
		"spotify":    73,
		"github":     74,
		"steam":      75,
		"blizzard":   76,
		"riotgames":  77,
		"epicgames":  78,
		"ea":         79,
		"bytedance":  80,
		"bilibili":   81,
		"netactuate": 82,
		// 主要交换中心
		"hkix":    90,
		"linx":    91,
		"jpix":    92,
		"equinix": 93,
		"sgix":    94,
		"de-cix":  95,
		"ams-ix":  96,
		// 教育科研
		"cern":     100,
		"mit":      101,
		"stanford": 102,
		"tsinghua": 103,
		"pku":      104,
		// 金融行业
		"visa":       110,
		"mastercard": 111,
		"paypal":     112,
		"stripe":     113,
		"alipay":     114,
		"wechatpay":  115,
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
		22:    1,  // SSH
		25:    2,  // SMTP
		53:    3,  // DNS
		80:    4,  // HTTP
		110:   5,  // POP3
		143:   6,  // IMAP
		443:   7,  // HTTPS
		465:   8,  // SMTPS
		784:   3,  // DNS over QUIC (DoQ)
		853:   3,  // DNS over TLS (DoT)
		993:   9,  // IMAPS
		995:   10, // POP3S
		1194:  11, // OpenVPN
		1812:  12, // RADIUS
		3306:  13, // MySQL
		5053:  3,  // DNS备用端口
		5353:  3,  // mDNS
		5355:  3,  // LLMNR
		5432:  14, // PostgreSQL
		6379:  15, // Redis
		8853:  3,  // DoT备用端口
		9953:  3,  // DNS管理端口
		27017: 16, // MongoDB
		6660:  17, // IRC
		6665:  17, // IRC
		6666:  17, // IRC
		6667:  17, // IRC
		6668:  17, // IRC
		6669:  17, // IRC
		8000:  18, // 常见HTTP替代端口
		8008:  18, // 常见HTTP替代端口
		8080:  18, // 常见HTTP替代端口
		8443:  19, // 常见HTTPS替代端口
		8883:  20, // MQTT over TLS
	}

	// 端口范围分类
	portRanges = []struct {
		min, max uint16
		category int
	}{
		{0, 1023, 20},      // 系统端口
		{1024, 49151, 21},  // 注册端口
		{49152, 65535, 22}, // 动态端口
	}

	apiServicePorts = map[uint16]bool{
		8080: true, 8443: true, 9000: true, 9001: true, 9002: true, // 常见API端口
		3000: true, 3001: true, 5000: true, 5001: true, // 开发API端口
		8000: true, 8001: true, 8888: true, 4000: true, 4001: true, // 其他API服务端口
		6000: true, 6001: true, 7000: true, 7001: true, // 微服务API端口
	}

	dnsServicePorts = map[uint16]bool{
		53:   true, // 传统DNS (UDP/TCP)
		853:  true, // DNS over TLS (DoT)
		784:  true, // DNS over QUIC (DoQ) - IANA分配的端口
		5053: true, // 一些DNS服务的备用端口
		5353: true, // mDNS (Multicast DNS)
		5355: true, // LLMNR (Link-Local Multicast Name Resolution)
		8853: true, // 一些DoT服务使用的备用端口
		9953: true, // 一些DNS服务使用的管理端口
	}

	gameSpecificPorts = map[uint16]bool{
		25565: true,                                                                  // Minecraft
		27015: true, 27016: true, 27017: true, 27018: true, 27019: true, 27020: true, // Steam/Counter-Strike
		27031: true, 27036: true, // Steam In-Home Streaming
		3074: true,             // Xbox Live
		3478: true, 3479: true, // PlayStation Network / Nintendo Switch Online
		3659: true,                                                 // 腾讯游戏
		6250: true,                                                 // 网易游戏
		7000: true, 7001: true, 7002: true, 7003: true, 7004: true, // 多种游戏服务
		8393: true, 8394: true, // Origin
		9000: true, 9001: true, // QQ游戏
		9330: true, 9331: true, // 多种游戏服务
		9339:  true,                                                                  // 多种游戏服务
		14000: true, 14001: true, 14002: true, 14003: true, 14004: true, 14008: true, // Battlefield
		16000: true,                                                                  // Battlefield
		18000: true, 18060: true, 18120: true, 18180: true, 18240: true, 18300: true, // Fortnite
		19000: true, 19132: true, // Minecraft PE
		20000: true, 20001: true, 20002: true, // Garena
		22100: true, 22101: true, 22102: true, // Valorant
		30000: true, 30001: true, 30002: true, 30003: true, 30004: true, // Call of Duty
		35000: true, 35001: true, 35002: true, // PUBG/和平精英
		40000: true, 40001: true, 40002: true, // 多种游戏服务
		50000: true, 50001: true, 50002: true, // League of Legends（外服）
		50505: true,              // Arena of Valor / 王者荣耀
		65010: true, 65050: true, // LOL手游
		3724: true, // World of Warcraft
		6112: true, // Warcraft III/Battle.net
		6881: true, // BitTorrent
	}

	// 通信服务专用端口
	communicationPorts = map[uint16]bool{
		5060: true, 5061: true, // SIP
		1720: true,             // H.323
		1080: true, 1443: true, // 多种代理和通信服务
		3478: true, 3479: true, // STUN/TURN
		5349: true, 5350: true, // STUN/TURN over TLS
		5222: true, 5269: true, // XMPP
		5938: true, // TeamViewer
		6881: true, 6882: true, 6883: true, 6884: true, 6885: true,
		6886: true, 6887: true, 6888: true, 6889: true, // BT
		8801: true, 8802: true, // 多种 P2P 通信
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

	apiServiceKeywords = []string{
		"api.cloudflare.com", "api.amazonaws.com", "api.azure.com", "googleapis.com",
		"api.fastly.com", "api.maxcdn.com", "api.keycdn.com", "api.bunnycdn.com",
		"api.digitalocean.com", "api.vultr.com", "api.linode.com", "api.hetzner.com",

		"api.vercel.com", "api.netlify.com", "api.heroku.com", "api.railway.app",
		"api.render.com", "api.fly.io", "registry.npmjs.org", "pypi.org",
		"hub.docker.com", "registry.docker.io", "rubygems.org", "crates.io",

		"api.datadog.com", "api.newrelic.com", "api.segment.com", "api.mixpanel.com",
		"api.amplitude.com", "api.hotjar.com", "api.sentry.io", "api.rollbar.com",

		"api.auth0.com", "api.okta.com", "api.twilio.com", "api.sendgrid.com",
		"api.mailgun.com", "api.stripe.com",

		"ecs.aliyuncs.com", "api.qcloud.com", "api.ucloud.cn", "api.huaweicloud.com",
		"api.baidubce.com", "api.volcengine.com",

		"gateway.", "api-gateway.", "apigateway.", "/api/", "/v1/", "/v2/", "/v3/", "/v4/",
		"/rest/", "/graphql/", "rest.", "graphql.", "webhook.", "rpc.",
	}

	dnsServiceKeywords = []string{
		"8.8.8.8", "8.8.4.4", "1.1.1.1", "1.0.0.1", "9.9.9.9", "149.112.112.112",
		"208.67.222.222", "208.67.220.220",

		"dns.google", "dns.google.com", "cloudflare-dns.com", "dns.cloudflare.com",
		"one.one.one.one", "family.cloudflare-dns.com", "security.cloudflare-dns.com",
		"dns.quad9.net", "dns9.quad9.net", "dns10.quad9.net", "dns11.quad9.net",
		"doh.opendns.com", "doh.familyshield.opendns.com", "doh.sandbox.opendns.com",
		"mozilla.cloudflare-dns.com", "firefox.dns.nextdns.io",
		"dns.adguard.com", "dns-family.adguard.com", "dns-unfiltered.adguard.com",
		"doh.cleanbrowsing.org", "family-filter-dns.cleanbrowsing.org",

		"dot.cloudflare-dns.com", "dot.alidns.com", "dot.dns.sb", "dot.360.cn",

		"doh.pub", "dns.pub", "doh.360.cn", "dns.alidns.com", "doh.alidns.com",
		"doh.dns.sb", "rubyfish.cn", "dns.rubyfish.cn", "pdns.fkgfw.cf",

		"commons.host", "odvr.nic.cz", "doh.libredns.gr", "dns.digitale-gesellschaft.ch",
		"dns.switch.ch", "jp.tiar.app", "jp.tiarap.org", "kaitain.restena.lu",
		"dns.twnic.tw", "dns.hinet.net",

		"dns", "doh", "doq", "dot", "resolver", "nameserver", "recursive",
		"authoritative", "secure-dns", "private-dns",
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
	model      *leaves.Ensemble
	transforms *FeatureTransforms
	lastUpdate time.Time
	mutex      sync.RWMutex
}

func GetModel() *WeightModel {
    modelOnce.Do(func() {
        m := &WeightModel{}
        modelPath := C.Path.SmartModel()

        if _, err := os.Stat(modelPath); err == nil {
            if err := m.loadModel(modelPath); err != nil {
                log.Warnln("[Smart] Model.bin invalid, remove and download: %v", err)
                if rmErr := os.Remove(modelPath); rmErr != nil {
                    log.Errorln("[Smart] Failed to remove invalid Model.bin: %v", rmErr)
                    return
                }

                if downloadErr := downloadModel(modelPath); downloadErr != nil {
                    log.Errorln("[Smart] Failed to download Model.bin: %v", downloadErr)
                    return
                }

                if reloadErr := m.loadModel(modelPath); reloadErr != nil {
                    log.Errorln("[Smart] Failed to load downloaded Model.bin: %v", reloadErr)
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
                return
            }

            if loadErr := m.loadModel(modelPath); loadErr != nil {
                log.Errorln("[Smart] Failed to load downloaded Model.bin: %v", loadErr)
                return
            }

            log.Infoln("[Smart] Download Model.bin finish")
        }

        smartModel = m
    })

    return smartModel
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
			FeatureOrder:      getDefaultFeatureOrder(),
			Transforms:        []TransformParams{},
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

func ReloadModel() {
	if smartModel != nil {
		success, err, _ := reloadModel.Do("reload", func() (bool, error) {
			modelPath := C.Path.SmartModel()
			if _, err := os.Stat(modelPath); err == nil {
				if err := smartModel.loadModel(modelPath); err != nil {
					return false, err
				} else {
					return true, nil
				}
			}
			return false, nil
		})

		if err != nil {
			log.Errorln("[Smart] Model reload failed: %v", err)
		} else if success {
			log.Debugln("[Smart] Model reload completed successfully")
		}
	}
}

func downloadModel(path string) (err error) {
	modelUrl := GetModelDownloadURL()

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

func GetModelDownloadURL() string {
	return "https://github.com/vernesong/mihomo/releases/download/LightGBM-Model/Model.bin"
}

func (m *WeightModel) PredictWeight(input *smart.ModelInput, priorityFactor float64) (float64, bool) {
	if m == nil {
		return smart.CalculateWeight(input, priorityFactor)
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
		return smart.CalculateWeight(input, priorityFactor)
	}

	// 准备原始特征
	features := prepareFeatures(input)
	if len(features) == 0 {
		return smart.CalculateWeight(input, priorityFactor)
	}

	// 应用特征变换
	if transforms != nil && transforms.TransformsEnabled {
		features = transforms.ApplyTransforms(features)
	}

	var prediction float64

	defer func() {
		if r := recover(); r != nil {
			log.Errorln("[Smart] Model prediction panic: %v", r)
			prediction, _ = smart.CalculateWeight(input, priorityFactor)
		}
	}()

	prediction = model.PredictSingle(features, 0)

	if math.IsNaN(prediction) || prediction <= 0 {
		return smart.CalculateWeight(input, priorityFactor)
	}

	return prediction * priorityFactor, true
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

func prepareFeatures(input *smart.ModelInput) []float64 {
	features := make([]float64, 0, MaxFeatureSize)

	// 1. 最后使用时间间隔
	uploadMB := input.UploadTotal
	downloadMB := input.DownloadTotal
	maxUploadRateKB := input.MaxuploadRate
	maxDownloadRateKB := input.MaxdownloadRate
	durationMinutes := input.ConnectionDuration
	lastUsedSeconds := 0.0
	if input.LastUsed > 0 {
		lastUsedSeconds = float64(time.Now().Unix() - input.LastUsed)
	}

	// 核心性能指标
	features = append(features, float64(input.Success))                   // 成功次数
	features = append(features, float64(input.Failure))                   // 失败次数
	features = append(features, math.Log1p(float64(input.ConnectTime)))   // 连接时间（对数变换）
	features = append(features, math.Log1p(float64(input.Latency)))       // 延迟（对数变换）
	features = append(features, math.Log1p(uploadMB))                     // 上传流量MB
	features = append(features, math.Log1p(input.HistoryUploadTotal))     // 历史上传流量
	features = append(features, math.Log1p(maxUploadRateKB))              // 最大上传速率
	features = append(features, math.Log1p(input.HistoryMaxUploadRate))   // 历史最大上传速率
	features = append(features, math.Log1p(downloadMB))                   // 下载流量MB
	features = append(features, math.Log1p(input.HistoryDownloadTotal))   // 历史下载流量
	features = append(features, math.Log1p(maxDownloadRateKB))            // 最大下载速率
	features = append(features, math.Log1p(input.HistoryMaxDownloadRate)) // 历史最大下载速率
	features = append(features, math.Log1p(durationMinutes))              // 连接持续时间分钟（对数变换）
	features = append(features, math.Log1p(lastUsedSeconds))              // 上次使用至今秒数（对数变换）

	// 网络协议特征
	features = append(features, boolToFloat(input.IsUDP)) // 是否UDP协议
	features = append(features, boolToFloat(input.IsTCP)) // 是否TCP协议

	// 2. ASN特征提取
	asnFeature := extractASNFeature(input.DestIPASN)
	features = append(features, float64(asnFeature)) // ASN类别特征

	// 3. GeoIP特征提取
	countryFeature := extractGeoIPFeature(input.DestGeoIP)
	features = append(features, float64(countryFeature)) // 国家/地区特征/大洲特征

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
			trafficRatio = downloadMB / uploadMB // 0-1之间，上传为主
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
				return 50 // 早期分配的ASN
			case asnNum < 10000:
				return 51 // 较早分配的ASN
			case asnNum < 50000:
				return 52 // 中期分配的ASN
			case asnNum < 150000:
				return 53 // 较新分配的ASN
			default:
				return 54 // 最新分配的ASN
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

	return 0
}

func extractDomainTypeFeature(host string) int {
	if host == "" {
		return 0
	}

	host = strings.ToLower(host)

	// 1. 检查是否为IP地址形式
	if strings.Contains(host, "[") || (strings.Count(host, ".") == 3 &&
		ipv4Regex.MatchString(host)) {
		return 1
	}

	// 2.1 DNS服务优先 - 基础设施服务
	for _, keyword := range dnsServiceKeywords {
		if strings.Contains(host, keyword) {
			return 6
		}
	}

	// 2.2 API服务 - 开发和基础设施服务
	for _, keyword := range apiServiceKeywords {
		if strings.Contains(host, keyword) {
			return 5
		}
	}

	// 2.3 游戏服务 - 高延迟敏感
	for _, keyword := range gameKeywords {
		if strings.Contains(host, keyword) {
			return 3
		}
	}

	// 2.4 通讯/会议服务 - 实时性要求高
	for _, keyword := range communicationKeywords {
		if strings.Contains(host, keyword) {
			return 4
		}
	}

	// 2.5 流媒体/视频服务 - 带宽敏感
	for _, keyword := range streamingKeywords {
		if strings.Contains(host, keyword) {
			return 2
		}
	}

	// 3.1 顶级域名类型检查
	if strings.HasSuffix(host, ".gov") {
		return 14
	} else if strings.HasSuffix(host, ".edu") {
		return 15
	} else if strings.HasSuffix(host, ".cn") {
		return 10
	} else if strings.HasSuffix(host, ".com") {
		return 11
	} else if strings.HasSuffix(host, ".net") {
		return 12
	} else if strings.HasSuffix(host, ".org") {
		return 13
	}

	// 3.2 域名结构分析
	if matches := domainRegex.FindStringSubmatch(host); len(matches) > 1 {
		domainParts := strings.Split(host, ".")
		if len(domainParts) >= 3 {
			return 30
		} else {
			return 31
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
			return network.category + 100 // 101-104为不同私有IP类型
		}
	}

	if addr.Is4() {
		return 110 // IPv4公网地址
	} else {
		return 111 // IPv6公网地址
	}
}

func extractPortFeature(port uint16) int {

	// 1.1 DNS服务端口
	if _, isDNS := dnsServicePorts[port]; isDNS {
		return 36
	}

	// 1.2 API服务端口
	if _, isAPI := apiServicePorts[port]; isAPI {
		return 35
	}

	// 1.3 游戏专用端口 - 高延迟敏感
	if _, isGame := gameSpecificPorts[port]; isGame {
		return 30
	}

	// 1.4 通信专用端口 - 实时性要求高
	if _, isComm := communicationPorts[port]; isComm {
		return 31
	}

	// 2. 已知标准端口检查
	if category, exists := wellKnownPorts[port]; exists {
		return category
	}

	// 3.1 游戏/通信端口范围
	for _, r := range gameCommRanges {
		if port >= r.min && port <= r.max {
			switch r.category {
			case 1:
				return 32
			case 2:
				return 33
			case 3:
				return 34
			}
		}
	}

	// 3.2 通用端口范围
	for _, r := range portRanges {
		if port >= r.min && port <= r.max {
			return r.category
		}
	}

	return 0
}

func deriveConnectionType(port uint16, addressFeature, portFeature int) int {

	// 1.1 DNS服务特征
	if addressFeature == 6 || portFeature == 36 {
		return 7
	}

	// 1.2 检查DNS专用端口
	if _, isDNS := dnsServicePorts[port]; isDNS {
		return 7
	}

	// 1.3 API服务特征 - 开发和基础设施服务
	if addressFeature == 5 || portFeature == 35 {
		return 6
	}

	// 1.4 检查API专用端口
	if _, isAPI := apiServicePorts[port]; isAPI {
		return 6
	}

	// 1.5 游戏/通讯特征 - 实时性服务
	if addressFeature == 3 || addressFeature == 4 {
		return 3
	}

	// 1.6 检查游戏专用端口
	if _, isGamePort := gameSpecificPorts[port]; isGamePort {
		return 3
	}

	// 1.7 检查通信专用端口
	if _, isCommPort := communicationPorts[port]; isCommPort {
		return 3
	}

	// 1.8 流媒体特征 - 带宽敏感服务
	if addressFeature == 2 {
		return 2
	}

	// 2.1 网页浏览特征
	if port == 80 || port == 443 || portFeature == 4 || portFeature == 7 {
		return 1
	}

	// 2.2 数据库访问特征
	if portFeature == 13 || portFeature == 14 || portFeature == 15 || portFeature == 16 {
		return 4
	}

	// 2.3 文件传输特征
	if port == 20 || port == 21 || port == 22 || port == 989 || port == 990 {
		return 5
	}

	// 3.1 检查游戏/通信端口范围
	for _, r := range gameCommRanges {
		if port >= r.min && port <= r.max {
			return 3
		}
	}

	// 3.2 高位端口推测
	if port > 10000 && port < 65000 {
		return 3
	}

	return 0
}

func boolToFloat(b bool) float64 {
	if b {
		return 1.0
	}
	return 0.0
}

func CreateModelInputFromStatsRecord(atomicRecord *smart.AtomicStatsRecord, metadata *C.Metadata, uploadTotal, downloadTotal, maxUploadRate, maxDownloadRate float64, wildcardTarget string) *smart.ModelInput {
	input := &smart.ModelInput{
		Success:                atomicRecord.Get("success").(int64),
		Failure:                atomicRecord.Get("failure").(int64),
		ConnectTime:            atomicRecord.Get("connectTime").(int64),
		Latency:                atomicRecord.Get("latency").(int64),
		UploadTotal:            uploadTotal,
		HistoryUploadTotal:     atomicRecord.Get("uploadTotal").(float64),
		MaxuploadRate:          maxUploadRate,
		HistoryMaxUploadRate:   atomicRecord.Get("maxUploadRate").(float64),
		DownloadTotal:          downloadTotal,
		HistoryDownloadTotal:   atomicRecord.Get("downloadTotal").(float64),
		MaxdownloadRate:        maxDownloadRate,
		HistoryMaxDownloadRate: atomicRecord.Get("maxDownloadRate").(float64),
		ConnectionDuration:     atomicRecord.Get("duration").(float64),
		LastUsed:               atomicRecord.Get("lastUsed").(int64),
		IsUDP:                  metadata.NetWork == C.UDP,
		IsTCP:                  metadata.NetWork == C.TCP,
	}

	if metadata.DstIPASN == "unknown" {
		input.DestIPASN = ""
	} else {
		input.DestIPASN = metadata.DstIPASN
	}

	input.Host = wildcardTarget
	if metadata.DstIP.IsValid() {
		input.DestIP = metadata.DstIP.String()
	}

	input.DestPort = metadata.DstPort
	input.DestGeoIP = metadata.DstGeoIP

	return input
}
