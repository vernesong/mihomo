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

	"github.com/metacubex/mihomo/common/singleflight"
	mihomoHttp "github.com/metacubex/mihomo/component/http"
	"github.com/metacubex/mihomo/component/smart"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"

	"github.com/vernesong/leaves"
)

const (
	MaxFeatureSize = 32

	// a corrupt max_feature_idx must not drive an unbounded allocation in the tail fill
	maxModelFeatureSlots = 4096
)

type keywordEntry struct {
	keyword  string
	category int
}

type WeightModel struct {
	model      *leaves.Ensemble
	transforms *FeatureTransforms
	lastUpdate time.Time
	mutex      sync.RWMutex
}

var (
	smartModel  *WeightModel
	reloadModel = singleflight.Group[bool]{StoreResult: false}
	modelOnce   sync.Once
	lgbmUrl     string

	domainRegex = regexp.MustCompile(`([a-zA-Z0-9-]+)(\.[a-zA-Z0-9-]+)+$`)

	// provider keywords; enums start at 100, the 1-99 block is reserved for computed codes
	asnCategories = []keywordEntry{
		// global tech
		{"google", 100},
		{"amazon", 101},
		{"microsoft", 102},
		{"facebook", 103},
		{"apple", 104},
		{"cloudflare", 105},
		{"akamai", 106},
		{"fastly", 107},
		{"netflix", 108},
		{"alibaba", 109},
		{"tencent", 110},
		{"baidu", 111},
		{"huawei", 112},
		{"bytedance", 113},
		{"tiktok", 113},
		{"xiaomi", 114},
		{"salesforce", 115},
		{"adobe", 116},
		{"nvidia", 117},
		// china carriers
		{"chinatelecom", 120},
		{"china telecom", 120},
		{"chinanet", 120},
		{"chinaunicom", 121},
		{"china unicom", 121},
		{"chinamobile", 122},
		{"china mobile", 122},
		{"cmnet", 122},
		{"chinaedu", 123},
		{"china education", 123},
		{"cernet", 123},
		{"cstnet", 124},
		// international carriers
		{"verizon", 140},
		{"comcast", 141},
		{"at&t", 142},
		{"sprint", 143},
		{"t-mobile", 144},
		{"tmobile", 144},
		{"level3", 145},
		{"level 3", 145},
		{"lumen", 145},
		{"centurylink", 145},
		{"ntt", 146},
		{"kddi", 147},
		{"softbank", 148},
		{"telstra", 149},
		{"singtel", 150},
		{"starhub", 151},
		{"m1", 152},
		{"pccw", 153},
		{"hkbn", 154},
		{"smartone", 155},
		{"hgc", 156},
		{"cht", 157},
		{"chunghwa", 157},
		{"hinet", 157},
		{"fetnet", 158},
		{"far eastone", 158},
		{"twm", 159},
		{"taiwan mobile", 159},
		{"hkt", 160},
		{"deutsche telekom", 161},
		{"vodafone", 162},
		{"telefonica", 163},
		{"optus", 164},
		{"bell canada", 165},
		{"rogers communications", 166},
		{"telus", 167},
		{"rakuten", 168},
		{"korea telecom", 169},
		{"viettel", 170},
		{"pldt", 171},
		{"sk broadband", 172},
		{"charter communications", 173},
		// global CDN and cloud
		{"cdn77", 180},
		{"limelight", 181},
		{"edgecast", 182},
		{"edgio", 182},
		{"stackpath", 183},
		{"imperva", 184},
		{"oracle", 185},
		{"ibm", 186},
		{"digitalocean", 187},
		{"linode", 188},
		{"ovh", 189},
		{"hetzner", 190},
		{"vultr", 191},
		{"cogent", 192},
		{"leaseweb", 193},
		{"upyun", 194},
		{"qingcloud", 195},
		{"ucloud", 196},
		{"contabo", 197},
		{"rackspace", 198},
		{"bunnyway", 199},
		{"bunnycdn", 199},
		{"netactuate", 200},
		{"wangsu", 201},
		{"kingsoft", 202},
		{"scaleway", 203},
		{"ionos", 204},
		// content providers
		{"twitter", 220},
		{"twitch", 221},
		{"discord", 222},
		{"spotify", 223},
		{"github", 224},
		{"steam", 225},
		{"blizzard", 226},
		{"riotgames", 227},
		{"riot games", 227},
		{"epicgames", 228},
		{"electronic arts", 229},
		{"valve", 230},
		{"mozilla", 231},
		{"reddit", 232},
		{"openai", 233},
		{"bilibili", 234},
		// education and research
		{"cern", 240},
		{"stanford", 241},
		{"tsinghua", 242},
		{"pku", 243},
		// finance
		{"visa", 250},
		{"mastercard", 251},
		{"paypal", 252},
		{"stripe", 253},
		{"alipay", 254},
		{"wechatpay", 255},
		{"unionpay", 256},
		// internet exchanges
		{"hkix", 260},
		{"linx", 261},
		{"jpix", 262},
		{"equinix", 263},
		{"sgix", 264},
		{"de-cix", 265},
		{"ams-ix", 266},
	}

	// region enums start at 100, the 1-99 block is reserved for the computed hash fallback
	geoCategories = map[string]int{
		"CN": 100,
		"HK": 101,
		"TW": 102,
		"JP": 103,
		"KR": 104,
		"SG": 105,
		"US": 106,
		"CA": 107,
		"GB": 108,
		"DE": 109,
		"FR": 110,
		"RU": 111,
		"AU": 112,
		"IN": 113,
		"BR": 114,
		"IT": 115,
		"ES": 116,
		"NL": 117,
		"SE": 118,
		"CH": 119,
		"PL": 120,
		"TR": 121,
		"MX": 122,
		"ZA": 123,
		"AR": 124,
		"ID": 125,
		"TH": 126,
		"VN": 127,
		"PH": 128,
		"MY": 129,
		"MO": 130,
		"AE": 131,
		"AT": 132,
		"BD": 133,
		"BE": 134,
		"CL": 135,
		"CO": 136,
		"CZ": 137,
		"DK": 138,
		"EG": 139,
		"FI": 140,
		"GR": 141,
		"IE": 142,
		"IL": 143,
		"IS": 144,
		"KE": 145,
		"KH": 146,
		"KZ": 147,
		"LK": 148,
		"LU": 149,
		"MA": 150,
		"MM": 151,
		"NG": 152,
		"NO": 153,
		"NZ": 154,
		"PE": 155,
		"PK": 156,
		"PT": 157,
		"RO": 158,
		"SA": 159,
		"UA": 160,
	}

	// well known ports
	wellKnownPorts = map[uint16]int{
		22:    100, // SSH
		25:    101, // SMTP
		53:    102, // DNS
		80:    103, // HTTP
		110:   104, // POP3
		123:   120, // NTP
		143:   105, // IMAP
		389:   130, // LDAP
		443:   106, // HTTPS
		445:   122, // SMB
		465:   107, // SMTPS
		587:   121, // SMTP submission
		636:   131, // LDAPS
		784:   102, // DNS over QUIC (early draft port, final is 853)
		853:   102, // DNS over TLS / QUIC
		993:   108, // IMAPS
		995:   109, // POP3S
		1194:  110, // OpenVPN
		1433:  124, // MSSQL
		1521:  125, // Oracle DB
		1812:  111, // RADIUS
		2049:  126, // NFS
		3306:  112, // MySQL
		3389:  123, // RDP
		5053:  102, // DNS backup port
		5353:  102, // mDNS
		5355:  102, // LLMNR
		5432:  113, // PostgreSQL
		5672:  128, // AMQP
		5900:  127, // VNC
		6379:  114, // Redis
		6660:  116, // IRC
		6665:  116, // IRC
		6666:  116, // IRC
		6667:  116, // IRC
		6668:  116, // IRC
		6669:  116, // IRC
		8000:  117, // common alternative HTTP port
		8008:  117, // common alternative HTTP port
		8080:  117, // common alternative HTTP port
		8443:  118, // common alternative HTTPS port
		8853:  102, // DoT backup port
		8883:  119, // MQTT over TLS
		9200:  129, // Elasticsearch
		9953:  102, // DNS management port
		11211: 132, // memcached
		27017: 115, // MongoDB
	}

	// port ranges
	portRanges = []struct {
		min, max uint16
		category int
	}{
		{0, 1023, 8},       // system ports
		{1024, 49151, 9},   // registered ports
		{49152, 65535, 10}, // dynamic ports
	}

	apiServicePorts = map[uint16]bool{
		8080: true, 8443: true, 9000: true, 9001: true, 9002: true, // common API ports
		3000: true, 3001: true, 5000: true, 5001: true, // development API ports
		8000: true, 8001: true, 8888: true, 4000: true, 4001: true, // other API service ports
		6000: true, 6001: true, 7000: true, 7001: true, // microservice API ports
	}

	dnsServicePorts = map[uint16]bool{
		53:   true, // classic DNS (UDP/TCP)
		853:  true, // DNS over TLS (DoT) / QUIC (DoQ)
		784:  true, // DNS over QUIC (early draft port)
		5053: true, // backup port of some DNS services
		5353: true, // mDNS (Multicast DNS)
		5355: true, // LLMNR (Link-Local Multicast Name Resolution)
		8853: true, // backup port some DoT services use
		9953: true, // management port some DNS services use
	}

	gameSpecificPorts = map[uint16]bool{
		25565: true,                                                                  // Minecraft
		27015: true, 27016: true, 27017: true, 27018: true, 27019: true, 27020: true, // Steam/Counter-Strike
		27031: true, 27036: true, // Steam In-Home Streaming
		3074: true,             // Xbox Live
		3478: true, 3479: true, // PlayStation Network / Nintendo Switch Online
		3659: true,                                                 // Tencent games
		6250: true,                                                 // NetEase games
		7000: true, 7001: true, 7002: true, 7003: true, 7004: true, // various game services
		8393: true, 8394: true, // Origin
		9000: true, 9001: true, // QQ games
		9330: true, 9331: true, // various game services
		9339:  true,                                                                  // various game services
		14000: true, 14001: true, 14002: true, 14003: true, 14004: true, 14008: true, // Battlefield
		16000: true,                                                                  // Battlefield
		18000: true, 18060: true, 18120: true, 18180: true, 18240: true, 18300: true, // Fortnite
		19000: true, 19132: true, // Minecraft PE
		20000: true, 20001: true, 20002: true, // Garena
		22100: true, 22101: true, 22102: true, // Valorant
		30000: true, 30001: true, 30002: true, 30003: true, 30004: true, // Call of Duty
		35000: true, 35001: true, 35002: true, // PUBG / Game for Peace
		40000: true, 40001: true, 40002: true, // various game services
		50000: true, 50001: true, 50002: true, // League of Legends (international)
		50505: true,              // Arena of Valor
		65010: true, 65050: true, // League of Legends: Wild Rift
		3724: true, // World of Warcraft
		6112: true, // Warcraft III/Battle.net
		6881: true, // BitTorrent
	}

	// communication ports
	communicationPorts = map[uint16]bool{
		5060: true, 5061: true, // SIP
		1720: true,             // H.323
		1080: true, 1443: true, // proxy and communication services
		3478: true, 3479: true, // STUN/TURN
		5349: true, 5350: true, // STUN/TURN over TLS
		5222: true, 5269: true, // XMPP
		5938: true, // TeamViewer
		6881: true, 6882: true, 6883: true, 6884: true, 6885: true,
		6886: true, 6887: true, 6888: true, 6889: true, // BT
		8801: true, 8802: true, // P2P communication services
		8443:  true,              // common WebRTC / video conference
		10000: true, 10001: true, // WebRTC Media
		19302: true, 19303: true, // Google STUN
		50000: true, 50001: true, 50002: true, // common RTP media ports
		50003: true, 50004: true, 50005: true, // common RTP media ports
		55000: true, 55001: true, // communication applications
		1863:  true, // MSN Messenger
		5228:  true, // Google GCM/FCM
		34784: true, // Zoom
	}

	// game and communication port ranges
	gameCommRanges = []struct {
		min, max uint16
		category int // 1=game, 2=communication, 3=mixed
	}{
		{3000, 3999, 3},   // mixed range of game and communication apps
		{5000, 5999, 2},   // communication applications
		{6000, 7000, 3},   // mixed range of P2P and games
		{8000, 9000, 3},   // mixed range of games and communication
		{10000, 20000, 3}, // mixed range of WebRTC and game services
		{27000, 28000, 1}, // Steam and related game ports
		{30000, 32000, 1}, // common game service ports
		{49000, 50000, 2}, // high ports used by RTP and communication
		{50000, 55000, 3}, // mixed range of communication and games
		{55000, 60000, 2}, // high ports used by communication services
	}

	// streaming and game domain keywords
	streamingKeywords = []string{
		"youtube", "netflix", "hulu", "spotify", "tiktok", "douyin", "youku", "iqiyi",
		"bilibili", "twitch", "hbo", "disney", "vimeo", "vod", "stream", "video",
		"media", "movie", "tv", "music", "audio", "cdm", "cdn", "content",
		"live", "livestream", "replay", "shorts", "kuaishou", "huya", "douyu",
		"googlevideo", "ytimg", "primevideo", "hotstar", "wetv", "kugou", "acfun",
	}

	gameKeywords = []string{
		"game", "play.google", "steam", "xbox", "playstation", "nintendo", "ea.com", "riotcdn",
		"blizzard", "ubisoft", "epic", "cod.", "callofduty", "minecraft", "roblox", "pubg", "fortnite",
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
		"meeting", "huddle", "gather",
		"qq", "msn", "icq", "line.me", "linecorp", "line-apps", "line-scdn", "kakao", "viber", "imo.im", "element.io",
	}

	apiServiceKeywords = []string{
		"api.cloudflare.com", "api.amazonaws.com", "api.azure.com", "googleapis.com",
		"api.fastly.com", "api.maxcdn.com", "api.keycdn.com", "api.bunnycdn.com",
		"api.digitalocean.com", "api.vultr.com", "api.linode.com", "api.hetzner.com", "amazonaws.com",

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
		"mozilla.cloudflare-dns.com", "firefox.dns.nextdns.io", "nextdns.io",
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
		// IPv4 private ranges
		{netip.MustParsePrefix("10.0.0.0/8"), 10},
		{netip.MustParsePrefix("172.16.0.0/12"), 10},
		{netip.MustParsePrefix("192.168.0.0/16"), 10},
		{netip.MustParsePrefix("127.0.0.0/8"), 11},
		{netip.MustParsePrefix("169.254.0.0/16"), 12},

		// IPv4 special ranges
		{netip.MustParsePrefix("100.64.0.0/10"), 13},
		{netip.MustParsePrefix("192.0.2.0/24"), 13},
		{netip.MustParsePrefix("198.51.100.0/24"), 13},
		{netip.MustParsePrefix("203.0.113.0/24"), 13},
		{netip.MustParsePrefix("240.0.0.0/4"), 13},

		// IPv6 private ranges
		{netip.MustParsePrefix("::1/128"), 11},
		{netip.MustParsePrefix("fe80::/10"), 12},
		{netip.MustParsePrefix("fc00::/7"), 10},
		{netip.MustParsePrefix("2001:db8::/32"), 13},

		// IPv6 special ranges
		{netip.MustParsePrefix("ff00::/8"), 13},
	}
)

var allDomainKeywords []keywordEntry

func init() {
	total := len(dnsServiceKeywords) + len(apiServiceKeywords) + len(gameKeywords) +
		len(communicationKeywords) + len(streamingKeywords)
	allDomainKeywords = make([]keywordEntry, 0, total)
	for _, kw := range dnsServiceKeywords {
		allDomainKeywords = append(allDomainKeywords, keywordEntry{kw, 104})
	}
	for _, kw := range apiServiceKeywords {
		allDomainKeywords = append(allDomainKeywords, keywordEntry{kw, 103})
	}
	for _, kw := range gameKeywords {
		allDomainKeywords = append(allDomainKeywords, keywordEntry{kw, 101})
	}
	for _, kw := range communicationKeywords {
		allDomainKeywords = append(allDomainKeywords, keywordEntry{kw, 102})
	}
	for _, kw := range streamingKeywords {
		allDomainKeywords = append(allDomainKeywords, keywordEntry{kw, 100})
	}
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

	// a model with more slots still predicts, the missing tail is filled with zeros
	if n := model.NFeatures(); n > MaxFeatureSize {
		log.Warnln("[Smart] Model needs [%d] features, this build computes [%d], the missing slots are filled with zeros", n, MaxFeatureSize)
	}

	transforms, err := LoadTransformsFromModel(path)
	if err != nil {
		log.Warnln("[Smart] Failed to load transforms parameters: %v, using default config", err)
		transforms = &FeatureTransforms{
			TransformsEnabled: false,
			FeatureOrder:      defaultFeatureOrder,
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

func SetLgbmUrl(newUrl string) {
	lgbmUrl = newUrl
}

func LgbmUrl() string {
	return lgbmUrl
}

func downloadModel(path string) (err error) {
	modelUrl := LgbmUrl()
	if modelUrl == "" {
		modelUrl = GetModelDownloadURL()
	}

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

// PredictWeight scores a connection with the model, falling back to the heuristic whenever
// the model cannot answer. A panic anywhere in the model path (feature sizing, transforms,
// prediction) falls back as well: this runs in the stats goroutine, where an escaping panic
// would end the process (L1).
func (m *WeightModel) PredictWeight(input *smart.ModelInput, priorityFactor float64) (weight float64, predicted bool) {
	if m == nil {
		return smart.CalculateWeight(input, priorityFactor)
	}

	total := input.Success + input.Failure
	if total < smart.DefaultMinSampleCount {
		return 0, false
	}

	defer func() {
		if r := recover(); r != nil {
			log.Errorln("[Smart] Model prediction panic: %v", r)
			weight, predicted = smart.CalculateWeight(input, priorityFactor)
		}
	}()

	m.mutex.RLock()
	model := m.model
	transforms := m.transforms
	m.mutex.RUnlock()

	if model == nil {
		return smart.CalculateWeight(input, priorityFactor)
	}

	features := prepareFeatures(input)
	if len(features) == 0 {
		return smart.CalculateWeight(input, priorityFactor)
	}

	// a newer model may use uncomputed slots, the tail is filled up to the capped size
	if n := model.NFeatures(); n > len(features) && n <= maxModelFeatureSlots {
		features = append(features, make([]float64, n-len(features))...)
	}

	if transforms != nil && !transforms.IsCompatible() {
		return smart.CalculateWeight(input, priorityFactor)
	}

	if transforms != nil && transforms.TransformsEnabled {
		features = transforms.ApplyTransforms(features)
	}

	prediction := model.PredictSingle(features, 0)

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

	uploadMB := input.UploadTotal
	downloadMB := input.DownloadTotal
	maxUploadRateKB := input.MaxuploadRate
	maxDownloadRateKB := input.MaxdownloadRate
	durationMinutes := input.ConnectionDuration
	lastUsedSeconds := 0.0
	if input.LastUsed > 0 {
		lastUsedSeconds = float64(time.Now().Unix() - input.LastUsed)
	}

	// the slot order is the feature order of the trained model, a new slot must be appended
	features = append(features, float64(input.Success))
	features = append(features, float64(input.Failure))
	features = append(features, math.Log1p(float64(input.ConnectTime)))
	features = append(features, math.Log1p(float64(input.Latency)))
	features = append(features, math.Log1p(uploadMB))
	features = append(features, math.Log1p(input.HistoryUploadTotal))
	features = append(features, math.Log1p(maxUploadRateKB))
	features = append(features, math.Log1p(input.HistoryMaxUploadRate))
	features = append(features, math.Log1p(downloadMB))
	features = append(features, math.Log1p(input.HistoryDownloadTotal))
	features = append(features, math.Log1p(maxDownloadRateKB))
	features = append(features, math.Log1p(input.HistoryMaxDownloadRate))
	features = append(features, math.Log1p(durationMinutes))
	features = append(features, math.Log1p(input.HistoryConnectionDuration))
	features = append(features, math.Log1p(lastUsedSeconds))

	features = append(features, boolToFloat(input.IsUDP))
	features = append(features, boolToFloat(input.IsTCP))
	features = append(features, input.LossRate)
	features = append(features, input.CumulLossRate)

	asnFeature := extractASNFeature(input.DestIPASN)
	features = append(features, float64(asnFeature))

	countryFeature := extractGeoIPFeature(input.DestGeoIP)
	features = append(features, float64(countryFeature))

	var addressFeature int
	if input.Host != "" {
		addressFeature = extractDomainTypeFeature(input.Host)
	} else if input.DestIP != "" {
		addressFeature = extractIPFeature(input.DestIP)
	}
	features = append(features, float64(addressFeature))

	portFeature := extractPortFeature(input.DestPort)
	features = append(features, float64(portFeature))

	trafficRatio := 0.0
	if uploadMB > 0 && downloadMB > 0 {
		if uploadMB > downloadMB {
			trafficRatio = downloadMB / uploadMB // 0..1, upload heavy
		} else {
			trafficRatio = -uploadMB / downloadMB // -1..0, download heavy
		}
	}
	features = append(features, trafficRatio)

	trafficDensity := 0.0
	if durationMinutes > 0 {
		trafficDensity = math.Log1p((uploadMB + downloadMB) / durationMinutes)
	}
	features = append(features, trafficDensity)

	connectionTypeFeature := deriveConnectionType(input.DestPort, addressFeature, portFeature)
	features = append(features, float64(connectionTypeFeature))

	features = append(features, hashStringToFloat(input.DestIPASN, 500))
	features = append(features, hashStringToFloat(input.Host, 1000))
	features = append(features, hashStringToFloat(input.DestIP, 10000))
	geoHash := 0.0
	if len(input.DestGeoIP) > 0 {
		geoHash = hashStringToFloat(input.DestGeoIP[0], 200)
	}
	features = append(features, geoHash)

	// the trailing slots are ignored by a model trained before they existed
	features = append(features, float64(extractExitRegionFeature(input.ExitRegion)))
	features = append(features, hashStringToFloat(input.ExitASN, 500))

	// the model was trained with MaxFeatureSize slots
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

	// the entry order decides the priority, list a longer keyword before its substrings
	for _, entry := range asnCategories {
		if strings.Contains(asnInfo, entry.keyword) {
			return entry.category
		}
	}

	s := asnInfo
	if len(s) >= 2 && s[0] == 'a' && s[1] == 's' {
		s = s[2:]
	}
	j := 0
	for j < len(s) && s[j] >= '0' && s[j] <= '9' {
		j++
	}
	if j > 0 {
		// ParseUint keeps 32 bit builds from erroring on the upper ASN range
		if asnNum, err := strconv.ParseUint(s[:j], 10, 64); err == nil {
			switch {
			case asnNum < 1000:
				return 1 // early allocation era
			case asnNum < 10000:
				return 2 // older allocation era
			case asnNum < 50000:
				return 3 // mid allocation era
			case asnNum < 150000:
				return 4 // newer allocation era
			default:
				return 5 // latest allocation era
			}
		}
	}

	return 0
}

func extractGeoIPFeature(geoIPInfo []string) int {
	if len(geoIPInfo) == 0 {
		return 0
	}

	countryCode := ""
	if len(geoIPInfo) > 0 {
		countryCode = geoIPInfo[0]
	}

	if category, exists := geoCategories[countryCode]; exists {
		return category
	}

	if countryCode != "" {
		hashValue := 0
		for _, r := range countryCode {
			hashValue = hashValue*31 + int(r)
		}
		// unlisted regions hash into the reserved 1-99 block
		return 1 + (hashValue % 20)
	}

	return 0
}

// extractExitRegionFeature maps the landing region, an empty answer stays 0 (unknown).
func extractExitRegionFeature(region string) int {
	if region == "" {
		return 0
	}
	region = strings.ToUpper(region)
	if category, exists := geoCategories[region]; exists {
		return category
	}
	hashValue := 0
	for _, r := range region {
		hashValue = hashValue*31 + int(r)
	}
	// unlisted regions hash into the reserved 1-99 block
	return 1 + (hashValue % 20)
}

// extractDomainTypeFeature returns the domain feature code of the model: 1 IP literal,
// 2-3 generic domains, 100-104 service kinds, 110-115 TLD kinds.
func extractDomainTypeFeature(host string) int {
	if host == "" {
		return 0
	}

	host = strings.ToLower(host)

	if strings.Contains(host, "[") {
		return 1
	}
	if _, err := netip.ParseAddr(host); err == nil {
		return 1
	}

	// keyword matching, the order of allDomainKeywords decides the priority
	for _, entry := range allDomainKeywords {
		if strings.Contains(host, entry.keyword) {
			return entry.category
		}
	}

	if strings.HasSuffix(host, ".gov") {
		return 114
	} else if strings.HasSuffix(host, ".edu") {
		return 115
	} else if strings.HasSuffix(host, ".cn") {
		return 110
	} else if strings.HasSuffix(host, ".com") {
		return 111
	} else if strings.HasSuffix(host, ".net") {
		return 112
	} else if strings.HasSuffix(host, ".org") {
		return 113
	}

	if domainRegex.MatchString(host) {
		if strings.Count(host, ".") >= 2 {
			return 2
		}
		return 3
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
			return network.category
		}
	}

	if addr.Is4() {
		return 14 // public IPv4
	} else {
		return 15 // public IPv6
	}
}

// extractPortFeature returns the port feature code of the model: 1 game, 2 communication,
// 3-5 game or communication ranges, 6 API, 7 DNS, 8-10 port ranges, otherwise 100+ enum;
// the special tables win over the enum in the order DNS, API, game, communication.
func extractPortFeature(port uint16) int {
	if _, isDNS := dnsServicePorts[port]; isDNS {
		return 7
	}

	if _, isAPI := apiServicePorts[port]; isAPI {
		return 6
	}

	if _, isGame := gameSpecificPorts[port]; isGame {
		return 1
	}

	if _, isComm := communicationPorts[port]; isComm {
		return 2
	}

	if category, exists := wellKnownPorts[port]; exists {
		return category
	}

	for _, r := range gameCommRanges {
		if port >= r.min && port <= r.max {
			switch r.category {
			case 1:
				return 3
			case 2:
				return 4
			case 3:
				return 5
			}
		}
	}

	for _, r := range portRanges {
		if port >= r.min && port <= r.max {
			return r.category
		}
	}

	return 0
}

// deriveConnectionType returns the connection type code of the model: 1 web, 2 streaming,
// 3 realtime, 4 database, 5 file transfer, 6 API, 7 DNS.
func deriveConnectionType(port uint16, addressFeature, portFeature int) int {
	if addressFeature == 104 || portFeature == 7 {
		return 7
	}
	if _, isDNS := dnsServicePorts[port]; isDNS {
		return 7
	}

	if addressFeature == 103 || portFeature == 6 {
		return 6
	}
	if _, isAPI := apiServicePorts[port]; isAPI {
		return 6
	}

	if addressFeature == 101 || addressFeature == 102 {
		return 3
	}
	if _, isGamePort := gameSpecificPorts[port]; isGamePort {
		return 3
	}
	if _, isCommPort := communicationPorts[port]; isCommPort {
		return 3
	}

	if addressFeature == 100 {
		return 2
	}

	if port == 80 || port == 443 || portFeature == 103 || portFeature == 106 {
		return 1
	}

	if portFeature == 112 || portFeature == 113 || portFeature == 114 || portFeature == 115 {
		return 4
	}

	if port == 20 || port == 21 || port == 22 || port == 989 || port == 990 {
		return 5
	}

	for _, r := range gameCommRanges {
		if port >= r.min && port <= r.max {
			return 3
		}
	}

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

func CreateModelInputFromStatsRecord(atomicRecord *smart.AtomicStatsRecord, metadata *C.Metadata, uploadTotal, downloadTotal, maxUploadRate, maxDownloadRate, connectionDuration float64, wildcardTarget string, lossRate, cumulLossRate float64) *smart.ModelInput {
	input := &smart.ModelInput{
		Success:                   atomicRecord.Success(),
		Failure:                   atomicRecord.Failure(),
		ConnectTime:               atomicRecord.ConnectTime(),
		Latency:                   atomicRecord.Latency(),
		UploadTotal:               uploadTotal,
		HistoryUploadTotal:        atomicRecord.UploadTotal(),
		MaxuploadRate:             maxUploadRate,
		HistoryMaxUploadRate:      atomicRecord.MaxUploadRate(),
		DownloadTotal:             downloadTotal,
		HistoryDownloadTotal:      atomicRecord.DownloadTotal(),
		MaxdownloadRate:           maxDownloadRate,
		HistoryMaxDownloadRate:    atomicRecord.MaxDownloadRate(),
		HistoryConnectionDuration: atomicRecord.Duration(),
		ConnectionDuration:        connectionDuration,
		LastUsed:                  atomicRecord.LastUsed(),
		IsUDP:                     metadata.NetWork == C.UDP,
		IsTCP:                     metadata.NetWork == C.TCP,
		LossRate:                  lossRate,
		CumulLossRate:             cumulLossRate,
		EmaLossRate:               atomicRecord.LossRate(),
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
	input.ExitRegion = metadata.ExitGeoIP
	input.ExitASN = metadata.ExitIPASN

	return input
}
