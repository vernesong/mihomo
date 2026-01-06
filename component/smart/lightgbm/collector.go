package lightgbm

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/metacubex/mihomo/component/smart"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
)

var (
	collectMutex           sync.Mutex
	smartCollector         *DataCollector
)

type DataCollector struct {
	mutex                  sync.Mutex
	sampleCount            int
	dataPath               string
	file                   *os.File
	writer                 *csv.Writer
	configured             bool
	smartCollectorSize     int64
}

const (
	defaultSmartCollectorSize = 100 * 1024 * 1024
)

func InitCollector(collectSize float64) {
	var smartCollectorSize int64
	if collectSize > 0 {
		smartCollectorSize = int64(collectSize * 1024 * 1024)
	} else {
		smartCollectorSize = defaultSmartCollectorSize
	}

	smartCollector = &DataCollector{
		dataPath:           filepath.Join(C.Path.HomeDir(), "smart_weight_data.csv"),
		smartCollectorSize: smartCollectorSize,
	}

	log.Infoln("[Smart] Data collector initialized, max file size: %d MB", smartCollector.smartCollectorSize/(1024*1024))
}

func GetCollector() *DataCollector {
	return smartCollector
}

func (c *DataCollector) AddSample(input *smart.ModelInput, metadata *C.Metadata, actualWeight float64, weightSource string) {
	if c == nil {
		return
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.configured {
		if _, err := os.Stat(c.dataPath); os.IsNotExist(err) {
			log.Infoln("[Smart] Data file was deleted, reinitializing collector")
			c.configured = false
			if c.file != nil {
				c.file.Close()
				c.file = nil
			}
			c.writer = nil
		}
	}

	// 检查文件大小限制
	if c.file != nil {
		stat, err := c.file.Stat()
		if err == nil && stat.Size() > c.smartCollectorSize {
			log.Infoln("[Smart] Maximum file size limit reached (%d MB), stopping data collection", c.smartCollectorSize/(1024*1024))
			return
		}
	}

	if !c.configured {
		err := c.initializeWriter()
		if err != nil {
			log.Warnln("[Smart] Failed to initialize training data collector: %v", err)
			return
		}
	}

	features := prepareFeatures(input)
	if len(features) == 0 {
		log.Debugln("[Smart] Feature extraction failed, skipping sample collection")
		return
	}

	featureStrings := make([]string, len(features))
	for i, f := range features {
		featureStrings[i] = fmt.Sprintf("%.6f", f)
	}

	var geoIPStr string
	if metadata.DstGeoIP != nil {
		geoIPStr = strings.Join(metadata.DstGeoIP, ",")
	} else {
		geoIPStr = "unknown"
	}

	var dstASN string
	if metadata.DstIPASN != "" {
		dstASN = metadata.DstIPASN
	} else {
		dstASN = "unknown"
	}

	dstIP := "unknown"
	if metadata.DstIP.IsValid() {
		dstIP = metadata.DstIP.String()
	}

	host := "unknown"
	if metadata.Host != "" {
		host = metadata.Host
	}

	standardizedSource := weightSource
	if standardizedSource == "" {
		standardizedSource = "unknown"
	}

	sample := append(featureStrings,
		input.GroupName,
		input.NodeName,
		dstASN,
		host,
		dstIP,
		fmt.Sprintf("%d", metadata.DstPort),
		geoIPStr,
		fmt.Sprintf("%.6f", actualWeight),
		standardizedSource,
		time.Now().Format(time.RFC3339),
	)

	expectedColumns := MaxFeatureSize + 10
	if len(sample) != expectedColumns {
		return
	}

	if err := c.writer.Write(sample); err != nil {
		log.Warnln("[Smart] Failed to write training data: %v", err)
		c.configured = false
		if c.file != nil {
			c.file.Close()
			c.file = nil
		}
		c.writer = nil
		return
	}

	c.sampleCount++

	// 每100条记录刷新一次
	if c.sampleCount%100 == 0 {
		c.writer.Flush()
	}
}

func (c *DataCollector) initializeWriter() error {
	var err error

	log.Infoln("[Smart] Initializing data collector for %s", c.dataPath)

	fileExists := false
	if _, err := os.Stat(c.dataPath); err == nil {
		fileExists = true
	}

	needUpgrade := false
	if fileExists {
		f, err := os.Open(c.dataPath)
		if err == nil {
			defer f.Close()
			reader := csv.NewReader(f)
			headers, err := reader.Read()
			if err == nil {
				hasMax := false
				for _, h := range headers {
					if h == "history_upload_mb" {
						hasMax = true
						break
					}
				}
				if !hasMax {
					needUpgrade = true
				}
			}
		}
	}

	if needUpgrade {
		backupPath := c.dataPath + ".bak." + time.Now().Format("20060102150405")
		os.Rename(c.dataPath, backupPath)
		log.Infoln("[Smart] Old CSV file does not contain hash columns, backup to %s and create new file", backupPath)
		fileExists = false
	}

	file, err := os.OpenFile(c.dataPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	c.file = file
	c.writer = csv.NewWriter(c.file)

	if !fileExists {
		headers := []string{
			"success", "failure", "connect_time", "latency",
			"upload_mb", "history_upload_mb", "maxuploadrate_kb", "history_maxuploadrate_kb",
			"download_mb", "history_download_mb", "maxdownloadrate_kb", "history_maxdownloadrate_kb",
			"duration_minutes", "last_used_seconds", "is_udp", "is_tcp",
			"asn_feature", "country_feature", "address_feature", "port_feature",
			"traffic_ratio", "traffic_density", "connection_type_feature",
			"asn_hash", "host_hash", "ip_hash", "geoip_hash",
			"group_name", "node_name",
			"asn_raw", "host_raw", "ip_raw", "port_raw", "geoip_raw",
			"weight", "weight_source", "timestamp",
		}

		if err := c.writer.Write(headers); err != nil {
			c.file.Close()
			return err
		}
		c.writer.Flush()
	}

	c.configured = true
	return nil
}

func (c *DataCollector) Flush() error {
	if c == nil {
		return nil
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.writer != nil {
		c.writer.Flush()
	}

	return nil
}

func (c *DataCollector) Close() error {
	if c == nil {
		return nil
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.writer != nil {
		c.writer.Flush()
	}

	if c.file != nil {
		return c.file.Close()
	}

	return nil
}

func CloseAllCollectors() {
	collectMutex.Lock()
	defer collectMutex.Unlock()

	if smartCollector != nil {
		smartCollector.Close()
	}
}
