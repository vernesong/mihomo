package lightgbm

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"strconv"
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
	lastFileCheck          time.Time
}

const (
	defaultSmartCollectorSize = 100 * 1024 * 1024
	expectedColumns           = MaxFeatureSize + 10
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

	if c.configured && time.Since(c.lastFileCheck) > 5 * time.Second {
		c.lastFileCheck = time.Now()
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
	var buf [32]byte
	for i, f := range features {
		featureStrings[i] = string(strconv.AppendFloat(buf[:0], f, 'f', 6, 64))
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

	sample := make([]string, 0, expectedColumns)
	sample = append(sample, featureStrings...)
	sample = append(sample,
		input.GroupName,
		input.NodeName,
		dstASN,
		host,
		dstIP,
		strconv.FormatUint(uint64(metadata.DstPort), 10),
		geoIPStr,
		strconv.FormatFloat(actualWeight, 'f', 6, 64),
		standardizedSource,
		time.Now().Format(time.RFC3339),
	)

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
					if h == "loss_rate" {
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

	file, err := os.OpenFile(c.dataPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	if fileExists {
		if stat, err2 := file.Stat(); err2 == nil && stat.Size() > 0 {
			last := make([]byte, 1)
			if _, err2 = file.ReadAt(last, stat.Size() - 1); err2 == nil && last[0] != '\n' {
				const scanSize = int64(65536)
				readStart := stat.Size() - scanSize
				if readStart < 0 {
					readStart = 0
				}
				buf := make([]byte, stat.Size() - readStart)
				if _, err3 := file.ReadAt(buf, readStart); err3 == nil {
					newlinePos := int64(-1)
					for i := int64(len(buf)) - 1; i >= 0; i-- {
						if buf[i] == '\n' {
							newlinePos = readStart + i + 1
							break
						}
					}
					if newlinePos > 0 {
						_ = file.Truncate(newlinePos)
						log.Warnln("[Smart] Removed incomplete CSV row from %s", c.dataPath)
					} else {
						_ = file.Truncate(0)
						fileExists = false
						log.Warnln("[Smart] No valid CSV rows found, reinitializing %s", c.dataPath)
					}
				}
			}
		}
	}

	c.file = file
	c.writer = csv.NewWriter(c.file)

	if !fileExists {
		headers := []string{
			"success", "failure", "connect_time", "latency",
			"upload_mb", "history_upload_mb", "maxuploadrate_kb", "history_maxuploadrate_kb",
			"download_mb", "history_download_mb", "maxdownloadrate_kb", "history_maxdownloadrate_kb",
			"duration_minutes", "history_duration_minutes", "last_used_seconds",
			"is_udp", "is_tcp",
			"loss_rate",
			"asn_feature",
			"country_feature",
			"address_feature",
			"port_feature",
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
