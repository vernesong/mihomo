package smart

import (
	"math"
	"time"
)

var (
	presetSceneParams = map[string]SceneParams{
		"interactive": {0.6, 0.1, 0.3, 1.2, 1.0, 1.3, 0.3},
		"streaming":   {0.5, 0.2, 0.3, 1.5, 0.8, 1.2, 0.2},
		"transfer":    {0.5, 0.2, 0.3, 1.8, 0.7, 0.9, 0.1},
		"web":         {0.5, 0.1, 0.4, 0.8, 0.6, 1.0, 0.2},
	}
)

type (
	SceneParams struct {
		successRateWeight float64
		connectTimeWeight float64
		latencyWeight     float64
		trafficWeight     float64
		durationWeight    float64
		qualityWeight     float64
		minDecayFactor    float64
	}
)

// 计算权重
func CalculateWeight(input *ModelInput, priorityFactor float64) (float64, bool) {
	// 1. 数据准备
	success := input.Success
	failure := input.Failure
	connectTime := input.ConnectTime
	latency := input.Latency
	isUDP := input.IsUDP
	uploadMB := input.UploadTotal
	downloadMB := input.DownloadTotal
	maxUploadRateKB := input.MaxuploadRate
	maxDownloadRateKB := input.MaxdownloadRate
	durationMinutes := input.ConnectionDuration
	lastConnectTimestamp := input.LastUsed
	
	// 2. 检查样本数量
	total := success + failure
	if total < DefaultMinSampleCount {
		return 0, false
	}

	// 3. 场景识别和参数获取
	sceneType := identifyConnectionScene(isUDP, latency, uploadMB, downloadMB, maxUploadRateKB, maxDownloadRateKB, durationMinutes)

	var params SceneParams
	if p, ok := presetSceneParams[sceneType]; ok {
		params = p
	} else {
		params = presetSceneParams["web"]
	}

	// 4. 计算时间衰减因子
	timeFactor := 1.0
	if lastConnectTimestamp > 0 {
		timeFactor = GetTimeDecayWithCache(lastConnectTimestamp, time.Now().Unix(), params.minDecayFactor)
	}

	// 5. 对所有历史数据应用时间衰减
	decayedSuccess := float64(success) * timeFactor
	decayedFailure := float64(failure) * timeFactor
	decayedTotal := decayedSuccess + decayedFailure

	if decayedTotal < 1.0 {
		decayedSuccess = math.Max(0.5, decayedSuccess)
		decayedFailure = math.Max(0.5, decayedFailure)
		decayedTotal = decayedSuccess + decayedFailure
	}

	// 6. 基础指标计算
	if connectTime == 0 {
		connectTime = 2000
	}

	if latency == 0 {
		latency = 2000
	}

	successRate := decayedSuccess / decayedTotal
	connectScore := math.Exp(-float64(connectTime)/1500.0) * timeFactor
	latencyScore := math.Exp(-float64(latency)/1500.0) * timeFactor

	connectScore = math.Min(0.8, connectScore)
	latencyScore = math.Min(0.8, latencyScore)

	connectScore = math.Max(0.3, connectScore)
	latencyScore = math.Max(0.3, latencyScore)

	// 7. UDP协议调整
	if isUDP {
		params.latencyWeight = math.Min(0.5, params.latencyWeight*1.2)
		params.successRateWeight = math.Min(0.6, params.successRateWeight*1.1)
		params.connectTimeWeight = 1.0 - params.successRateWeight - params.latencyWeight
	}

	// 8. 连接类型判断
	isShortConnection := durationMinutes <= 1
	isLongConnection := durationMinutes > 10

	// 9. 基础权重计算
	baseWeight := (successRate * params.successRateWeight) +
		(connectScore * params.connectTimeWeight) +
		(latencyScore * params.latencyWeight)

	// 10. 流量因子计算
	var trafficFactor float64 = 0
	if uploadMB > 0 || downloadMB > 0 {
		uploadFactor := calculateTrafficFactor(uploadMB, maxUploadRateKB, durationMinutes, isShortConnection)
		downloadFactor := calculateTrafficFactor(downloadMB, maxDownloadRateKB, durationMinutes, isShortConnection)

		// 根据场景调整上下行权重
		var uploadWeight, downloadWeight float64
		if sceneType == "streaming" {
			uploadWeight, downloadWeight = 0.2, 0.8
		} else if sceneType == "transfer" && uploadMB > downloadMB*2 {
			uploadWeight, downloadWeight = 0.7, 0.3
		} else {
			uploadWeight, downloadWeight = 0.4, 0.6
		}

		trafficFactor = (uploadFactor * uploadWeight) + (downloadFactor * downloadWeight)
	}

	// 11. 持续时间因子计算
	var durationFactor float64 = 0.1
	if durationMinutes > 0 {
		if isShortConnection {
			durationFactor = math.Min(0.3, 0.1+math.Log1p(durationMinutes)*0.08)
		} else if isLongConnection {
			durationFactor = math.Min(0.5, 0.2+math.Log1p(durationMinutes)*0.1)
		} else {
			durationFactor = math.Min(0.4, 0.15+math.Log1p(durationMinutes)*0.09)
		}
	}

	// 12. 质量加成计算
	var qualityBonus float64 = 0

	if latency > 0 && latency < 100 {
		qualityBonus += 0.1
	}
	if connectTime > 0 && connectTime < 10 {
		qualityBonus += 0.1
	}
	if successRate > 0.95 {
		qualityBonus += 0.1
	}
	if (sceneType == "streaming" || sceneType == "transfer") && downloadMB > 20 {
		qualityBonus += 0.1
	}
	if sceneType == "interactive" && latency > 0 && latency < 100 && successRate > 0.9 {
		qualityBonus += 0.1
	}

	qualityBonus = math.Min(0.3, qualityBonus)

	return baseWeight * (1 +
		trafficFactor*params.trafficWeight +
		durationFactor*params.durationWeight +
		qualityBonus*params.qualityWeight) * priorityFactor, false
}

// 识别连接的使用场景类型
func identifyConnectionScene(isUDP bool, latency int64, uploadMB, downloadMB, maxUploadRateKB, maxDownloadRateKB, durationMinutes float64) string {
	const (
		SceneInteractive = "interactive" // 游戏/互动场景
		SceneStreaming   = "streaming"   // 流媒体场景
		SceneTransfer    = "transfer"    // 大流量传输场景
		SceneWeb         = "web"         // Web场景
	)

	// 游戏/互动场景特征：低延迟，持续连接，流量相对平衡
	if (isUDP && latency < 150 && durationMinutes > 3 &&
		uploadMB > 0.2 && downloadMB > 0.2 &&
		maxUploadRateKB > 200 && maxDownloadRateKB > 200 &&
		(uploadMB+downloadMB)/durationMinutes > 0.1 && (uploadMB+downloadMB)/durationMinutes < 10) ||
		(!isUDP && latency < 250 && durationMinutes > 3 &&
			uploadMB > 0.1 && downloadMB > 0.1 &&
			uploadMB < 150 && downloadMB < 150 &&
			(uploadMB/downloadMB > 0.2) && (uploadMB/downloadMB < 5) &&
			maxUploadRateKB > 150 && maxDownloadRateKB > 150 &&
			(uploadMB+downloadMB)/durationMinutes > 0.05 && (uploadMB+downloadMB)/durationMinutes < 15) {
		return SceneInteractive
	}

	// 大流量传输场景
	if (uploadMB > 100 || downloadMB > 100 || maxUploadRateKB > 5000) && durationMinutes > 0.5 {
		totalThroughput := (uploadMB + downloadMB) / durationMinutes
		if totalThroughput > 5 {
			return SceneTransfer
		}
	}

	// 流媒体场景
	if durationMinutes > 1 {
		downloadThroughput := downloadMB / durationMinutes
		if ((downloadMB > 60 && downloadMB/uploadMB > 3 && maxDownloadRateKB > 2000 && maxDownloadRateKB/maxUploadRateKB > 4 && downloadThroughput > 5) ||
			(downloadMB > 15 && downloadMB/uploadMB > 3 && maxDownloadRateKB > 1000 && maxDownloadRateKB/maxUploadRateKB > 3 && downloadThroughput > 2)) {
			return SceneStreaming
		}
	}

	// 默认为Web场景
	return SceneWeb
}

// 计算流量因子
func calculateTrafficFactor(trafficMB, maxRateKB, durationMinutes float64, isShort bool) float64 {
	if trafficMB <= 0 || durationMinutes <= 0 {
		return 0.0
	}

	var baseFactor float64
	switch {
	case trafficMB < 0.005: // <5KB
		baseFactor = 0.10 + 0.05*math.Log10(trafficMB/0.001)
	case trafficMB < 0.01:
		baseFactor = 0.18 + 0.08*math.Log10(trafficMB/0.005)
	case trafficMB < 0.05:
		baseFactor = 0.35 + 0.10*math.Log10(trafficMB/0.01)
	case trafficMB < 0.1:
		baseFactor = 0.53 + 0.15*math.Log10(trafficMB/0.05)
	case trafficMB < 0.5:
		baseFactor = 0.72 + 0.18*math.Log10(trafficMB/0.1)
	case trafficMB < 1:
		baseFactor = 0.98 + 0.15*math.Log10(trafficMB/0.5)
	case trafficMB < 5:
		baseFactor = 1.18 + 0.10*math.Log10(trafficMB/1)
	case trafficMB < 20:
		baseFactor = 1.32 + 0.08*math.Log10(trafficMB/5)
	case trafficMB < 100:
		baseFactor = 1.45 + 0.06*math.Log10(trafficMB/20)
	case trafficMB < 500:
		baseFactor = 1.56 + 0.05*math.Log10(trafficMB/100)
	case trafficMB < 3000:
		baseFactor = 1.66 + 0.04*math.Log10(trafficMB/500)
	default:
		baseFactor = 1.74 + 0.02*math.Log10(trafficMB/3000)
	}

	// 吞吐量加成
	var rateBonus float64
	switch {
	case maxRateKB < 20:
		rateBonus = 1.0 + 0.05*(maxRateKB/20.0)
	case maxRateKB < 100:
		rateBonus = 1.05 + 0.05*((maxRateKB-20)/80.0)
	case maxRateKB < 500:
		rateBonus = 1.10 + 0.05*((maxRateKB-100)/400.0)
	case maxRateKB < 2000:
		rateBonus = 1.15 + 0.05*((maxRateKB-500)/1500.0)
	case maxRateKB < 5000:
		rateBonus = 1.20 + 0.04*((maxRateKB-2000)/3000.0)
	case maxRateKB < 20000:
		rateBonus = 1.24 + 0.04*((maxRateKB-5000)/15000.0)
	case maxRateKB < 100000:
		rateBonus = 1.28 + 0.03*math.Log10(maxRateKB/20000.0)
		rateBonus = math.Min(rateBonus, 1.32)
	default:
		rateBonus = 1.32 + 0.02*math.Log10(maxRateKB/100000.0)
		rateBonus = math.Min(rateBonus, 1.36)
	}
	baseFactor *= rateBonus

	// 平均流量加成
	var connectionFactor float64
	throughput := trafficMB / math.Max(1.0, durationMinutes)
	if isShort {
		connectionFactor = 0.85 + 0.15*math.Min(1, throughput/25.0)
	} else {
		connectionFactor = 1.0
		if throughput > 5 {
			baseFactor *= 1.0 + 0.15*math.Min(1, (throughput-5)/80.0)
		}
	}

	factor := baseFactor * connectionFactor

	return math.Min(1.25, factor)
}
