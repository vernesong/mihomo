package smart

import (
	"math"
	"time"
)

const (
	AllowedWeight         = 0.4
	DefaultMinSampleCount = 2
)

type sceneKind int

const (
	sceneWeb sceneKind = iota
	sceneInteractive
	sceneStreaming
	sceneTransfer
)

var presetSceneParams = [4]SceneParams{
	sceneWeb:         {0.5, 0.1, 0.4, 0.8, 0.6, 1.0, 0.3, 0.2},
	sceneInteractive: {0.6, 0.1, 0.3, 1.2, 1.0, 1.3, 0.5, 0.3},
	sceneStreaming:   {0.5, 0.2, 0.3, 1.5, 0.8, 1.2, 0.8, 0.2},
	sceneTransfer:    {0.5, 0.2, 0.3, 1.8, 0.7, 0.9, 1.0, 0.1},
}

type SceneParams struct {
	successRateWeight float64
	connectTimeWeight float64
	latencyWeight     float64
	trafficWeight     float64
	durationWeight    float64
	qualityWeight     float64
	lossWeight        float64
	minDecayFactor    float64
}

type ModelInput struct {
	Success     int64 // successes
	Failure     int64 // failures
	ConnectTime int64 // connect time (ms)
	Latency     int64 // latency (ms)

	UploadTotal          float64 // uploaded (MB)
	HistoryUploadTotal   float64 // uploaded total (MB)
	MaxuploadRate        float64 // max upload rate (KB/s)
	HistoryMaxUploadRate float64 // max upload rate total (KB/s)

	DownloadTotal          float64 // downloaded (MB)
	HistoryDownloadTotal   float64 // downloaded total (MB)
	MaxdownloadRate        float64 // max download rate (KB/s)
	HistoryMaxDownloadRate float64 // max download rate total (KB/s)

	ConnectionDuration        float64 // duration (minutes)
	HistoryConnectionDuration float64 // average duration (minutes)
	LastUsed                  int64   // last used timestamp

	IsUDP            bool    // UDP connection
	IsTCP            bool    // TCP connection
	ConnectionFailed bool    // this connection failed, a reuse counts as success
	LossRate         float64 // judged window loss 0.0-1.0, 0 = no window / unsupported / UDP
	CumulLossRate    float64 // cumulative loss rate cumulRetrans/cumulSent
	EmaLossRate      float64 // EMA loss rate, decays and reflects the recent trend

	DestIPASN string   // ASN of the target IP
	Host      string   // target host
	DestIP    string   // target IP
	DestPort  uint16   // target port
	DestGeoIP []string // geolocation of the target IP

	ExitRegion string // region the node really lands in for this target
	ExitASN    string // network the node runs on for this target

	GroupName string // group name
	NodeName  string // node name
}

func CalculateWeight(input *ModelInput, priorityFactor float64) (float64, bool) {
	success := input.Success
	failure := input.Failure
	connectTime := input.ConnectTime
	latency := input.Latency
	isUDP := input.IsUDP
	uploadMB := input.UploadTotal
	historyUploadTotal := input.HistoryUploadTotal
	downloadMB := input.DownloadTotal
	historyDownloadTotal := input.HistoryDownloadTotal
	maxUploadRateKB := input.MaxuploadRate
	historyMaxUploadRate := input.HistoryMaxUploadRate
	maxDownloadRateKB := input.MaxdownloadRate
	historyMaxDownloadRate := input.HistoryMaxDownloadRate
	durationMinutes := input.ConnectionDuration
	historyConnectionDuration := input.HistoryConnectionDuration
	lastConnectTimestamp := input.LastUsed

	total := success + failure
	if total < DefaultMinSampleCount {
		return 0, false
	}

	scene := identifyConnectionScene(isUDP, latency, uploadMB, downloadMB, maxUploadRateKB, maxDownloadRateKB, durationMinutes)
	params := presetSceneParams[scene]

	timeFactor := 1.0
	if lastConnectTimestamp > 0 {
		timeFactor = GetTimeDecayWithCache(lastConnectTimestamp, time.Now().Unix(), params.minDecayFactor)
	}

	decayedSuccess := float64(success) * timeFactor
	decayedFailure := float64(failure) * timeFactor
	decayedTotal := decayedSuccess + decayedFailure

	if decayedTotal < 1.0 {
		decayedSuccess = math.Max(0.5, decayedSuccess)
		decayedFailure = math.Max(0.5, decayedFailure)
		decayedTotal = decayedSuccess + decayedFailure
	}

	if connectTime == 0 {
		if !input.ConnectionFailed {
			connectTime = 1
		} else {
			connectTime = 2000
		}
	}

	// A first-read latency of 0 means no byte ever came back, i.e. the latency is unknown, not
	// 1 ms (W1). A failed connection is scored as slow; an unanswered one gets a neutral score
	// and no low-latency bonus. (A connect time of 0 is different: it is a real sub-ms dial on
	// multiplexed transports and stays fast.)
	latencyKnown := latency > 0
	if !latencyKnown && input.ConnectionFailed {
		latency, latencyKnown = 2000, true
	}

	successRate := decayedSuccess / decayedTotal
	connectScore := scoreDelay(float64(connectTime), connectScoreScale, timeFactor)
	latencyScore := neutralDelayScore * timeFactor
	if latencyKnown {
		latencyScore = scoreDelay(float64(latency), latencyScoreScale, timeFactor)
	}

	connectScore = math.Min(maxDelayScore, math.Max(minDelayScore, connectScore))
	latencyScore = math.Min(maxDelayScore, math.Max(minDelayScore, latencyScore))

	if isUDP {
		params.latencyWeight = math.Min(0.5, params.latencyWeight*1.2)
		params.successRateWeight = math.Min(0.6, params.successRateWeight*1.1)
		params.connectTimeWeight = 1.0 - params.successRateWeight - params.latencyWeight
	}

	isShortConnection := durationMinutes <= 1
	isLongConnection := durationMinutes > 10

	baseWeight := (successRate * params.successRateWeight) +
		(connectScore * params.connectTimeWeight) +
		(latencyScore * params.latencyWeight)

	var trafficFactor float64 = 0
	if uploadMB > 0 || downloadMB > 0 {
		uploadFactor := calculateTrafficFactor(uploadMB, maxUploadRateKB, durationMinutes, historyMaxUploadRate, historyUploadTotal, historyConnectionDuration, isShortConnection)
		downloadFactor := calculateTrafficFactor(downloadMB, maxDownloadRateKB, durationMinutes, historyMaxDownloadRate, historyDownloadTotal, historyConnectionDuration, isShortConnection)

		var uploadWeight, downloadWeight float64
		if scene == sceneStreaming {
			uploadWeight, downloadWeight = 0.2, 0.8
		} else if scene == sceneTransfer && uploadMB > downloadMB*2 {
			uploadWeight, downloadWeight = 0.7, 0.3
		} else {
			uploadWeight, downloadWeight = 0.4, 0.6
		}

		trafficFactor = (uploadFactor * uploadWeight) + (downloadFactor * downloadWeight)
	}

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

	var qualityBonus float64 = 0

	if latencyKnown && latency < 100 {
		qualityBonus += 0.1
	}
	if connectTime > 0 && connectTime < 10 {
		qualityBonus += 0.1
	}
	if (scene == sceneStreaming || scene == sceneTransfer) && downloadMB > 20 {
		qualityBonus += 0.1
	}
	if scene == sceneInteractive && latencyKnown && latency < 100 && successRate > 0.9 {
		qualityBonus += 0.1
	}

	qualityBonus = math.Min(0.3, qualityBonus)

	// currentPenalty: judged window loss, sensitive but floor gated
	// cumulPenalty:   cumulative loss, stable but never decays
	// emaPenalty:     EMA loss, decays and reflects the recent trend
	// improvementFactor: EMA < cumulative means the quality is recovering
	lossFactor := 0.0
	if input.LossRate > 0 || input.CumulLossRate > 0 {
		currentPenalty := 0.0
		if input.LossRate > 0 {
			currentPenalty = 1.0 - math.Exp(-input.LossRate*10.0)
		}

		cumulPenalty := 0.0
		if input.CumulLossRate > 0 {
			cumulPenalty = 1.0 - math.Exp(-input.CumulLossRate*50.0)
		}

		emaPenalty := 0.0
		if input.EmaLossRate > 0 {
			emaPenalty = 1.0 - math.Exp(-input.EmaLossRate*50.0)
		}

		// trustCumul in [0,1]: the higher the cumulative loss, the more it is trusted
		trustCumul := math.Min(1.0, cumulPenalty*5.0)

		// EMA correction: EMA < cumulative means the recent quality improved
		improvementFactor := 1.0
		if cumulPenalty > 0 && emaPenalty > 0 && emaPenalty < cumulPenalty {
			improvementFactor = emaPenalty / cumulPenalty
		}

		// trusted cumulative: max(current, cumulative) * trend correction,
		// untrusted cumulative: current * 0.3, a transient burst is discounted
		lossFactor = trustCumul*math.Max(currentPenalty, cumulPenalty)*improvementFactor +
			(1.0-trustCumul)*currentPenalty*0.3
	}

	return baseWeight * (1 +
		trafficFactor*params.trafficWeight +
		durationFactor*params.durationWeight +
		qualityBonus*params.qualityWeight -
		lossFactor*params.lossWeight) * priorityFactor, false
}

const (
	minDelayScore = 0.3
	maxDelayScore = 0.8
	// neutralDelayScore is used when a delay was not measured
	neutralDelayScore = (minDelayScore + maxDelayScore) / 2
	// The scales set where a delay loses most of its score. The old curve exp(-ms/1500) was
	// clamped at 0.8, so every delay below ~335 ms scored the same (W2); these keep the same
	// [0.3, 0.8] range but resolve the range that matters for interactive traffic.
	latencyScoreScale = 400.0 // ms: 30 ms -> 0.76, 120 ms -> 0.67, 330 ms -> 0.52, 1 s -> 0.34
	connectScoreScale = 600.0 // ms: a connect includes the proxy handshake, so it is longer
)

// scoreDelay maps a delay to [minDelayScore, maxDelayScore], scaled down by the time decay.
func scoreDelay(ms, scale, timeFactor float64) float64 {
	return minDelayScore + (maxDelayScore-minDelayScore)*math.Exp(-ms/scale)*timeFactor
}

func GetTimeDecayWithCache(lastUsedTime int64, now int64, minDecay float64) float64 {
	fuzzyLastUsedTime := (lastUsedTime / 3600) * 3600

	hoursSinceLastConn := float64(now-fuzzyLastUsedTime) / 3600.0
	var decay float64

	switch {
	case hoursSinceLastConn <= 24:
		decay = 1.0
	case hoursSinceLastConn <= 72:
		decay = 1.0 - (hoursSinceLastConn-24.0)/48.0*0.2
	case hoursSinceLastConn <= 168:
		decay = 0.8 - (hoursSinceLastConn-72.0)/96.0*0.3
	case hoursSinceLastConn <= 720:
		decay = 0.5 - (hoursSinceLastConn-168.0)/552.0*0.2
	default:
		decay = 0.1
	}

	decay = math.Max(minDecay, decay)
	return decay
}

func identifyConnectionScene(isUDP bool, latency int64, uploadMB, downloadMB, maxUploadRateKB, maxDownloadRateKB, durationMinutes float64) sceneKind {
	totalRate := (uploadMB + downloadMB) / durationMinutes

	if (isUDP && latency < 150 && durationMinutes > 3 &&
		uploadMB > 0.2 && downloadMB > 0.2 &&
		maxUploadRateKB > 200 && maxDownloadRateKB > 200 &&
		totalRate > 0.1 && totalRate < 10) ||
		(!isUDP && latency < 250 && durationMinutes > 3 &&
			uploadMB > 0.1 && downloadMB > 0.1 &&
			uploadMB < 150 && downloadMB < 150 &&
			(uploadMB/downloadMB > 0.2) && (uploadMB/downloadMB < 5) &&
			maxUploadRateKB > 150 && maxDownloadRateKB > 150 &&
			totalRate > 0.05 && totalRate < 15) {
		return sceneInteractive
	}

	if (uploadMB > 100 || downloadMB > 100 || maxUploadRateKB > 5000) && durationMinutes > 0.5 {
		if totalRate > 5 {
			return sceneTransfer
		}
	}

	if durationMinutes > 1 {
		downloadThroughput := downloadMB / durationMinutes
		if (downloadMB > 60 && downloadMB/uploadMB > 3 && maxDownloadRateKB > 2000 && maxDownloadRateKB/maxUploadRateKB > 4 && downloadThroughput > 5) ||
			(downloadMB > 15 && downloadMB/uploadMB > 3 && maxDownloadRateKB > 1000 && maxDownloadRateKB/maxUploadRateKB > 3 && downloadThroughput > 2) {
			return sceneStreaming
		}
	}

	return sceneWeb
}

func calculateTrafficFactor(trafficMB, maxRateKB, durationMinutes, historyMaxRateKB, historyTotalMB, historyConnDuration float64, isShort bool) float64 {
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

	throughputKBs := (trafficMB * 1024.0) / math.Max(1.0, durationMinutes*60.0)

	accelBonus := 1.0
	if throughputKBs > 0 {
		ratio := maxRateKB / throughputKBs
		if ratio > 2.0 {
			accelBonus = 1.0 + math.Min(0.12, 0.02*(ratio-2.0))
		}
	}

	historyPenalty := 1.0
	if historyMaxRateKB > 0 {
		r := maxRateKB / historyMaxRateKB
		if r < 0.5 {
			historyPenalty = 0.6 + (1.0-0.6)*r
		} else if r < 0.9 {
			historyPenalty = 0.85 + 0.15*r
		} else if r > 1.2 {
			historyPenalty = 1.0 + math.Min(0.05, 0.02*(r-1.2))
		}
	}

	combinedRate := rateBonus * accelBonus * historyPenalty

	if historyMaxRateKB > 0 {
		historyRatio := maxRateKB / historyMaxRateKB
		historyAvgKBs := 0.0
		if historyTotalMB > 0 && historyConnDuration > 0 {
			historyAvgKBs = (historyTotalMB * 1024.0) / math.Max(1.0, historyConnDuration*60.0)
		}

		lowThroughput := false
		if historyAvgKBs > 0 {
			lowThroughput = throughputKBs < 0.3*historyAvgKBs
		} else {
			lowThroughput = throughputKBs < 10.0
		}

		if historyRatio < 0.1 && lowThroughput {
			evidence := 0.0
			if historyConnDuration > 0 && durationMinutes > 0 {
				ratio := historyConnDuration / durationMinutes
				evidence = math.Min(1.0, math.Max(0.0, (ratio-1.0)/4.0))
			}

			penalty := 1.0 - 0.5*evidence
			combinedRate *= penalty
		}
	}

	if combinedRate > 1.25 {
		combinedRate = 1.25
	}

	var connectionFactor float64
	throughput := trafficMB / math.Max(1.0, durationMinutes)
	if isShort {
		connectionFactor = 1.0 + 0.06*math.Min(1, throughput/25.0)
	} else {
		connectionFactor = 1.0
		if throughput > 5 {
			connectionFactor += 0.05 * math.Min(1, (throughput-5)/80.0)
		}
	}

	factor := baseFactor * combinedRate * connectionFactor

	return math.Min(1.25, factor)
}
