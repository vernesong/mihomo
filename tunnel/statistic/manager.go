package statistic

import (
	"os"
	"time"

	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/common/xsync"
	"github.com/metacubex/mihomo/component/memory"
)

var DefaultManager *Manager

func init() {
	DefaultManager = &Manager{
		uploadTemp:    atomic.NewInt64(0),
		downloadTemp:  atomic.NewInt64(0),
		uploadBlip:    atomic.NewInt64(0),
		downloadBlip:  atomic.NewInt64(0),
		uploadTotal:   atomic.NewInt64(0),
		downloadTotal: atomic.NewInt64(0),
		pid:           int32(os.Getpid()),
	}

	go DefaultManager.handle()
}

type Manager struct {
	connections   xsync.Map[string, Tracker]
	smartTarget   xsync.Map[string, *xsync.Map[string, bool]]
	uploadTemp    atomic.Int64
	downloadTemp  atomic.Int64
	uploadBlip    atomic.Int64
	downloadBlip  atomic.Int64
	uploadTotal   atomic.Int64
	downloadTotal atomic.Int64
	pid           int32
	memory        uint64
}

func (m *Manager) Join(c Tracker) {
	m.connections.Store(c.ID(), c)
	m.joinSmartTarget(c)
}

func (m *Manager) Leave(c Tracker) {
	m.connections.Delete(c.ID())
	m.leaveSmartTarget(c)
}

func (m *Manager) Get(id string) (c Tracker) {
	if value, ok := m.connections.Load(id); ok {
		c = value
	}
	return
}

func (m *Manager) Range(f func(c Tracker) bool) {
	m.connections.Range(func(key string, value Tracker) bool {
		return f(value)
	})
}

func (m *Manager) PushUploaded(size int64) {
	m.uploadTemp.Add(size)
	m.uploadTotal.Add(size)
}

func (m *Manager) PushDownloaded(size int64) {
	m.downloadTemp.Add(size)
	m.downloadTotal.Add(size)
}

func (m *Manager) Now() (up int64, down int64) {
	return m.uploadBlip.Load(), m.downloadBlip.Load()
}

func (m *Manager) Total() (up, down int64) {
	return m.uploadTotal.Load(), m.downloadTotal.Load()
}

func (m *Manager) Memory() uint64 {
	m.updateMemory()
	return m.memory
}

func (m *Manager) Snapshot() *Snapshot {
	var connections []*TrackerInfo
	m.Range(func(c Tracker) bool {
		connections = append(connections, c.Info())
		return true
	})
	return &Snapshot{
		UploadTotal:   m.uploadTotal.Load(),
		DownloadTotal: m.downloadTotal.Load(),
		Connections:   connections,
		Memory:        m.memory,
	}
}

func (m *Manager) updateMemory() {
	stat, err := memory.GetMemoryInfo(m.pid)
	if err != nil {
		return
	}
	m.memory = stat.RSS
}

func (m *Manager) ResetStatistic() {
	m.uploadTemp.Store(0)
	m.uploadBlip.Store(0)
	m.uploadTotal.Store(0)
	m.downloadTemp.Store(0)
	m.downloadBlip.Store(0)
	m.downloadTotal.Store(0)
}

func (m *Manager) handle() {
	ticker := time.NewTicker(time.Second)

	for range ticker.C {
		m.uploadBlip.Store(m.uploadTemp.Swap(0))
		m.downloadBlip.Store(m.downloadTemp.Swap(0))
	}
}

type Snapshot struct {
	DownloadTotal int64          `json:"downloadTotal"`
	UploadTotal   int64          `json:"uploadTotal"`
	Connections   []*TrackerInfo `json:"connections"`
	Memory        uint64         `json:"memory"`
}

func (m *Manager) joinSmartTarget(c Tracker) {
	target := c.Info().Metadata.SmartTarget

	if target == "" {
		return
	}

	result, _ := m.smartTarget.LoadOrStore(target, xsync.NewMap[string, bool]())
	result.Store(c.ID(), true)

	asn := c.Info().Metadata.DstIPASN
	if asn != "" && asn != "unknown" {
		result, _ := m.smartTarget.LoadOrStore(asn, xsync.NewMap[string, bool]())
		result.Store(c.ID(), true)
	}
}

func (m *Manager) leaveSmartTarget(c Tracker) {
	target := c.Info().Metadata.SmartTarget

	if target == "" {
		return
	}

	m.smartTarget.Compute(target, func(result *xsync.Map[string, bool], loaded bool) (*xsync.Map[string, bool], xsync.ComputeOp) {
		if loaded {
			result.Delete(c.ID())
			if result.Size() == 0 {
				return result, xsync.DeleteOp
			} else {
				return result, xsync.UpdateOp
			}
		}
		return result, xsync.CancelOp
	})

	asn := c.Info().Metadata.DstIPASN
	if asn != "" && asn != "unknown" {
		m.smartTarget.Compute(asn, func(result *xsync.Map[string, bool], loaded bool) (*xsync.Map[string, bool], xsync.ComputeOp) {
			if loaded {
				result.Delete(c.ID())
				if result.Size() == 0 {
					return result, xsync.DeleteOp
				} else {
					return result, xsync.UpdateOp
				}
			}
			return result, xsync.CancelOp
		})
	}
}

func (m *Manager) GetSmartTargetIDs(target, asn string) (map[string]bool) {
	targetIDs := make(map[string]bool)

	if result, ok := m.smartTarget.Load(target); ok {
		result.Range(func(id string, _ bool) bool {
			targetIDs[id] = true
			return true
		})
	}

	if asn != "" && asn != "unknown" {
		if result, ok := m.smartTarget.Load(asn); ok {
			result.Range(func(id string, _ bool) bool {
				targetIDs[id] = true
				return true
			})
		}
	}

	return targetIDs
}