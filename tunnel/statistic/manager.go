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
	target        xsync.Map[string, []string]
	asn           xsync.Map[string, []string]
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
	m.joinTargetID(c)
	m.joinASNID(c)
}

func (m *Manager) Leave(c Tracker) {
	m.connections.Delete(c.ID())
	m.leaveTargetID(c)
	m.leaveASNID(c)
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

func (m *Manager) joinTargetID(c Tracker) {
	target := c.Info().Metadata.SmartTarget
	if ids, ok := m.target.Load(target); ok {
		ids = append(ids, c.ID())
		m.target.Store(target, ids)
	} else {
		m.target.Store(target, []string{c.ID()})
	}
}

func (m *Manager) leaveTargetID(c Tracker) {
	target := c.Info().Metadata.SmartTarget
	if ids, ok := m.target.Load(target); ok {
		newIDs := make([]string, 0, len(ids))
		for _, id := range ids {
			if id != c.ID() {
				newIDs = append(newIDs, id)
			}
		}
		if len(newIDs) > 0 {
			m.target.Store(target, newIDs)
		} else {
			m.target.Delete(target)
		}
	}
}

func (m *Manager) joinASNID(c Tracker) {
	if asn := c.Info().Metadata.DstIPASN; asn != "" && asn != "unknown" {
        if ids, ok := m.asn.Load(asn); ok {
            ids = append(ids, c.ID())
            m.asn.Store(asn, ids)
        } else {
            m.asn.Store(asn, []string{c.ID()})
        }
    }
}

func (m *Manager) leaveASNID(c Tracker) {
	if asn := c.Info().Metadata.DstIPASN; asn != "" && asn != "unknown" {
		if ids, ok := m.asn.Load(asn); ok {
			newIDs := make([]string, 0, len(ids))
			for _, id := range ids {
				if id != c.ID() {
					newIDs = append(newIDs, id)
				}
			}
			if len(newIDs) > 0 {
				m.asn.Store(asn, newIDs)
			} else {
				m.asn.Delete(asn)
			}
		}
	}
}

func (m *Manager) GetTargetIDs(target string) []string {
	if ids, ok := m.target.Load(target); ok {
		return ids
	}
	return nil
}

func (m *Manager) GetASNIDs(asn string) []string {
	if ids, ok := m.asn.Load(asn); ok {
		return ids
	}
	return nil
}