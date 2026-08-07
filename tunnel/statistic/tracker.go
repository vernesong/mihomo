package statistic

import (
	"io"
	"net"
	"sync"
	"time"

	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/common/buf"
	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"

	"github.com/gofrs/uuid/v5"
)

type Tracker interface {
	ID() string
	Close() error
	Info() *TrackerInfo
	C.Connection
}

type timeBucket struct {
	startMs int64
	bytes   int64
}

type bucketWindow struct {
	buckets    []timeBucket
	interval   int64
	windowMs   int64
	mu         sync.Mutex
	lastSlot   int64
	cachedRate atomic.Int64
}

type TrackerInfo struct {
	UUID            uuid.UUID    `json:"id"`
	Metadata        *C.Metadata  `json:"metadata"`
	UploadTotal     atomic.Int64 `json:"upload"`
	DownloadTotal   atomic.Int64 `json:"download"`
	Start           time.Time    `json:"start"`
	Chain           C.Chain      `json:"chains"`
	ProviderChain   C.Chain      `json:"providerChains"`
	Rule            string       `json:"rule"`
	RulePayload     string       `json:"rulePayload"`
	MaxUploadRate   atomic.Int64 `json:"maxUploadRate"`
	MaxDownloadRate atomic.Int64 `json:"maxDownloadRate"`
}

type tcpTracker struct {
	C.Conn `json:"-"`
	*TrackerInfo
	manager *Manager

	pushToManager bool `json:"-"`

	uploadBucketWindow   *bucketWindow
	downloadBucketWindow *bucketWindow
}

func (tt *tcpTracker) ID() string {
	return tt.UUID.String()
}

func (tt *tcpTracker) Info() *TrackerInfo {
	return tt.TrackerInfo
}

func (tt *tcpTracker) Read(b []byte) (int, error) {
	n, err := tt.Conn.Read(b)
	download := int64(n)
	if tt.pushToManager {
		tt.manager.PushDownloaded(download)
	}
	tt.DownloadTotal.Add(download)
	tt.TrackerInfo.MaxDownloadRate.Store(tt.downloadBucketWindow.updateMaxRate(download))
	return n, err
}

func (tt *tcpTracker) ReadBuffer(buffer *buf.Buffer) (err error) {
	err = tt.Conn.ReadBuffer(buffer)
	download := int64(buffer.Len())
	if tt.pushToManager {
		tt.manager.PushDownloaded(download)
	}
	tt.DownloadTotal.Add(download)
	tt.TrackerInfo.MaxDownloadRate.Store(tt.downloadBucketWindow.updateMaxRate(download))
	return
}

func (tt *tcpTracker) UnwrapReader() (io.Reader, []N.CountFunc) {
	return tt.Conn, []N.CountFunc{func(download int64) {
		if tt.pushToManager {
			tt.manager.PushDownloaded(download)
		}
		tt.DownloadTotal.Add(download)
		tt.TrackerInfo.MaxDownloadRate.Store(tt.downloadBucketWindow.updateMaxRate(download))
	}}
}

func (tt *tcpTracker) Write(b []byte) (int, error) {
	n, err := tt.Conn.Write(b)
	upload := int64(n)
	if tt.pushToManager {
		tt.manager.PushUploaded(upload)
	}
	tt.UploadTotal.Add(upload)
	tt.TrackerInfo.MaxUploadRate.Store(tt.uploadBucketWindow.updateMaxRate(upload))
	return n, err
}

func (tt *tcpTracker) WriteBuffer(buffer *buf.Buffer) (err error) {
	upload := int64(buffer.Len())
	err = tt.Conn.WriteBuffer(buffer)
	if tt.pushToManager {
		tt.manager.PushUploaded(upload)
	}
	tt.UploadTotal.Add(upload)
	tt.TrackerInfo.MaxUploadRate.Store(tt.uploadBucketWindow.updateMaxRate(upload))
	return
}

func (tt *tcpTracker) UnwrapWriter() (io.Writer, []N.CountFunc) {
	return tt.Conn, []N.CountFunc{func(upload int64) {
		if tt.pushToManager {
			tt.manager.PushUploaded(upload)
		}
		tt.UploadTotal.Add(upload)
		tt.TrackerInfo.MaxUploadRate.Store(tt.uploadBucketWindow.updateMaxRate(upload))
	}}
}

func (tt *tcpTracker) Close() error {
	connErr := tt.Conn.Close()
	tt.manager.Leave(tt)
	return connErr
}

func (tt *tcpTracker) Upstream() any {
	return tt.Conn
}

func NewTCPTracker(conn C.Conn, manager *Manager, metadata *C.Metadata, rule C.Rule, uploadTotal int64, downloadTotal int64, pushToManager bool) *tcpTracker {
	metadata.RemoteDst = conn.RemoteDestination()

	trackerUUID := utils.NewUUIDV4()

	metadata.UUID = trackerUUID.String()

	t := &tcpTracker{
		Conn:    conn,
		manager: manager,
		TrackerInfo: &TrackerInfo{
			UUID:          trackerUUID,
			Start:         time.Now(),
			Metadata:      metadata,
			Chain:         conn.Chains(),
			ProviderChain: conn.ProviderChains(),
			Rule:          "",
			UploadTotal:   atomic.NewInt64(uploadTotal),
			DownloadTotal: atomic.NewInt64(downloadTotal),
		},
		pushToManager:        pushToManager,
		uploadBucketWindow:   newBucketWindow(10, 100),
		downloadBucketWindow: newBucketWindow(10, 100),
	}

	if pushToManager {
		if uploadTotal > 0 {
			manager.PushUploaded(uploadTotal)
		}
		if downloadTotal > 0 {
			manager.PushDownloaded(downloadTotal)
		}
	}

	if rule != nil {
		t.TrackerInfo.Rule = rule.RuleType().String()
		t.TrackerInfo.RulePayload = rule.Payload()
	}

	manager.Join(t)
	return t
}

type udpTracker struct {
	C.PacketConn `json:"-"`
	*TrackerInfo
	manager *Manager

	pushToManager bool `json:"-"`

	uploadBucketWindow   *bucketWindow
	downloadBucketWindow *bucketWindow
}

func (ut *udpTracker) ID() string {
	return ut.UUID.String()
}

func (ut *udpTracker) Info() *TrackerInfo {
	return ut.TrackerInfo
}

func (ut *udpTracker) ReadFrom(b []byte) (int, net.Addr, error) {
	n, addr, err := ut.PacketConn.ReadFrom(b)
	download := int64(n)
	if ut.pushToManager {
		ut.manager.PushDownloaded(download)
	}
	ut.DownloadTotal.Add(download)
	ut.TrackerInfo.MaxDownloadRate.Store(ut.downloadBucketWindow.updateMaxRate(download))
	return n, addr, err
}

func (ut *udpTracker) WaitReadFrom() (data []byte, put func(), addr net.Addr, err error) {
	data, put, addr, err = ut.PacketConn.WaitReadFrom()
	download := int64(len(data))
	if ut.pushToManager {
		ut.manager.PushDownloaded(download)
	}
	ut.DownloadTotal.Add(download)
	ut.TrackerInfo.MaxDownloadRate.Store(ut.downloadBucketWindow.updateMaxRate(download))
	return
}

func (ut *udpTracker) WriteTo(b []byte, addr net.Addr) (int, error) {
	n, err := ut.PacketConn.WriteTo(b, addr)
	upload := int64(n)
	if ut.pushToManager {
		ut.manager.PushUploaded(upload)
	}
	ut.UploadTotal.Add(upload)
	ut.TrackerInfo.MaxUploadRate.Store(ut.uploadBucketWindow.updateMaxRate(upload))
	return n, err
}

func (ut *udpTracker) Close() error {
	connErr := ut.PacketConn.Close()
	ut.manager.Leave(ut)
	return connErr
}

func (ut *udpTracker) Upstream() any {
	return ut.PacketConn
}

func NewUDPTracker(conn C.PacketConn, manager *Manager, metadata *C.Metadata, rule C.Rule, uploadTotal int64, downloadTotal int64, pushToManager bool) *udpTracker {
	metadata.RemoteDst = conn.RemoteDestination()

	trackerUUID := utils.NewUUIDV4()

	metadata.UUID = trackerUUID.String()

	ut := &udpTracker{
		PacketConn: conn,
		manager:    manager,
		TrackerInfo: &TrackerInfo{
			UUID:          trackerUUID,
			Start:         time.Now(),
			Metadata:      metadata,
			Chain:         conn.Chains(),
			ProviderChain: conn.ProviderChains(),
			Rule:          "",
			UploadTotal:   atomic.NewInt64(uploadTotal),
			DownloadTotal: atomic.NewInt64(downloadTotal),
		},
		pushToManager:        pushToManager,
		uploadBucketWindow:   newBucketWindow(10, 100),
		downloadBucketWindow: newBucketWindow(10, 100),
	}

	if pushToManager {
		if uploadTotal > 0 {
			manager.PushUploaded(uploadTotal)
		}
		if downloadTotal > 0 {
			manager.PushDownloaded(downloadTotal)
		}
	}

	if rule != nil {
		ut.TrackerInfo.Rule = rule.RuleType().String()
		ut.TrackerInfo.RulePayload = rule.Payload()
	}

	manager.Join(ut)
	return ut
}

func newBucketWindow(bucketCount int, intervalMs int64) *bucketWindow {
	return &bucketWindow{
		buckets:  make([]timeBucket, bucketCount),
		interval: intervalMs,
		windowMs: intervalMs * int64(bucketCount),
	}
}

func (w *bucketWindow) updateMaxRate(bytes int64) int64 {
	if bytes <= 0 {
		return w.cachedRate.Load()
	}
	nowMs := time.Now().UnixNano() / 1e6
	slot := nowMs / w.interval
	idx := int(slot % int64(len(w.buckets)))
	bucketStart := slot * w.interval

	w.mu.Lock()
	if w.buckets[idx].startMs != bucketStart {
		w.buckets[idx].startMs = bucketStart
		w.buckets[idx].bytes = 0
	}
	w.buckets[idx].bytes += bytes

	if slot != w.lastSlot {
		w.lastSlot = slot
		windowStart := nowMs - w.windowMs
		maxRate := int64(0)
		for _, b := range w.buckets {
			if b.startMs >= windowStart && b.bytes > 0 {
				rate := b.bytes * 1000 / w.interval
				if rate > maxRate {
					maxRate = rate
				}
			}
		}
		w.cachedRate.Store(maxRate)
	} else {
		if r := w.buckets[idx].bytes * 1000 / w.interval; r > w.cachedRate.Load() {
			w.cachedRate.Store(r)
		}
	}
	result := w.cachedRate.Load()
	w.mu.Unlock()
	return result
}
