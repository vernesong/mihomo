//go:build windows

package tcpstats

import (
	"syscall"
	"unsafe"
)

const sioTCPInfo = 0xD8000027

type tcpInfoV0 struct {
	State             uint32
	Mss               uint32
	ConnectionTimeMs  uint64
	TimestampsEnabled uint32
	RttUs             uint32
	MinRttUs          uint32
	BytesInFlight     uint32
	Cwnd              uint32
	SndWnd            uint32
	RcvWnd            uint32
	RcvBuf            uint32
	BytesOut          uint64
	BytesIn           uint64
	BytesReordered    uint32
	BytesRetrans      uint32
	FastRetrans       uint32
	DupAcksIn         uint32
	TimeoutEpisodes   uint32
	SynRetrans        uint32
}

func readTCPStats(rawConn syscall.RawConn) *Stats {
	var info tcpInfoV0
	var ctrlErr error
	rawConn.Control(func(fd uintptr) {
		var bytesReturned uint32
		version := uint32(0)
		ctrlErr = syscall.WSAIoctl(
			syscall.Handle(fd),
			sioTCPInfo,
			(*byte)(unsafe.Pointer(&version)),
			uint32(unsafe.Sizeof(version)),
			(*byte)(unsafe.Pointer(&info)),
			uint32(unsafe.Sizeof(info)),
			&bytesReturned,
			nil,
			0,
		)
	})
	if ctrlErr != nil {
		return nil
	}
	return &Stats{
		BytesSent:    info.BytesOut,
		BytesRetrans: uint64(info.BytesRetrans),
	}
}
