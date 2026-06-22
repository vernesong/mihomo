//go:build freebsd

package tcpstats

import (
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

const tcpPerfInfo = 0x4e

type tcpPerfInfoFreeBSD struct {
	CntCounters [13]uint64
	ProcTime    [13]uint64
	Timebase    uint64
	TbIsStable  uint8
}

// VOI (Variables of Interest) indices into CntCounters[].
const (
	voiTCPTxPB   = 0 // transmit payload bytes
	voiTCPRetxPB = 1 // retransmit payload bytes
)

func readTCPStats(rawConn syscall.RawConn) *Stats {
	var info tcpPerfInfoFreeBSD
	var ctrlErr error
	rawConn.Control(func(fd uintptr) {
		if int(fd) <= 2 {
			ctrlErr = syscall.EBADF
			return
		}
		var size uint32 = uint32(unsafe.Sizeof(info))
		_, _, errno := syscall.Syscall6(
			syscall.SYS_GETSOCKOPT,
			fd,
			uintptr(unix.IPPROTO_TCP),
			uintptr(tcpPerfInfo),
			uintptr(unsafe.Pointer(&info)),
			uintptr(unsafe.Pointer(&size)),
			0,
		)
		if errno != 0 {
			ctrlErr = errno
		}
	})
	if ctrlErr != nil {
		return nil
	}
	return &Stats{
		BytesSent:    info.CntCounters[voiTCPTxPB],
		BytesRetrans: info.CntCounters[voiTCPRetxPB],
	}
}
