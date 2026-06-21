//go:build linux || freebsd

package tcpstats

import (
	"syscall"

	"golang.org/x/sys/unix"
)

func readTCPStats(rawConn syscall.RawConn) *Stats {
	var tcpInfo *unix.TCPInfo
	var ctrlErr error
	rawConn.Control(func(fd uintptr) {
		if int(fd) <= 2 {
			ctrlErr = syscall.EBADF
			return
		}
		tcpInfo, ctrlErr = unix.GetsockoptTCPInfo(int(fd), unix.IPPROTO_TCP, unix.TCP_INFO)
	})
	if ctrlErr != nil || tcpInfo == nil {
		return nil
	}
	return &Stats{
		SegsOut:     uint64(tcpInfo.Segs_out),
		RetransSegs: uint64(tcpInfo.Total_retrans),
	}
}
