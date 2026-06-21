//go:build darwin

package tcpstats

import (
	"syscall"

	"golang.org/x/sys/unix"
)

func readTCPStats(rawConn syscall.RawConn) *Stats {
	var tcpInfo *unix.TCPConnectionInfo
	var ctrlErr error
	rawConn.Control(func(fd uintptr) {
		if int(fd) <= 2 {
			ctrlErr = syscall.EBADF
			return
		}
		tcpInfo, ctrlErr = unix.GetsockoptTCPConnectionInfo(int(fd), unix.IPPROTO_TCP, unix.TCP_CONNECTION_INFO)
	})
	if ctrlErr != nil || tcpInfo == nil {
		return nil
	}
	return &Stats{
		BytesSent:    uint64(tcpInfo.Txbytes),
		BytesRetrans: uint64(tcpInfo.Txretransmitbytes),
		SegsOut:      uint64(tcpInfo.Txpackets),
		RetransSegs:  uint64(tcpInfo.Txretransmitpackets),
	}
}
