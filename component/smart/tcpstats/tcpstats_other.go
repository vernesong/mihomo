//go:build !linux && !darwin && !freebsd && !windows

package tcpstats

import "syscall"

func readTCPStats(rawConn syscall.RawConn) *Stats {
	return nil
}
