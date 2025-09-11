package syscall

import (
	"syscall"

	"golang.org/x/sys/unix"
)

func GetUnsentBytes(c syscall.RawConn) (int, error) {
	var n int
	var ce error
	var ge error
	ce = c.Control(func(fd uintptr) {
		n, ge = unix.IoctlGetInt(int(fd), unix.SIOCOUTQ)
	})
	if ce != nil {
		return 0, ce
	}
	return n, ge
}
