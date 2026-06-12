package inbound

import (
	"syscall"
)

func bindMarkToControl(mark int) controlFn {
	return func(network, address string, c syscall.RawConn) (err error) {
		var innerErr error
		err = c.Control(func(fd uintptr) {
			innerErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_MARK, mark)
		})
		if innerErr != nil {
			err = innerErr
		}
		return
	}
}
