//go:build !linux

package inbound

import (
	"syscall"
)

func bindMarkToControl(mark int) controlFn {
	return func(network, address string, c syscall.RawConn) (err error) {
		return
	}
}
