//go:build !(linux || darwin)

package syscall

import "syscall"

func GetUnsentBytes(c syscall.RawConn) (int, error) {
	panic("not implemented")
}
