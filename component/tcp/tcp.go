//go:build !windows

package tcp

import (
	"context"
	"net"
	"time"

	U "github.com/metacubex/mihomo/component/sys/unix"
	"github.com/metacubex/randv2"
	"golang.org/x/sys/unix"
)

func WaitAllAcks(ctx context.Context, tc *net.TCPConn) error {
	if rc, err := tc.SyscallConn(); err == nil {
		intv := time.Duration((1.0 + randv2.Float32()) * float32(time.Millisecond))
		var n int
		var cerr error
		err = rc.Control(func(fd uintptr) {
			for {
				select {
				case <-ctx.Done():
					cerr = ctx.Err()
					return
				default:
					if n, cerr = unix.IoctlGetInt(int(fd), U.SO_NWRITE()); cerr != nil || n == 0 {
						return
					}
				}
				time.Sleep(intv)
			}
		})
		if err != nil {
			return err
		}
		return cerr
	}
	return nil
}
