package tcp

import (
	"context"
	"net"
	"runtime"

	"github.com/metacubex/mihomo/component/syscall"
)

func WaitAllAcks(ctx context.Context, tc *net.TCPConn) error {
	if rc, err := tc.SyscallConn(); err == nil {
		for {
			n, err := syscall.GetUnsentBytes(rc)
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				if err != nil {
					return err
				}
				if n == 0 {
					return nil
				}
				runtime.Gosched()
			}
		}
	}
	return nil
}
