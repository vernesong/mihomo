package tcpstats

import (
	"net"
	"reflect"
	"syscall"
)

func getTCPStats(conn net.Conn) *Stats {
	seen := make(map[uintptr]bool, 12)
outer:
	for depth := 0; depth < 32; depth++ {
		if rv := reflect.ValueOf(conn); rv.Kind() == reflect.Ptr {
			ptr := rv.Pointer()
			if seen[ptr] {
				return nil
			}
			seen[ptr] = true
		}

		if sc, ok := conn.(interface {
			SyscallConn() (syscall.RawConn, error)
		}); ok {
			rawConn, err := sc.SyscallConn()
			if err != nil {
				return nil
			}
			return readTCPStats(rawConn)
		}
		if u, ok := conn.(interface{ Upstream() any }); ok {
			if next, ok2 := u.Upstream().(net.Conn); ok2 {
				conn = next
				continue outer
			}
			return nil
		}
		if nc, ok := conn.(interface{ NetConn() net.Conn }); ok {
			conn = nc.NetConn()
			continue outer
		}
		{
			v := reflect.ValueOf(conn)
			if v.Kind() == reflect.Ptr {
				v = v.Elem()
			}
			if v.Kind() == reflect.Struct {
				t := v.Type()
				for i := 0; i < v.NumField(); i++ {
					if !t.Field(i).IsExported() {
						continue
					}
					f := v.Field(i)
					if f.Kind() == reflect.Interface {
						if inner, ok := f.Interface().(net.Conn); ok {
							conn = inner
							continue outer
						}
					}
				}
			}
		}
		return nil
	}
	return nil
}
