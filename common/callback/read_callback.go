package callback

import (
	"sync/atomic"

	"github.com/metacubex/mihomo/common/buf"
	N "github.com/metacubex/mihomo/common/net"
	C "github.com/metacubex/mihomo/constant"
)

type firstReadCallBackConn struct {
	C.Conn
	callback func(error)
	read     atomic.Bool
}

func (c *firstReadCallBackConn) Read(b []byte) (n int, err error) {
	defer func() {
		if c.read.CompareAndSwap(false, true) {
			c.callback(err)
		}
	}()
	return c.Conn.Read(b)
}

func (c *firstReadCallBackConn) ReadBuffer(buffer *buf.Buffer) (err error) {
	defer func() {
		if c.read.CompareAndSwap(false, true) {
			c.callback(err)
		}
	}()
	return c.Conn.ReadBuffer(buffer)
}

func (c *firstReadCallBackConn) Upstream() any {
	return c.Conn
}

func (c *firstReadCallBackConn) WriterReplaceable() bool {
	return true
}

func (c *firstReadCallBackConn) ReaderReplaceable() bool {
	return c.read.Load()
}

var _ N.ExtendedConn = (*firstReadCallBackConn)(nil)

func NewFirstReadCallBackConn(c C.Conn, callback func(error)) C.Conn {
	return &firstReadCallBackConn{
		Conn:     c,
		callback: callback,
	}
}
