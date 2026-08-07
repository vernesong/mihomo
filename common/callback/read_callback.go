package callback

import (
	"net"
	"sync/atomic"
	"time"

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

type firstReadCallBackPacketConn struct {
	C.PacketConn
	callback   func(latency int64)
	firstWrite atomic.Int64
	called     atomic.Bool
}

func (c *firstReadCallBackPacketConn) WriteTo(b []byte, addr net.Addr) (n int, err error) {
	n, err = c.PacketConn.WriteTo(b, addr)
	if err == nil {
		c.firstWrite.CompareAndSwap(0, time.Now().UnixNano())
	}
	return
}

func (c *firstReadCallBackPacketConn) onRead() {
	if first := c.firstWrite.Load(); first != 0 {
		if c.called.CompareAndSwap(false, true) {
			latency := (time.Now().UnixNano() - first) / int64(time.Millisecond)
			c.callback(latency)
		}
	}
}

func (c *firstReadCallBackPacketConn) ReadFrom(b []byte) (n int, addr net.Addr, err error) {
	n, addr, err = c.PacketConn.ReadFrom(b)
	if err == nil {
		c.onRead()
	}
	return
}

func (c *firstReadCallBackPacketConn) WaitReadFrom() (data []byte, put func(), addr net.Addr, err error) {
	data, put, addr, err = c.PacketConn.WaitReadFrom()
	if err == nil {
		c.onRead()
	}
	return
}

func (c *firstReadCallBackPacketConn) Upstream() any {
	return c.PacketConn
}

func (c *firstReadCallBackPacketConn) ReaderReplaceable() bool {
	return c.called.Load()
}

func (c *firstReadCallBackPacketConn) WriterReplaceable() bool {
	return c.firstWrite.Load() != 0
}

var _ C.PacketConn = (*firstReadCallBackPacketConn)(nil)

func NewFirstReadCallBackPacketConn(pc C.PacketConn, callback func(latency int64)) C.PacketConn {
	return &firstReadCallBackPacketConn{
		PacketConn: pc,
		callback:   callback,
	}
}
