package callback

import (
	"sync"

	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/sing/common/network"
)

type closeCallbackConn struct {
	C.Conn
	closeFunc func()
	closeOnce sync.Once
}

func (w *closeCallbackConn) Close() error {
	w.closeOnce.Do(w.closeFunc)
	return w.Conn.Close()
}

func (w *closeCallbackConn) CloseWrite() error {
	w.closeOnce.Do(w.closeFunc)
	if wc, ok := w.Conn.(network.WriteCloser); ok {
		return wc.CloseWrite()
	}
	return w.Conn.Close()
}

func (w *closeCallbackConn) ReaderReplaceable() bool {
	return true
}

func (w *closeCallbackConn) WriterReplaceable() bool {
	return true
}

func (w *closeCallbackConn) Upstream() any {
	return w.Conn
}

func NewCloseCallbackConn(conn C.Conn, callback func()) C.Conn {
	return &closeCallbackConn{Conn: conn, closeFunc: callback}
}

type closeCallbackPacketConn struct {
	C.PacketConn
	closeFunc func()
	closeOnce sync.Once
}

func (w *closeCallbackPacketConn) Close() error {
	w.closeOnce.Do(w.closeFunc)
	return w.PacketConn.Close()
}

func (w *closeCallbackPacketConn) CloseWrite() error {
	go w.closeOnce.Do(func() {
		w.closeFunc()
	})
	if wc, ok := w.PacketConn.(network.WriteCloser); ok {
		return wc.CloseWrite()
	}
	return w.PacketConn.Close()
}

func (w *closeCallbackPacketConn) ReaderReplaceable() bool {
	return true
}

func (w *closeCallbackPacketConn) WriterReplaceable() bool {
	return true
}

func (w *closeCallbackPacketConn) Upstream() any {
	return w.PacketConn
}

func NewCloseCallbackPacketConn(conn C.PacketConn, callback func()) C.PacketConn {
	return &closeCallbackPacketConn{PacketConn: conn, closeFunc: callback}
}