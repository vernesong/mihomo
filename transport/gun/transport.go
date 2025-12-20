package gun

import (
	"context"
	"net"
	"sync"

	"github.com/metacubex/http"
)

type TransportWrap struct {
	*http.Http2Transport
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
}

func (tw *TransportWrap) Close() error {
	tw.closeOnce.Do(func() {
		tw.cancel()
		closeTransport(tw.Http2Transport)
	})
	return nil
}

type netAddr struct {
	remoteAddr net.Addr
	localAddr  net.Addr
}

func (addr netAddr) RemoteAddr() net.Addr {
	return addr.remoteAddr
}

func (addr netAddr) LocalAddr() net.Addr {
	return addr.localAddr
}
