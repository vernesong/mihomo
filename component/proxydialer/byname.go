package proxydialer

import (
	"context"
	"fmt"
	"net"
	"net/netip"

	C "github.com/metacubex/mihomo/constant"
)

type Tunnel interface {
	C.Tunnel
	Proxies() map[string]C.Proxy
}

type byNameProxyDialer struct {
	proxyName string
	tunnel    C.Tunnel
}

func (d byNameProxyDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	tunnel, _ := d.tunnel.(Tunnel)
	if tunnel == nil {
		return nil, fmt.Errorf("tunnel is invalid, must be proxydialer.Tunnel, but got: %T", d.tunnel)
	}
	proxies := tunnel.Proxies()
	proxy, ok := proxies[d.proxyName]
	if !ok {
		return nil, fmt.Errorf("proxyName[%s] not found", d.proxyName)
	}
	return New(proxy, true).DialContext(ctx, network, address)
}

func (d byNameProxyDialer) ListenPacket(ctx context.Context, network, address string, rAddrPort netip.AddrPort) (net.PacketConn, error) {
	tunnel, _ := d.tunnel.(Tunnel)
	if tunnel == nil {
		return nil, fmt.Errorf("tunnel is invalid, must be proxydialer.Tunnel, but got: %T", d.tunnel)
	}
	proxies := tunnel.Proxies()
	proxy, ok := proxies[d.proxyName]
	if !ok {
		return nil, fmt.Errorf("proxyName[%s] not found", d.proxyName)
	}
	return New(proxy, true).ListenPacket(ctx, network, address, rAddrPort)
}

func NewByName(proxyName string, tunnel C.Tunnel) C.Dialer {
	return byNameProxyDialer{proxyName: proxyName, tunnel: tunnel}
}
