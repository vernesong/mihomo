package inbound_test

import (
	"net"
	"net/netip"
	"testing"

	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/listener/inbound"
	shadowtls "github.com/metacubex/mihomo/transport/sing-shadowtls"

	"github.com/stretchr/testify/assert"
)

func testInboundSnell(t *testing.T, inboundOptions inbound.SnellOption, outboundOptions outbound.SnellOption) {
	t.Parallel()
	inboundOptions.BaseOption = inbound.BaseOption{
		NameStr: "snell_inbound",
		Listen:  "127.0.0.1",
		Port:    "0",
	}
	inboundOptions.Psk = userUUID
	in, err := inbound.NewSnell(&inboundOptions)
	if !assert.NoError(t, err) {
		return
	}

	tunnel := NewHttpTestTunnel()
	defer tunnel.Close()

	err = in.Listen(tunnel)
	if !assert.NoError(t, err) {
		return
	}
	defer in.Close()

	addrPort, err := netip.ParseAddrPort(in.Address())
	if !assert.NoError(t, err) {
		return
	}

	outboundOptions.Name = "snell_outbound"
	outboundOptions.Server = addrPort.Addr().String()
	outboundOptions.Port = int(addrPort.Port())
	outboundOptions.Psk = userUUID
	outboundOptions.DialerForAPI = tunnel.NewDialer()
	outboundOptions.TunnelForAPI = tunnel

	out, err := outbound.NewSnell(outboundOptions)
	if !assert.NoError(t, err) {
		return
	}
	defer out.Close()

	tunnel.DoTest(t, out)
}

func TestInboundSnell(t *testing.T) {
	testCase := []struct {
		name    string
		version int
	}{
		{"v4", 4},
		{"v5", 5},
	}
	for _, tc := range testCase {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			inboundOptions := inbound.SnellOption{Version: tc.version}
			outboundOptions := outbound.SnellOption{Version: tc.version}
			testInboundSnell(t, inboundOptions, outboundOptions)
		})
	}
}

func testInboundSnellShadowTls(t *testing.T, inboundOptions inbound.SnellOption, outboundOptions outbound.SnellOption) {
	t.Parallel()
	t.Run("Conn", func(t *testing.T) {
		inboundOptions, outboundOptions := inboundOptions, outboundOptions // don't modify outside options value
		testInboundSnell(t, inboundOptions, outboundOptions)
	})
	t.Run("UConn", func(t *testing.T) {
		inboundOptions, outboundOptions := inboundOptions, outboundOptions // don't modify outside options value
		outboundOptions.ClientFingerprint = "chrome"
		testInboundSnell(t, inboundOptions, outboundOptions)
	})
}

func TestInboundSnell_ShadowTlsv2(t *testing.T) {
	inboundOptions := inbound.SnellOption{
		ShadowTLS: inbound.ShadowTLS{
			Enable:    true,
			Version:   2,
			Password:  shadowsocksPassword16,
			Handshake: inbound.ShadowTLSHandshakeOptions{Dest: net.JoinHostPort(realityDest, "443")},
		},
	}
	outboundOptions := outbound.SnellOption{
		Version:  5,
		ObfsOpts: map[string]any{"mode": shadowtls.Mode, "host": realityDest, "password": shadowsocksPassword16, "fingerprint": tlsFingerprint, "version": 2},
	}
	outboundOptions.ObfsOpts["alpn"] = []string{"http/1.1"} // shadowtls v2 work confuse with http/2 server, so we set alpn to http/1.1 to pass the test
	testInboundSnellShadowTls(t, inboundOptions, outboundOptions)
}

func TestInboundSnell_ShadowTlsv3(t *testing.T) {
	inboundOptions := inbound.SnellOption{
		ShadowTLS: inbound.ShadowTLS{
			Enable:    true,
			Version:   3,
			Users:     []inbound.ShadowTLSUser{{Name: "test", Password: shadowsocksPassword16}},
			Handshake: inbound.ShadowTLSHandshakeOptions{Dest: net.JoinHostPort(realityDest, "443")},
		},
	}
	outboundOptions := outbound.SnellOption{
		Version:  5,
		ObfsOpts: map[string]any{"mode": shadowtls.Mode, "host": realityDest, "password": shadowsocksPassword16, "fingerprint": tlsFingerprint, "version": 3},
	}
	testInboundSnellShadowTls(t, inboundOptions, outboundOptions)
}
