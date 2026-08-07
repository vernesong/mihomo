package inbound_test

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/listener/inbound"

	"github.com/stretchr/testify/require"
)

func TestInboundVMess_MKCP_V2RayInterop(t *testing.T) {
	vmessInteropSkip(t)

	v2rayBin := vmessInteropV2RayBinary(t)
	mkcpInteropTestCase(t, v2rayBin, "default", "", "")
	mkcpInteropTestCase(t, v2rayBin, "seed", "mihomo-mkcp-interop", "")
	mkcpInteropTestCase(t, v2rayBin, "header srtp", "", "srtp")
}

func mkcpInteropTestCase(t *testing.T, v2rayBin, name, seed, header string) {
	t.Run(name+"/mihomo client to v2ray server", func(t *testing.T) {
		echoAddr := startVMessInteropEcho(t)
		v2rayPort := vmessInteropReserveUDPPort(t)
		config := mkcpInteropServerConfig(t, v2rayPort.Port(), userUUID, seed, header)

		startVMessInteropV2Ray(t, v2rayBin, config, v2rayPort.Release, "")

		out, err := outbound.NewVmess(outbound.VmessOption{
			Name:     "vmess_mkcp_v2ray_server",
			Server:   "127.0.0.1",
			Port:     v2rayPort.Port(),
			UUID:     userUUID,
			Cipher:   "auto",
			Network:  "mkcp",
			MKCPOpts: outbound.MKCPOptions{Seed: seed, Header: header},
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = out.Close() })

		vmessInteropRoundTripWithRetry(t, func() (net.Conn, error) {
			return out.DialContext(context.Background(), vmessInteropMetadata(t, echoAddr))
		}, 128*1024)
	})

	t.Run(name+"/v2ray client to mihomo server", func(t *testing.T) {
		echoAddr := startVMessInteropEcho(t)
		v2rayPort := vmessInteropReserveTCPPort(t)

		in, err := inbound.NewVmess(&inbound.VmessOption{
			BaseOption: inbound.BaseOption{
				NameStr: "vmess_mkcp_v2ray_client",
				Listen:  "127.0.0.1",
				Port:    "0",
			},
			Users: []inbound.VmessUser{
				{Username: "test", UUID: userUUID},
			},
			MKCPConfig: inbound.MKCPConfig{
				Enable: true,
				Seed:   seed,
				Header: header,
			},
		})
		require.NoError(t, err)

		tunnel := vmessInteropDirectTunnel(t)
		require.NoError(t, in.Listen(tunnel))
		t.Cleanup(func() { _ = in.Close() })
		inboundPort := vmessInteropParsePort(t, vmessInteropPort(in.Address()))

		config := mkcpInteropClientConfig(t, v2rayPort.Port(), inboundPort, vmessInteropPort(echoAddr), userUUID, seed, header)
		startVMessInteropV2Ray(t, v2rayBin, config, v2rayPort.Release, net.JoinHostPort("127.0.0.1", fmt.Sprint(v2rayPort.Port())))

		vmessInteropRoundTripWithRetry(t, func() (net.Conn, error) {
			return net.Dial("tcp", net.JoinHostPort("127.0.0.1", fmt.Sprint(v2rayPort.Port())))
		}, 128*1024)
	})
}

func mkcpInteropServerConfig(t *testing.T, listenPort int, userID, seed, header string) []byte {
	t.Helper()
	config := vmessInteropBaseConfig()
	config["inbounds"] = []any{map[string]any{
		"protocol": "vmess",
		"listen":   "127.0.0.1",
		"port":     listenPort,
		"settings": map[string]any{
			"users": []string{userID},
		},
		"streamSettings": mkcpInteropStreamConfig(seed, header),
	}}
	config["outbounds"] = []any{vmessInteropDirectOutbound()}
	return vmessInteropMarshalJSONConfig(t, config)
}

func mkcpInteropClientConfig(t *testing.T, listenPort, serverPort int, targetPort string, userID, seed, header string) []byte {
	t.Helper()
	targetPortValue := vmessInteropParsePort(t, targetPort)
	config := vmessInteropBaseConfig()
	config["inbounds"] = []any{map[string]any{
		"protocol": "dokodemo-door",
		"listen":   "127.0.0.1",
		"port":     listenPort,
		"settings": map[string]any{
			"address":  "127.0.0.1",
			"port":     targetPortValue,
			"networks": "tcp",
		},
	}}
	config["outbounds"] = []any{
		map[string]any{
			"protocol":       "vmess",
			"streamSettings": mkcpInteropStreamConfig(seed, header),
			"settings": map[string]any{
				"address": "127.0.0.1",
				"port":    serverPort,
				"uuid":    userID,
			},
		},
	}
	return vmessInteropMarshalJSONConfig(t, config)
}

func mkcpInteropStreamConfig(seed, header string) map[string]any {
	return map[string]any{
		"transport":         "kcp",
		"transportSettings": mkcpInteropTransportSettings(seed, header),
	}
}

func mkcpInteropTransportSettings(seed, header string) map[string]any {
	settings := map[string]any{}
	if seed == "" {
		if header == "" {
			return settings
		}
	} else {
		settings["seed"] = map[string]any{
			"seed": seed,
		}
	}
	if header != "" {
		settings["headerConfig"] = mkcpInteropHeaderConfig(header)
	}
	return settings
}

func mkcpInteropHeaderConfig(header string) map[string]any {
	typeName := map[string]string{
		"srtp":         "v2ray.core.transport.internet.headers.srtp.Config",
		"utp":          "v2ray.core.transport.internet.headers.utp.Config",
		"wechat-video": "v2ray.core.transport.internet.headers.wechat.VideoConfig",
		"dtls":         "v2ray.core.transport.internet.headers.tls.PacketConfig",
		"wireguard":    "v2ray.core.transport.internet.headers.wireguard.WireguardConfig",
	}[header]
	return map[string]any{
		"@type": "types.v2fly.org/" + typeName,
	}
}
