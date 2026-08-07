package inbound_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/listener/inbound"

	"github.com/stretchr/testify/require"
)

func TestInboundVMess_Mekya_V2RayInterop(t *testing.T) {
	vmessInteropSkip(t)

	v2rayBin := vmessInteropV2RayBinary(t)
	t.Run("mihomo client to v2ray server", func(t *testing.T) {
		echoAddr := startVMessInteropEcho(t)
		v2rayPort := vmessInteropReserveTCPPort(t)
		certFile, keyFile := mekyaInteropCertificateFiles(t)
		config := mekyaInteropServerConfig(t, v2rayPort.Port(), userUUID, certFile, keyFile)

		startVMessInteropV2Ray(t, v2rayBin, config, v2rayPort.Release, net.JoinHostPort("127.0.0.1", fmt.Sprint(v2rayPort.Port())))

		out, err := outbound.NewVmess(outbound.VmessOption{
			Name:        "vmess_mekya_v2ray_server",
			Server:      "127.0.0.1",
			Port:        v2rayPort.Port(),
			UUID:        userUUID,
			Cipher:      "auto",
			Network:     "mekya",
			TLS:         true,
			Fingerprint: tlsFingerprint,
			MekyaOpts:   mekyaInteropOutboundOptions(net.JoinHostPort("127.0.0.1", fmt.Sprint(v2rayPort.Port()))),
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = out.Close() })

		vmessInteropRoundTripWithRetry(t, func() (net.Conn, error) {
			return out.DialContext(context.Background(), vmessInteropMetadata(t, echoAddr))
		}, 64*1024)
	})

	t.Run("v2ray client to mihomo server", func(t *testing.T) {
		echoAddr := startVMessInteropEcho(t)
		v2rayPort := vmessInteropReserveTCPPort(t)

		in, err := inbound.NewVmess(&inbound.VmessOption{
			BaseOption: inbound.BaseOption{
				NameStr: "vmess_mekya_v2ray_client",
				Listen:  "127.0.0.1",
				Port:    "0",
			},
			Users: []inbound.VmessUser{
				{Username: "test", UUID: userUUID},
			},
			Certificate: tlsCertificate,
			PrivateKey:  tlsPrivateKey,
			MekyaConfig: mekyaInteropInboundConfig(),
		})
		require.NoError(t, err)

		tunnel := vmessInteropDirectTunnel(t)
		require.NoError(t, in.Listen(tunnel))
		t.Cleanup(func() { _ = in.Close() })
		inboundPort := vmessInteropParsePort(t, vmessInteropPort(in.Address()))

		config := mekyaInteropClientConfig(t, v2rayPort.Port(), inboundPort, vmessInteropPort(echoAddr), userUUID)
		startVMessInteropV2Ray(t, v2rayBin, config, v2rayPort.Release, net.JoinHostPort("127.0.0.1", fmt.Sprint(v2rayPort.Port())))

		vmessInteropRoundTripWithRetry(t, func() (net.Conn, error) {
			return net.Dial("tcp", net.JoinHostPort("127.0.0.1", fmt.Sprint(v2rayPort.Port())))
		}, 64*1024)
	})
}

func mekyaInteropCertificateFiles(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")
	require.NoError(t, os.WriteFile(certFile, []byte(tlsCertificate), 0o600))
	require.NoError(t, os.WriteFile(keyFile, []byte(tlsPrivateKey), 0o600))
	return certFile, keyFile
}

func mekyaInteropInboundConfig() inbound.MekyaConfig {
	return inbound.MekyaConfig{
		Enable:                         true,
		MaxWriteSize:                   10 * 1024 * 1024,
		MaxWriteDurationMs:             500,
		MaxSimultaneousWriteConnection: 128,
		PacketWritingBuffer:            65536,
		KCP: inbound.MKCPConfig{
			MTU:              1350,
			TTI:              15,
			UplinkCapacity:   40,
			DownlinkCapacity: 2000,
			WriteBuffer:      64 * 1024 * 1024,
			ReadBuffer:       64 * 1024 * 1024,
		},
	}
}

func mekyaInteropOutboundOptions(addr string) outbound.MekyaOptions {
	return outbound.MekyaOptions{
		URL:                    "https://" + addr + "/mekya",
		MaxWriteDelay:          80,
		MaxRequestSize:         96000,
		PollingIntervalInitial: 200,
		H2PoolSize:             8,
		KCP: outbound.MKCPOptions{
			MTU:              1350,
			TTI:              15,
			UplinkCapacity:   40,
			DownlinkCapacity: 2000,
			WriteBuffer:      64 * 1024 * 1024,
			ReadBuffer:       64 * 1024 * 1024,
		},
	}
}

func mekyaInteropServerConfig(t *testing.T, listenPort int, userID, certFile, keyFile string) []byte {
	t.Helper()
	config := vmessInteropBaseConfig()
	config["inbounds"] = []any{map[string]any{
		"protocol": "vmess",
		"listen":   "127.0.0.1",
		"port":     listenPort,
		"settings": map[string]any{
			"users": []string{userID},
		},
		"streamSettings": mekyaInteropStreamConfig(
			"http://127.0.0.1:"+fmt.Sprint(listenPort),
			mekyaInteropServerSecuritySettings(certFile, keyFile),
		),
	}}
	config["outbounds"] = []any{vmessInteropDirectOutbound()}
	return vmessInteropMarshalJSONConfig(t, config)
}

func mekyaInteropClientConfig(t *testing.T, listenPort, serverPort int, targetPort string, userID string) []byte {
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
			"protocol": "vmess",
			"streamSettings": mekyaInteropStreamConfig(
				"https://127.0.0.1:"+fmt.Sprint(serverPort)+"/mekya",
				mekyaInteropClientSecuritySettings(),
			),
			"settings": map[string]any{
				"address": "127.0.0.1",
				"port":    serverPort,
				"uuid":    userID,
			},
		},
	}
	return vmessInteropMarshalJSONConfig(t, config)
}

func mekyaInteropStreamConfig(url string, securitySettings map[string]any) map[string]any {
	return map[string]any{
		"transport":         "mekya",
		"transportSettings": mekyaInteropTransportSettings(url),
		"security":          "tls",
		"securitySettings":  securitySettings,
	}
}

func mekyaInteropTransportSettings(url string) map[string]any {
	return map[string]any{
		"url":                            url,
		"maxWriteDelay":                  80,
		"maxRequestSize":                 96000,
		"pollingIntervalInitial":         200,
		"h2_pool_size":                   8,
		"maxWriteSize":                   10 * 1024 * 1024,
		"maxWriteDurationMs":             500,
		"maxSimultaneousWriteConnection": 128,
		"packetWritingBuffer":            65536,
		"kcp": map[string]any{
			"mtu":               map[string]any{"value": 1350},
			"tti":               map[string]any{"value": 15},
			"uplink_capacity":   map[string]any{"value": 40},
			"downlink_capacity": map[string]any{"value": 2000},
			"congestion":        false,
			"write_buffer":      map[string]any{"size": 64 * 1024 * 1024},
			"read_buffer":       map[string]any{"size": 64 * 1024 * 1024},
		},
	}
}

func mekyaInteropServerSecuritySettings(certFile, keyFile string) map[string]any {
	return map[string]any{
		"certificate": []any{map[string]any{
			"usage":           "ENCIPHERMENT",
			"certificateFile": certFile,
			"keyFile":         keyFile,
		}},
	}
}

func mekyaInteropClientSecuritySettings() map[string]any {
	return map[string]any{
		"pinnedPeerCertificateChainSha256":     []string{vmessInteropCertChainHash([]byte(tlsCertificate))},
		"allowInsecureIfPinnedPeerCertificate": true,
	}
}
