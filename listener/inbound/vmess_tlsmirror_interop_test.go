package inbound_test

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/component/ca"
	"github.com/metacubex/mihomo/listener/inbound"
	"github.com/metacubex/mihomo/transport/tlsmirror"

	"github.com/metacubex/http"
	"github.com/metacubex/tls"
	"github.com/stretchr/testify/require"
)

var tlsMirrorInteropPrimaryKey = tlsmirror.GeneratePrimaryKey()

func TestInboundVMess_TLSMirror_V2RayInterop(t *testing.T) {
	vmessInteropSkip(t)

	v2rayBin := vmessInteropV2RayBinary(t)

	tlsMirrorInteropTestCase(t, v2rayBin, "default", tlsMirrorInteropAdvanced{})
	tlsMirrorInteropTestCase(t, v2rayBin, "padding", tlsMirrorInteropAdvanced{
		config: tlsmirror.Config{
			TransportLayerPadding: tlsmirror.TransportLayerPadding{Enabled: true},
		},
		payloadSize: 128,
	})
	tlsMirrorInteropTestCase(t, v2rayBin, "watermark", tlsMirrorInteropAdvanced{
		config: tlsmirror.Config{
			SequenceWatermarkingEnabled: true,
		},
		payloadSize: 128,
	})
	tlsMirrorInteropTestCase(t, v2rayBin, "tls12 explicit nonce", tlsMirrorInteropAdvanced{
		config: tlsmirror.Config{
			ExplicitNonceCipherSuites: tlsmirror.RecommendedExplicitNonceCipherSuites,
		},
		configureCarrierTLS: func(config *tls.Config) {
			config.MinVersion = tls.VersionTLS12
			config.MaxVersion = tls.VersionTLS12
			config.CipherSuites = []uint16{tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256}
		},
		tls12:       true,
		payloadSize: 128,
	})
	tlsMirrorInteropTestCase(t, v2rayBin, "advanced tls12 padding watermark", tlsMirrorInteropAdvanced{
		config: tlsmirror.Config{
			ExplicitNonceCipherSuites:   tlsmirror.RecommendedExplicitNonceCipherSuites,
			TransportLayerPadding:       tlsmirror.TransportLayerPadding{Enabled: true},
			SequenceWatermarkingEnabled: true,
		},
		configureCarrierTLS: func(config *tls.Config) {
			config.MinVersion = tls.VersionTLS12
			config.MaxVersion = tls.VersionTLS12
			config.CipherSuites = []uint16{tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256}
		},
		tls12:       true,
		payloadSize: 128,
	})
	tlsMirrorInteropTestCase(t, v2rayBin, "connection enrolment", tlsMirrorInteropAdvanced{
		config: tlsmirror.Config{
			ConnectionEnrolment: &tlsmirror.ConnectionEnrolment{
				PrimaryIngressOutbound: "tlsmirror-enrollment",
			},
		},
		payloadSize: 128,
	})
	tlsMirrorInteropMihomoClientH2EmbeddedTrafficGenerator(t, v2rayBin)
}

type tlsMirrorInteropAdvanced struct {
	config              tlsmirror.Config
	configureCarrierTLS func(*tls.Config)
	tls12               bool
	payloadSize         int
}

type tlsMirrorInteropCarrier struct {
	addr          string
	fingerprint   string
	certChainHash string
}

func tlsMirrorInteropMihomoClientH2EmbeddedTrafficGenerator(t *testing.T, v2rayBin string) {
	t.Run("h2 embedded traffic/mihomo client to v2ray server", func(t *testing.T) {
		echoAddr := startVMessInteropEcho(t)
		forward := startTLSMirrorInteropCarrierHTTP2(t)
		v2rayPort := vmessInteropReserveTCPPort(t)
		config := tlsMirrorInteropServerConfig(t, v2rayPort.Port(), vmessInteropPort(forward.addr), userUUID, tlsMirrorInteropAdvanced{})

		startVMessInteropV2Ray(t, v2rayBin, config, v2rayPort.Release, net.JoinHostPort("127.0.0.1", fmt.Sprint(v2rayPort.Port())))

		out, err := outbound.NewVmess(outbound.VmessOption{
			Name:        "vmess_tlsmirror_v2ray_server_h2",
			Server:      "127.0.0.1",
			Port:        v2rayPort.Port(),
			UUID:        userUUID,
			Cipher:      "auto",
			TLS:         true,
			ALPN:        []string{"h2"},
			ServerName:  "localhost",
			Fingerprint: forward.fingerprint,
			TLSMirrorOpts: outbound.TLSMirrorOptions{
				PrimaryKey: tlsMirrorInteropPrimaryKey,
				EmbeddedTrafficGenerator: outbound.TLSMirrorTrafficGenerator{Steps: []outbound.TLSMirrorTrafficStep{{
					Host:                         "localhost",
					Path:                         "/",
					Method:                       "GET",
					ConnectionReady:              true,
					ConnectionRecallExit:         true,
					H2DoNotWaitForDownloadFinish: true,
					WaitTime: outbound.TLSMirrorTimeSpec{
						BaseNanoseconds: uint64((10 * time.Millisecond).Nanoseconds()),
					},
					NextStep: []outbound.TLSMirrorTrafficTransferCandidate{{
						Weight:       1,
						GotoLocation: 0,
					}},
				}}},
			},
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = out.Close() })

		conn, err := out.DialContext(context.Background(), vmessInteropMetadata(t, echoAddr))
		require.NoError(t, err)
		require.NoError(t, vmessInteropRoundTripConn(conn, 128))
	})
}

func tlsMirrorInteropTestCase(t *testing.T, v2rayBin, name string, advanced tlsMirrorInteropAdvanced) {
	t.Run(name+"/mihomo client to v2ray server", func(t *testing.T) {
		echoAddr := startVMessInteropEcho(t)
		forward := startTLSMirrorInteropCarrierTLS(t, advanced.configureCarrierTLS)
		v2rayPort := vmessInteropReserveTCPPort(t)
		config := tlsMirrorInteropServerConfig(t, v2rayPort.Port(), vmessInteropPort(forward.addr), userUUID, advanced)

		startVMessInteropV2Ray(t, v2rayBin, config, v2rayPort.Release, net.JoinHostPort("127.0.0.1", fmt.Sprint(v2rayPort.Port())))

		out, err := outbound.NewVmess(outbound.VmessOption{
			Name:        "vmess_tlsmirror_v2ray_server",
			Server:      "127.0.0.1",
			Port:        v2rayPort.Port(),
			UUID:        userUUID,
			Cipher:      "auto",
			TLS:         true,
			ServerName:  "localhost",
			Fingerprint: forward.fingerprint,
			TLSMirrorOpts: outbound.TLSMirrorOptions{
				PrimaryKey:                  tlsMirrorInteropPrimaryKey,
				ExplicitNonceCipherSuites:   advanced.config.ExplicitNonceCipherSuites,
				TransportLayerPadding:       outbound.TLSMirrorTransportLayerPadding{Enabled: advanced.config.TransportLayerPadding.Enabled},
				ConnectionEnrolment:         tlsMirrorInteropOutboundConnectionEnrolment(advanced),
				SequenceWatermarkingEnabled: advanced.config.SequenceWatermarkingEnabled,
			},
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = out.Close() })

		conn, err := out.DialContext(context.Background(), vmessInteropMetadata(t, echoAddr))
		require.NoError(t, err)
		require.NoError(t, vmessInteropRoundTripConn(conn, advanced.payloadSize))
	})

	t.Run(name+"/v2ray client to mihomo server", func(t *testing.T) {
		echoAddr := startVMessInteropEcho(t)
		forward := startTLSMirrorInteropCarrierTLS(t, advanced.configureCarrierTLS)
		v2rayPort := vmessInteropReserveTCPPort(t)

		in, err := inbound.NewVmess(&inbound.VmessOption{
			BaseOption: inbound.BaseOption{
				NameStr: "vmess_tlsmirror_v2ray_client",
				Listen:  "127.0.0.1",
				Port:    "0",
			},
			Users: []inbound.VmessUser{
				{Username: "test", UUID: userUUID},
			},
			TLSMirrorConfig: inbound.TLSMirrorConfig{
				PrimaryKey:                  tlsMirrorInteropPrimaryKey,
				Dest:                        forward.addr,
				ExplicitNonceCipherSuites:   advanced.config.ExplicitNonceCipherSuites,
				TransportLayerPadding:       inbound.TLSMirrorTransportLayerPadding{Enabled: advanced.config.TransportLayerPadding.Enabled},
				ConnectionEnrolment:         tlsMirrorInteropInboundConnectionEnrolment(advanced),
				SequenceWatermarkingEnabled: advanced.config.SequenceWatermarkingEnabled,
			},
		})
		require.NoError(t, err)

		tunnel := vmessInteropDirectTunnel(t)
		require.NoError(t, in.Listen(tunnel))
		t.Cleanup(func() { _ = in.Close() })
		inboundPort := vmessInteropParsePort(t, vmessInteropPort(in.Address()))

		config := tlsMirrorInteropClientConfig(t, v2rayPort.Port(), inboundPort, vmessInteropPort(echoAddr), userUUID, forward.certChainHash, advanced)
		startVMessInteropV2Ray(t, v2rayBin, config, v2rayPort.Release, "")

		vmessInteropRoundTripWithRetry(t, func() (net.Conn, error) {
			return net.Dial("tcp", net.JoinHostPort("127.0.0.1", fmt.Sprint(v2rayPort.Port())))
		}, advanced.payloadSize)
	})
}

func tlsMirrorInteropServerConfig(t *testing.T, listenPort int, forwardPort string, userID string, advanced tlsMirrorInteropAdvanced) []byte {
	t.Helper()
	forwardPortValue := vmessInteropParsePort(t, forwardPort)
	config := vmessInteropBaseConfig()
	config["inbounds"] = []any{map[string]any{
		"protocol": "vmess",
		"listen":   "127.0.0.1",
		"port":     listenPort,
		"settings": map[string]any{
			"users": []string{userID},
		},
		"streamSettings": tlsMirrorInteropStreamConfig(tlsMirrorInteropServerSettings(advanced, forwardPortValue), nil),
	}}
	config["outbounds"] = []any{vmessInteropDirectOutbound()}
	if advanced.config.ConnectionEnrolment != nil {
		config["router"] = map[string]any{
			"rule": []any{map[string]any{
				"tag": advanced.config.ConnectionEnrolment.PrimaryIngressOutbound,
				"domain": []any{map[string]any{
					"type":  "Full",
					"value": tlsMirrorInteropEnrollmentControlHost(t),
				}},
			}},
		}
	}
	return vmessInteropMarshalJSONConfig(t, config)
}

func tlsMirrorInteropClientConfig(t *testing.T, listenPort, serverPort int, targetPort string, userID, carrierCertHash string, advanced tlsMirrorInteropAdvanced) []byte {
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
			"tag":            "vmess-tlsmirror",
			"streamSettings": tlsMirrorInteropStreamConfig(tlsMirrorInteropClientSettings(advanced), tlsMirrorInteropSecuritySettings(advanced, carrierCertHash)),
			"settings": map[string]any{
				"address": "127.0.0.1",
				"port":    serverPort,
				"uuid":    userID,
			},
		},
		vmessInteropDirectOutbound(),
	}
	if advanced.config.ConnectionEnrolment != nil {
		controlAdvanced := advanced
		controlAdvanced.config.ConnectionEnrolment = nil
		config["outbounds"] = append(config["outbounds"].([]any), map[string]any{
			"protocol":       "vmess",
			"tag":            "vmess-tlsmirror-control",
			"streamSettings": tlsMirrorInteropStreamConfig(tlsMirrorInteropClientControlSettings(controlAdvanced), tlsMirrorInteropSecuritySettings(advanced, carrierCertHash)),
			"settings": map[string]any{
				"address": "127.0.0.1",
				"port":    serverPort,
				"uuid":    userID,
			},
		})
	}
	return vmessInteropMarshalJSONConfig(t, config)
}

func tlsMirrorInteropStreamConfig(tlsMirrorSettings map[string]any, securitySettings map[string]any) map[string]any {
	config := map[string]any{
		"transport":         "tlsmirror",
		"transportSettings": tlsMirrorSettings,
	}
	if securitySettings != nil {
		config["security"] = "tls"
		config["securitySettings"] = securitySettings
	}
	return config
}

func tlsMirrorInteropServerSettings(advanced tlsMirrorInteropAdvanced, forwardPort int) map[string]any {
	settings := tlsMirrorInteropTLSMirrorSettings(advanced)
	settings["forwardAddress"] = "127.0.0.1"
	settings["forwardPort"] = forwardPort
	return settings
}

func tlsMirrorInteropClientSettings(advanced tlsMirrorInteropAdvanced) map[string]any {
	settings := tlsMirrorInteropTLSMirrorSettings(advanced)
	settings["carrierConnectionTag"] = "tlsmirror-carrier"
	settings["forwardTag"] = "direct"
	if advanced.config.ConnectionEnrolment != nil {
		settings["connectionEnrolment"].(map[string]any)["primaryEgressOutbound"] = "vmess-tlsmirror-control"
	}
	settings["embeddedTrafficGenerator"] = tlsMirrorInteropEmbeddedTrafficGeneratorSettings()
	return settings
}

func tlsMirrorInteropClientControlSettings(advanced tlsMirrorInteropAdvanced) map[string]any {
	settings := tlsMirrorInteropTLSMirrorSettings(advanced)
	settings["carrierConnectionTag"] = "tlsmirror-carrier-control"
	settings["forwardTag"] = "direct"
	settings["embeddedTrafficGenerator"] = tlsMirrorInteropEmbeddedTrafficGeneratorSettings()
	return settings
}

func tlsMirrorInteropEmbeddedTrafficGeneratorSettings() map[string]any {
	return map[string]any{
		"steps": []any{map[string]any{
			"host":                 "localhost",
			"path":                 "/",
			"method":               "GET",
			"connectionReady":      true,
			"connectionRecallExit": true,
			"waitTime": map[string]any{
				"baseNanoseconds": uint64(time.Second),
			},
			"nextStep": []any{map[string]any{
				"weight":       1,
				"gotoLocation": 0,
			}},
		}},
	}
}

func tlsMirrorInteropSecuritySettings(advanced tlsMirrorInteropAdvanced, carrierCertHash string) map[string]any {
	return map[string]any{
		"allowInsecureIfPinnedPeerCertificate": true,
		"pinnedPeerCertificateChainSha256":     []string{carrierCertHash},
		"serverName":                           "localhost",
		"minVersion":                           tlsMirrorInteropTLSVersion(advanced),
		"maxVersion":                           tlsMirrorInteropTLSVersion(advanced),
	}
}

func tlsMirrorInteropTLSMirrorSettings(advanced tlsMirrorInteropAdvanced) map[string]any {
	settings := map[string]any{
		"primaryKey":                  tlsMirrorInteropPrimaryKey,
		"sequenceWatermarkingEnabled": advanced.config.SequenceWatermarkingEnabled,
	}
	if advanced.config.ConnectionEnrolment != nil {
		settings["connectionEnrolment"] = map[string]any{
			"primaryIngressOutbound": advanced.config.ConnectionEnrolment.PrimaryIngressOutbound,
			"primaryEgressOutbound":  advanced.config.ConnectionEnrolment.PrimaryEgressOutbound,
		}
	}
	if advanced.config.TransportLayerPadding.Enabled {
		settings["transportLayerPadding"] = map[string]any{"enabled": true}
	}
	if advanced.tls12 {
		settings["explicitNonceCiphersuites"] = []uint32{0xc02b}
	}
	return settings
}

func tlsMirrorInteropOutboundConnectionEnrolment(advanced tlsMirrorInteropAdvanced) *outbound.TLSMirrorConnectionEnrolment {
	if advanced.config.ConnectionEnrolment == nil {
		return nil
	}
	return &outbound.TLSMirrorConnectionEnrolment{
		PrimaryIngressOutbound: advanced.config.ConnectionEnrolment.PrimaryIngressOutbound,
		PrimaryEgressOutbound:  advanced.config.ConnectionEnrolment.PrimaryEgressOutbound,
	}
}

func tlsMirrorInteropInboundConnectionEnrolment(advanced tlsMirrorInteropAdvanced) *inbound.TLSMirrorConnectionEnrolment {
	if advanced.config.ConnectionEnrolment == nil {
		return nil
	}
	return &inbound.TLSMirrorConnectionEnrolment{
		PrimaryIngressOutbound: advanced.config.ConnectionEnrolment.PrimaryIngressOutbound,
		PrimaryEgressOutbound:  advanced.config.ConnectionEnrolment.PrimaryEgressOutbound,
	}
}

func tlsMirrorInteropEnrollmentControlHost(t *testing.T) string {
	t.Helper()
	key, err := tlsmirror.DecodePrimaryKey(tlsMirrorInteropPrimaryKey)
	require.NoError(t, err)
	host, err := tlsmirror.ServerIdentifierHost(key)
	require.NoError(t, err)
	return host
}

func tlsMirrorInteropTLSVersion(advanced tlsMirrorInteropAdvanced) string {
	if advanced.tls12 {
		return "TLS1_2"
	}
	return "TLS1_3"
}

func startTLSMirrorInteropCarrierTLS(t *testing.T, configure ...func(*tls.Config)) tlsMirrorInteropCarrier {
	t.Helper()
	certPEM, keyPEM, fingerprint, err := ca.NewRandomTLSKeyPair(ca.KeyPairTypeP256)
	require.NoError(t, err)
	cert, err := tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
	require.NoError(t, err)
	config := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
	}
	for _, configure := range configure {
		if configure != nil {
			configure(config)
		}
	}
	ln, err := tls.Listen("tcp", "127.0.0.1:0", config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				reader := bufio.NewReader(conn)
				for {
					line, err := reader.ReadString('\n')
					if err != nil {
						return
					}
					if line == "\r\n" {
						break
					}
				}
				_, _ = conn.Write([]byte("HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n"))
				_, _ = io.Copy(io.Discard, reader)
			}()
		}
	}()
	return tlsMirrorInteropCarrier{
		addr:          ln.Addr().String(),
		fingerprint:   fingerprint,
		certChainHash: vmessInteropCertChainHash([]byte(certPEM)),
	}
}

func startTLSMirrorInteropCarrierHTTP2(t *testing.T) tlsMirrorInteropCarrier {
	t.Helper()
	certPEM, keyPEM, fingerprint, err := ca.NewRandomTLSKeyPair(ca.KeyPairTypeP256)
	require.NoError(t, err)
	cert, err := tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
	require.NoError(t, err)
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
		NextProtos:   []string{"h2"},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	server := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		}),
		Protocols: new(http.Protocols),
	}
	server.Protocols.SetHTTP2(true)
	server.Protocols.SetUnencryptedHTTP2(true)
	go func() { _ = server.Serve(ln) }()
	t.Cleanup(func() { _ = server.Close() })
	return tlsMirrorInteropCarrier{
		addr:          ln.Addr().String(),
		fingerprint:   fingerprint,
		certChainHash: vmessInteropCertChainHash([]byte(certPEM)),
	}
}
