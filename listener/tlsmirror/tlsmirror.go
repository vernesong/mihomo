package tlsmirror

import (
	"context"
	"net"
	"runtime/debug"
	"strings"

	N "github.com/metacubex/mihomo/common/net"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/listener/inner"
	"github.com/metacubex/mihomo/log"
	"github.com/metacubex/mihomo/transport/tlsmirror"
)

type Config struct {
	PrimaryKey                    string
	Dest                          string
	Proxy                         string
	ExplicitNonceCipherSuites     []uint16
	DeferInstanceDerivedWriteTime tlsmirror.TimeSpec
	TransportLayerPadding         tlsmirror.TransportLayerPadding
	ConnectionEnrolment           *tlsmirror.ConnectionEnrolment
	SequenceWatermarkingEnabled   bool
}

func (c Config) Build(tunnel C.Tunnel) *Builder {
	return &Builder{
		config: c,
		tunnel: tunnel,
	}
}

func (b Builder) WrapTunnel(tunnel C.Tunnel) C.Tunnel {
	if b.config.ConnectionEnrolment == nil {
		return tunnel
	}
	key, err := tlsmirror.DecodePrimaryKey(b.config.PrimaryKey)
	if err != nil {
		return tunnel
	}
	controlHost, err := tlsmirror.ServerIdentifierHost(key)
	if err != nil {
		return tunnel
	}
	return &enrollmentTunnel{
		Tunnel:      tunnel,
		primaryKey:  b.config.PrimaryKey,
		controlHost: controlHost,
	}
}

type Builder struct {
	config Config
	tunnel C.Tunnel
}

func (b Builder) NewListener(l net.Listener) net.Listener {
	return N.NewHandleContextListener(context.Background(), l, func(ctx context.Context, conn net.Conn) (net.Conn, error) {
		forwardConn, err := inner.HandleTcp(b.tunnel, b.config.Dest, b.config.Proxy)
		if err != nil {
			return nil, err
		}
		hiddenConn, err := tlsmirror.ServeConnReady(ctx, conn, forwardConn, tlsmirror.ServerConfig{
			PrimaryKey:                  b.config.PrimaryKey,
			ExplicitNonceCipherSuites:   b.config.ExplicitNonceCipherSuites,
			DeferInstanceDerivedWrite:   b.config.DeferInstanceDerivedWriteTime,
			TransportLayerPadding:       b.config.TransportLayerPadding,
			ConnectionEnrolment:         b.config.ConnectionEnrolment,
			SequenceWatermarkingEnabled: b.config.SequenceWatermarkingEnabled,
		})
		if err != nil {
			_ = forwardConn.Close()
			return nil, err
		}
		return hiddenConn, nil
	}, func(a any) {
		stack := debug.Stack()
		log.Errorln("tlsmirror server panic: %s\n%s", a, stack)
	})
}

type enrollmentTunnel struct {
	C.Tunnel
	primaryKey  string
	controlHost string
}

func (t *enrollmentTunnel) HandleTCPConn(conn net.Conn, metadata *C.Metadata) {
	if metadata.NetWork == C.TCP && metadata.DstPort == 80 && strings.EqualFold(metadata.Host, t.controlHost) {
		if err := tlsmirror.ServeEnrollmentControlConnection(context.Background(), conn, t.primaryKey); err != nil {
			log.Warnln("tlsmirror enrollment control connection failed: %s", err)
			_ = conn.Close()
		}
		return
	}
	t.Tunnel.HandleTCPConn(conn, metadata)
}
