package tlsmirror

import (
	"context"
	"net"
)

func ServeConnReady(ctx context.Context, carrierConn net.Conn, forwardConn net.Conn, cfg ServerConfig) (net.Conn, error) {
	ready := make(chan *Conn, 1)
	mirror, err := serveConn(ctx, carrierConn, forwardConn, cfg, func(hidden *Conn) {
		select {
		case ready <- hidden:
		default:
		}
	})
	if err != nil {
		return nil, err
	}
	select {
	case hidden := <-ready:
		return hidden, nil
	case <-mirror.ctx.Done():
		return nil, mirror.ctx.Err()
	case <-ctx.Done():
		_ = mirror.Close()
		return nil, ctx.Err()
	}
}

func serveConn(ctx context.Context, carrierConn net.Conn, forwardConn net.Conn, cfg ServerConfig, onReady func(*Conn)) (*mirrorConn, error) {
	key, err := DecodePrimaryKey(cfg.PrimaryKey)
	if err != nil {
		_ = carrierConn.Close()
		_ = forwardConn.Close()
		return nil, err
	}

	var hidden *Conn
	var activated bool
	var enrolled bool
	mirror := newMirrorConn(ctx, carrierConn, forwardConn,
		cfg,
		func(rec *record) (bool, error) {
			drop, err := hidden.handleInboundRecord(rec)
			if drop {
				if !activated {
					activated = true
					onReady(hidden)
				}
			}
			return drop, err
		},
		func(rec *record) (bool, error) {
			if cfg.ConnectionEnrolment != nil && rec.recordType == recordTypeHandshake && !enrolled {
				clientRandom, serverRandom, randomErr := hidden.mirror.handshakeRandom()
				if randomErr != nil {
					return false, nil
				}
				remove, addErr := enrollmentProcessorFor(key).add(clientRandom, serverRandom, hidden)
				if addErr != nil {
					return false, nil
				}
				hidden.setEnrollmentRemove(remove)
				enrolled = true
			}
			return false, nil
		},
		nil,
		nil,
	)
	hidden, err = newHiddenConn(ctx, mirror, key, true, cfg)
	if err != nil {
		_ = carrierConn.Close()
		_ = forwardConn.Close()
		return nil, err
	}
	mirror.onClose = hidden.removeEnrollment
	mirror.onS2CMessageTx = hidden.handleOutboundRecordTx
	mirror.start()
	return mirror, nil
}
