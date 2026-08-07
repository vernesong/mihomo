package sing

import (
	"context"
	"net"

	E "github.com/metacubex/sing/common/exceptions"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

type FnHandler struct {
	NewConnectionFn       func(ctx context.Context, conn net.Conn, metadata M.Metadata) error
	NewPacketConnectionFn func(ctx context.Context, conn N.PacketConn, metadata M.Metadata) error
	NewErrorFn            func(ctx context.Context, err error)
}

func (h FnHandler) NewConnection(ctx context.Context, conn net.Conn, metadata M.Metadata) error {
	if h.NewConnectionFn == nil {
		return nil
	}
	return h.NewConnectionFn(ctx, conn, metadata)
}

func (h FnHandler) NewPacketConnection(ctx context.Context, conn N.PacketConn, metadata M.Metadata) error {
	if h.NewPacketConnectionFn == nil {
		return nil
	}
	return h.NewPacketConnectionFn(ctx, conn, metadata)
}

func (h FnHandler) NewError(ctx context.Context, err error) {
	if h.NewErrorFn == nil {
		return
	}
	h.NewErrorFn(ctx, err)
}

var _ N.TCPConnectionHandler = FnHandler{}
var _ N.UDPConnectionHandler = FnHandler{}
var _ E.Handler = FnHandler{}
