package constant

import (
	"fmt"
	"strings"
)

// SniffProtocol is an application protocol identified from connection data.
// It is intentionally separate from NetWork, which only describes TCP or UDP.
type SniffProtocol string

const (
	SniffProtocolUnknown    SniffProtocol = ""
	SniffProtocolHTTP       SniffProtocol = "HTTP"
	SniffProtocolTLS        SniffProtocol = "TLS"
	SniffProtocolQUIC       SniffProtocol = "QUIC"
	SniffProtocolSTUN       SniffProtocol = "STUN"
	SniffProtocolBitTorrent SniffProtocol = "BITTORRENT"
)

var SniffProtocols = [...]SniffProtocol{
	SniffProtocolHTTP,
	SniffProtocolTLS,
	SniffProtocolQUIC,
	SniffProtocolSTUN,
	SniffProtocolBitTorrent,
}

func ParseSniffProtocol(protocol string) (SniffProtocol, error) {
	protocol = strings.ToUpper(strings.TrimSpace(protocol))
	for _, candidate := range SniffProtocols {
		if protocol == string(candidate) {
			return candidate, nil
		}
	}
	return SniffProtocolUnknown, fmt.Errorf("unsupported protocol: %s", protocol)
}

// ProtocolRequirement is implemented by rules and rule-provider strategies
// that need one or more application protocol detectors.
type ProtocolRequirement interface {
	RequiredProtocols() []SniffProtocol
}
