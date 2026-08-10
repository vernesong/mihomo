package sniffer

import (
	"bytes"
	"errors"
	"time"

	N "github.com/metacubex/mihomo/common/net"
	C "github.com/metacubex/mihomo/constant"
)

var errProtocolMismatch = errors.New("protocol mismatch")

type protocolDetector struct {
	protocol C.SniffProtocol
	detect   func([]byte) error
}

// ProtocolDispatcher identifies only the protocols referenced by loaded
// PROTOCOL rules. It is immutable after construction and safe for concurrent use.
type ProtocolDispatcher struct {
	tcp []protocolDetector
	udp []protocolDetector
}

func NewProtocolDispatcher(required []C.SniffProtocol) *ProtocolDispatcher {
	enabled := make(map[C.SniffProtocol]struct{}, len(required))
	for _, protocol := range required {
		enabled[protocol] = struct{}{}
	}

	dispatcher := &ProtocolDispatcher{}
	add := func(protocol C.SniffProtocol, network C.NetWork, detect func([]byte) error) {
		if _, ok := enabled[protocol]; !ok {
			return
		}
		detector := protocolDetector{protocol: protocol, detect: detect}
		switch network {
		case C.TCP:
			dispatcher.tcp = append(dispatcher.tcp, detector)
		case C.UDP:
			dispatcher.udp = append(dispatcher.udp, detector)
		}
	}

	// Prefer the most specific application protocol when signatures overlap.
	add(C.SniffProtocolBitTorrent, C.TCP, detectBitTorrentTCP)
	add(C.SniffProtocolBitTorrent, C.UDP, detectBitTorrentUDP)
	add(C.SniffProtocolSTUN, C.TCP, func(data []byte) error { return detectSTUN(data, false) })
	add(C.SniffProtocolSTUN, C.UDP, func(data []byte) error { return detectSTUN(data, true) })
	add(C.SniffProtocolQUIC, C.UDP, detectQUICProtocol)
	add(C.SniffProtocolTLS, C.TCP, detectTLSProtocol)
	add(C.SniffProtocolHTTP, C.TCP, detectHTTPProtocol)

	return dispatcher
}

func (d *ProtocolDispatcher) Enable() bool {
	return d != nil && (len(d.tcp) != 0 || len(d.udp) != 0)
}

// TCPSniff identifies a client-first protocol without consuming connection data.
func (d *ProtocolDispatcher) TCPSniff(conn *N.BufferedConn, metadata *C.Metadata) bool {
	if d == nil || len(d.tcp) == 0 {
		return false
	}

	type candidate struct {
		protocolDetector
		want int
	}
	candidates := make([]candidate, 0, len(d.tcp))
	for _, detector := range d.tcp {
		candidates = append(candidates, candidate{protocolDetector: detector, want: 1})
	}

	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	_, err := conn.Peek(1)
	_ = conn.SetReadDeadline(time.Time{})
	if err != nil {
		return false
	}

	deadline := time.Now().Add(time.Second)
	want := conn.Buffered()
	if want > maxSniffBufferSize {
		want = maxSniffBufferSize
	}
	for len(candidates) > 0 {
		if want <= 0 || want > maxSniffBufferSize {
			return false
		}
		conn.Grow(want)

		_ = conn.SetReadDeadline(deadline)
		_, err = conn.Peek(want)
		_ = conn.SetReadDeadline(time.Time{})
		if err != nil {
			return false
		}

		buffered := conn.Buffered()
		if buffered > maxSniffBufferSize {
			buffered = maxSniffBufferSize
		}
		data, err := conn.Peek(buffered)
		if err != nil {
			return false
		}

		remaining := candidates[:0]
		nextWant := 0
		for _, current := range candidates {
			if current.want <= len(data) {
				err = current.detect(data)
				if err == nil {
					metadata.Protocol = current.protocol
					return true
				}

				var need *errNeedAtLeastData
				if !errors.As(err, &need) || need.length <= len(data) {
					continue
				}
				current.want = need.length
			}

			remaining = append(remaining, current)
			if nextWant == 0 || current.want < nextWant {
				nextWant = current.want
			}
		}

		candidates = remaining
		want = nextWant
	}

	return false
}

// UDPSniff identifies a protocol from the first complete datagram.
func (d *ProtocolDispatcher) UDPSniff(data []byte, metadata *C.Metadata) bool {
	if d == nil {
		return false
	}
	for _, detector := range d.udp {
		if detector.detect(data) == nil {
			metadata.Protocol = detector.protocol
			return true
		}
	}
	return false
}

func detectTLSProtocol(data []byte) error {
	if len(data) > 0 && data[0] != tlsRecordTypeHandshake {
		return errProtocolMismatch
	}
	if len(data) >= 3 && !IsValidTLSVersion(data[1], data[2]) {
		return errProtocolMismatch
	}
	_, err := SniffTLS(data)
	if err == nil || errors.Is(err, errTLSNoServerName) {
		return nil
	}
	return err
}

func detectHTTPProtocol(data []byte) error {
	if len(data) < len(h2ClientPreface) {
		if bytes.HasPrefix(h2ClientPreface, data) {
			return &errNeedAtLeastData{length: len(data) + 1, err: ErrNoClue}
		}
	} else if bytes.HasPrefix(data, h2ClientPreface) {
		return nil
	}

	method, rest, found := bytes.Cut(data, []byte(" "))
	if !found {
		if isHTTPMethodPrefix(data) {
			return &errNeedAtLeastData{length: len(data) + 1, err: ErrNoClue}
		}
		return errProtocolMismatch
	}
	if !isHTTPMethod(method) {
		return errProtocolMismatch
	}

	lineTail, _, found := bytes.Cut(rest, []byte("\r\n"))
	if !found {
		return &errNeedAtLeastData{length: len(data) + 1, err: ErrNoClue}
	}
	target, version, found := bytes.Cut(lineTail, []byte(" "))
	if !found || len(target) == 0 || bytes.Contains(version, []byte(" ")) {
		return errProtocolMismatch
	}
	if !bytes.Equal(version, []byte("HTTP/1.0")) && !bytes.Equal(version, []byte("HTTP/1.1")) {
		return errProtocolMismatch
	}
	return nil
}
