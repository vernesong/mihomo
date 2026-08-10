package sniffer

import (
	"bytes"
	"encoding/binary"
)

var bitTorrentHandshake = []byte("BitTorrent protocol")

const bitTorrentTrackerConnectionID uint64 = 0x41727101980

func detectBitTorrentTCP(data []byte) error {
	if len(data) == 0 {
		return &errNeedAtLeastData{length: 1, err: ErrNoClue}
	}
	if data[0] == byte(len(bitTorrentHandshake)) {
		handshakeLength := 1 + len(bitTorrentHandshake)
		if len(data) < handshakeLength {
			return &errNeedAtLeastData{length: handshakeLength, err: ErrNoClue}
		}
		if bytes.Equal(data[1:handshakeLength], bitTorrentHandshake) {
			return nil
		}
		return errProtocolMismatch
	}

	// Plain HTTP tracker requests are classified as BitTorrent rather than HTTP
	// when their request target carries the protocol-defined info_hash field.
	method, rest, found := bytes.Cut(data, []byte(" "))
	if !found {
		if bytes.HasPrefix([]byte("GET"), data) || bytes.HasPrefix([]byte("POST"), data) {
			return &errNeedAtLeastData{length: len(data) + 1, err: ErrNoClue}
		}
		return errProtocolMismatch
	}
	if !bytes.Equal(method, []byte("GET")) && !bytes.Equal(method, []byte("POST")) {
		return errProtocolMismatch
	}
	requestLine, _, found := bytes.Cut(rest, []byte("\r\n"))
	if !found {
		return &errNeedAtLeastData{length: len(data) + 1, err: ErrNoClue}
	}
	target, version, found := bytes.Cut(requestLine, []byte(" "))
	if !found || (!bytes.Equal(version, []byte("HTTP/1.0")) && !bytes.Equal(version, []byte("HTTP/1.1"))) {
		return errProtocolMismatch
	}
	if bytes.Contains(target, []byte("info_hash=")) {
		return nil
	}
	return errProtocolMismatch
}

func detectBitTorrentUDP(data []byte) error {
	if detectBitTorrentTracker(data) || detectBitTorrentDHT(data) || detectMicroTransport(data) {
		return nil
	}
	return errProtocolMismatch
}

func detectBitTorrentTracker(data []byte) bool {
	if len(data) < 16 {
		return false
	}
	action := binary.BigEndian.Uint32(data[8:12])
	switch action {
	case 0: // connect request
		return len(data) == 16 && binary.BigEndian.Uint64(data[:8]) == bitTorrentTrackerConnectionID
	case 1: // announce request using a recently issued connection ID
		if len(data) != 98 {
			return false
		}
		event := binary.BigEndian.Uint32(data[80:84])
		return event <= 3 && binary.BigEndian.Uint16(data[96:98]) != 0
	case 2: // scrape request
		return len(data) >= 36 && (len(data)-16)%20 == 0
	default:
		return false
	}
}

// detectMicroTransport recognizes the first µTP SYN packet (BEP 29).
func detectMicroTransport(data []byte) bool {
	if len(data) < 20 || data[0] != 0x41 { // ST_SYN=4, version=1
		return false
	}
	// A SYN has not received a peer timestamp or sequence to acknowledge yet.
	if binary.BigEndian.Uint32(data[8:12]) != 0 || binary.BigEndian.Uint16(data[18:20]) != 0 {
		return false
	}
	extension := data[1]
	offset := 20
	for extension != 0 {
		if extension > 2 || offset+2 > len(data) {
			return false
		}
		nextExtension := data[offset]
		extensionLength := int(data[offset+1])
		offset += 2
		if extensionLength > len(data)-offset {
			return false
		}
		offset += extensionLength
		extension = nextExtension
	}
	return true
}

func detectBitTorrentDHT(data []byte) bool {
	if len(data) < 2 || data[0] != 'd' {
		return false
	}

	var (
		offset         = 1
		hasTransaction bool
		messageType    byte
		hasQuery       bool
		hasArguments   bool
		hasResponse    bool
		hasError       bool
	)
	for offset < len(data) && data[offset] != 'e' {
		key, next, ok := parseBencodedString(data, offset)
		if !ok {
			return false
		}
		offset = next
		valueStart := offset
		valueEnd, ok := parseBencodedValue(data, offset, 0)
		if !ok {
			return false
		}

		switch string(key) {
		case "t":
			value, _, valid := parseBencodedString(data, valueStart)
			hasTransaction = valid && len(value) > 0 && len(value) <= 32
		case "y":
			value, _, valid := parseBencodedString(data, valueStart)
			if valid && len(value) == 1 {
				messageType = value[0]
			}
		case "q":
			value, _, valid := parseBencodedString(data, valueStart)
			hasQuery = valid && len(value) > 0
		case "a":
			hasArguments = data[valueStart] == 'd'
		case "r":
			hasResponse = data[valueStart] == 'd'
		case "e":
			hasError = data[valueStart] == 'l'
		}
		offset = valueEnd
	}

	if offset != len(data)-1 || data[offset] != 'e' || !hasTransaction {
		return false
	}
	switch messageType {
	case 'q':
		return hasQuery && hasArguments
	case 'r':
		return hasResponse
	case 'e':
		return hasError
	default:
		return false
	}
}

func parseBencodedValue(data []byte, offset, depth int) (int, bool) {
	if offset >= len(data) || depth > 32 {
		return offset, false
	}
	switch data[offset] {
	case 'i':
		offset++
		if offset < len(data) && data[offset] == '-' {
			offset++
		}
		start := offset
		for offset < len(data) && data[offset] >= '0' && data[offset] <= '9' {
			offset++
		}
		if offset == start || offset >= len(data) || data[offset] != 'e' {
			return offset, false
		}
		if offset-start > 1 && data[start] == '0' {
			return offset, false
		}
		return offset + 1, true
	case 'l', 'd':
		isDictionary := data[offset] == 'd'
		offset++
		for offset < len(data) && data[offset] != 'e' {
			if isDictionary {
				_, next, ok := parseBencodedString(data, offset)
				if !ok {
					return offset, false
				}
				offset = next
			}
			next, ok := parseBencodedValue(data, offset, depth+1)
			if !ok {
				return offset, false
			}
			offset = next
		}
		if offset >= len(data) || data[offset] != 'e' {
			return offset, false
		}
		return offset + 1, true
	default:
		_, next, ok := parseBencodedString(data, offset)
		return next, ok
	}
}

func parseBencodedString(data []byte, offset int) ([]byte, int, bool) {
	if offset >= len(data) || data[offset] < '0' || data[offset] > '9' {
		return nil, offset, false
	}
	start := offset
	length := 0
	for offset < len(data) && data[offset] >= '0' && data[offset] <= '9' {
		if length > (len(data)-int(data[offset]-'0'))/10 {
			return nil, offset, false
		}
		length = length*10 + int(data[offset]-'0')
		offset++
	}
	if offset >= len(data) || data[offset] != ':' || offset-start > 1 && data[start] == '0' {
		return nil, offset, false
	}
	offset++
	if length > len(data)-offset {
		return nil, offset, false
	}
	return data[offset : offset+length], offset + length, true
}
