package sniffer

import "encoding/binary"

const (
	stunHeaderLength = 20
	stunMagicCookie  = 0x2112a442
)

func detectSTUN(data []byte, datagram bool) error {
	if len(data) == 0 {
		return &errNeedAtLeastData{length: 1, err: ErrNoClue}
	}
	if data[0]&0xc0 != 0 {
		return errProtocolMismatch
	}
	if len(data) < 8 {
		return &errNeedAtLeastData{length: 8, err: ErrNoClue}
	}

	messageType := binary.BigEndian.Uint16(data[:2])
	if messageType&0xc000 != 0 || binary.BigEndian.Uint32(data[4:8]) != stunMagicCookie {
		return errProtocolMismatch
	}
	if len(data) < stunHeaderLength {
		return &errNeedAtLeastData{length: stunHeaderLength, err: ErrNoClue}
	}

	bodyLength := int(binary.BigEndian.Uint16(data[2:4]))
	if bodyLength%4 != 0 {
		return errProtocolMismatch
	}
	messageLength := stunHeaderLength + bodyLength
	if len(data) < messageLength {
		return &errNeedAtLeastData{length: messageLength, err: ErrNoClue}
	}
	if datagram && len(data) != messageLength {
		return errProtocolMismatch
	}

	for offset := stunHeaderLength; offset < messageLength; {
		if offset+4 > messageLength {
			return errProtocolMismatch
		}
		attributeLength := int(binary.BigEndian.Uint16(data[offset+2 : offset+4]))
		offset += 4
		paddedLength := (attributeLength + 3) &^ 3
		if paddedLength > messageLength-offset {
			return errProtocolMismatch
		}
		offset += paddedLength
	}

	return nil
}
