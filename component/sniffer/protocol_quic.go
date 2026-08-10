package sniffer

import "encoding/binary"

func detectQUICProtocol(data []byte) error {
	if len(data) < 6 {
		return &errNeedAtLeastData{length: 6, err: ErrNoClue}
	}
	firstByte := data[0]
	if firstByte&0xc0 != 0xc0 {
		return errProtocolMismatch
	}

	version := binary.BigEndian.Uint32(data[1:5])
	var structure *quicStructure
	for _, candidate := range listQUICVersions {
		if candidate.ver == version {
			structure = candidate
			break
		}
	}
	if structure == nil || (firstByte&0x30)>>4 != structure.typeInitial {
		return errProtocolMismatch
	}

	offset := 5
	destinationIDLength := int(data[offset])
	offset++
	if destinationIDLength == 0 || destinationIDLength > 20 {
		return errProtocolMismatch
	}
	if len(data) < offset+destinationIDLength+1 {
		return &errNeedAtLeastData{length: offset + destinationIDLength + 1, err: ErrNoClue}
	}
	offset += destinationIDLength

	sourceIDLength := int(data[offset])
	offset++
	if sourceIDLength > 20 {
		return errProtocolMismatch
	}
	if len(data) < offset+sourceIDLength {
		return &errNeedAtLeastData{length: offset + sourceIDLength, err: ErrNoClue}
	}
	offset += sourceIDLength

	tokenLength, nextOffset, ok := readQUICVarint(data, offset)
	if !ok {
		return &errNeedAtLeastData{length: offset + 8, err: ErrNoClue}
	}
	offset = nextOffset
	if tokenLength > uint64(len(data)-offset) {
		return errProtocolMismatch
	}
	offset += int(tokenLength)

	packetLength, nextOffset, ok := readQUICVarint(data, offset)
	if !ok {
		return &errNeedAtLeastData{length: offset + 8, err: ErrNoClue}
	}
	offset = nextOffset
	// Packet Number plus the AEAD authentication tag require at least 17 bytes.
	if packetLength < 17 || packetLength > uint64(len(data)-offset) {
		return errProtocolMismatch
	}

	return nil
}

func readQUICVarint(data []byte, offset int) (uint64, int, bool) {
	if offset >= len(data) {
		return 0, offset, false
	}
	length := 1 << (data[offset] >> 6)
	if len(data)-offset < int(length) {
		return 0, offset, false
	}
	value := uint64(data[offset] & 0x3f)
	for index := 1; index < int(length); index++ {
		value = value<<8 | uint64(data[offset+index])
	}
	return value, offset + int(length), true
}
