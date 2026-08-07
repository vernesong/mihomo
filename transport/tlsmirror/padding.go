package tlsmirror

import "encoding/binary"

func packPadding(data []byte, paddingLength int) []byte {
	dataLength := len(data)
	data = append(data, make([]byte, paddingLength)...)
	data = binary.BigEndian.AppendUint32(data, uint32(dataLength))
	return data
}

func unpackPadding(data []byte) ([]byte, int) {
	dataLength := len(data)
	if dataLength < 4 {
		return nil, dataLength
	}
	payloadLength := int(binary.BigEndian.Uint32(data[dataLength-4:]))
	if payloadLength > dataLength-4 {
		return nil, 0
	}
	paddingLength := dataLength - payloadLength - 4
	if paddingLength < 0 {
		return nil, paddingLength
	}
	return data[:payloadLength], paddingLength
}
