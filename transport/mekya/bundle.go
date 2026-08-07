package mekya

import (
	"encoding/binary"
	"io"
)

const packetBundleOverhead = 2

func writePacketBundle(w io.Writer, packet []byte) error {
	if len(packet) > 0xffff {
		return io.ErrShortBuffer
	}
	var header [packetBundleOverhead]byte
	binary.BigEndian.PutUint16(header[:], uint16(len(packet)))
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	_, err := w.Write(packet)
	return err
}

func readPacketBundle(r io.Reader) ([]byte, error) {
	var header [packetBundleOverhead]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint16(header[:])
	packet := make([]byte, length)
	if _, err := io.ReadFull(r, packet); err != nil {
		return nil, err
	}
	return packet, nil
}
