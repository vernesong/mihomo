package mkcp

import (
	"encoding/binary"
	"math/rand"
	"strings"
)

type packetHeader interface {
	size() int
	serialize([]byte)
}

type noopHeader struct{}

func (noopHeader) size() int        { return 0 }
func (noopHeader) serialize([]byte) {}
func newPacketHeader(name string) packetHeader {
	switch strings.ToLower(name) {
	case "", "none", "noop":
		return noopHeader{}
	case "srtp":
		return &srtpHeader{header: 0xb5e8, number: uint16(rand.Int63() >> 47)}
	case "utp":
		return &utpHeader{header: 1, connectionID: uint16(rand.Int63() >> 47)}
	case "wechat-video", "wechat":
		return &wechatVideoHeader{sn: uint32(uint16(rand.Int63() >> 47))}
	case "dtls":
		return &dtlsHeader{epoch: uint16(rand.Int63() >> 47), length: 17}
	case "wireguard":
		return wireguardHeader{}
	default:
		return noopHeader{}
	}
}

type srtpHeader struct {
	header uint16
	number uint16
}

func (*srtpHeader) size() int { return 4 }

func (h *srtpHeader) serialize(b []byte) {
	h.number++
	binary.BigEndian.PutUint16(b, h.header)
	binary.BigEndian.PutUint16(b[2:], h.number)
}

type utpHeader struct {
	header       byte
	extension    byte
	connectionID uint16
}

func (*utpHeader) size() int { return 4 }

func (h *utpHeader) serialize(b []byte) {
	binary.BigEndian.PutUint16(b, h.connectionID)
	b[2] = h.header
	b[3] = h.extension
}

type wechatVideoHeader struct {
	sn uint32
}

func (*wechatVideoHeader) size() int { return 13 }

func (h *wechatVideoHeader) serialize(b []byte) {
	h.sn++
	b[0] = 0xa1
	b[1] = 0x08
	binary.BigEndian.PutUint32(b[2:], h.sn)
	b[6] = 0x00
	b[7] = 0x10
	b[8] = 0x11
	b[9] = 0x18
	b[10] = 0x30
	b[11] = 0x22
	b[12] = 0x30
}

type dtlsHeader struct {
	epoch    uint16
	length   uint16
	sequence uint32
}

func (*dtlsHeader) size() int { return 13 }

func (h *dtlsHeader) serialize(b []byte) {
	b[0] = 23
	b[1] = 254
	b[2] = 253
	b[3] = byte(h.epoch >> 8)
	b[4] = byte(h.epoch)
	b[5] = 0
	b[6] = 0
	b[7] = byte(h.sequence >> 24)
	b[8] = byte(h.sequence >> 16)
	b[9] = byte(h.sequence >> 8)
	b[10] = byte(h.sequence)
	h.sequence++
	b[11] = byte(h.length >> 8)
	b[12] = byte(h.length)
	h.length += 17
	if h.length > 100 {
		h.length -= 50
	}
}

type wireguardHeader struct{}

func (wireguardHeader) size() int { return 4 }

func (wireguardHeader) serialize(b []byte) {
	b[0] = 0x04
	b[1] = 0x00
	b[2] = 0x00
	b[3] = 0x00
}
