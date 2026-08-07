package mkcp

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
)

type Config struct {
	MTU              uint32
	TTI              uint32
	UplinkCapacity   uint32
	DownlinkCapacity uint32
	Congestion       bool
	WriteBuffer      uint32
	ReadBuffer       uint32
	Seed             string
	Header           string
}

func (c Config) mtu() uint32 {
	if c.MTU == 0 {
		return 1350
	}
	return c.MTU
}

func (c Config) tti() uint32 {
	if c.TTI == 0 {
		return 50
	}
	return c.TTI
}

func (c Config) uplinkCapacity() uint32 {
	if c.UplinkCapacity == 0 {
		return 5
	}
	return c.UplinkCapacity
}

func (c Config) downlinkCapacity() uint32 {
	if c.DownlinkCapacity == 0 {
		return 20
	}
	return c.DownlinkCapacity
}

func (c Config) writeBuffer() uint32 {
	if c.WriteBuffer == 0 {
		return 2 * 1024 * 1024
	}
	return c.WriteBuffer
}

func (c Config) readBuffer() uint32 {
	if c.ReadBuffer == 0 {
		return 2 * 1024 * 1024
	}
	return c.ReadBuffer
}

func (c Config) sendingInFlightSize() uint32 {
	size := c.uplinkCapacity() * 1024 * 1024 / c.mtu() / (1000 / c.tti())
	if size < 8 {
		size = 8
	}
	return size
}

func (c Config) receivingInFlightSize() uint32 {
	size := c.downlinkCapacity() * 1024 * 1024 / c.mtu() / (1000 / c.tti())
	if size < 8 {
		size = 8
	}
	return size
}

func (c Config) sendingBufferSize() uint32 {
	return c.writeBuffer() / c.mtu()
}

func (c Config) receivingBufferSize() uint32 {
	return c.readBuffer() / c.mtu()
}

func (c Config) security() (cipher.AEAD, error) {
	if c.Seed == "" {
		return newSimpleAuthenticator(), nil
	}
	hashedSeed := sha256.Sum256([]byte(c.Seed))
	block, err := aes.NewCipher(hashedSeed[:16])
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func (c Config) packetHeader() packetHeader {
	return newPacketHeader(c.Header)
}
