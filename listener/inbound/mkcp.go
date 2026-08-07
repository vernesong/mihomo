package inbound

import LC "github.com/metacubex/mihomo/listener/config"

type MKCPConfig struct {
	Enable           bool   `inbound:"enable,omitempty"`
	MTU              uint32 `inbound:"mtu,omitempty"`
	TTI              uint32 `inbound:"tti,omitempty"`
	UplinkCapacity   uint32 `inbound:"uplink-capacity,omitempty"`
	DownlinkCapacity uint32 `inbound:"downlink-capacity,omitempty"`
	Congestion       bool   `inbound:"congestion,omitempty"`
	WriteBuffer      uint32 `inbound:"write-buffer,omitempty"`
	ReadBuffer       uint32 `inbound:"read-buffer,omitempty"`
	Seed             string `inbound:"seed,omitempty"`
	Header           string `inbound:"header,omitempty"`
}

func (c MKCPConfig) Build() LC.MKCPConfig {
	return LC.MKCPConfig{
		Enable:           c.Enable,
		MTU:              c.MTU,
		TTI:              c.TTI,
		UplinkCapacity:   c.UplinkCapacity,
		DownlinkCapacity: c.DownlinkCapacity,
		Congestion:       c.Congestion,
		WriteBuffer:      c.WriteBuffer,
		ReadBuffer:       c.ReadBuffer,
		Seed:             c.Seed,
		Header:           c.Header,
	}
}
