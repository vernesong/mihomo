package config

import "github.com/metacubex/mihomo/transport/mkcp"

type MKCPConfig struct {
	Enable           bool   `yaml:"enable" json:"enable,omitempty"`
	MTU              uint32 `yaml:"mtu" json:"mtu,omitempty"`
	TTI              uint32 `yaml:"tti" json:"tti,omitempty"`
	UplinkCapacity   uint32 `yaml:"uplink-capacity" json:"uplink-capacity,omitempty"`
	DownlinkCapacity uint32 `yaml:"downlink-capacity" json:"downlink-capacity,omitempty"`
	Congestion       bool   `yaml:"congestion" json:"congestion,omitempty"`
	WriteBuffer      uint32 `yaml:"write-buffer" json:"write-buffer,omitempty"`
	ReadBuffer       uint32 `yaml:"read-buffer" json:"read-buffer,omitempty"`
	Seed             string `yaml:"seed" json:"seed,omitempty"`
	Header           string `yaml:"header" json:"header,omitempty"`
}

func (c MKCPConfig) Build() mkcp.Config {
	return mkcp.Config{
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
