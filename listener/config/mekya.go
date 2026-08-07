package config

import "github.com/metacubex/mihomo/transport/mekya"

type MekyaConfig struct {
	Enable                         bool       `yaml:"enable" json:"enable,omitempty"`
	URL                            string     `yaml:"url" json:"url,omitempty"`
	H2PoolSize                     int        `yaml:"h2-pool-size" json:"h2-pool-size,omitempty"`
	MaxWriteDelay                  int        `yaml:"max-write-delay" json:"max-write-delay,omitempty"`
	MaxRequestSize                 int        `yaml:"max-request-size" json:"max-request-size,omitempty"`
	PollingIntervalInitial         int        `yaml:"polling-interval-initial" json:"polling-interval-initial,omitempty"`
	MaxWriteSize                   int        `yaml:"max-write-size" json:"max-write-size,omitempty"`
	MaxWriteDurationMs             int        `yaml:"max-write-duration-ms" json:"max-write-duration-ms,omitempty"`
	MaxSimultaneousWriteConnection int        `yaml:"max-simultaneous-write-connection" json:"max-simultaneous-write-connection,omitempty"`
	PacketWritingBuffer            int        `yaml:"packet-writing-buffer" json:"packet-writing-buffer,omitempty"`
	KCP                            MKCPConfig `yaml:"kcp" json:"kcp,omitempty"`
}

func (c MekyaConfig) Build() mekya.Config {
	return mekya.Config{
		KCP:                            c.KCP.Build(),
		URL:                            c.URL,
		H2PoolSize:                     c.H2PoolSize,
		MaxWriteDelay:                  c.MaxWriteDelay,
		MaxRequestSize:                 c.MaxRequestSize,
		PollingIntervalInitial:         c.PollingIntervalInitial,
		MaxWriteSize:                   c.MaxWriteSize,
		MaxWriteDurationMs:             c.MaxWriteDurationMs,
		MaxSimultaneousWriteConnection: c.MaxSimultaneousWriteConnection,
		PacketWritingBuffer:            c.PacketWritingBuffer,
	}
}
