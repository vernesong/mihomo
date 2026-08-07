package inbound

import LC "github.com/metacubex/mihomo/listener/config"

type MekyaConfig struct {
	Enable                         bool       `inbound:"enable,omitempty"`
	URL                            string     `inbound:"url,omitempty"`
	H2PoolSize                     int        `inbound:"h2-pool-size,omitempty"`
	MaxWriteDelay                  int        `inbound:"max-write-delay,omitempty"`
	MaxRequestSize                 int        `inbound:"max-request-size,omitempty"`
	PollingIntervalInitial         int        `inbound:"polling-interval-initial,omitempty"`
	MaxWriteSize                   int        `inbound:"max-write-size,omitempty"`
	MaxWriteDurationMs             int        `inbound:"max-write-duration-ms,omitempty"`
	MaxSimultaneousWriteConnection int        `inbound:"max-simultaneous-write-connection,omitempty"`
	PacketWritingBuffer            int        `inbound:"packet-writing-buffer,omitempty"`
	KCP                            MKCPConfig `inbound:"kcp,omitempty"`
}

func (c MekyaConfig) Build() LC.MekyaConfig {
	return LC.MekyaConfig{
		Enable:                         c.Enable,
		URL:                            c.URL,
		H2PoolSize:                     c.H2PoolSize,
		MaxWriteDelay:                  c.MaxWriteDelay,
		MaxRequestSize:                 c.MaxRequestSize,
		PollingIntervalInitial:         c.PollingIntervalInitial,
		MaxWriteSize:                   c.MaxWriteSize,
		MaxWriteDurationMs:             c.MaxWriteDurationMs,
		MaxSimultaneousWriteConnection: c.MaxSimultaneousWriteConnection,
		PacketWritingBuffer:            c.PacketWritingBuffer,
		KCP:                            c.KCP.Build(),
	}
}
