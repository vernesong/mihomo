package inbound

import (
	LC "github.com/metacubex/mihomo/listener/config"
)

type TLSMirrorConfig struct {
	PrimaryKey                    string                         `inbound:"primary-key,omitempty"`
	Dest                          string                         `inbound:"dest,omitempty"`
	Proxy                         string                         `inbound:"proxy,omitempty"`
	ExplicitNonceCipherSuites     []uint16                       `inbound:"explicit-nonce-ciphersuites,omitempty"`
	DeferInstanceDerivedWriteTime TLSMirrorTimeSpec              `inbound:"defer-instance-derived-write-time,omitempty"`
	TransportLayerPadding         TLSMirrorTransportLayerPadding `inbound:"transport-layer-padding,omitempty"`
	ConnectionEnrolment           *TLSMirrorConnectionEnrolment  `inbound:"connection-enrolment,omitempty"`
	SequenceWatermarkingEnabled   bool                           `inbound:"sequence-watermarking-enabled,omitempty"`
}

type TLSMirrorConnectionEnrolment struct {
	PrimaryIngressOutbound string `inbound:"primary-ingress-outbound,omitempty"`
	PrimaryEgressOutbound  string `inbound:"primary-egress-outbound,omitempty"`
}

func (e *TLSMirrorConnectionEnrolment) Build() *LC.TLSMirrorConnectionEnrolment {
	if e == nil {
		return nil
	}
	return &LC.TLSMirrorConnectionEnrolment{
		PrimaryIngressOutbound: e.PrimaryIngressOutbound,
		PrimaryEgressOutbound:  e.PrimaryEgressOutbound,
	}
}

type TLSMirrorTimeSpec struct {
	BaseNanoseconds                    uint64 `inbound:"base-nanoseconds,omitempty"`
	UniformRandomMultiplierNanoseconds uint64 `inbound:"uniform-random-multiplier-nanoseconds,omitempty"`
}

func (s TLSMirrorTimeSpec) Build() LC.TLSMirrorTimeSpec {
	return LC.TLSMirrorTimeSpec{
		BaseNanoseconds:                    s.BaseNanoseconds,
		UniformRandomMultiplierNanoseconds: s.UniformRandomMultiplierNanoseconds,
	}
}

type TLSMirrorTransportLayerPadding struct {
	Enabled bool `inbound:"enabled,omitempty"`
}

func (p TLSMirrorTransportLayerPadding) Build() LC.TLSMirrorTransportLayerPadding {
	return LC.TLSMirrorTransportLayerPadding{
		Enabled: p.Enabled,
	}
}

func (c TLSMirrorConfig) Build() LC.TLSMirrorConfig {
	return LC.TLSMirrorConfig{
		PrimaryKey:                    c.PrimaryKey,
		Dest:                          c.Dest,
		Proxy:                         c.Proxy,
		ExplicitNonceCipherSuites:     c.ExplicitNonceCipherSuites,
		DeferInstanceDerivedWriteTime: c.DeferInstanceDerivedWriteTime.Build(),
		TransportLayerPadding:         c.TransportLayerPadding.Build(),
		ConnectionEnrolment:           c.ConnectionEnrolment.Build(),
		SequenceWatermarkingEnabled:   c.SequenceWatermarkingEnabled,
	}
}
