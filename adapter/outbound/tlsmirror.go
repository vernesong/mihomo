package outbound

import "github.com/metacubex/mihomo/transport/tlsmirror"

type TLSMirrorOptions struct {
	PrimaryKey                    string                         `proxy:"primary-key,omitempty"`
	ExplicitNonceCipherSuites     []uint16                       `proxy:"explicit-nonce-ciphersuites,omitempty"`
	DeferInstanceDerivedWriteTime TLSMirrorTimeSpec              `proxy:"defer-instance-derived-write-time,omitempty"`
	TransportLayerPadding         TLSMirrorTransportLayerPadding `proxy:"transport-layer-padding,omitempty"`
	ConnectionEnrolment           *TLSMirrorConnectionEnrolment  `proxy:"connection-enrolment,omitempty"`
	EmbeddedTrafficGenerator      TLSMirrorTrafficGenerator      `proxy:"embedded-traffic-generator,omitempty"`
	SequenceWatermarkingEnabled   bool                           `proxy:"sequence-watermarking-enabled,omitempty"`
}

type TLSMirrorConnectionEnrolment struct {
	PrimaryIngressOutbound string `proxy:"primary-ingress-outbound,omitempty"`
	PrimaryEgressOutbound  string `proxy:"primary-egress-outbound,omitempty"`
}

func (e *TLSMirrorConnectionEnrolment) Build() *tlsmirror.ConnectionEnrolment {
	if e == nil {
		return nil
	}
	return &tlsmirror.ConnectionEnrolment{
		PrimaryIngressOutbound: e.PrimaryIngressOutbound,
		PrimaryEgressOutbound:  e.PrimaryEgressOutbound,
	}
}

type TLSMirrorTimeSpec struct {
	BaseNanoseconds                    uint64 `proxy:"base-nanoseconds,omitempty"`
	UniformRandomMultiplierNanoseconds uint64 `proxy:"uniform-random-multiplier-nanoseconds,omitempty"`
}

func (s TLSMirrorTimeSpec) Build() tlsmirror.TimeSpec {
	return tlsmirror.TimeSpec{
		BaseNanoseconds:                    s.BaseNanoseconds,
		UniformRandomMultiplierNanoseconds: s.UniformRandomMultiplierNanoseconds,
	}
}

type TLSMirrorTransportLayerPadding struct {
	Enabled bool `proxy:"enabled,omitempty"`
}

func (p TLSMirrorTransportLayerPadding) Build() tlsmirror.TransportLayerPadding {
	return tlsmirror.TransportLayerPadding{
		Enabled: p.Enabled,
	}
}

type TLSMirrorTrafficGenerator struct {
	Steps []TLSMirrorTrafficStep `proxy:"steps,omitempty"`
}

func (g TLSMirrorTrafficGenerator) Build() *tlsmirror.TrafficGenerator {
	if len(g.Steps) == 0 {
		return nil
	}
	steps := make([]tlsmirror.TrafficStep, 0, len(g.Steps))
	for _, step := range g.Steps {
		steps = append(steps, step.Build())
	}
	return &tlsmirror.TrafficGenerator{Steps: steps}
}

type TLSMirrorTrafficStep struct {
	Name                         string                              `proxy:"name,omitempty"`
	Host                         string                              `proxy:"host,omitempty"`
	Path                         string                              `proxy:"path,omitempty"`
	Method                       string                              `proxy:"method,omitempty"`
	Headers                      []TLSMirrorTrafficHeader            `proxy:"headers,omitempty"`
	NextStep                     []TLSMirrorTrafficTransferCandidate `proxy:"next-step,omitempty"`
	ConnectionReady              bool                                `proxy:"connection-ready,omitempty"`
	ConnectionRecallExit         bool                                `proxy:"connection-recall-exit,omitempty"`
	WaitTime                     TLSMirrorTimeSpec                   `proxy:"wait-time,omitempty"`
	H2DoNotWaitForDownloadFinish bool                                `proxy:"h2-do-not-wait-for-download-finish,omitempty"`
}

func (s TLSMirrorTrafficStep) Build() tlsmirror.TrafficStep {
	headers := make([]tlsmirror.TrafficHeader, 0, len(s.Headers))
	for _, header := range s.Headers {
		headers = append(headers, header.Build())
	}
	nextStep := make([]tlsmirror.TrafficTransferCandidate, 0, len(s.NextStep))
	for _, candidate := range s.NextStep {
		nextStep = append(nextStep, candidate.Build())
	}
	return tlsmirror.TrafficStep{
		Name:                         s.Name,
		Host:                         s.Host,
		Path:                         s.Path,
		Method:                       s.Method,
		Headers:                      headers,
		NextStep:                     nextStep,
		ConnectionReady:              s.ConnectionReady,
		ConnectionRecallExit:         s.ConnectionRecallExit,
		WaitTime:                     s.WaitTime.Build(),
		H2DoNotWaitForDownloadFinish: s.H2DoNotWaitForDownloadFinish,
	}
}

type TLSMirrorTrafficHeader struct {
	Name   string   `proxy:"name,omitempty"`
	Value  string   `proxy:"value,omitempty"`
	Values []string `proxy:"values,omitempty"`
}

func (h TLSMirrorTrafficHeader) Build() tlsmirror.TrafficHeader {
	return tlsmirror.TrafficHeader{
		Name:   h.Name,
		Value:  h.Value,
		Values: h.Values,
	}
}

type TLSMirrorTrafficTransferCandidate struct {
	Weight       int32 `proxy:"weight,omitempty"`
	GotoLocation int   `proxy:"goto-location,omitempty"`
}

func (c TLSMirrorTrafficTransferCandidate) Build() tlsmirror.TrafficTransferCandidate {
	return tlsmirror.TrafficTransferCandidate{
		Weight:       c.Weight,
		GotoLocation: c.GotoLocation,
	}
}

func (o TLSMirrorOptions) Build() *tlsmirror.Config {
	if o.PrimaryKey == "" {
		return nil
	}
	return &tlsmirror.Config{
		PrimaryKey:                  o.PrimaryKey,
		ExplicitNonceCipherSuites:   o.ExplicitNonceCipherSuites,
		DeferInstanceDerivedWrite:   o.DeferInstanceDerivedWriteTime.Build(),
		TransportLayerPadding:       o.TransportLayerPadding.Build(),
		ConnectionEnrolment:         o.ConnectionEnrolment.Build(),
		SequenceWatermarkingEnabled: o.SequenceWatermarkingEnabled,
		EmbeddedTrafficGenerator:    o.EmbeddedTrafficGenerator.Build(),
	}
}
