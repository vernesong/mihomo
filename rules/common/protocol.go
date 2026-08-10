package common

import (
	C "github.com/metacubex/mihomo/constant"
)

type Protocol struct {
	Base
	protocol C.SniffProtocol
	adapter  string
}

func NewProtocol(protocol, adapter string) (*Protocol, error) {
	parsed, err := C.ParseSniffProtocol(protocol)
	if err != nil {
		return nil, err
	}
	return &Protocol{
		Base:     Base{},
		protocol: parsed,
		adapter:  adapter,
	}, nil
}

func (p *Protocol) RuleType() C.RuleType {
	return C.Protocol
}

func (p *Protocol) Match(metadata *C.Metadata, _ C.RuleMatchHelper) (bool, string) {
	return p.protocol == metadata.Protocol, p.adapter
}

func (p *Protocol) Adapter() string {
	return p.adapter
}

func (p *Protocol) Payload() string {
	return string(p.protocol)
}

func (p *Protocol) RequiredProtocols() []C.SniffProtocol {
	return []C.SniffProtocol{p.protocol}
}

var _ C.Rule = (*Protocol)(nil)
var _ C.ProtocolRequirement = (*Protocol)(nil)
