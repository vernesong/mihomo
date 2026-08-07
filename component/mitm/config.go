package mitm

import (
	"fmt"
	"net/netip"
	"strings"

	"github.com/metacubex/mihomo/component/wildcard"
)

type Options struct {
	H2                  bool
	Hostname            []string
	HostnameExclude     []string
	ClientSourceAddress []netip.Prefix
	Passphrase          string
	CAP12               []byte
}

type Config struct {
	H2                  bool           `json:"h2"`
	Hostname            []string       `json:"hostname"`
	HostnameExclude     []string       `json:"hostname-exclude"`
	ClientSourceAddress []netip.Prefix `json:"client-source-address"`

	authority *authority
}

func NewConfig(options Options) (*Config, error) {
	if len(options.CAP12) == 0 {
		return nil, fmt.Errorf("CA PKCS#12 data is empty")
	}

	authority, err := newAuthority(options.CAP12, options.Passphrase)
	if err != nil {
		return nil, err
	}

	hostname := append([]string(nil), options.Hostname...)
	for index := range hostname {
		hostname[index] = strings.ToLower(hostname[index])
	}
	hostnameExclude := append([]string(nil), options.HostnameExclude...)
	for index := range hostnameExclude {
		hostnameExclude[index] = strings.ToLower(hostnameExclude[index])
	}

	return &Config{
		H2:                  options.H2,
		Hostname:            hostname,
		HostnameExclude:     hostnameExclude,
		ClientSourceAddress: append([]netip.Prefix(nil), options.ClientSourceAddress...),
		authority:           authority,
	}, nil
}

func (c *Config) Enabled() bool {
	return c != nil && c.authority != nil && len(c.Hostname) != 0
}

func (c *Config) Match(host string, source netip.Addr) bool {
	if !c.Enabled() || !source.IsValid() {
		return false
	}

	source = source.WithZone("")
	sourceMatched := false
	for _, prefix := range c.ClientSourceAddress {
		if prefix.Contains(source) {
			sourceMatched = true
			break
		}
	}
	if !sourceMatched {
		return false
	}

	return c.MatchHostname(host)
}

func (c *Config) MatchHostname(host string) bool {
	if !c.Enabled() {
		return false
	}

	host = strings.ToLower(host)
	for _, pattern := range c.HostnameExclude {
		if wildcard.Match(pattern, host) {
			return false
		}
	}
	for _, pattern := range c.Hostname {
		if wildcard.Match(pattern, host) {
			return true
		}
	}
	return false
}
