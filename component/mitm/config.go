package mitm

import (
	"fmt"
	"net/netip"
	"strconv"
	"strings"

	"github.com/metacubex/mihomo/component/wildcard"
)

const standardHTTPSPort uint16 = 443

var defaultClientSourceAddress = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/0"),
	netip.MustParsePrefix("::/0"),
}

type hostnamePattern struct {
	pattern string
	port    uint16
}

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

	authority       *authority
	hostname        []hostnamePattern
	hostnameExclude []hostnamePattern
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
	hostnamePatterns, err := parseHostnamePatterns("hostname", hostname)
	if err != nil {
		return nil, err
	}
	hostnameExcludePatterns, err := parseHostnamePatterns("hostname-exclude", hostnameExclude)
	if err != nil {
		return nil, err
	}

	clientSourceAddress := append([]netip.Prefix(nil), options.ClientSourceAddress...)
	if len(clientSourceAddress) == 0 {
		clientSourceAddress = append(clientSourceAddress, defaultClientSourceAddress...)
	}

	return &Config{
		H2:                  options.H2,
		Hostname:            hostname,
		HostnameExclude:     hostnameExclude,
		ClientSourceAddress: clientSourceAddress,
		authority:           authority,
		hostname:            hostnamePatterns,
		hostnameExclude:     hostnameExcludePatterns,
	}, nil
}

func (c *Config) Enabled() bool {
	return c != nil && c.authority != nil && len(c.hostname) != 0
}

func (c *Config) Match(host string, source netip.Addr) bool {
	return c.MatchHostPort(host, standardHTTPSPort, source)
}

func (c *Config) MatchHostPort(host string, port uint16, source netip.Addr) bool {
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

	return c.MatchHostnamePort(host, port)
}

func (c *Config) MatchHostname(host string) bool {
	return c.MatchHostnamePort(host, standardHTTPSPort)
}

func (c *Config) MatchHostnamePort(host string, port uint16) bool {
	if !c.Enabled() {
		return false
	}

	host = strings.ToLower(host)
	for _, pattern := range c.hostnameExclude {
		if pattern.match(host, port) {
			return false
		}
	}
	for _, pattern := range c.hostname {
		if pattern.match(host, port) {
			return true
		}
	}
	return false
}

func parseHostnamePatterns(field string, values []string) ([]hostnamePattern, error) {
	patterns := make([]hostnamePattern, 0, len(values))
	for index, value := range values {
		pattern, err := parseHostnamePattern(value)
		if err != nil {
			return nil, fmt.Errorf("mitm.%s[%d]: %w", field, index, err)
		}
		patterns = append(patterns, pattern)
	}
	return patterns, nil
}

func parseHostnamePattern(value string) (hostnamePattern, error) {
	host, portText, hasPort, err := splitHostnamePattern(value)
	if err != nil {
		return hostnamePattern{}, err
	}
	if host == "" {
		return hostnamePattern{}, fmt.Errorf("hostname is empty")
	}

	port := standardHTTPSPort
	if hasPort {
		parsedPort, err := strconv.ParseUint(portText, 10, 16)
		if err != nil {
			return hostnamePattern{}, fmt.Errorf("invalid port %q", portText)
		}
		port = uint16(parsedPort)
	}
	return hostnamePattern{pattern: host, port: port}, nil
}

func splitHostnamePattern(value string) (host string, port string, hasPort bool, err error) {
	if strings.HasPrefix(value, "[") {
		closingBracket := strings.LastIndexByte(value, ']')
		if closingBracket < 0 {
			return "", "", false, fmt.Errorf("invalid bracketed hostname %q", value)
		}
		host = value[1:closingBracket]
		remainder := value[closingBracket+1:]
		if remainder == "" {
			return host, "", false, nil
		}
		if !strings.HasPrefix(remainder, ":") || len(remainder) == 1 {
			return "", "", false, fmt.Errorf("invalid hostname port in %q", value)
		}
		return host, remainder[1:], true, nil
	}

	lastColon := strings.LastIndexByte(value, ':')
	if lastColon < 0 || strings.ContainsRune(value[:lastColon], ':') {
		return value, "", false, nil
	}
	if lastColon == 0 || lastColon == len(value)-1 {
		return "", "", false, fmt.Errorf("invalid hostname port in %q", value)
	}
	return value[:lastColon], value[lastColon+1:], true, nil
}

func (p hostnamePattern) match(host string, port uint16) bool {
	return (p.port == 0 || p.port == port) && wildcard.Match(p.pattern, host)
}
