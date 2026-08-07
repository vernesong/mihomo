package config

import "encoding/json"

type Hysteria2RealmServer struct {
	Enable             bool     `yaml:"enable" json:"enable"`
	Listen             string   `yaml:"listen" json:"listen"`
	Token              string   `yaml:"token" json:"token"`
	MaxRealms          int      `yaml:"max-realms" json:"max-realms,omitempty"`
	MaxRealmsPerIP     int      `yaml:"max-realms-per-ip" json:"max-realms-per-ip,omitempty"`
	TrustedProxyHeader string   `yaml:"trusted-proxy-header" json:"trusted-proxy-header,omitempty"`
	RealmNamePattern   string   `yaml:"realm-name-pattern" json:"realm-name-pattern,omitempty"`
	Certificate        string   `yaml:"certificate" json:"certificate"`
	PrivateKey         string   `yaml:"private-key" json:"private-key"`
	ClientAuthType     string   `yaml:"client-auth-type" json:"client-auth-type,omitempty"`
	ClientAuthCert     string   `yaml:"client-auth-cert" json:"client-auth-cert,omitempty"`
	EchKey             string   `yaml:"ech-key" json:"ech-key,omitempty"`
	ALPN               []string `yaml:"alpn" json:"alpn,omitempty"`
}

func (h Hysteria2RealmServer) String() string {
	b, _ := json.Marshal(h)
	return string(b)
}
