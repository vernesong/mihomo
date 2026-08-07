package mitm

import (
	"bytes"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/metacubex/mihomo/ntp"

	"github.com/metacubex/tls"
	pkcs12 "software.sslmate.com/src/go-pkcs12"
)

type authority struct {
	certificate *x509.Certificate
	chain       [][]byte
	signer      crypto.Signer
	leafKey     *rsa.PrivateKey
	cache       sync.Map
}

func newAuthority(data []byte, passphrase string) (*authority, error) {
	privateKey, certificate, chain, err := pkcs12.DecodeChain(data, passphrase)
	if err != nil {
		return nil, fmt.Errorf("decode CA PKCS#12: %w", err)
	}
	if !certificate.IsCA {
		return nil, fmt.Errorf("PKCS#12 certificate is not a CA")
	}
	signer, ok := privateKey.(crypto.Signer)
	if !ok {
		return nil, fmt.Errorf("PKCS#12 private key cannot sign certificates")
	}
	certificatePublicKey, err := x509.MarshalPKIXPublicKey(certificate.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("marshal CA public key: %w", err)
	}
	signerPublicKey, err := x509.MarshalPKIXPublicKey(signer.Public())
	if err != nil {
		return nil, fmt.Errorf("marshal CA private key public part: %w", err)
	}
	if !bytes.Equal(certificatePublicKey, signerPublicKey) {
		return nil, fmt.Errorf("PKCS#12 certificate and private key do not match")
	}

	leafKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, fmt.Errorf("generate MITM leaf key: %w", err)
	}
	certificateChain := make([][]byte, 0, len(chain)+1)
	certificateChain = append(certificateChain, certificate.Raw)
	for _, certificate := range chain {
		certificateChain = append(certificateChain, certificate.Raw)
	}

	return &authority{
		certificate: certificate,
		chain:       certificateChain,
		signer:      signer,
		leafKey:     leafKey,
	}, nil
}

func (a *authority) tlsConfig(host string, h2 bool) *tls.Config {
	nextProtos := []string{"http/1.1"}
	if h2 {
		nextProtos = []string{"h2", "http/1.1"}
	}

	return &tls.Config{
		Time:       ntp.Now,
		NextProtos: nextProtos,
		GetCertificate: func(clientHello *tls.ClientHelloInfo) (*tls.Certificate, error) {
			serverName := clientHello.ServerName
			if serverName == "" {
				serverName = host
			}
			return a.certificateForHost(serverName)
		},
	}
}

func (a *authority) certificateForHost(host string) (*tls.Certificate, error) {
	host = normalizeCertificateHost(host)
	if host == "" {
		return nil, fmt.Errorf("MITM certificate hostname is empty")
	}
	if cached, ok := a.cache.Load(host); ok {
		return cached.(*tls.Certificate), nil
	}

	now := ntp.Now()
	notBefore := now.Add(-time.Hour)
	if notBefore.Before(a.certificate.NotBefore) {
		notBefore = a.certificate.NotBefore
	}
	notAfter := now.Add(365 * 24 * time.Hour)
	if notAfter.After(a.certificate.NotAfter) {
		notAfter = a.certificate.NotAfter
	}
	if !notAfter.After(notBefore) {
		return nil, fmt.Errorf("CA certificate is not currently valid")
	}

	serialLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, serialLimit)
	if err != nil {
		return nil, fmt.Errorf("generate MITM certificate serial: %w", err)
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   host,
			Organization: []string{"mihomo MITM"},
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	if ip := net.ParseIP(host); ip != nil {
		template.IPAddresses = []net.IP{ip}
	} else {
		template.DNSNames = []string{host}
	}

	leafDER, err := x509.CreateCertificate(rand.Reader, template, a.certificate, a.leafKey.Public(), a.signer)
	if err != nil {
		return nil, fmt.Errorf("create MITM certificate for %s: %w", host, err)
	}
	leaf, err := x509.ParseCertificate(leafDER)
	if err != nil {
		return nil, fmt.Errorf("parse MITM certificate for %s: %w", host, err)
	}

	certificateChain := make([][]byte, 0, len(a.chain)+1)
	certificateChain = append(certificateChain, leafDER)
	certificateChain = append(certificateChain, a.chain...)
	certificate := &tls.Certificate{
		Certificate: certificateChain,
		PrivateKey:  a.leafKey,
		Leaf:        leaf,
	}
	actual, _ := a.cache.LoadOrStore(host, certificate)
	return actual.(*tls.Certificate), nil
}

func normalizeCertificateHost(host string) string {
	if parsedHost, _, err := net.SplitHostPort(host); err == nil {
		host = parsedHost
	}
	return strings.ToLower(strings.TrimSuffix(strings.Trim(host, "[]"), "."))
}
