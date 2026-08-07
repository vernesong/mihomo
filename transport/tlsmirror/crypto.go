package tlsmirror

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"errors"
	"io"

	"golang.org/x/crypto/chacha20"
	"golang.org/x/crypto/hkdf"
)

type xorNonceAEAD struct {
	nonceMask [12]byte
	aead      cipher.AEAD
}

func newXORNonceAEAD(key, nonceMask []byte) (cipher.AEAD, error) {
	if len(nonceMask) != 12 {
		return nil, errors.New("tlsmirror: invalid nonce mask size")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	ret := &xorNonceAEAD{aead: aead}
	copy(ret.nonceMask[:], nonceMask)
	return ret, nil
}

func (a *xorNonceAEAD) NonceSize() int {
	return 8
}

func (a *xorNonceAEAD) Overhead() int {
	return a.aead.Overhead()
}

func (a *xorNonceAEAD) Seal(dst, nonce, plaintext, additionalData []byte) []byte {
	mask := a.nonceMask
	for i, b := range nonce {
		mask[4+i] ^= b
	}
	return a.aead.Seal(dst, mask[:], plaintext, additionalData)
}

func (a *xorNonceAEAD) Open(dst, nonce, ciphertext, additionalData []byte) ([]byte, error) {
	mask := a.nonceMask
	for i, b := range nonce {
		mask[4+i] ^= b
	}
	return a.aead.Open(dst, mask[:], ciphertext, additionalData)
}

func deriveEncryptionKey(primaryKey []byte, clientRandom, serverRandom [32]byte, tag string) ([]byte, []byte, error) {
	if len(primaryKey) != 32 {
		return nil, nil, errors.New("tlsmirror: invalid primary key size")
	}
	combined := make([]byte, 0, 96)
	combined = append(combined, primaryKey...)
	combined = append(combined, clientRandom[:]...)
	combined = append(combined, serverRandom[:]...)

	encryptionKey := make([]byte, 16)
	if _, err := io.ReadFull(hkdf.Expand(sha256.New, combined, []byte("v2ray-sp76YMKM-EkGrFUNL-rTJRJMkU:tlsmirror-encryption"+tag)), encryptionKey); err != nil {
		return nil, nil, err
	}
	nonceMask := make([]byte, 12)
	if _, err := io.ReadFull(hkdf.Expand(sha256.New, combined, []byte("v2ray-sp76YMKM-EkGrFUNL-rTJRJMkU:tlsmirror-noncemask"+tag)), nonceMask); err != nil {
		return nil, nil, err
	}
	return encryptionKey, nonceMask, nil
}

func deriveSequenceWatermarkingKey(primaryKey []byte, clientRandom, serverRandom [32]byte, tag string) ([]byte, []byte, error) {
	if len(primaryKey) != 32 {
		return nil, nil, errors.New("tlsmirror: invalid primary key size")
	}
	combined := make([]byte, 0, 96)
	combined = append(combined, primaryKey...)
	combined = append(combined, clientRandom[:]...)
	combined = append(combined, serverRandom[:]...)

	encryptionKey := make([]byte, chacha20.KeySize)
	if _, err := io.ReadFull(hkdf.Expand(sha256.New, combined, []byte("v2ray-xv64FXUU-GxMn8UYz-bTy6UDeE:tlsmirror-sequence-watermark-encryption"+tag)), encryptionKey); err != nil {
		return nil, nil, err
	}
	nonce := make([]byte, chacha20.NonceSizeX)
	if _, err := io.ReadFull(hkdf.Expand(sha256.New, combined, []byte("v2ray-xv64FXUU-GxMn8UYz-bTy6UDeE:tlsmirror-sequence-watermark-noncemask"+tag)), nonce); err != nil {
		return nil, nil, err
	}
	return encryptionKey, nonce, nil
}

func newSequenceWatermark(primaryKey []byte, clientRandom, serverRandom [32]byte, tag string) (cipher.Stream, error) {
	key, nonce, err := deriveSequenceWatermarkingKey(primaryKey, clientRandom, serverRandom, tag)
	if err != nil {
		return nil, err
	}
	return chacha20.NewUnauthenticatedCipher(key, nonce)
}

type nonceGenerator struct {
	next [8]byte
}

func newNonceGenerator() nonceGenerator {
	return nonceGenerator{next: [8]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}}
}

func (g *nonceGenerator) Next() []byte {
	for i := range g.next {
		g.next[i]++
		if g.next[i] != 0 {
			break
		}
	}
	return g.next[:]
}

type explicitNonceGenerator struct {
	next [8]byte
}

func (g *explicitNonceGenerator) Next() []byte {
	for i := len(g.next) - 1; i >= 0; i-- {
		g.next[i]++
		if g.next[i] != 0 {
			break
		}
	}
	return g.next[:]
}

type encryptor struct {
	nonce nonceGenerator
	aead  cipher.AEAD
}

func newEncryptor(key, mask []byte) (*encryptor, error) {
	aead, err := newXORNonceAEAD(key, mask)
	if err != nil {
		return nil, err
	}
	return &encryptor{nonce: newNonceGenerator(), aead: aead}, nil
}

func (e *encryptor) Seal(dst, src []byte) []byte {
	return e.aead.Seal(dst, e.nonce.Next(), src, nil)
}

func (e *encryptor) NonceSize() int {
	return e.aead.NonceSize()
}

func (e *encryptor) Overhead() int {
	return e.aead.Overhead()
}

type decryptor struct {
	nonce     nonceGenerator
	aead      cipher.AEAD
	nextNonce []byte
}

func newDecryptor(key, mask []byte) (*decryptor, error) {
	aead, err := newXORNonceAEAD(key, mask)
	if err != nil {
		return nil, err
	}
	return &decryptor{nonce: newNonceGenerator(), aead: aead}, nil
}

func (d *decryptor) Open(dst, src []byte) ([]byte, error) {
	if d.nextNonce == nil {
		d.nextNonce = append([]byte(nil), d.nonce.Next()...)
	}
	out, err := d.aead.Open(dst, d.nextNonce, src, nil)
	if err != nil {
		return nil, err
	}
	d.nextNonce = nil
	return out, nil
}

func (d *decryptor) NonceSize() int {
	return d.aead.NonceSize()
}
