package encryption

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
)

// NewClient new client from encryption string
// maybe return a nil *ClientInstance without any error, that means don't need to encrypt
func NewClient(encryption string) (*ClientInstance, error) {
	switch encryption {
	case "", "none": // We will not reject empty string like xray-core does, because we need to ensure compatibility
		return nil, nil
	}
	if s := strings.Split(encryption, "."); len(s) == 5 && s[2] == "mlkem768Client" {
		var minutes uint32
		if s[0] != "1rtt" {
			t := strings.TrimSuffix(s[0], "min")
			if t == s[0] {
				return nil, fmt.Errorf("invaild vless encryption value: %s", encryption)
			}
			i, err := strconv.Atoi(t)
			if err != nil {
				return nil, fmt.Errorf("invaild vless encryption value: %s", encryption)
			}
			minutes = uint32(i)
		}
		var xorMode uint32
		switch s[1] {
		case "native":
		case "divide":
			xorMode = 1
		case "random":
			xorMode = 2
		default:
			return nil, fmt.Errorf("invaild vless encryption value: %s", encryption)
		}
		xorPKeyBytes, err := base64.RawURLEncoding.DecodeString(s[3])
		if err != nil {
			return nil, fmt.Errorf("invaild vless encryption value: %s", encryption)
		}
		if len(xorPKeyBytes) != X25519PasswordSize {
			return nil, fmt.Errorf("invaild vless encryption value: %s", encryption)
		}
		nfsEKeyBytes, err := base64.RawURLEncoding.DecodeString(s[4])
		if err != nil {
			return nil, fmt.Errorf("invaild vless encryption value: %s", encryption)
		}
		if len(nfsEKeyBytes) != MLKEM768ClientLength {
			return nil, fmt.Errorf("invaild vless encryption value: %s", encryption)
		}
		client := &ClientInstance{}
		if err = client.Init(nfsEKeyBytes, xorPKeyBytes, xorMode, minutes); err != nil {
			return nil, fmt.Errorf("failed to use mlkem768seed: %w", err)
		}
		return client, nil
	}
	return nil, fmt.Errorf("invaild vless encryption value: %s", encryption)
}

// NewServer new server from decryption string
// maybe return a nil *ServerInstance without any error, that means don't need to decrypt
func NewServer(decryption string) (*ServerInstance, error) {
	switch decryption {
	case "", "none": // We will not reject empty string like xray-core does, because we need to ensure compatibility
		return nil, nil
	}
	if s := strings.Split(decryption, "."); len(s) == 5 && s[2] == "mlkem768Seed" {
		var minutes uint32
		if s[0] != "1rtt" {
			t := strings.TrimSuffix(s[0], "min")
			if t == s[0] {
				return nil, fmt.Errorf("invaild vless decryption value: %s", decryption)
			}
			i, err := strconv.Atoi(t)
			if err != nil {
				return nil, fmt.Errorf("invaild vless decryption value: %s", decryption)
			}
			minutes = uint32(i)
		}
		var xorMode uint32
		switch s[1] {
		case "native":
		case "divide":
			xorMode = 1
		case "random":
			xorMode = 2
		default:
			return nil, fmt.Errorf("invaild vless decryption value: %s", decryption)
		}
		xorSKeyBytes, err := base64.RawURLEncoding.DecodeString(s[3])
		if err != nil {
			return nil, fmt.Errorf("invaild vless decryption value: %s", decryption)
		}
		if len(xorSKeyBytes) != X25519PrivateKeySize {
			return nil, fmt.Errorf("invaild vless decryption value: %s", decryption)
		}
		nfsDKeySeed, err := base64.RawURLEncoding.DecodeString(s[4])
		if err != nil {
			return nil, fmt.Errorf("invaild vless decryption value: %s", decryption)
		}
		if len(nfsDKeySeed) != MLKEM768SeedLength {
			return nil, fmt.Errorf("invaild vless decryption value: %s", decryption)
		}
		server := &ServerInstance{}
		if err = server.Init(nfsDKeySeed, xorSKeyBytes, xorMode, minutes); err != nil {
			return nil, fmt.Errorf("failed to use mlkem768seed: %w", err)
		}
		return server, nil
	}
	return nil, fmt.Errorf("invaild vless decryption value: %s", decryption)
}
