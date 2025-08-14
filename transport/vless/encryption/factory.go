package encryption

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// NewClient new client from encryption string
// maybe return a nil *ClientInstance without any error, that means don't need to encrypt
func NewClient(encryption string) (*ClientInstance, error) {
	switch encryption {
	case "", "none": // We will not reject empty string like xray-core does, because we need to ensure compatibility
		return nil, nil
	}
	if s := strings.SplitN(encryption, "-", 4); len(s) == 4 && s[2] == "mlkem768client" {
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
		var xor uint32
		switch s[1] {
		case "vless":
		case "xored":
			xor = 1
		default:
			return nil, fmt.Errorf("invaild vless encryption value: %s", encryption)
		}
		b, err := base64.RawURLEncoding.DecodeString(s[3])
		if err != nil {
			return nil, fmt.Errorf("invaild vless encryption value: %s", encryption)
		}
		if len(b) == MLKEM768ClientLength {
			client := &ClientInstance{}
			if err = client.Init(b, xor, time.Duration(minutes)*time.Minute); err != nil {
				return nil, fmt.Errorf("failed to use mlkem768seed: %w", err)
			}
			return client, nil
		} else {
			return nil, fmt.Errorf("invaild vless encryption value: %s", encryption)
		}
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
	if s := strings.SplitN(decryption, "-", 4); len(s) == 4 && s[2] == "mlkem768seed" {
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
		var xor uint32
		switch s[1] {
		case "vless":
		case "xored":
			xor = 1
		default:
			return nil, fmt.Errorf("invaild vless decryption value: %s", decryption)
		}
		b, err := base64.RawURLEncoding.DecodeString(s[3])
		if err != nil {
			return nil, fmt.Errorf("invaild vless decryption value: %s", decryption)
		}
		if len(b) == MLKEM768SeedLength {
			server := &ServerInstance{}
			if err = server.Init(b, xor, time.Duration(minutes)*time.Minute); err != nil {
				return nil, fmt.Errorf("failed to use mlkem768seed: %w", err)
			}
			return server, nil
		} else {
			return nil, fmt.Errorf("invaild vless decryption value: %s", decryption)
		}
	}
	return nil, fmt.Errorf("invaild vless decryption value: %s", decryption)
}
