package age

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/metacubex/age"
	"github.com/metacubex/age/armor"
)

const FileHeader = armor.Header

type Identity = age.Identity
type Recipient = age.Recipient

var globalIdentities []Identity

func ParseIdentities(secretKey string) ([]Identity, error) {
	return age.ParseIdentities(strings.NewReader(secretKey))
}

func ParseRecipients(publicKey string) ([]Recipient, error) {
	return age.ParseRecipients(strings.NewReader(publicKey))
}

func SetGlobalIdentities(id []Identity) {
	globalIdentities = append(globalIdentities[:0], id...)
}

// DecryptBytes decrypt age armor format encrypted data
// if not the age armor format, return original data
func DecryptBytes(data []byte, identities ...Identity) ([]byte, error) {
	if !strings.HasPrefix(string(data), FileHeader) { // not age armor format
		return data, nil
	}
	identities = append(identities[:len(identities):len(identities)], globalIdentities...)
	r, err := age.Decrypt(armor.NewReader(bytes.NewReader(data)), identities...)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(r)
}

// EncryptBytes encrypt data with age armor format
func EncryptBytes(data []byte, recipients ...Recipient) ([]byte, error) {
	buf := &bytes.Buffer{}
	armorWriter := armor.NewWriter(buf)
	w, err := age.Encrypt(armorWriter, recipients...)
	if err != nil {
		return nil, err
	}
	_, err = w.Write(data)
	if err != nil {
		return nil, err
	}
	err = w.Close()
	if err != nil {
		return nil, err
	}
	err = armorWriter.Close()
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ConvertToRecipient convert age.Identity to age.Recipient
func ConvertToRecipient(identity Identity) (Recipient, error) {
	switch identity := identity.(type) {
	case *age.X25519Identity:
		return identity.Recipient(), nil
	case *age.HybridIdentity:
		return identity.Recipient(), nil
	default:
		return nil, fmt.Errorf("unexpected identity type: %T", identity)
	}
}

func GenX25519KeyPair() (string, string, error) {
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		return "", "", err
	}
	return identity.String(), identity.Recipient().String(), nil
}

func GenHybridKeyPair() (string, string, error) {
	identity, err := age.GenerateHybridIdentity()
	if err != nil {
		return "", "", err
	}
	return identity.String(), identity.Recipient().String(), nil
}

func Main(args []string) {
	if len(args) < 1 {
		panic("Using: age keygen/keygen-pq/convert/decrypt/encrypt")
	}
	switch args[0] {
	case "keygen":
		secretKey, publicKey, err := GenX25519KeyPair()
		if err != nil {
			panic(err)
		}
		fmt.Printf("# created: %s\n", time.Now().Format(time.RFC3339))
		fmt.Printf("# public key: %s\n", publicKey)
		fmt.Printf("%s\n", secretKey)
	case "keygen-pq":
		secretKey, publicKey, err := GenHybridKeyPair()
		if err != nil {
			panic(err)
		}
		fmt.Printf("# created: %s\n", time.Now().Format(time.RFC3339))
		fmt.Printf("# public key: %s\n", publicKey)
		fmt.Printf("%s\n", secretKey)
	case "convert":
		if len(args) < 1 {
			panic("Using: age convert <secret_key>")
		}
		identities, err := ParseIdentities(args[1])
		if err != nil {
			panic(err)
		}
		if len(identities) == 0 {
			panic("no identities found in the input")
		}
		for _, identity := range identities {
			recipient, err := ConvertToRecipient(identity)
			if err != nil {
				panic(err)
			}
			fmt.Println(recipient)
		}
	case "decrypt":
		if len(args) < 3 {
			panic("Using: age decrypt <secret_key> <source_file> <target_file>")
		}
		identities, err := ParseIdentities(args[1])
		if err != nil {
			panic(err)
		}
		var data []byte
		if args[2] == "-" {
			data, err = io.ReadAll(os.Stdin)
		} else {
			data, err = os.ReadFile(args[2])
		}
		if err != nil {
			panic(err)
		}
		result, err := DecryptBytes(data, identities...)
		if err != nil {
			panic(err)
		}
		if args[3] == "-" {
			_, err = os.Stdout.Write(result)
		} else {
			err = os.WriteFile(args[3], result, 0644)
		}
		if err != nil {
			panic(err)
		}
	case "encrypt":
		if len(args) < 3 {
			panic("Using: age encrypt <public_key> <source_file> <target_file>")
		}
		recipients, err := ParseRecipients(args[1])
		if err != nil {
			panic(err)
		}
		var data []byte
		if args[2] == "-" {
			data, err = io.ReadAll(os.Stdin)
		} else {
			data, err = os.ReadFile(args[2])
		}
		if err != nil {
			panic(err)
		}
		result, err := EncryptBytes(data, recipients...)
		if err != nil {
			panic(err)
		}
		if args[3] == "-" {
			_, err = os.Stdout.Write(result)
		} else {
			err = os.WriteFile(args[3], result, 0644)
		}
		if err != nil {
			panic(err)
		}
	}
}
