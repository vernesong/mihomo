package age_test

import (
	"fmt"
	"testing"

	"github.com/metacubex/mihomo/component/age"
)

func TestAge(t *testing.T) {
	testCases := []struct {
		name string
		gen  func() (string, string, error)
	}{
		{"X25519", age.GenX25519KeyPair},
		{"MLKEM768X25519", age.GenHybridKeyPair},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			secretKey, publicKey, err := tc.gen()
			if err != nil {
				t.Fatal(err)
			}
			t.Log(secretKey, publicKey)
			identities, err := age.ParseIdentities(secretKey)
			if err != nil {
				t.Fatal(err)
			}
			recipients, err := age.ParseRecipients(publicKey)
			if err != nil {
				t.Fatal(err)
			}
			if len(identities) != len(recipients) {
				t.Fatal("identities and recipients are not equal")
			}
			for i, identity := range identities {
				recipient, err := age.ConvertToRecipient(identity)
				if err != nil {
					t.Fatal(err)
				}
				if fmt.Sprint(recipient) != fmt.Sprint(recipients[i]) {
					t.Fatal("recipient is not equal to recipients: ", recipient, " != ", recipients[i], "")
				}
			}
			rawData := []byte("hello world")
			encryptData, err := age.EncryptBytes(rawData, recipients...)
			if err != nil {
				t.Fatal(err)
			}
			t.Log(string(encryptData))
			decryptData, err := age.DecryptBytes(encryptData, identities...)
			if err != nil {
				t.Fatal(err)
			}
			if string(decryptData) != string(rawData) {
				t.Fatal("decrypt data is not equal to raw data")
			}
		})
	}

}
