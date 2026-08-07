package tlsmirror

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
)

const (
	recordTypeChangeCipherSpec = 20
	recordTypeAlert            = 21
	recordTypeHandshake        = 22
	recordTypeApplicationData  = 23

	maxTLSRecordPayload = 16384
)

type record struct {
	recordType byte
	version    [2]byte
	fragment   []byte
	inserted   bool
}

func readRecord(reader *bufio.Reader) (*record, []byte, error) {
	header := make([]byte, 5)
	n, err := io.ReadFull(reader, header)
	if err != nil {
		return nil, header[:n], err
	}
	length := int(binary.BigEndian.Uint16(header[3:5]))
	if length > maxTLSRecordPayload {
		return nil, header, errors.New("tlsmirror: tls record is too large")
	}
	fragment := make([]byte, length)
	n, err = io.ReadFull(reader, fragment)
	raw := append(append([]byte(nil), header...), fragment[:n]...)
	if err != nil {
		return nil, raw, err
	}
	return &record{
		recordType: header[0],
		version:    [2]byte{header[1], header[2]},
		fragment:   fragment,
	}, raw, nil
}

func writeRecord(writer *bufio.Writer, rec *record) error {
	if len(rec.fragment) > maxTLSRecordPayload {
		return errors.New("tlsmirror: tls record is too large")
	}
	var header [5]byte
	header[0] = rec.recordType
	header[1] = rec.version[0]
	header[2] = rec.version[1]
	binary.BigEndian.PutUint16(header[3:5], uint16(len(rec.fragment)))
	if _, err := writer.Write(header[:]); err != nil {
		return err
	}
	if _, err := writer.Write(rec.fragment); err != nil {
		return err
	}
	return writer.Flush()
}

func duplicateRecord(rec *record) *record {
	dup := *rec
	dup.fragment = append([]byte(nil), rec.fragment...)
	return &dup
}

func parseClientRandom(fragment []byte) ([32]byte, error) {
	var random [32]byte
	if len(fragment) < 38 || fragment[0] != 1 {
		return random, errors.New("tlsmirror: invalid client hello")
	}
	copy(random[:], fragment[6:38])
	return random, nil
}

func parseServerHello(fragment []byte) ([32]byte, uint16, error) {
	var random [32]byte
	if len(fragment) < 41 || fragment[0] != 2 {
		return random, 0, errors.New("tlsmirror: invalid server hello")
	}
	copy(random[:], fragment[6:38])
	sessionIDLen := int(fragment[38])
	cipherSuiteOffset := 39 + sessionIDLen
	if len(fragment) < cipherSuiteOffset+2 {
		return random, 0, errors.New("tlsmirror: invalid server hello session id")
	}
	return random, binary.BigEndian.Uint16(fragment[cipherSuiteOffset : cipherSuiteOffset+2]), nil
}

func hasZeroExplicitNonce(fragment []byte) bool {
	if len(fragment) < 8 {
		return false
	}
	for _, b := range fragment[:8] {
		if b != 0 {
			return false
		}
	}
	return true
}
