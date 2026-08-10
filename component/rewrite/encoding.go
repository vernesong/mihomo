package rewrite

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"fmt"
	"io"
	"strings"

	"github.com/andybalholm/brotli"
)

type contentCoding struct {
	name       string
	rawDeflate bool
}

func decodeBody(body []byte, headerValues []string) ([]byte, []contentCoding, error) {
	codings := parseContentCodings(headerValues)
	decoded := body
	for index := len(codings) - 1; index >= 0; index-- {
		coding := &codings[index]
		var err error
		switch coding.name {
		case "identity":
			continue
		case "gzip", "x-gzip":
			decoded, err = readGzip(decoded)
		case "deflate":
			decoded, coding.rawDeflate, err = readDeflate(decoded)
		case "br":
			decoded, err = io.ReadAll(brotli.NewReader(bytes.NewReader(decoded)))
		default:
			return nil, nil, fmt.Errorf("unsupported content encoding %q", coding.name)
		}
		if err != nil {
			return nil, nil, err
		}
	}
	return decoded, codings, nil
}

func encodeBody(body []byte, codings []contentCoding) ([]byte, error) {
	encoded := body
	for _, coding := range codings {
		var err error
		switch coding.name {
		case "identity":
			continue
		case "gzip", "x-gzip":
			encoded, err = writeGzip(encoded)
		case "deflate":
			encoded, err = writeDeflate(encoded, coding.rawDeflate)
		case "br":
			encoded, err = writeBrotli(encoded)
		default:
			return nil, fmt.Errorf("unsupported content encoding %q", coding.name)
		}
		if err != nil {
			return nil, err
		}
	}
	return encoded, nil
}

func parseContentCodings(headerValues []string) []contentCoding {
	var codings []contentCoding
	for _, headerValue := range headerValues {
		for _, value := range strings.Split(headerValue, ",") {
			value = strings.ToLower(strings.TrimSpace(value))
			if value != "" {
				codings = append(codings, contentCoding{name: value})
			}
		}
	}
	return codings
}

func readGzip(body []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

func readDeflate(body []byte) ([]byte, bool, error) {
	reader, err := zlib.NewReader(bytes.NewReader(body))
	if err == nil {
		defer reader.Close()
		decoded, readErr := io.ReadAll(reader)
		return decoded, false, readErr
	}

	rawReader := flate.NewReader(bytes.NewReader(body))
	defer rawReader.Close()
	decoded, rawErr := io.ReadAll(rawReader)
	return decoded, true, rawErr
}

func writeGzip(body []byte) ([]byte, error) {
	var buffer bytes.Buffer
	writer := gzip.NewWriter(&buffer)
	if _, err := writer.Write(body); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func writeDeflate(body []byte, raw bool) ([]byte, error) {
	var buffer bytes.Buffer
	var writer io.WriteCloser
	if raw {
		writer, _ = flate.NewWriter(&buffer, flate.DefaultCompression)
	} else {
		writer = zlib.NewWriter(&buffer)
	}
	if _, err := writer.Write(body); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func writeBrotli(body []byte) ([]byte, error) {
	var buffer bytes.Buffer
	writer := brotli.NewWriter(&buffer)
	if _, err := writer.Write(body); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}
