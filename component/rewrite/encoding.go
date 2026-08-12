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

// BodyEncoding records the Content-Encoding chain needed to restore a decoded HTTP body.
type BodyEncoding struct {
	codings []contentCoding
}

// DecodeBodyContent decodes an HTTP body according to its Content-Encoding header values.
func DecodeBodyContent(body []byte, headerValues []string, maxSize int64) ([]byte, BodyEncoding, bool, error) {
	decoded, codings, tooLarge, err := decodeBodyLimit(body, headerValues, maxSize)
	return decoded, BodyEncoding{codings: codings}, tooLarge, err
}

// ParseBodyEncoding records a Content-Encoding chain without decoding a body.
func ParseBodyEncoding(headerValues []string) BodyEncoding {
	return BodyEncoding{codings: parseContentCodings(headerValues)}
}

// Matches reports whether headerValues describe the same Content-Encoding chain.
func (e BodyEncoding) Matches(headerValues []string) bool {
	other := parseContentCodings(headerValues)
	if len(e.codings) != len(other) {
		return false
	}
	for index := range e.codings {
		if e.codings[index].name != other[index].name {
			return false
		}
	}
	return true
}

// Encode restores the recorded Content-Encoding chain to a decoded HTTP body.
func (e BodyEncoding) Encode(body []byte) ([]byte, error) {
	return encodeBody(body, e.codings)
}

func decodeBody(body []byte, headerValues []string) ([]byte, []contentCoding, error) {
	decoded, codings, _, err := decodeBodyLimit(body, headerValues, -1)
	return decoded, codings, err
}

func decodeBodyLimit(body []byte, headerValues []string, maxSize int64) ([]byte, []contentCoding, bool, error) {
	codings := parseContentCodings(headerValues)
	decoded := body
	if maxSize >= 0 && int64(len(decoded)) > maxSize {
		return nil, codings, true, nil
	}
	for index := len(codings) - 1; index >= 0; index-- {
		coding := &codings[index]
		var tooLarge bool
		var err error
		switch coding.name {
		case "identity":
			continue
		case "gzip", "x-gzip":
			decoded, tooLarge, err = readGzip(decoded, maxSize)
		case "deflate":
			decoded, coding.rawDeflate, tooLarge, err = readDeflate(decoded, maxSize)
		case "br":
			decoded, tooLarge, err = readBodyLimit(brotli.NewReader(bytes.NewReader(decoded)), maxSize)
		default:
			return nil, nil, false, fmt.Errorf("unsupported content encoding %q", coding.name)
		}
		if err != nil {
			return nil, nil, false, err
		}
		if tooLarge {
			return nil, codings, true, nil
		}
	}
	return decoded, codings, false, nil
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

func readGzip(body []byte, maxSize int64) ([]byte, bool, error) {
	reader, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		return nil, false, err
	}
	defer reader.Close()
	return readBodyLimit(reader, maxSize)
}

func readDeflate(body []byte, maxSize int64) ([]byte, bool, bool, error) {
	reader, err := zlib.NewReader(bytes.NewReader(body))
	if err == nil {
		defer reader.Close()
		decoded, tooLarge, readErr := readBodyLimit(reader, maxSize)
		return decoded, false, tooLarge, readErr
	}

	rawReader := flate.NewReader(bytes.NewReader(body))
	defer rawReader.Close()
	decoded, tooLarge, rawErr := readBodyLimit(rawReader, maxSize)
	return decoded, true, tooLarge, rawErr
}

func readBodyLimit(reader io.Reader, maxSize int64) ([]byte, bool, error) {
	if maxSize < 0 {
		content, err := io.ReadAll(reader)
		return content, false, err
	}
	content, err := io.ReadAll(io.LimitReader(reader, maxSize+1))
	if err != nil {
		return nil, false, err
	}
	if int64(len(content)) > maxSize {
		return nil, true, nil
	}
	return content, false, nil
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
