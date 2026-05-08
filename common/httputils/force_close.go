package httputils

import (
	"io"

	"github.com/metacubex/http"
)

type closeIdleTransport interface {
	CloseIdleConnections()
}

type closeHttp2Connections interface {
	CloseHttp2Connections()
}

func CloseTransport(roundTripper http.RoundTripper) {
	if tr, ok := roundTripper.(closeIdleTransport); ok {
		tr.CloseIdleConnections() // for *http.Transport
	}
	if tr, ok := roundTripper.(closeHttp2Connections); ok {
		tr.CloseHttp2Connections() // for *http.Transport in our own fork
	}
	if tr, ok := roundTripper.(io.Closer); ok {
		_ = tr.Close() // for *http3.Transport
	}
}
