package vmess

import (
	"context"
	"io"
	"net"
	"net/url"

	N "github.com/metacubex/mihomo/common/net"

	"github.com/metacubex/http"
	"github.com/metacubex/randv2"
)

type h2Conn struct {
	net.Conn
	*http.ClientConn
	pwriter *io.PipeWriter
	res     *http.Response
	cfg     *H2Config
}

type H2Config struct {
	Hosts []string
	Path  string
}

func (hc *h2Conn) establishConn() error {
	preader, pwriter := io.Pipe()

	host := hc.cfg.Hosts[randv2.IntN(len(hc.cfg.Hosts))]
	path := hc.cfg.Path
	// TODO: connect use VMess Host instead of H2 Host
	req := http.Request{
		Method: "PUT",
		Host:   host,
		URL: &url.URL{
			Scheme: "https",
			Host:   host,
			Path:   path,
		},
		Proto:      "HTTP/2",
		ProtoMajor: 2,
		ProtoMinor: 0,
		Body:       preader,
		Header: map[string][]string{
			"Accept-Encoding": {"identity"},
		},
	}

	// it will be close at :  `func (hc *h2Conn) Close() error`
	res, err := hc.ClientConn.RoundTrip(&req)
	if err != nil {
		return err
	}

	hc.pwriter = pwriter
	hc.res = res

	return nil
}

// Read implements net.Conn.Read()
func (hc *h2Conn) Read(b []byte) (int, error) {
	if hc.res != nil && !hc.res.Close {
		n, err := hc.res.Body.Read(b)
		return n, err
	}

	if err := hc.establishConn(); err != nil {
		return 0, err
	}
	return hc.res.Body.Read(b)
}

// Write implements io.Writer.
func (hc *h2Conn) Write(b []byte) (int, error) {
	if hc.pwriter != nil {
		return hc.pwriter.Write(b)
	}

	if err := hc.establishConn(); err != nil {
		return 0, err
	}
	return hc.pwriter.Write(b)
}

func (hc *h2Conn) Close() error {
	if hc.pwriter != nil {
		if err := hc.pwriter.Close(); err != nil {
			return err
		}
	}
	return hc.Conn.Close()
}

func StreamH2Conn(ctx context.Context, conn net.Conn, cfg *H2Config) (_ net.Conn, err error) {
	if ctx.Done() != nil {
		done := N.SetupContextForConn(ctx, conn)
		defer done(&err)
	}

	// use h2c mode to disallow the net/http fallback to http1.1
	//
	// Note that this usage is only applicable to our own net/http fork.
	// The standard library also needs to mask the tls.Conn type for the conn returned by DialTLSContext,
	// see: https://github.com/golang/go/issues/79293#issuecomment-4426393534
	protocols := new(http.Protocols)
	protocols.SetUnencryptedHTTP2(true)
	transport := &http.Transport{
		DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return conn, nil
		},
		Protocols: protocols,
	}

	clientConn, err := transport.NewClientConn(ctx, "https", ":0")
	if err != nil {
		return nil, err
	}

	return &h2Conn{
		Conn:       conn,
		ClientConn: clientConn,
		cfg:        cfg,
	}, nil
}
