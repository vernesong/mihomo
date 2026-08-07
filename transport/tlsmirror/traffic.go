package tlsmirror

import (
	"bufio"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/metacubex/http"
)

type trafficHTTPTransport interface {
	RoundTrip(req *http.Request) (*http.Response, error)
}

type trafficHTTP1Transport struct {
	conn   net.Conn
	reader *bufio.Reader
}

func (t *trafficHTTP1Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Proto = "HTTP/1.1"
	req.ProtoMajor = 1
	req.ProtoMinor = 1
	if err := req.Write(t.conn); err != nil {
		return nil, err
	}
	return http.ReadResponse(t.reader, req)
}

type trafficHTTP2Transport struct {
	*http.ClientConn
}

func (t *trafficHTTP2Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Proto = "HTTP/2"
	req.ProtoMajor = 2
	req.ProtoMinor = 0
	return t.ClientConn.RoundTrip(req)
}

func newTrafficHTTPTransport(ctx context.Context, conn net.Conn, alpn string) (trafficHTTPTransport, error) {
	switch alpn {
	case "h2":
		protocols := new(http.Protocols)
		protocols.SetUnencryptedHTTP2(true)
		transport := &http.Transport{
			DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return conn, nil
			},
			Protocols:          protocols,
			DisableCompression: true,
		}
		clientConn, err := transport.NewClientConn(ctx, "https", ":0")
		if err != nil {
			return nil, err
		}
		return &trafficHTTP2Transport{ClientConn: clientConn}, nil
	case "http/1.1", "":
		return &trafficHTTP1Transport{
			conn:   conn,
			reader: bufio.NewReader(conn),
		}, nil
	default:
		return nil, fmt.Errorf("tlsmirror: unknown carrier ALPN %q", alpn)
	}
}

func runTrafficGenerator(ctx context.Context, conn net.Conn, cfg *TrafficGenerator, alpn string, ready func(), recall <-chan struct{}) {
	if cfg == nil || len(cfg.Steps) == 0 {
		_, _ = io.Copy(io.Discard, conn)
		return
	}

	transport, err := newTrafficHTTPTransport(ctx, conn, alpn)
	if err != nil {
		_ = conn.Close()
		return
	}

	var readyOnce sync.Once
	markReady := func() {
		readyOnce.Do(ready)
	}

	for current := 0; ctx.Err() == nil; {
		if current < 0 || current >= len(cfg.Steps) {
			_ = conn.Close()
			return
		}

		step := cfg.Steps[current]
		if err := runTrafficStep(ctx, transport, step, alpn); err != nil {
			return
		}
		if step.ConnectionReady {
			markReady()
		}
		if step.ConnectionRecallExit {
			select {
			case <-recall:
				_ = conn.Close()
				return
			case <-ctx.Done():
				_ = conn.Close()
				return
			default:
			}
		}
		next, ok, err := chooseNextTrafficStep(step, current)
		if err != nil {
			return
		}
		if !ok {
			current++
		} else {
			current = next
		}
	}
}

func trafficGeneratorWaitsForReady(cfg *TrafficGenerator) bool {
	if cfg == nil {
		return false
	}
	for _, step := range cfg.Steps {
		if step.ConnectionReady {
			return true
		}
	}
	return false
}

func runTrafficStep(ctx context.Context, transport trafficHTTPTransport, step TrafficStep, alpn string) error {
	return runTrafficStepWithClock(ctx, transport, step, alpn, time.Now, waitTrafficStep)
}

func runTrafficStepWithClock(ctx context.Context, transport trafficHTTPTransport, step TrafficStep, alpn string, now func() time.Time, wait func(context.Context, time.Duration) error) error {
	requestURL := &url.URL{
		Scheme: "https",
		Host:   step.Host,
		Path:   step.Path,
	}
	req := &http.Request{
		Method: step.Method,
		URL:    requestURL,
		Host:   requestURL.Hostname(),
		Header: make(http.Header, len(step.Headers)),
	}
	if len(step.Headers) > 0 {
		for _, header := range step.Headers {
			if header.Name == "" {
				continue
			}
			if header.Value != "" {
				req.Header.Add(header.Name, header.Value)
			}
			for _, value := range header.Values {
				req.Header.Add(header.Name, value)
			}
		}
	}

	start := now()
	resp, err := transport.RoundTrip(req)
	if err != nil {
		return err
	}

	finishRequest := func() error {
		_, copyErr := io.Copy(io.Discard, resp.Body)
		closeErr := resp.Body.Close()
		if copyErr != nil {
			return copyErr
		}
		return closeErr
	}
	if step.H2DoNotWaitForDownloadFinish && alpn == "h2" {
		go func() { _ = finishRequest() }()
	} else if err := finishRequest(); err != nil {
		return err
	}
	elapsed := now().Sub(start)
	if delay, err := step.WaitTime.Duration(); err != nil {
		return err
	} else if delay > elapsed {
		return wait(ctx, delay-elapsed)
	}
	return nil
}

func waitTrafficStep(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func chooseNextTrafficStep(step TrafficStep, current int) (int, bool, error) {
	if len(step.NextStep) == 0 {
		return 0, false, nil
	}
	total := int32(0)
	for _, candidate := range step.NextStep {
		total += candidate.Weight
	}
	if total <= 0 {
		return 0, false, fmt.Errorf("tlsmirror: invalid next-step weight total %d", total)
	}
	n, err := rand.Int(rand.Reader, big.NewInt(int64(total)))
	if err != nil {
		return 0, false, err
	}
	selected := int32(n.Int64())
	cursor := int32(0)
	for _, candidate := range step.NextStep {
		if cursor >= selected {
			return candidate.GotoLocation, true, nil
		}
		cursor += candidate.Weight
	}
	return current + 1, true, nil
}
