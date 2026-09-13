package adapter

import (
	"context"
	"encoding/pem"
	"errors"
	"net"
	stdhttp "net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/component/ca"
	"github.com/metacubex/mihomo/component/smart"
	C "github.com/metacubex/mihomo/constant"

	"github.com/metacubex/http"
)

// fakeNode is a proxy adapter that "exits" by dialing a local address chosen per target.
type fakeNode struct {
	*outbound.Base
	mu     sync.Mutex
	routes map[string]string // host:port -> local listener
	dials  atomic.Int32
	dialed []string
}

func newFakeNode(name string, routes map[string]string) *fakeNode {
	return &fakeNode{Base: outbound.NewBase(outbound.BaseOption{Name: name, Type: C.Direct}), routes: routes}
}

func (n *fakeNode) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	n.dials.Add(1)
	target := metadata.RemoteAddress()
	n.mu.Lock()
	n.dialed = append(n.dialed, target)
	addr, ok := n.routes[target]
	n.mu.Unlock()
	if !ok {
		return nil, errors.New("fake node: no route to " + target)
	}
	var d net.Dialer
	c, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	return outbound.NewConn(c, n), nil
}

func trustServer(t *testing.T, srv *httptest.Server) {
	t.Helper()
	pemCert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	if err := ca.AddCertificate(string(pemCert)); err != nil {
		t.Fatal(err)
	}
}

// guardDefaultResolver fails the test if anything resolves through net.DefaultResolver,
// which is what the Transport's package-level dialer would do without DialContext.
func guardDefaultResolver(t *testing.T) *atomic.Int32 {
	t.Helper()
	var hits atomic.Int32
	old := net.DefaultResolver
	net.DefaultResolver = &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
		hits.Add(1)
		return nil, errors.New("default resolver must not be used")
	}}
	t.Cleanup(func() { net.DefaultResolver = old })
	return &hits
}

func TestStatusRequestTransportDialHooks(t *testing.T) {
	var checked atomic.Bool
	statusTestTransport = func(transport *http.Transport) {
		if transport.DialContext == nil {
			t.Errorf("Transport.DialContext is nil")
		}
		if transport.DialTLSContext == nil {
			t.Errorf("Transport.DialTLSContext is nil")
		}
		checked.Store(true)
	}
	t.Cleanup(func() { statusTestTransport = nil })

	node := NewProxy(newFakeNode("hooks", map[string]string{}))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, _, _ = node.StatusTest(ctx, "https://example.com/")
	_, _ = node.StatusProbe(ctx, "https://example.com/", smart.ProbeOptions{})
	if !checked.Load() {
		t.Fatal("transport hook not reached")
	}
}

func TestStatusRedirectToHTTPDialsThroughNode(t *testing.T) {
	resolverHits := guardDefaultResolver(t)

	plain := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		w.WriteHeader(stdhttp.StatusOK)
	}))
	defer plain.Close()
	tlsSrv := httptest.NewTLSServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		stdhttp.Redirect(w, r, "http://example.com/final", stdhttp.StatusFound)
	}))
	defer tlsSrv.Close()
	trustServer(t, tlsSrv)

	routes := map[string]string{
		"example.com:443": tlsSrv.Listener.Addr().String(),
		"example.com:80":  plain.Listener.Addr().String(),
	}

	t.Run("StatusTest", func(t *testing.T) {
		fn := newFakeNode("redir-test", routes)
		status, ok, err := NewProxy(fn).StatusTest(context.Background(), "https://example.com/")
		if err != nil || status != 200 || !ok {
			t.Fatalf("status=%d ok=%v err=%v", status, ok, err)
		}
		if fn.dials.Load() != 2 {
			t.Fatalf("node dials = %d (%v), want 2", fn.dials.Load(), fn.dialed)
		}
	})
	t.Run("StatusProbe same host", func(t *testing.T) {
		fn := newFakeNode("redir-probe", routes)
		res, err := NewProxy(fn).StatusProbe(context.Background(), "https://example.com/", smart.ProbeOptions{MaxBodyBytes: smart.ProbeMaxBodyBytes})
		if err != nil || res.StatusCode != 200 {
			t.Fatalf("res=%+v err=%v", res, err)
		}
		if fn.dials.Load() != 2 {
			t.Fatalf("node dials = %d (%v), want 2", fn.dials.Load(), fn.dialed)
		}
	})
	if resolverHits.Load() != 0 {
		t.Fatalf("net.DefaultResolver used %d times", resolverHits.Load())
	}
}

func TestStatusProbeCrossHostRedirectNotFollowed(t *testing.T) {
	var otherHits atomic.Int32
	other := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		otherHits.Add(1)
		w.WriteHeader(stdhttp.StatusTooManyRequests)
	}))
	defer other.Close()
	tlsSrv := httptest.NewTLSServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		stdhttp.Redirect(w, r, "http://other.test/", stdhttp.StatusMovedPermanently)
	}))
	defer tlsSrv.Close()
	trustServer(t, tlsSrv)

	fn := newFakeNode("cross", map[string]string{
		"example.com:443": tlsSrv.Listener.Addr().String(),
		"other.test:80":   other.Listener.Addr().String(),
	})
	res, err := NewProxy(fn).StatusProbe(context.Background(), "https://example.com/", smart.ProbeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != 301 || otherHits.Load() != 0 {
		t.Fatalf("status=%d otherHits=%d", res.StatusCode, otherHits.Load())
	}
	if v := smart.ClassifyResponse(res.StatusCode, res.Header, res.Body, time.Now()); v.Kind != smart.VerdictOK {
		t.Fatalf("cross-host redirect verdict %s", v.Kind)
	}
}

func TestStatusProbeScenarios(t *testing.T) {
	var gotAuth, gotCookie, gotEncoding, gotRange atomic.Value
	srv := httptest.NewTLSServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		gotAuth.Store(r.Header.Get("Authorization"))
		gotCookie.Store(r.Header.Get("Cookie"))
		gotEncoding.Store(r.Header.Get("Accept-Encoding"))
		gotRange.Store(r.Header.Get("Range"))
		switch r.URL.Path {
		case "/ok":
			_, _ = w.Write([]byte("fine"))
		case "/429":
			w.Header().Set("Retry-After", "120")
			w.WriteHeader(stdhttp.StatusTooManyRequests)
		case "/challenge":
			w.Header().Set("Server", "cloudflare")
			w.Header().Set("Cf-Mitigated", "challenge")
			w.WriteHeader(stdhttp.StatusForbidden)
			_, _ = w.Write([]byte("<html><head><title>Just a moment...</title></head></html>"))
		case "/longpoll":
			select {
			case <-time.After(30 * time.Second):
				_, _ = w.Write([]byte(`{"ok":true,"result":[]}`))
			case <-r.Context().Done():
			}
		}
	}))
	defer srv.Close()
	trustServer(t, srv)

	node := NewProxy(newFakeNode("scenarios", map[string]string{"example.com:443": srv.Listener.Addr().String()}))
	probe := func(path string) (*smart.ProbeResult, smart.ResponseVerdict, time.Duration) {
		ctx, cancel := context.WithTimeout(context.Background(), C.DefaultTCPTimeout)
		defer cancel()
		start := time.Now()
		res, err := node.StatusProbe(ctx, "https://example.com"+path, smart.ProbeOptions{MaxBodyBytes: smart.ProbeMaxBodyBytes})
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		return res, smart.ClassifyResponse(res.StatusCode, res.Header, res.Body, time.Now()), time.Since(start)
	}

	if res, v, _ := probe("/ok"); res.StatusCode != 200 || v.Kind != smart.VerdictOK || string(res.Body) != "fine" {
		t.Fatalf("/ok: %d %s %q", res.StatusCode, v.Kind, res.Body)
	}
	if gotAuth.Load() != "" || gotCookie.Load() != "" {
		t.Fatalf("probe carried credentials: auth=%q cookie=%q", gotAuth.Load(), gotCookie.Load())
	}
	if gotEncoding.Load() != "identity" || gotRange.Load() != "bytes=0-4095" {
		t.Fatalf("encoding=%q range=%q", gotEncoding.Load(), gotRange.Load())
	}
	if res, v, _ := probe("/429"); res.StatusCode != 429 || v.Kind != smart.VerdictRateLimited || v.Cooldown != 120*time.Second {
		t.Fatalf("/429: %d %s %s", res.StatusCode, v.Kind, v.Cooldown)
	}
	if res, v, _ := probe("/challenge"); res.StatusCode != 403 || v.Kind != smart.VerdictSuspect || v.Class != "challenge" {
		t.Fatalf("/challenge: %d %s %s", res.StatusCode, v.Kind, v.Class)
	}
	// long polling: the probe times out after C.DefaultTCPTimeout, which is only a counted
	// transport verdict, never rate limited / blocked
	res, v, took := probe("/longpoll")
	if res.StatusCode != smart.ProbeTimeoutStatus || v.Kind != smart.VerdictCount || v.Class != "timeout" {
		t.Fatalf("/longpoll: %d %s %s", res.StatusCode, v.Kind, v.Class)
	}
	if took > C.DefaultTCPTimeout+2*time.Second {
		t.Fatalf("/longpoll probe took %s", took)
	}

	// legacy StatusTest semantics are unchanged: 429 ok, cloudflare 403 ok, timeout -> 599 not ok
	ctx, cancel := context.WithTimeout(context.Background(), C.DefaultTCPTimeout)
	defer cancel()
	if status, ok, err := node.StatusTest(ctx, "https://example.com/429"); err != nil || status != 429 || !ok {
		t.Fatalf("StatusTest /429: %d %v %v", status, ok, err)
	}
	if status, ok, err := node.StatusTest(ctx, "https://example.com/challenge"); err != nil || status != 403 || !ok {
		t.Fatalf("StatusTest /challenge: %d %v %v", status, ok, err)
	}
}

func TestStatusProbeUntrustedCertificate(t *testing.T) {
	// the node answers with a certificate that is not valid for the probed host (untrusted CA when
	// run alone, hostname mismatch once another test trusted the shared httptest certificate)
	srv := httptest.NewUnstartedServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {}))
	srv.StartTLS()
	defer srv.Close()
	if srv.Certificate() == nil {
		t.Skip("no certificate")
	}
	node := NewProxy(newFakeNode("mitm", map[string]string{"hijack.test:443": srv.Listener.Addr().String()}))
	ctx, cancel := context.WithTimeout(context.Background(), C.DefaultTCPTimeout)
	defer cancel()
	_, err := node.StatusProbe(ctx, "https://hijack.test/", smart.ProbeOptions{})
	if err == nil {
		t.Fatal("expected certificate error")
	}
	if v := smart.ClassifyProbeError(err); v.Kind != smart.VerdictSuspect || v.Class != "tls-hijack" {
		t.Fatalf("verdict %s/%s for %v", v.Kind, v.Class, err)
	}
}
