package outboundgroup

import (
	"context"
	"encoding/pem"
	"errors"
	"net"
	stdhttp "net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/adapter/provider"
	"github.com/metacubex/mihomo/common/structure"
	"github.com/metacubex/mihomo/component/ca"
	"github.com/metacubex/mihomo/component/smart"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

func TestParseResponseProbeURLs(t *testing.T) {
	r, err := parseResponseProbeURLs(map[string]string{
		"raw.githubusercontent.com": "https://raw.githubusercontent.com/github/gitignore/main/README.md",
		"API.themoviedb.org":        "https://api.themoviedb.org/3/configuration",
		"*.tmdb.org":                "https://image.tmdb.org/t/p/w92/a.png",
		"*.img.tmdb.org":            "https://img.tmdb.org/small.png",
		"plain.test":                "http://plain.test:8080/health",
	})
	if err != nil {
		t.Fatal(err)
	}
	for host, want := range map[string]string{
		"raw.githubusercontent.com":     "https://raw.githubusercontent.com/github/gitignore/main/README.md",
		"api.themoviedb.org":            "https://api.themoviedb.org/3/configuration",
		"Api.TheMovieDB.org.":           "https://api.themoviedb.org/3/configuration",
		"image.tmdb.org":                "https://image.tmdb.org/t/p/w92/a.png",
		"a.b.tmdb.org":                  "https://image.tmdb.org/t/p/w92/a.png",
		"x.img.tmdb.org":                "https://img.tmdb.org/small.png",
		"plain.test":                    "http://plain.test:8080/health",
		"tmdb.org":                      "",
		"githubusercontent.com":         "",
		"evilraw.githubusercontent.com": "",
	} {
		got, ok := r.match(host)
		if got != want || ok != (want != "") {
			t.Errorf("match(%q) = %q,%v want %q", host, got, ok, want)
		}
	}

	var nilURLs *responseProbeURLs
	if _, ok := nilURLs.match("a.com"); ok {
		t.Error("nil matcher matched")
	}

	for name, raw := range map[string]map[string]string{
		"relative url":   {"a.com": "/path"},
		"bad scheme":     {"a.com": "ftp://a.com/x"},
		"unparsable url": {"a.com": "https://a.com/%zz"},
		"no host":        {"a.com": "https:///x"},
		"empty host key": {"": "https://a.com/"},
		"bare wildcard":  {"*": "https://a.com/"},
		"empty wildcard": {"*.": "https://a.com/"},
		"host with path": {"a.com/x": "https://a.com/"},
		"host with port": {"a.com:443": "https://a.com/"},
		"inner wildcard": {"a.*.com": "https://a.com/"},
	} {
		if _, err := parseResponseProbeURLs(raw); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func TestSmartOptionDecodeResponseProbeURLs(t *testing.T) {
	decoder := structure.NewDecoder(structure.Option{TagName: "group", WeaklyTypedInput: true})
	opt := SmartOption{}
	err := decoder.Decode(map[string]any{
		"prefer-asn": true,
		"response-probe-urls": map[string]any{
			"raw.githubusercontent.com": "https://raw.githubusercontent.com/github/gitignore/main/README.md",
			"*.tmdb.org":                "https://api.themoviedb.org/3/configuration",
		},
	}, &opt)
	if err != nil {
		t.Fatal(err)
	}
	if len(opt.ResponseProbeURLs) != 2 || opt.ResponseProbeURLs["*.tmdb.org"] != "https://api.themoviedb.org/3/configuration" || !opt.PreferASN {
		t.Fatalf("decoded %+v", opt)
	}

	empty := SmartOption{}
	if err := decoder.Decode(map[string]any{}, &empty); err != nil || empty.ResponseProbeURLs != nil {
		t.Fatalf("default: %v %v", empty.ResponseProbeURLs, err)
	}
}

func TestParseProxyGroupRejectsInvalidResponseProbeURL(t *testing.T) {
	proxyMap := map[string]C.Proxy{
		"COMPATIBLE": adapter.NewProxy(outbound.NewCompatible()),
		"n1":         adapter.NewProxy(outbound.NewCompatible()),
	}
	_, err := ParseProxyGroup(map[string]any{
		"name":    "probe-invalid",
		"type":    "smart",
		"proxies": []string{"n1"},
		"response-probe-urls": map[string]any{
			"api.github.com": "api.github.com/zen",
		},
	}, proxyMap, map[string]P.ProxyProvider{}, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "response-probe-urls") {
		t.Fatalf("expected response-probe-urls error, got %v", err)
	}
}

func TestProbeThrottle(t *testing.T) {
	var th probeThrottle
	now := time.Unix(1_800_000_000, 0)

	if !th.allowNode("t", "n1", now) {
		t.Fatal("first probe denied")
	}
	if th.allowNode("t", "n1", now.Add(time.Second)) {
		t.Fatal("same (target,node) allowed within interval")
	}
	// throttling is per (target, node): other nodes and targets are not refreshed
	if !th.allowNode("t", "n2", now.Add(time.Second)) || !th.allowNode("t2", "n1", now.Add(time.Second)) {
		t.Fatal("other pair denied")
	}
	if th.allowNode("t", "n1", now.Add(smart.ProbeMinIntervalPerNode-time.Second)) {
		t.Fatal("allowed before 120s")
	}
	if !th.allowNode("t", "n1", now.Add(smart.ProbeMinIntervalPerNode)) {
		t.Fatal("denied after 120s")
	}

	// recent failure shortens the interval to 30s
	base := now.Add(10 * time.Minute)
	th.allowNode("f", "n1", base)
	th.noteFailure("f", "n1", base)
	if th.allowNode("f", "n1", base.Add(smart.ProbeMinIntervalAfterFailure-time.Second)) {
		t.Fatal("allowed before 30s after failure")
	}
	if !th.allowNode("f", "n1", base.Add(smart.ProbeMinIntervalAfterFailure)) {
		t.Fatal("denied 30s after failure")
	}

	// per target: at most 6 probes per minute, control probes included
	var tt probeThrottle
	base = now.Add(time.Hour)
	allowed := 0
	for i := 0; i < 10; i++ {
		if tt.allowNode("busy", "node"+string(rune('a'+i)), base) {
			allowed++
		}
	}
	if allowed != smart.ProbePerTargetPerMinute {
		t.Fatalf("per-target allowed %d", allowed)
	}
	if tt.allowControl("busy", base) {
		t.Fatal("control probe exceeded per-target budget")
	}
	if !tt.allowNode("busy", "nodez", base.Add(time.Minute)) {
		t.Fatal("per-target window did not reset")
	}

	// per group: burst of 10, refilled at 30 per minute
	var tg probeThrottle
	base = now.Add(2 * time.Hour)
	allowed = 0
	for i := 0; i < 20; i++ {
		if tg.allowNode("target"+string(rune('a'+i)), "n", base) {
			allowed++
		}
	}
	if allowed != smart.ProbeGroupBurst {
		t.Fatalf("group burst allowed %d", allowed)
	}
	if !tg.allowNode("other", "n", base.Add(2*time.Second)) {
		t.Fatal("group bucket did not refill")
	}

	// site-level suppression
	var ts probeThrottle
	ts.suppressClass("t", "challenge", now)
	if !ts.suppressed("t", "challenge", now.Add(29*time.Minute)) || ts.suppressed("t", "forbidden", now) || ts.suppressed("t", "challenge", now.Add(smart.ProbeSiteLevelSuppress)) {
		t.Fatal("suppression window wrong")
	}
}

// ---- integration: real adapter.Proxy -> StatusProbe -> verdict -> host status store ----

type probeTestNode struct {
	*outbound.Base
	addr string
}

func (n *probeTestNode) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	if metadata.RemoteAddress() != "example.com:443" {
		return nil, errors.New("unexpected target " + metadata.RemoteAddress())
	}
	var d net.Dialer
	c, err := d.DialContext(ctx, "tcp", n.addr)
	if err != nil {
		return nil, err
	}
	return outbound.NewConn(c, n), nil
}

// probeSite is a local HTTPS "site" whose answer is switched by the test.
type probeSite struct {
	srv  *httptest.Server
	mode atomic.Value
	hits atomic.Int32
}

func newProbeSite(t *testing.T) *probeSite {
	site := &probeSite{}
	site.mode.Store("ok")
	site.srv = httptest.NewTLSServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		site.hits.Add(1)
		switch site.mode.Load().(string) {
		case "ok":
			_, _ = w.Write([]byte("# gitignore"))
		case "429":
			w.Header().Set("Retry-After", "120")
			w.WriteHeader(stdhttp.StatusTooManyRequests)
		case "challenge":
			w.Header().Set("Server", "cloudflare")
			w.Header().Set("Cf-Mitigated", "challenge")
			w.WriteHeader(stdhttp.StatusForbidden)
			_, _ = w.Write([]byte("<!DOCTYPE html><html><head><title>Just a moment...</title>"))
		case "longpoll":
			select {
			case <-time.After(30 * time.Second):
				_, _ = w.Write([]byte(`{"ok":true,"result":[]}`))
			case <-r.Context().Done():
			}
		case "502":
			w.WriteHeader(stdhttp.StatusBadGateway)
		}
	}))
	t.Cleanup(site.srv.Close)
	pemCert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: site.srv.Certificate().Raw})
	if err := ca.AddCertificate(string(pemCert)); err != nil {
		t.Fatal(err)
	}
	return site
}

func (site *probeSite) node(name string) C.Proxy {
	return adapter.NewProxy(&probeTestNode{
		Base: outbound.NewBase(outbound.BaseOption{Name: name, Type: C.Direct}),
		addr: site.srv.Listener.Addr().String(),
	})
}

func newProbeTestSmart(t *testing.T, name string, nodes ...C.Proxy) *Smart {
	t.Helper()
	pd, err := provider.NewCompatibleProvider(name, nodes, provider.NewHealthCheck(nodes, "", 0, 0, true, nil))
	if err != nil {
		t.Fatal(err)
	}
	urls, err := parseResponseProbeURLs(map[string]string{"*.probe.test": "https://example.com/probe-file"})
	if err != nil {
		t.Fatal(err)
	}
	s := &Smart{
		GroupBase: NewGroupBase(GroupBaseOption{
			Name:           name,
			Type:           C.Smart,
			MaxFailedTimes: 2,
			Providers:      []P.ProxyProvider{pd},
		}),
		store:      smart.NewStore(nil),
		configName: "probe-test-config",
		testUrl:    C.DefaultTestURL,
		probeURLs:  urls,
	}
	s.hostFailLimit.Store(10)
	return s
}

func probeMetadata(host string) *C.Metadata {
	return &C.Metadata{NetWork: C.TCP, Type: C.HTTP, Host: host, DstPort: 443, WildcardTarget: host, SmartTarget: host, UUID: "probe-" + host}
}

func (s *Smart) testFailNodes(md *C.Metadata) map[string]int {
	nodes, _, _, _ := s.store.GetHostStatus(s.Name(), s.configName, md.WildcardTarget, int(s.hostFailLimit.Load()), md.SmartTarget)
	return nodes
}

func TestSmartResponseProbeIntegration(t *testing.T) {
	limited := newProbeSite(t)
	healthy := newProbeSite(t)
	bad := limited.node("node-limited")
	good := healthy.node("node-good")
	s := newProbeTestSmart(t, "probe-integration", bad, good)
	probeURL := s.responseProbeURL("raw.probe.test", 443)
	if probeURL != "https://example.com/probe-file" {
		t.Fatalf("probe url %s", probeURL)
	}

	t.Run("200 reachable", func(t *testing.T) {
		md := probeMetadata("ok.probe.test")
		limited.mode.Store("ok")
		if v := s.runResponseProbe(md, bad, probeURL, ""); v.Kind != smart.VerdictOK {
			t.Fatalf("verdict %s %s", v.Kind, v.Reason)
		}
		if nodes := s.testFailNodes(md); len(nodes) != 0 {
			t.Fatalf("records %v", nodes)
		}
	})

	t.Run("429 rate limited", func(t *testing.T) {
		md := probeMetadata("raw.probe.test")
		limited.mode.Store("429")
		v := s.runResponseProbe(md, bad, probeURL, "")
		if v.Kind != smart.VerdictRateLimited || v.Cooldown != 120*time.Second {
			t.Fatalf("verdict %s %s %s", v.Kind, v.Cooldown, v.Reason)
		}
		if nodes := s.testFailNodes(md); nodes["node-limited"] != smart.HostCodeRateLimited || nodes["node-good"] != 0 {
			t.Fatalf("records %v", nodes)
		}
	})

	t.Run("challenge confirmed by control node", func(t *testing.T) {
		md := probeMetadata("cf.probe.test")
		limited.mode.Store("challenge")
		healthy.mode.Store("ok")
		before := healthy.hits.Load()
		v := s.runResponseProbe(md, bad, probeURL, "")
		if v.Kind != smart.VerdictBlocked || v.Class != "challenge" {
			t.Fatalf("verdict %s %s", v.Kind, v.Reason)
		}
		if healthy.hits.Load() != before+1 {
			t.Fatalf("control node not probed")
		}
		if nodes := s.testFailNodes(md); nodes["node-limited"] != 2 || nodes["node-good"] != 0 {
			t.Fatalf("records %v", nodes)
		}
	})

	t.Run("site-level challenge not counted", func(t *testing.T) {
		md := probeMetadata("uam.probe.test")
		limited.mode.Store("challenge")
		healthy.mode.Store("challenge")
		v := s.runResponseProbe(md, bad, probeURL, "")
		if v.Kind != smart.VerdictIgnore {
			t.Fatalf("verdict %s %s", v.Kind, v.Reason)
		}
		if nodes := s.testFailNodes(md); len(nodes) != 0 {
			t.Fatalf("records %v", nodes)
		}
		// suppressed afterwards: no further control probe
		before := healthy.hits.Load()
		if v := s.runResponseProbe(md, bad, probeURL, ""); v.Kind != smart.VerdictIgnore || healthy.hits.Load() != before {
			t.Fatalf("not suppressed: %s, control hits %d", v.Kind, healthy.hits.Load()-before)
		}
		healthy.mode.Store("ok")
	})

	t.Run("origin 502 ignored", func(t *testing.T) {
		md := probeMetadata("down.probe.test")
		limited.mode.Store("502")
		if v := s.runResponseProbe(md, bad, probeURL, ""); v.Kind != smart.VerdictIgnore {
			t.Fatalf("verdict %s", v.Kind)
		}
		if nodes := s.testFailNodes(md); len(nodes) != 0 {
			t.Fatalf("records %v", nodes)
		}
	})

	t.Run("pinned group records nothing", func(t *testing.T) {
		md := probeMetadata("pinned.probe.test")
		limited.mode.Store("429")
		s.selected = "node-good"
		defer func() { s.selected = "" }()
		s.runResponseProbe(md, bad, probeURL, "")
		if nodes := s.testFailNodes(md); len(nodes) != 0 {
			t.Fatalf("records %v", nodes)
		}
	})
}

func TestSmartResponseProbeNoControlOnlyCounts(t *testing.T) {
	site := newProbeSite(t)
	only := site.node("node-only")
	s := newProbeTestSmart(t, "probe-no-control", only)
	md := probeMetadata("solo.probe.test")
	site.mode.Store("challenge")
	probeURL := s.responseProbeURL(md.Host, 443)

	if v := s.runResponseProbe(md, only, probeURL, ""); v.Kind != smart.VerdictCount {
		t.Fatalf("verdict %s %s", v.Kind, v.Reason)
	}
	if nodes := s.testFailNodes(md); len(nodes) != 0 {
		t.Fatalf("blocked immediately without control: %v", nodes)
	}
	// maxFailedTimes (2) counts turn into a code 3 block
	s.runResponseProbe(md, only, probeURL, "")
	if nodes := s.testFailNodes(md); nodes["node-only"] != 3 {
		t.Fatalf("records %v", nodes)
	}
}

// Long polling: the site holds the request for 30s. The probe must end at the probe
// timeout, never produce a rate limit / block verdict, and only count code 3.
func TestSmartResponseProbeLongPollingNotMisjudged(t *testing.T) {
	site := newProbeSite(t)
	node := site.node("node-longpoll")
	other := newProbeSite(t).node("node-other")
	s := newProbeTestSmart(t, "probe-longpoll", node, other)
	md := probeMetadata("api.probe.test")
	site.mode.Store("longpoll")
	probeURL := s.responseProbeURL(md.Host, 443)

	start := time.Now()
	v := s.runResponseProbe(md, node, probeURL, "")
	if took := time.Since(start); took > C.DefaultTCPTimeout+2*time.Second {
		t.Fatalf("probe took %s", took)
	}
	if v.Kind != smart.VerdictCount || v.Class != "timeout" {
		t.Fatalf("verdict %s %s", v.Kind, v.Reason)
	}
	if nodes := s.testFailNodes(md); len(nodes) != 0 {
		t.Fatalf("first timeout blocked the node: %v", nodes)
	}

	v = s.runResponseProbe(md, node, probeURL, "")
	if v.Kind != smart.VerdictCount {
		t.Fatalf("verdict %s", v.Kind)
	}
	if nodes := s.testFailNodes(md); nodes["node-longpoll"] != 3 || nodes["node-other"] != 0 {
		t.Fatalf("expected only a code 3 count block, got %v", nodes)
	}
}

func TestResponseProbeEligible(t *testing.T) {
	s := newProbeTestSmart(t, "probe-eligible", adapter.NewProxy(outbound.NewCompatible()))
	md := func(host string, port uint16) *C.Metadata {
		return &C.Metadata{NetWork: C.TCP, Type: C.HTTP, Host: host, DstPort: port}
	}
	inner := md("a.com", 443)
	inner.Type = C.INNER
	for _, tt := range []struct {
		name string
		md   *C.Metadata
		down float64
		udp  bool
		want bool
	}{
		{"443 small", md("a.com", 443), 0.01, false, true},
		{"443 up to 256KiB", md("a.com", 443), 0.2, false, true},
		{"443 large download", md("a.com", 443), 0.3, false, false},
		{"udp", md("a.com", 443), 0.01, true, false},
		{"no host", md("", 443), 0.01, false, false},
		{"inner", inner, 0.01, false, false},
		{"8443 unconfigured", md("a.com", 8443), 0.01, false, false},
		{"8443 configured", md("x.probe.test", 8443), 0.01, false, true},
	} {
		if got := s.responseProbeEligible(tt.md, tt.down, tt.udp); got != tt.want {
			t.Errorf("%s: got %v", tt.name, got)
		}
	}
	if u := s.responseProbeURL("a.com", 8443); !strings.HasPrefix(u, "https://a.com:8443/?z=") {
		t.Errorf("default url %s", u)
	}
	if u := s.responseProbeURL("a.com", 443); !strings.HasPrefix(u, "https://a.com/?z=") {
		t.Errorf("default url %s", u)
	}
}

// checkNodeQuality only schedules the probe; the verdict lands asynchronously.
func TestCheckNodeQualitySchedulesProbe(t *testing.T) {
	site := newProbeSite(t)
	node := site.node("node-async")
	other := newProbeSite(t).node("node-async-other")
	s := newProbeTestSmart(t, "probe-async", node, other)
	md := probeMetadata("async.probe.test")
	site.mode.Store("429")

	weight, degraded, checked, code := s.checkNodeQuality(nil, md, node, md.WildcardTarget, "addr", node.Name(),
		1, 1, 50, 0.001, 0.001, "tcp", "", false, 0, 0)
	if weight != 1 || degraded || checked || code != 0 {
		t.Fatalf("checkNodeQuality returned %v %v %v %v", weight, degraded, checked, code)
	}
	deadline := time.Now().Add(C.DefaultTCPTimeout + 2*time.Second)
	for time.Now().Before(deadline) {
		if s.testFailNodes(md)["node-async"] == smart.HostCodeRateLimited {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if nodes := s.testFailNodes(md); nodes["node-async"] != smart.HostCodeRateLimited {
		t.Fatalf("records %v", nodes)
	}
	// throttled: an immediate second connection close does not probe again
	hits := site.hits.Load()
	s.checkNodeQuality(nil, md, node, md.WildcardTarget, "addr", node.Name(), 1, 1, 50, 0.001, 0.001, "tcp", "", false, 0, 0)
	time.Sleep(200 * time.Millisecond)
	if site.hits.Load() != hits {
		t.Fatal("probe not throttled")
	}
}
