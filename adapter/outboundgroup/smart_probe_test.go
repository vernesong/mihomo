package outboundgroup

import (
	"context"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	stdhttp "net/http"
	"net/http/httptest"
	"strconv"
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
		"Dotted.Test.":              "https://dotted.test/ping",
		"*.wild.test.":              "https://wild.test/ping",
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
		"dotted.test":                   "https://dotted.test/ping",
		"dotted.test.":                  "https://dotted.test/ping",
		"a.wild.test":                   "https://wild.test/ping",
		"a.wild.test.":                  "https://wild.test/ping",
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
		"only dot":       {".": "https://a.com/"},
		"wildcard dot":   {"*..": "https://a.com/"},
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
	if !ts.suppressed("t", "challenge", now.Add(smart.ProbeSiteLevelSuppress-time.Second)) || ts.suppressed("t", "forbidden", now) || ts.suppressed("t", "challenge", now.Add(smart.ProbeSiteLevelSuppress)) {
		t.Fatal("suppression window wrong")
	}
}

// ---- integration: real adapter.Proxy -> StatusProbe -> verdict -> host status store ----

var probeTestSeq atomic.Int64

// uniqueName keeps global smart caches (hostStatusCache, blocked nodes) apart across -count runs.
func uniqueName(prefix string) string {
	return prefix + "-" + strconv.FormatInt(time.Now().UnixNano(), 36) + "-" + strconv.FormatInt(probeTestSeq.Add(1), 10)
}

type probeTestNode struct {
	*outbound.Base
	addr    string
	dialErr bool
}

func (n *probeTestNode) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	if n.dialErr {
		return nil, errors.New("node down")
	}
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
		case "503":
			w.Header().Set("Retry-After", "300")
			w.WriteHeader(stdhttp.StatusServiceUnavailable)
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
		case "reset":
			if hj, ok := w.(stdhttp.Hijacker); ok {
				if c, _, err := hj.Hijack(); err == nil {
					_ = c.Close()
				}
			}
		}
	}))
	t.Cleanup(site.srv.Close)
	pemCert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: site.srv.Certificate().Raw})
	if err := ca.AddCertificate(string(pemCert)); err != nil {
		t.Fatal(err)
	}
	return site
}

// node builds a proxy exiting through this site; provider and server identify it for control selection.
func (site *probeSite) node(name, providerName, server string) C.Proxy {
	return adapter.NewProxy(&probeTestNode{
		Base: outbound.NewBase(outbound.BaseOption{Name: name, Type: C.Direct, ProviderName: providerName, Addr: server}),
		addr: site.srv.Listener.Addr().String(),
	})
}

func newProbeTestSmart(t *testing.T, name string, nodes ...C.Proxy) *Smart {
	t.Helper()
	name = uniqueName(name)
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

const probeTestURL = "https://example.com/probe-file"

func TestSmartResponseProbeIntegration(t *testing.T) {
	limited := newProbeSite(t)
	control1 := newProbeSite(t)
	control2 := newProbeSite(t)
	bad := limited.node("node-limited", "sub1", "a.server:443")
	c1 := control1.node("node-control1", "sub2", "b.server:443")
	c2 := control2.node("node-control2", "sub3", "c.server:443")
	s := newProbeTestSmart(t, "probe-integration", bad, c1, c2)
	if u := s.responseProbeURL("raw.probe.test", 443); u != probeTestURL {
		t.Fatalf("probe url %s", u)
	}
	setModes := func(b, m1, m2 string) {
		limited.mode.Store(b)
		control1.mode.Store(m1)
		control2.mode.Store(m2)
	}

	t.Run("200 reachable", func(t *testing.T) {
		md := probeMetadata("ok.probe.test")
		setModes("ok", "ok", "ok")
		if v := s.runResponseProbe(md, bad, probeTestURL, ""); v.Kind != smart.VerdictOK {
			t.Fatalf("verdict %s %s", v.Kind, v.Reason)
		}
		if nodes := s.testFailNodes(md); len(nodes) != 0 {
			t.Fatalf("records %v", nodes)
		}
	})

	t.Run("429 rate limited without confirmation", func(t *testing.T) {
		md := probeMetadata("raw.probe.test")
		setModes("429", "ok", "ok")
		before := control1.hits.Load() + control2.hits.Load()
		v := s.runResponseProbe(md, bad, probeTestURL, "")
		if v.Kind != smart.VerdictRateLimited || v.Cooldown != 120*time.Second {
			t.Fatalf("verdict %s %s %s", v.Kind, v.Cooldown, v.Reason)
		}
		if control1.hits.Load()+control2.hits.Load() != before {
			t.Fatal("429 probed control nodes")
		}
		if nodes := s.testFailNodes(md); nodes["node-limited"] != smart.HostCodeRateLimited || len(nodes) != 1 {
			t.Fatalf("records %v", nodes)
		}
	})

	t.Run("challenge confirmed by a control node", func(t *testing.T) {
		md := probeMetadata("cf.probe.test")
		setModes("challenge", "ok", "ok")
		v := s.runResponseProbe(md, bad, probeTestURL, "")
		if v.Kind != smart.VerdictBlocked || v.Class != "challenge" {
			t.Fatalf("verdict %s %s", v.Kind, v.Reason)
		}
		if nodes := s.testFailNodes(md); nodes["node-limited"] != 2 || len(nodes) != 1 {
			t.Fatalf("records %v", nodes)
		}
	})

	t.Run("both controls challenged is site-level", func(t *testing.T) {
		md := probeMetadata("uam.probe.test")
		setModes("challenge", "challenge", "challenge")
		before1, before2 := control1.hits.Load(), control2.hits.Load()
		v := s.runResponseProbe(md, bad, probeTestURL, "")
		if v.Kind != smart.VerdictIgnore || !strings.Contains(v.Reason, "site-level") {
			t.Fatalf("verdict %s %s", v.Kind, v.Reason)
		}
		if control1.hits.Load() != before1+1 || control2.hits.Load() != before2+1 {
			t.Fatal("expected exactly two control probes")
		}
		if nodes := s.testFailNodes(md); len(nodes) != 0 {
			t.Fatalf("records %v", nodes)
		}
		// suppressed afterwards for this (target, class) only
		before1, before2 = control1.hits.Load(), control2.hits.Load()
		if v := s.runResponseProbe(md, bad, probeTestURL, ""); v.Kind != smart.VerdictIgnore || control1.hits.Load()+control2.hits.Load() != before1+before2 {
			t.Fatalf("not suppressed: %s", v.Reason)
		}
		if s.probeThrottle.suppressed("other.probe.test", "challenge", time.Now()) || s.probeThrottle.suppressed(md.WildcardTarget, "forbidden", time.Now()) {
			t.Fatal("suppression leaked to another target or class")
		}
	})

	t.Run("one control challenged one down is unconfirmed", func(t *testing.T) {
		down := adapter.NewProxy(&probeTestNode{Base: outbound.NewBase(outbound.BaseOption{Name: "node-down", Type: C.Direct, ProviderName: "sub4", Addr: "d.server:443"}), dialErr: true})
		s2 := newProbeTestSmart(t, "probe-one-control", bad, c1, down)
		md := probeMetadata("half.probe.test")
		setModes("challenge", "challenge", "ok")
		v := s2.runResponseProbe(md, bad, probeTestURL, "")
		if v.Kind != smart.VerdictIgnore || strings.Contains(v.Reason, "site-level") {
			t.Fatalf("verdict %s %s", v.Kind, v.Reason)
		}
		if nodes := s2.testFailNodes(md); len(nodes) != 0 {
			t.Fatalf("records %v", nodes)
		}
		if s2.probeThrottle.suppressed(md.WildcardTarget, "challenge", time.Now()) {
			t.Fatal("single refusal suppressed the target")
		}
	})

	t.Run("refused next to OK is mixed and records nothing", func(t *testing.T) {
		// own group: four control probes would eat into the shared per-group burst
		s := newProbeTestSmart(t, "probe-mixed", bad, c1, c2)
		for i, modes := range [][2]string{{"challenge", "ok"}, {"ok", "challenge"}} {
			md := probeMetadata(fmt.Sprintf("mixed%d.probe.test", i))
			setModes("challenge", modes[0], modes[1])
			before1, before2 := control1.hits.Load(), control2.hits.Load()
			v := s.runResponseProbe(md, bad, probeTestURL, "")
			if v.Kind != smart.VerdictIgnore || !strings.Contains(v.Reason, "unconfirmed") {
				t.Fatalf("%v: verdict %s %s", modes, v.Kind, v.Reason)
			}
			if control1.hits.Load() != before1+1 || control2.hits.Load() != before2+1 {
				t.Fatalf("%v: both controls must be probed", modes)
			}
			if nodes := s.testFailNodes(md); len(nodes) != 0 {
				t.Fatalf("%v: records %v", modes, nodes)
			}
			if s.probeThrottle.suppressed(md.WildcardTarget, "challenge", time.Now()) {
				t.Fatalf("%v: mixed result suppressed the target", modes)
			}
		}
	})

	t.Run("OK next to a dead control is unconfirmed", func(t *testing.T) {
		down := adapter.NewProxy(&probeTestNode{Base: outbound.NewBase(outbound.BaseOption{Name: "node-down2", Type: C.Direct, ProviderName: "sub5", Addr: "e.server:443"}), dialErr: true})
		s2 := newProbeTestSmart(t, "probe-ok-dead", bad, c1, down)
		md := probeMetadata("okdead.probe.test")
		setModes("challenge", "ok", "ok")
		if v := s2.runResponseProbe(md, bad, probeTestURL, ""); v.Kind != smart.VerdictIgnore {
			t.Fatalf("verdict %s %s", v.Kind, v.Reason)
		}
		if nodes := s2.testFailNodes(md); len(nodes) != 0 {
			t.Fatalf("records %v", nodes)
		}
	})

	t.Run("single independent control OK confirms", func(t *testing.T) {
		s2 := newProbeTestSmart(t, "probe-single-control", bad, c1)
		md := probeMetadata("single.probe.test")
		setModes("challenge", "ok", "ok")
		if v := s2.runResponseProbe(md, bad, probeTestURL, ""); v.Kind != smart.VerdictBlocked {
			t.Fatalf("verdict %s %s", v.Kind, v.Reason)
		}
		if nodes := s2.testFailNodes(md); nodes["node-limited"] != 2 {
			t.Fatalf("records %v", nodes)
		}
	})

	t.Run("control budget exhausted records nothing", func(t *testing.T) {
		// own group: the throttle is per group and must not starve the other subtests
		s := newProbeTestSmart(t, "probe-budget", bad, c1, c2)
		md := probeMetadata("budget.probe.test")
		setModes("challenge", "ok", "ok")
		// use up the per-target budget with probes of other nodes
		for i := 0; i < smart.ProbePerTargetPerMinute; i++ {
			if !s.probeThrottle.allowNode(md.WildcardTarget, fmt.Sprintf("filler-%d", i), time.Now()) {
				t.Fatalf("filler %d denied", i)
			}
		}
		before1, before2 := control1.hits.Load(), control2.hits.Load()
		v := s.runResponseProbe(md, bad, probeTestURL, "")
		if v.Kind != smart.VerdictIgnore || !strings.Contains(v.Reason, "budget") {
			t.Fatalf("verdict %s %s", v.Kind, v.Reason)
		}
		if control1.hits.Load() != before1 || control2.hits.Load() != before2 {
			t.Fatal("control probed beyond the budget")
		}
		if nodes := s.testFailNodes(md); len(nodes) != 0 {
			t.Fatalf("records %v", nodes)
		}
	})

	t.Run("503 retry-after confirmed becomes code 7", func(t *testing.T) {
		md := probeMetadata("maint.probe.test")
		setModes("503", "ok", "ok")
		v := s.runResponseProbe(md, bad, probeTestURL, "")
		if v.Kind != smart.VerdictRateLimited || v.Cooldown != 300*time.Second {
			t.Fatalf("verdict %s %s %s", v.Kind, v.Cooldown, v.Reason)
		}
		if nodes := s.testFailNodes(md); nodes["node-limited"] != smart.HostCodeRateLimited {
			t.Fatalf("records %v", nodes)
		}
	})

	t.Run("503 retry-after everywhere is site-level", func(t *testing.T) {
		md := probeMetadata("maint2.probe.test")
		setModes("503", "503", "503")
		if v := s.runResponseProbe(md, bad, probeTestURL, ""); v.Kind != smart.VerdictIgnore || !strings.Contains(v.Reason, "site-level") {
			t.Fatalf("verdict %s %s", v.Kind, v.Reason)
		}
		if nodes := s.testFailNodes(md); len(nodes) != 0 {
			t.Fatalf("records %v", nodes)
		}
	})

	t.Run("origin 502 and reset ignored", func(t *testing.T) {
		for _, mode := range []string{"502", "reset"} {
			md := probeMetadata(mode + ".probe.test")
			setModes(mode, "ok", "ok")
			if v := s.runResponseProbe(md, bad, probeTestURL, ""); v.Kind != smart.VerdictIgnore {
				t.Fatalf("%s: verdict %s %s", mode, v.Kind, v.Reason)
			}
			if nodes := s.testFailNodes(md); len(nodes) != 0 {
				t.Fatalf("%s: records %v", mode, nodes)
			}
		}
	})

	t.Run("pinned group records nothing", func(t *testing.T) {
		md := probeMetadata("pinned.probe.test")
		setModes("429", "ok", "ok")
		s.selected = "node-control1"
		defer func() { s.selected = "" }()
		s.runResponseProbe(md, bad, probeTestURL, "")
		if nodes := s.testFailNodes(md); len(nodes) != 0 {
			t.Fatalf("records %v", nodes)
		}
	})

	t.Run("node no longer in group records nothing", func(t *testing.T) {
		md := probeMetadata("gone.probe.test")
		setModes("429", "ok", "ok")
		outsider := limited.node("node-removed", "sub9", "z.server:443")
		s.runResponseProbe(md, outsider, probeTestURL, "")
		if nodes := s.testFailNodes(md); len(nodes) != 0 {
			t.Fatalf("records %v", nodes)
		}
	})
}

func TestSmartResponseProbeControlSelection(t *testing.T) {
	site := newProbeSite(t)
	judged := site.node("judged", "sub1", "a.server:443")
	s := newProbeTestSmart(t, "probe-control-select",
		judged,
		site.node("same-provider", "sub1", "b.server:443"),
		site.node("same-address", "sub2", "A.SERVER:443"),
		site.node("no-identity", "", ""),
		site.node("unknown-provider", "", "c.server:443"),
		site.node("independent-1", "sub3", "d.server:443"),
		site.node("independent-dup-addr", "sub4", "d.server:443"),
		site.node("independent-dup-provider", "sub3", "e.server:443"),
	)
	md := probeMetadata("select.probe.test")
	allowed := map[string]bool{"unknown-provider": true, "independent-1": true, "independent-dup-addr": true, "independent-dup-provider": true}
	sawPair := false
	for i := 0; i < 50; i++ {
		controls := s.pickControlNodes(md, judged)
		if len(controls) == 0 || len(controls) > smart.ProbeMaxControls {
			t.Fatalf("got %d controls", len(controls))
		}
		addrs, providers := map[string]bool{}, map[string]bool{}
		for _, c := range controls {
			if !allowed[c.Name()] {
				t.Fatalf("control %s is not independent", c.Name())
			}
			provider, addr := proxyServerIdentity(c)
			if addrs[addr] {
				t.Fatalf("two controls share address %s", addr)
			}
			if provider != "" && providers[provider] {
				t.Fatalf("two controls share provider %s", provider)
			}
			addrs[addr], providers[provider] = true, true
		}
		sawPair = sawPair || len(controls) == smart.ProbeMaxControls
	}
	if !sawPair {
		t.Fatal("never picked two controls")
	}

	// a group whose other nodes all share the provider has no control: nothing is recorded
	site.mode.Store("challenge")
	s2 := newProbeTestSmart(t, "probe-no-control", judged, site.node("sibling", "sub1", "b.server:443"))
	md2 := probeMetadata("solo.probe.test")
	for i := 0; i < 3; i++ {
		if v := s2.runResponseProbe(md2, judged, probeTestURL, ""); v.Kind != smart.VerdictIgnore {
			t.Fatalf("verdict %s %s", v.Kind, v.Reason)
		}
	}
	if nodes := s2.testFailNodes(md2); len(nodes) != 0 {
		t.Fatalf("records without control: %v", nodes)
	}
}

// Long polling: the site holds the request for 30s. The probe must end at the probe
// timeout, never produce a rate limit / block verdict, and only count code 3.
func TestSmartResponseProbeLongPollingNotMisjudged(t *testing.T) {
	site := newProbeSite(t)
	node := site.node("node-longpoll", "sub1", "a.server:443")
	other := newProbeSite(t).node("node-other", "sub2", "b.server:443")
	s := newProbeTestSmart(t, "probe-longpoll", node, other)
	md := probeMetadata("api.probe.test")
	site.mode.Store("longpoll")

	start := time.Now()
	v := s.runResponseProbe(md, node, probeTestURL, "")
	if took := time.Since(start); took > C.DefaultTCPTimeout+2*time.Second {
		t.Fatalf("probe took %s", took)
	}
	if v.Kind != smart.VerdictCount || v.Class != "timeout" {
		t.Fatalf("verdict %s %s", v.Kind, v.Reason)
	}
	if nodes := s.testFailNodes(md); len(nodes) != 0 {
		t.Fatalf("first timeout blocked the node: %v", nodes)
	}

	v = s.runResponseProbe(md, node, probeTestURL, "")
	if v.Kind != smart.VerdictCount {
		t.Fatalf("verdict %s", v.Kind)
	}
	if nodes := s.testFailNodes(md); nodes["node-longpoll"] != 3 || nodes["node-other"] != 0 {
		t.Fatalf("expected only a code 3 count block, got %v", nodes)
	}
}

func TestFilterProxiesPromotesEarliestRateLimitedNode(t *testing.T) {
	site := newProbeSite(t)
	late := site.node("late", "sub1", "a.server:443")
	mid := site.node("mid", "sub2", "b.server:443")
	early := site.node("early", "sub3", "c.server:443")
	md := probeMetadata("release.probe.test")
	mark := func(s *Smart, name string, code int64, ttl time.Duration) {
		s.store.UpdateHostStatusTTL(s.Name(), s.configName, md.WildcardTarget, md, name, s.maxFailedTimes, int(s.hostFailLimit.Load()), true, true, code, ttl)
	}

	s := newProbeTestSmart(t, "probe-release", late, mid, early)
	all := s.GetProxies(false)
	mark(s, "late", smart.HostCodeRateLimited, 20*time.Minute)
	mark(s, "mid", smart.HostCodeRateLimited, 10*time.Minute)
	// one node still clean: normal selection, no promotion
	if got := s.filterProxies(md, md.WildcardTarget, nil, nil, all, 2, false); len(got) != 1 || got[0].Name() != "early" {
		t.Fatalf("got %v", proxyNames(got))
	}

	mark(s, "early", smart.HostCodeRateLimited, 2*time.Minute)
	if _, _, _, blocked := s.store.GetHostStatus(s.Name(), s.configName, md.WildcardTarget, 1, md.SmartTarget); blocked {
		t.Fatal("code 7 triggered the stop-loss")
	}
	// every candidate excluded by code 7: upstream fallback size (alive nodes up to minCount) is kept,
	// the earliest expiring node ("early", listed last) comes first
	for _, minCount := range []int{1, 2, 3, 5} {
		want := minCount
		if want > len(all) {
			want = len(all)
		}
		for _, weights := range [][]float64{nil, {1, 1, 1}} {
			var names []string
			if weights != nil {
				names = []string{"late", "mid", "early"}
			}
			got := s.filterProxies(md, md.WildcardTarget, names, weights, all, minCount, false)
			if len(got) != want || got[0].Name() != "early" {
				t.Fatalf("minCount=%d weights=%v got %v", minCount, weights, proxyNames(got))
			}
		}
	}

	// code 2 nodes are never promoted, the code 7 one is; fallback size unchanged
	s3 := newProbeTestSmart(t, "probe-release-mixed", late, mid, early)
	mark(s3, "late", 2, 0)
	mark(s3, "mid", 2, 0)
	mark(s3, "early", smart.HostCodeRateLimited, 25*time.Minute)
	if got := s3.filterProxies(md, md.WildcardTarget, nil, nil, all, 2, false); len(got) != 2 || got[0].Name() != "early" {
		t.Fatalf("mixed got %v", proxyNames(got))
	}
}

func TestPromoteRateLimitedNodeSkipsBlockedAndDead(t *testing.T) {
	site := newProbeSite(t)
	late := site.node("late", "sub1", "a.server:443")
	early := site.node("early", "sub2", "b.server:443")
	earliestDead := adapter.NewProxy(&probeTestNode{Base: outbound.NewBase(outbound.BaseOption{Name: "earliest-dead", Type: C.Direct, ProviderName: "sub3", Addr: "c.server:443"}), dialErr: true})
	earliestBlocked := site.node("earliest-blocked", "sub4", "d.server:443")
	s := newProbeTestSmart(t, "probe-promote-skip", earliestDead, earliestBlocked, late, early)
	md := probeMetadata("skip.probe.test")
	for name, ttl := range map[string]time.Duration{"earliest-dead": time.Minute, "earliest-blocked": time.Minute, "early": 5 * time.Minute, "late": 20 * time.Minute} {
		s.store.UpdateHostStatusTTL(s.Name(), s.configName, md.WildcardTarget, md, name, s.maxFailedTimes, int(s.hostFailLimit.Load()), true, true, smart.HostCodeRateLimited, ttl)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, _ = earliestDead.URLTest(ctx, s.testUrl, nil)
	if earliestDead.AliveForTestUrl(s.testUrl) {
		t.Fatal("setup: node still alive")
	}
	s.store.UpdateBlockedNodesCache(s.Name(), s.configName, map[string]*smart.NodeState{"earliest-blocked": {BlockedUntil: time.Now().Add(time.Hour).Unix()}})
	if !s.store.GetBlockedNodes(s.Name(), s.configName)["earliest-blocked"] {
		t.Fatal("setup: node not globally blocked")
	}

	wtFailNodes, _, _, _ := s.store.GetHostStatus(s.Name(), s.configName, md.WildcardTarget, int(s.hostFailLimit.Load()), md.SmartTarget)
	blockedNodes := s.store.GetBlockedNodes(s.Name(), s.configName)
	fallback := s.GetProxies(false)
	got := s.promoteRateLimitedNode(md, md.WildcardTarget, wtFailNodes, blockedNodes, fallback, false)
	if len(got) != len(fallback) || got[0].Name() != "early" {
		t.Fatalf("got %v", proxyNames(got))
	}
}

func proxyNames(ps []C.Proxy) []string {
	names := make([]string, 0, len(ps))
	for _, p := range ps {
		names = append(names, p.Name())
	}
	return names
}

func TestRecheckBlockedHost(t *testing.T) {
	site := newProbeSite(t)
	node := site.node("node-recheck", "sub1", "a.server:443")
	s := newProbeTestSmart(t, "probe-recheck", node)
	limit := int(s.hostFailLimit.Load())
	tests := []struct {
		mode string
		want int
	}{
		{"ok", 0},
		{"429", smart.HostCodeRateLimited},
		{"challenge", 2},
		{"longpoll", 2},
		{"502", 2},
	}
	expiry := map[string]int64{}
	for _, tt := range tests {
		md := probeMetadata("recheck-" + tt.mode + ".probe.test")
		s.store.UpdateHostStatus(s.Name(), s.configName, md.WildcardTarget, md, "node-recheck", s.maxFailedTimes, limit, true, true, 2)
		exp := s.store.GetHostCodeExpiry(s.Name(), s.configName, 2, md.WildcardTarget)["node-recheck"]
		if exp == 0 {
			t.Fatalf("setup %s: no code 2 block", tt.mode)
		}
		expiry[tt.mode] = exp
	}
	// a renewed 24h block would move the expiry by at least one second
	time.Sleep(1100 * time.Millisecond)

	for _, tt := range tests {
		md := probeMetadata("recheck-" + tt.mode + ".probe.test")
		site.mode.Store(tt.mode)
		s.recheckBlockedHost(node, md.WildcardTarget, "node-recheck", md.Host)
		if got := s.testFailNodes(md)["node-recheck"]; got != tt.want {
			t.Fatalf("%s: code %d want %d", tt.mode, got, tt.want)
		}
		if tt.want == 2 {
			if exp := s.store.GetHostCodeExpiry(s.Name(), s.configName, 2, md.WildcardTarget)["node-recheck"]; exp != expiry[tt.mode] {
				t.Fatalf("%s: code 2 expiry changed %d -> %d", tt.mode, expiry[tt.mode], exp)
			}
		}
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

// checkNodeQuality only schedules the probe; the verdict lands asynchronously, and a second
// close of the same (target, node) inside the interval does not probe again.
func TestCheckNodeQualitySchedulesProbe(t *testing.T) {
	site := newProbeSite(t)
	node := site.node("node-async", "sub1", "a.server:443")
	other := newProbeSite(t).node("node-async-other", "sub2", "b.server:443")
	s := newProbeTestSmart(t, "probe-async", node, other)
	md := probeMetadata("async.probe.test")
	site.mode.Store("429")
	check := func() (float64, bool, bool, int64) {
		return s.checkNodeQuality(nil, md, node, md.WildcardTarget, "addr", node.Name(),
			1, 1, 50, 0.001, 0.001, "tcp", "", false, 0, 0)
	}
	waitHits := func(want int32) bool {
		deadline := time.Now().Add(C.DefaultTCPTimeout + 2*time.Second)
		for time.Now().Before(deadline) {
			if site.hits.Load() >= want {
				return true
			}
			time.Sleep(10 * time.Millisecond)
		}
		return false
	}

	weight, degraded, checked, code := check()
	if weight != 1 || degraded || checked || code != 0 {
		t.Fatalf("checkNodeQuality returned %v %v %v %v", weight, degraded, checked, code)
	}
	if !waitHits(1) {
		t.Fatal("probe never reached the site")
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && s.testFailNodes(md)["node-async"] != smart.HostCodeRateLimited {
		time.Sleep(10 * time.Millisecond)
	}
	if nodes := s.testFailNodes(md); nodes["node-async"] != smart.HostCodeRateLimited {
		t.Fatalf("records %v", nodes)
	}

	// clear the code 7 record so checkNodeQuality reaches the probe branch again;
	// only the (target, node) throttle can stop the second probe now
	s.store.UpdateHostStatus(s.Name(), s.configName, md.WildcardTarget, md, "node-async", s.maxFailedTimes, int(s.hostFailLimit.Load()), false, true, 0)
	if nodes := s.testFailNodes(md); len(nodes) != 0 {
		t.Fatalf("records not cleared: %v", nodes)
	}
	check()
	if waitHitsWithin(site, 2, 700*time.Millisecond) {
		t.Fatalf("probe not throttled: site hits %d", site.hits.Load())
	}
	if nodes := s.testFailNodes(md); len(nodes) != 0 {
		t.Fatalf("throttled check still recorded %v", nodes)
	}
}

func waitHitsWithin(site *probeSite, want int32, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if site.hits.Load() >= want {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}
