package smart

import (
	"context"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"syscall"
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"

	"github.com/metacubex/http"
)

func hdr(kv ...string) http.Header {
	h := http.Header{}
	for i := 0; i+1 < len(kv); i += 2 {
		h.Add(kv[i], kv[i+1])
	}
	return h
}

func TestClassifyResponse(t *testing.T) {
	now := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	epoch := func(d time.Duration) string { return strconv.FormatInt(now.Add(d).Unix(), 10) }

	tests := []struct {
		name     string
		status   int
		header   http.Header
		body     string
		kind     VerdictKind
		cooldown time.Duration
	}{
		{"429 retry-after seconds", 429, hdr("Retry-After", "120"), "", VerdictRateLimited, 120 * time.Second},
		{"429 retry-after http-date", 429, hdr("Retry-After", now.Add(5*time.Minute).Format(http.TimeFormat)), "", VerdictRateLimited, 5 * time.Minute},
		{"429 no headers", 429, nil, "", VerdictRateLimited, RateLimitDefaultCooldown},
		{"429 retry-after 0 is no signal", 429, hdr("Retry-After", "0"), "", VerdictRateLimited, RateLimitDefaultCooldown},
		{"429 retry-after past date is no signal", 429, hdr("Retry-After", now.Add(-time.Minute).Format(http.TimeFormat)), "", VerdictRateLimited, RateLimitDefaultCooldown},
		{"429 x-ratelimit-reset", 429, hdr("X-RateLimit-Reset", epoch(900*time.Second)), "", VerdictRateLimited, 900 * time.Second},
		{"429 retry-after wins over reset", 429, hdr("Retry-After", "90", "X-RateLimit-Reset", epoch(900*time.Second)), "", VerdictRateLimited, 90 * time.Second},
		{"429 cooldown clamped low", 429, hdr("Retry-After", "5"), "", VerdictRateLimited, RateLimitMinCooldown},
		{"429 cooldown clamped high", 429, hdr("Retry-After", "99999"), "", VerdictRateLimited, RateLimitMaxCooldown},
		{"429 reset clamped high", 429, hdr("X-RateLimit-Reset", epoch(3*time.Hour)), "", VerdictRateLimited, RateLimitMaxCooldown},
		{"429 reset in past clamped low", 429, hdr("X-RateLimit-Reset", epoch(-time.Hour)), "", VerdictRateLimited, RateLimitMinCooldown},
		{"403 remaining 0 with reset", 403, hdr("X-RateLimit-Remaining", "0", "X-RateLimit-Reset", epoch(900*time.Second)), "", VerdictRateLimited, 900 * time.Second},
		{"403 remaining 0 without reset", 403, hdr("X-RateLimit-Remaining", "0"), "", VerdictRateLimited, RateLimitDefaultCooldown},
		{"403 remaining 12 is featureless", 403, hdr("X-RateLimit-Remaining", "12"), "", VerdictSuspect, 0},
		{"github secondary 403 retry-after", 403, hdr("Retry-After", "60", "Server", "github.com"), "", VerdictRateLimited, 60 * time.Second},
		{"403 retry-after 0 is featureless", 403, hdr("Retry-After", "0"), "", VerdictSuspect, 0},
		{"503 retry-after", 503, hdr("Retry-After", "300"), "", VerdictRateLimited, 300 * time.Second},
		{"json 403 rate limit", 403, hdr("Content-Type", "application/json; charset=utf-8"),
			`{"message":"API rate limit exceeded for 1.2.3.4.","documentation_url":"https://docs.github.com"}`, VerdictRateLimited, RateLimitDefaultCooldown},
		{"html 403 mentioning rate limit is not json", 403, hdr("Content-Type", "text/html"), "rate limit", VerdictSuspect, 0},
		{"featureless 403", 403, hdr("Server", "nginx"), "Forbidden", VerdictSuspect, 0},
		{"bare cloudflare 403", 403, hdr("Server", "cloudflare"), "", VerdictSuspect, 0},
		{"cf-mitigated challenge", 403, hdr("Cf-Mitigated", "challenge", "Server", "cloudflare"), "", VerdictSuspect, 0},
		{"just a moment 200", 200, hdr("Server", "cloudflare"), "<html><head><title>Just a moment...</title>", VerdictSuspect, 0},
		{"just a moment 503", 503, nil, "<title>Just a moment...</title>", VerdictSuspect, 0},
		{"challenge-platform 403", 403, nil, `<script src="/cdn-cgi/challenge-platform/h/b/orchestrate"></script>`, VerdictSuspect, 0},
		{"challenge-platform script in normal 200", 200, nil, `<script src="/cdn-cgi/challenge-platform/scripts/jsd/main.js"></script>`, VerdictOK, 0},
		{"cloudflare waf block", 403, hdr("Server", "cloudflare"), `<span class="cf-error-code">1020</span> Sorry, you have been blocked`, VerdictSuspect, 0},
		{"cloudfront block", 403, hdr("Server", "CloudFront"), "Request blocked. We can't connect to the server", VerdictSuspect, 0},
		{"akamai block", 403, hdr("Server", "AkamaiGHost"), "Access Denied ... Reference #18.abc", VerdictSuspect, 0},
		{"aws waf challenge 202", 202, hdr("X-Amzn-Waf-Action", "challenge"), "", VerdictSuspect, 0},
		{"aws waf captcha 405", 405, hdr("X-Amzn-Waf-Action", "captcha"), "", VerdictSuspect, 0},
		{"451", 451, nil, "", VerdictBlocked, 0},
		{"405 ignored", 405, nil, "", VerdictIgnore, 0},
		{"501 ignored", 501, nil, "", VerdictIgnore, 0},
		{"500 ignored", 500, nil, "", VerdictIgnore, 0},
		{"502 ignored", 502, nil, "", VerdictIgnore, 0},
		{"503 plain ignored", 503, nil, "Service Unavailable", VerdictIgnore, 0},
		{"520 cloudflare ignored", 520, hdr("Server", "cloudflare"), "", VerdictIgnore, 0},
		{"522 cloudflare ignored", 522, hdr("Server", "cloudflare"), "", VerdictIgnore, 0},
		{"421 counted", 421, nil, "", VerdictCount, 0},
		{"599 counted", 599, nil, "", VerdictCount, 0},
		{"200 ok", 200, nil, "hello", VerdictOK, 0},
		{"206 ok", 206, nil, "", VerdictOK, 0},
		{"401 tmdb cloudfront reachable", 401, hdr("Server", "openresty", "X-Cache", "Error from cloudfront"), `{"status_code":7}`, VerdictOK, 0},
		{"404 with retry-after 0 reachable", 404, hdr("Retry-After", "0"), "", VerdictOK, 0},
		{"400 reachable", 400, nil, "", VerdictOK, 0},
		{"410 reachable", 410, nil, "", VerdictOK, 0},
		{"cross-host 301 reachable", 301, hdr("Location", "https://github.com/"), "", VerdictOK, 0},
		{"google sorry redirect", 302, hdr("Location", "https://www.google.com/sorry/index?continue=x"), "", VerdictSuspect, 0},
		{"captcha redirect", 302, hdr("Location", "/captcha?from=/"), "", VerdictSuspect, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := ClassifyResponse(tt.status, tt.header, []byte(tt.body), now)
			if v.Kind != tt.kind {
				t.Fatalf("kind = %s (%s), want %s", v.Kind, v.Reason, tt.kind)
			}
			if tt.kind == VerdictRateLimited && v.Cooldown != tt.cooldown {
				t.Fatalf("cooldown = %s, want %s", v.Cooldown, tt.cooldown)
			}
			if v.Class == "" {
				t.Fatalf("empty class")
			}
		})
	}
}

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

var _ net.Error = timeoutErr{}

func TestClassifyProbeError(t *testing.T) {
	cert := &x509.Certificate{}
	tests := []struct {
		name string
		err  error
		kind VerdictKind
	}{
		{"nil", nil, VerdictIgnore},
		{"hostname mismatch", &url.Error{Op: "Get", URL: "https://a", Err: x509.HostnameError{Certificate: cert, Host: "a"}}, VerdictSuspect},
		{"unknown authority", fmt.Errorf("tls: %w", x509.UnknownAuthorityError{Cert: cert}), VerdictSuspect},
		{"deadline", &url.Error{Op: "Get", URL: "https://a", Err: context.DeadlineExceeded}, VerdictCount},
		{"net timeout", timeoutErr{}, VerdictCount},
		{"reset", &net.OpError{Op: "read", Err: syscall.ECONNRESET}, VerdictCount},
		{"eof", &url.Error{Op: "Get", URL: "https://a", Err: io.EOF}, VerdictCount},
		{"canceled", context.Canceled, VerdictIgnore},
		{"dial refused", &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}, VerdictIgnore},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if v := ClassifyProbeError(tt.err); v.Kind != tt.kind {
				t.Fatalf("kind = %s (%s), want %s", v.Kind, v.Reason, tt.kind)
			}
		})
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	for _, tt := range []struct {
		in string
		d  time.Duration
		ok bool
	}{
		{"", 0, false},
		{"0", 0, false},
		{"-5", 0, false},
		{"abc", 0, false},
		{"30", 30 * time.Second, true},
		{" 45 ", 45 * time.Second, true},
		{"9223372036854775807", RateLimitMaxCooldown, true},
		{now.Add(2 * time.Minute).Format(http.TimeFormat), 2 * time.Minute, true},
		{now.Format(http.TimeFormat), 0, false},
	} {
		d, ok := ParseRetryAfter(tt.in, now)
		if d != tt.d || ok != tt.ok {
			t.Errorf("ParseRetryAfter(%q) = %s,%v want %s,%v", tt.in, d, ok, tt.d, tt.ok)
		}
	}
}

func TestClampCooldown(t *testing.T) {
	for in, want := range map[time.Duration]time.Duration{
		0:                 RateLimitMinCooldown,
		59 * time.Second:  RateLimitMinCooldown,
		60 * time.Second:  60 * time.Second,
		10 * time.Minute:  10 * time.Minute,
		30 * time.Minute:  30 * time.Minute,
		31 * time.Minute:  RateLimitMaxCooldown,
		-10 * time.Second: RateLimitMinCooldown,
	} {
		if got := ClampCooldown(in); got != want {
			t.Errorf("ClampCooldown(%s) = %s want %s", in, got, want)
		}
	}
}

func TestUpdateHostStatusRateLimited(t *testing.T) {
	store := NewStore(nil)
	const group, config, target = "rl-test-group", "rl-test-config", "raw.githubusercontent.com"
	md := &C.Metadata{Host: target}
	limit := 10

	// code 7 blocks immediately with its own TTL
	store.UpdateHostStatusTTL(group, config, target, md, "n1", 5, limit, true, true, HostCodeRateLimited, 2*time.Minute)
	nodes, _, _, blocked := store.GetHostStatus(group, config, target, limit)
	if nodes["n1"] != HostCodeRateLimited || blocked {
		t.Fatalf("nodes=%v blocked=%v", nodes, blocked)
	}
	hs, _ := hostStatusCache.Get(FormatDBKey(KeyTypeHostFailures, config, group, target))
	if exp := hs.Codes[HostCodeRateLimited].Nodes["n1"] - time.Now().Unix(); exp < 115 || exp > 125 {
		t.Fatalf("ttl %ds, want ~120s", exp)
	}

	// a later timeout count must not wipe out the rate limit block
	store.UpdateHostStatus(group, config, target, md, "n1", 5, limit, false, true, 3)
	if nodes, _, _, _ = store.GetHostStatus(group, config, target, limit); nodes["n1"] != HostCodeRateLimited {
		t.Fatalf("code 3 replaced code 7: %v", nodes)
	}

	// code 7 takes over a pending timeout count
	store.UpdateHostStatus(group, config, target, md, "n2", 5, limit, false, true, 3)
	store.UpdateHostStatusTTL(group, config, target, md, "n2", 5, limit, true, true, HostCodeRateLimited, 0)
	if nodes, _, _, _ = store.GetHostStatus(group, config, target, limit); nodes["n2"] != HostCodeRateLimited {
		t.Fatalf("code 7 did not replace pending code 3: %v", nodes)
	}
	if exp := hs.Codes[HostCodeRateLimited].Nodes["n2"] - time.Now().Unix(); exp < int64(RateLimitDefaultCooldown.Seconds())-5 {
		t.Fatalf("default ttl %ds", exp)
	}

	// code 2 still outranks code 7
	store.UpdateHostStatus(group, config, target, md, "n1", 5, limit, true, true, 2)
	if nodes, _, _, _ = store.GetHostStatus(group, config, target, limit); nodes["n1"] != 2 {
		t.Fatalf("code 2 did not replace code 7: %v", nodes)
	}

	// expired code 7 no longer counts
	hs.mu.Lock()
	hs.Codes[HostCodeRateLimited].Nodes["n2"] = time.Now().Unix() - 1
	hs.mu.Unlock()
	if nodes, _, _, _ = store.GetHostStatus(group, config, target, limit); nodes["n2"] != 0 {
		t.Fatalf("expired code 7 still active: %v", nodes)
	}

	// code 7 counts toward the stop-loss limit
	for i := 0; i < 3; i++ {
		store.UpdateHostStatusTTL(group, config, target, md, fmt.Sprintf("m%d", i), 5, 2, true, true, HostCodeRateLimited, time.Minute)
	}
	if _, _, _, blocked = store.GetHostStatus(group, config, target, 2); !blocked {
		t.Fatalf("code 7 nodes did not trigger stop-loss")
	}
}
