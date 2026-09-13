package smart

import (
	"bytes"
	"context"
	"crypto/x509"
	"errors"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/metacubex/http"
)

// Response-aware probing: the active StatusProbe fetches a small, credential-less
// response through a node and ClassifyResponse turns status code, headers and a
// short body prefix into a verdict about how the site treats that node's exit IP.

const (
	// HostCodeRateLimited marks a node rate limited by a target. Unlike code 2 it
	// carries its own short TTL and simply expires, no recovery probe needed.
	HostCodeRateLimited = 7

	RateLimitMinCooldown     = 60 * time.Second
	RateLimitDefaultCooldown = 10 * time.Minute
	RateLimitMaxCooldown     = 30 * time.Minute

	ProbeMinIntervalPerNode      = 120 * time.Second
	ProbeMinIntervalAfterFailure = 30 * time.Second
	ProbeRecentFailureWindow     = 10 * time.Minute
	ProbePerTargetPerMinute      = 6
	ProbeGroupPerMinute          = 30
	ProbeGroupBurst              = 10
	ProbeSiteLevelSuppress       = 10 * time.Minute
	// ProbeMaxControls is how many control nodes a suspect answer is compared against
	ProbeMaxControls = 2

	// ProbeMaxBodyBytes is how much of the probe body is kept for block page matching.
	ProbeMaxBodyBytes = 4096
	// ProbeTimeoutStatus is the pseudo status reported for a timed out probe.
	ProbeTimeoutStatus = 599
)

type VerdictKind uint8

const (
	// VerdictIgnore carries no evidence about the node (origin errors, unknown failures).
	VerdictIgnore VerdictKind = iota
	// VerdictOK means the probe reached the origin: clear the node's failure records.
	VerdictOK
	// VerdictRateLimited is IP-level rate limiting: block with code 7 for Cooldown.
	VerdictRateLimited
	// VerdictBlocked is an explicit block that needs no confirmation (451): code 2.
	VerdictBlocked
	// VerdictSuspect is a challenge / WAF block page / featureless 403 / TLS hijack /
	// 503 with retry-after. It only becomes ConfirmedKind after a control node on another
	// provider and server fetched the same URL fine.
	VerdictSuspect
	// VerdictCount is a probe timeout or 421: code 3 counting.
	VerdictCount
)

func (k VerdictKind) String() string {
	switch k {
	case VerdictOK:
		return "ok"
	case VerdictRateLimited:
		return "rate-limited"
	case VerdictBlocked:
		return "blocked"
	case VerdictSuspect:
		return "suspect"
	case VerdictCount:
		return "count"
	default:
		return "ignore"
	}
}

type ResponseVerdict struct {
	Kind VerdictKind
	// ConfirmedKind is what a VerdictSuspect becomes once confirmed (VerdictBlocked or VerdictRateLimited)
	ConfirmedKind VerdictKind
	Cooldown      time.Duration // for VerdictRateLimited, or a Suspect confirmed as rate limited
	Class         string        // stable short class, e.g. "challenge", used for site-level suppression
	Reason        string        // human readable detail for debug logs
}

type ProbeOptions struct {
	// MaxBodyBytes > 0 requests (Range) and keeps at most this many body bytes.
	MaxBodyBytes int
	// FollowCrossHostRedirect follows redirects to another host (legacy StatusTest behavior).
	FollowCrossHostRedirect bool
}

type ProbeResult struct {
	StatusCode int
	Header     http.Header
	Body       []byte
}

func verdict(kind VerdictKind, class, reason string) ResponseVerdict {
	v := ResponseVerdict{Kind: kind, Class: class, Reason: reason}
	if kind == VerdictSuspect {
		v.ConfirmedKind = VerdictBlocked
	}
	return v
}

// ClassifyResponse is the pure decision table for an active probe response.
// Rules are matched in order, the first match wins.
func ClassifyResponse(status int, header http.Header, body []byte, now time.Time) ResponseVerdict {
	statusText := strconv.Itoa(status)
	lowerBody := bytes.ToLower(body)
	server := strings.ToLower(header.Get("Server"))
	retryAfter, hasRetryAfter := ParseRetryAfter(header.Get("Retry-After"), now)

	rateLimited := func(reason string) ResponseVerdict {
		v := verdict(VerdictRateLimited, "rate-limited", reason)
		v.Cooldown = RateLimitCooldown(header, now)
		return v
	}

	// R2/R3: 429 is always rate limiting for a credential-less probe
	if status == http.StatusTooManyRequests {
		return rateLimited(statusText)
	}
	// R5: GitHub style primary rate limit
	if status == http.StatusForbidden && strings.TrimSpace(header.Get("X-Ratelimit-Remaining")) == "0" {
		return rateLimited(statusText + " x-ratelimit-remaining=0")
	}
	// R6: secondary rate limit with an explicit positive retry-after
	if status == http.StatusForbidden && hasRetryAfter {
		return rateLimited(statusText + " retry-after=" + retryAfter.String())
	}
	// R7: JSON 403 whose message talks about rate limiting
	if status == http.StatusForbidden &&
		strings.Contains(strings.ToLower(header.Get("Content-Type")), "json") &&
		bytes.Contains(lowerBody, []byte("rate limit")) {
		return rateLimited(statusText + " json rate limit")
	}
	// R7b: active probe of a quota endpoint (e.g. api.github.com/rate_limit) reporting an exhausted quota
	if status == http.StatusOK && strings.TrimSpace(header.Get("X-Ratelimit-Remaining")) == "0" {
		return rateLimited(statusText + " x-ratelimit-remaining=0")
	}

	// R8: challenges, checked before success so a 200 challenge page is not "reachable"
	if strings.EqualFold(strings.TrimSpace(header.Get("Cf-Mitigated")), "challenge") {
		return verdict(VerdictSuspect, "challenge", statusText+" cf-mitigated=challenge")
	}
	if action := strings.ToLower(strings.TrimSpace(header.Get("X-Amzn-Waf-Action"))); action == "challenge" || action == "captcha" {
		return verdict(VerdictSuspect, "challenge", statusText+" x-amzn-waf-action="+action)
	}
	switch status {
	case http.StatusOK, http.StatusForbidden, http.StatusServiceUnavailable:
		if bytes.Contains(lowerBody, []byte("<title>just a moment...</title>")) {
			return verdict(VerdictSuspect, "challenge", statusText+" just-a-moment page")
		}
		// challenge-platform scripts are also injected into normal 200 pages by Cloudflare
		if status != http.StatusOK && bytes.Contains(lowerBody, []byte("challenge-platform")) {
			return verdict(VerdictSuspect, "challenge", statusText+" challenge-platform page")
		}
	}

	if status == http.StatusForbidden {
		// R9: Cloudflare WAF block page
		if server == "cloudflare" && (bytes.Contains(lowerBody, []byte("cf-error-code")) ||
			bytes.Contains(lowerBody, []byte("sorry, you have been blocked")) ||
			bytes.Contains(lowerBody, []byte("attention required!"))) {
			return verdict(VerdictSuspect, "waf-block", statusText+" cloudflare block page")
		}
		// R10: CloudFront / Akamai block pages
		if strings.Contains(server, "cloudfront") && (bytes.Contains(lowerBody, []byte("request blocked")) ||
			bytes.Contains(lowerBody, []byte("could not be satisfied"))) {
			return verdict(VerdictSuspect, "waf-block", statusText+" cloudfront block page")
		}
		if strings.Contains(server, "akamaighost") && bytes.Contains(lowerBody, []byte("access denied")) &&
			bytes.Contains(lowerBody, []byte("reference #")) {
			return verdict(VerdictSuspect, "waf-block", statusText+" akamai block page")
		}
	}

	// R6b: 503 + retry-after may be site-wide maintenance: rate limit only if another node is served
	if status == http.StatusServiceUnavailable && hasRetryAfter {
		v := verdict(VerdictSuspect, "overloaded", statusText+" retry-after="+retryAfter.String())
		v.ConfirmedKind = VerdictRateLimited
		v.Cooldown = RateLimitCooldown(header, now)
		return v
	}

	// R11: legal / regional block
	if status == http.StatusUnavailableForLegalReasons {
		return verdict(VerdictBlocked, "blocked", statusText)
	}

	if status >= 300 && status < 400 {
		// R12: captcha style redirects (e.g. google.com/sorry/)
		location := strings.ToLower(header.Get("Location"))
		for _, keyword := range []string{"/sorry/", "captcha", "challenge", "/blocked"} {
			if strings.Contains(location, keyword) {
				return verdict(VerdictSuspect, "challenge", statusText+" redirect to "+keyword)
			}
		}
		// R1: same-host redirect not followed any further, or cross-host redirect: reachable
		return verdict(VerdictOK, "ok", statusText)
	}

	switch {
	case status >= 200 && status < 300:
		return verdict(VerdictOK, "ok", statusText)
	case status == http.StatusForbidden:
		// R16: featureless 403, only a node problem if another node is not refused
		return verdict(VerdictSuspect, "forbidden", statusText)
	case status == http.StatusMisdirectedRequest:
		// R18
		return verdict(VerdictCount, "misdirected", statusText)
	case status == ProbeTimeoutStatus:
		// R14
		return verdict(VerdictCount, "timeout", statusText)
	case status >= 500 || status == http.StatusMethodNotAllowed:
		// R19: origin side errors (incl. 501, cf 52x) say nothing about the node
		return verdict(VerdictIgnore, "origin-error", statusText)
	case status >= 400:
		// R1: 400/401/404/410 and other client errors mean the origin answered
		return verdict(VerdictOK, "ok", statusText)
	default:
		return verdict(VerdictIgnore, "unknown", statusText)
	}
}

// ClassifyProbeError maps a probe transport error to a verdict.
func ClassifyProbeError(err error) ResponseVerdict {
	if err == nil {
		return verdict(VerdictIgnore, "unknown", "")
	}
	// R13: certificate does not belong to the host: hijack / MITM on the node
	var hostnameErr x509.HostnameError
	if errors.As(err, &hostnameErr) {
		return verdict(VerdictSuspect, "tls-hijack", "x509 hostname mismatch")
	}
	var authorityErr x509.UnknownAuthorityError
	if errors.As(err, &authorityErr) {
		return verdict(VerdictSuspect, "tls-hijack", "x509 unknown authority")
	}
	// R14: a timeout is counted, never blocked at once
	if errors.Is(err, context.DeadlineExceeded) {
		return verdict(VerdictCount, "timeout", err.Error())
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return verdict(VerdictCount, "timeout", err.Error())
	}
	// other network errors (EOF, reset, dial failure) are ignored as before: the
	// connection's own error path already counts real transport failures
	return verdict(VerdictIgnore, "network-error", err.Error())
}

// ParseRetryAfter parses a Retry-After value (delay-seconds or HTTP-date).
// Only a strictly positive delay counts as a signal.
func ParseRetryAfter(value string, now time.Time) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
		if seconds <= 0 {
			return 0, false
		}
		if seconds > int64(RateLimitMaxCooldown/time.Second) {
			return RateLimitMaxCooldown, true
		}
		return time.Duration(seconds) * time.Second, true
	}
	if t, err := http.ParseTime(value); err == nil {
		if d := t.Sub(now); d > 0 {
			return d, true
		}
	}
	return 0, false
}

// parseRateLimitReset parses X-RateLimit-Reset as epoch seconds.
func parseRateLimitReset(value string, now time.Time) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	epoch, err := strconv.ParseInt(value, 10, 64)
	if err != nil || epoch <= 0 {
		return 0, false
	}
	if maxEpoch := now.Add(RateLimitMaxCooldown).Unix(); epoch > maxEpoch {
		return RateLimitMaxCooldown, true
	}
	return time.Unix(epoch, 0).Sub(now), true
}

// RateLimitCooldown derives the code 7 TTL, clamped to [RateLimitMinCooldown, RateLimitMaxCooldown]:
// a positive retry-after, else x-ratelimit-reset, else RateLimitMinCooldown when retry-after is
// present but zero / negative / already past (retry soon), else RateLimitDefaultCooldown.
func RateLimitCooldown(header http.Header, now time.Time) time.Duration {
	retryAfter := header.Get("Retry-After")
	if d, ok := ParseRetryAfter(retryAfter, now); ok {
		return ClampCooldown(d)
	}
	if d, ok := parseRateLimitReset(header.Get("X-Ratelimit-Reset"), now); ok {
		return ClampCooldown(d)
	}
	if retryAfterNotPositive(retryAfter, now) {
		return RateLimitMinCooldown
	}
	return RateLimitDefaultCooldown
}

// retryAfterNotPositive reports a well-formed retry-after asking to retry now or in the past.
func retryAfterNotPositive(value string, now time.Time) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
		return seconds <= 0
	}
	if t, err := http.ParseTime(value); err == nil {
		return !t.After(now)
	}
	return false
}

func ClampCooldown(d time.Duration) time.Duration {
	if d < RateLimitMinCooldown {
		return RateLimitMinCooldown
	}
	if d > RateLimitMaxCooldown {
		return RateLimitMaxCooldown
	}
	return d
}
