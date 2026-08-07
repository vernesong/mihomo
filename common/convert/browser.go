package convert

import (
	"github.com/metacubex/http"
	"github.com/metacubex/randv2"
)

type BrowserPreset struct {
	FingerprintName string
	Headers         http.Header
}

// Only fingerprints that are reasonably current (≤2 years old) are included.
// Older fingerprints (Edge 85, iOS 14, QQ 11.1, 360 7.5) are excluded because
// their TLS parameters are themselves a detectable anomaly.
//
// utls _Auto mapping:
//
//	chrome  → HelloChrome_Auto  → Chrome  133  (2025, ML-KEM)
//	firefox → HelloFirefox_Auto → Firefox 120  (2023)
//	safari  → HelloSafari_Auto  → Safari  16.0 (2022)
var browserPresets = []BrowserPreset{
	// ── Chrome 133 (Windows, en-US) ──
	{
		FingerprintName: "chrome",
		Headers: http.Header{
			"User-Agent":                {"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"},
			"Accept":                    {"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"},
			"Accept-Language":           {"en-US,en;q=0.9,zh-CN;q=0.8"},
			"Accept-Encoding":           {"gzip, deflate, br"},
			"Cache-Control":             {"no-cache"},
			"Pragma":                    {"no-cache"},
			"Sec-Ch-Ua":                 {`"Not/A)Brand";v="99", "Chromium";v="133", "Google Chrome";v="133"`},
			"Sec-Ch-Ua-Mobile":          {"?0"},
			"Sec-Ch-Ua-Platform":        {`"Windows"`},
			"Sec-Fetch-Dest":            {"document"},
			"Sec-Fetch-Mode":            {"navigate"},
			"Sec-Fetch-Site":            {"none"},
			"Sec-Fetch-User":            {"?1"},
			"Upgrade-Insecure-Requests": {"1"},
		},
	},
	// ── Chrome 133 (Windows, zh-CN) ──
	{
		FingerprintName: "chrome",
		Headers: http.Header{
			"User-Agent":                {"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"},
			"Accept":                    {"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"},
			"Accept-Language":           {"zh-CN,zh;q=0.9,en;q=0.8"},
			"Accept-Encoding":           {"gzip, deflate, br"},
			"Cache-Control":             {"no-cache"},
			"Pragma":                    {"no-cache"},
			"Sec-Ch-Ua":                 {`"Not/A)Brand";v="99", "Chromium";v="133", "Google Chrome";v="133"`},
			"Sec-Ch-Ua-Mobile":          {"?0"},
			"Sec-Ch-Ua-Platform":        {`"Windows"`},
			"Sec-Fetch-Dest":            {"document"},
			"Sec-Fetch-Mode":            {"navigate"},
			"Sec-Fetch-Site":            {"none"},
			"Sec-Fetch-User":            {"?1"},
			"Upgrade-Insecure-Requests": {"1"},
		},
	},
	// ── Chrome 133 (Linux, en-US) ──
	{
		FingerprintName: "chrome",
		Headers: http.Header{
			"User-Agent":                {"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"},
			"Accept":                    {"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"},
			"Accept-Language":           {"en-US,en;q=0.9"},
			"Accept-Encoding":           {"gzip, deflate, br"},
			"Cache-Control":             {"no-cache"},
			"Pragma":                    {"no-cache"},
			"Sec-Ch-Ua":                 {`"Not/A)Brand";v="99", "Chromium";v="133", "Google Chrome";v="133"`},
			"Sec-Ch-Ua-Mobile":          {"?0"},
			"Sec-Ch-Ua-Platform":        {`"Linux"`},
			"Sec-Fetch-Dest":            {"document"},
			"Sec-Fetch-Mode":            {"navigate"},
			"Sec-Fetch-Site":            {"none"},
			"Sec-Fetch-User":            {"?1"},
			"Upgrade-Insecure-Requests": {"1"},
		},
	},
	// ── Chrome 133 (macOS, en-US) ──
	{
		FingerprintName: "chrome",
		Headers: http.Header{
			"User-Agent":                {"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"},
			"Accept":                    {"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"},
			"Accept-Language":           {"en-US,en;q=0.9,ja;q=0.8"},
			"Accept-Encoding":           {"gzip, deflate, br"},
			"Cache-Control":             {"no-cache"},
			"Pragma":                    {"no-cache"},
			"Sec-Ch-Ua":                 {`"Not/A)Brand";v="99", "Chromium";v="133", "Google Chrome";v="133"`},
			"Sec-Ch-Ua-Mobile":          {"?0"},
			"Sec-Ch-Ua-Platform":        {`"macOS"`},
			"Sec-Fetch-Dest":            {"document"},
			"Sec-Fetch-Mode":            {"navigate"},
			"Sec-Fetch-Site":            {"none"},
			"Sec-Fetch-User":            {"?1"},
			"Upgrade-Insecure-Requests": {"1"},
		},
	},
	// ── Chrome 133 (Windows, en-GB) ──
	{
		FingerprintName: "chrome",
		Headers: http.Header{
			"User-Agent":                {"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"},
			"Accept":                    {"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"},
			"Accept-Language":           {"en-GB,en;q=0.9,fr;q=0.8"},
			"Accept-Encoding":           {"gzip, deflate, br"},
			"Cache-Control":             {"no-cache"},
			"Pragma":                    {"no-cache"},
			"Sec-Ch-Ua":                 {`"Not/A)Brand";v="99", "Chromium";v="133", "Google Chrome";v="133"`},
			"Sec-Ch-Ua-Mobile":          {"?0"},
			"Sec-Ch-Ua-Platform":        {`"Windows"`},
			"Sec-Fetch-Dest":            {"document"},
			"Sec-Fetch-Mode":            {"navigate"},
			"Sec-Fetch-Site":            {"none"},
			"Sec-Fetch-User":            {"?1"},
			"Upgrade-Insecure-Requests": {"1"},
		},
	},
	// ── Chrome 133 (Windows, de-DE) ──
	{
		FingerprintName: "chrome",
		Headers: http.Header{
			"User-Agent":                {"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"},
			"Accept":                    {"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"},
			"Accept-Language":           {"de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7"},
			"Accept-Encoding":           {"gzip, deflate, br"},
			"Cache-Control":             {"no-cache"},
			"Pragma":                    {"no-cache"},
			"Sec-Ch-Ua":                 {`"Not/A)Brand";v="99", "Chromium";v="133", "Google Chrome";v="133"`},
			"Sec-Ch-Ua-Mobile":          {"?0"},
			"Sec-Ch-Ua-Platform":        {`"Windows"`},
			"Sec-Fetch-Dest":            {"document"},
			"Sec-Fetch-Mode":            {"navigate"},
			"Sec-Fetch-Site":            {"none"},
			"Sec-Fetch-User":            {"?1"},
			"Upgrade-Insecure-Requests": {"1"},
		},
	},
	// ── Chrome 133 (macOS, zh-CN) ──
	{
		FingerprintName: "chrome",
		Headers: http.Header{
			"User-Agent":                {"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"},
			"Accept":                    {"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"},
			"Accept-Language":           {"zh-CN,zh;q=0.9,en;q=0.8,ja;q=0.7"},
			"Accept-Encoding":           {"gzip, deflate, br"},
			"Cache-Control":             {"no-cache"},
			"Pragma":                    {"no-cache"},
			"Sec-Ch-Ua":                 {`"Not/A)Brand";v="99", "Chromium";v="133", "Google Chrome";v="133"`},
			"Sec-Ch-Ua-Mobile":          {"?0"},
			"Sec-Ch-Ua-Platform":        {`"macOS"`},
			"Sec-Fetch-Dest":            {"document"},
			"Sec-Fetch-Mode":            {"navigate"},
			"Sec-Fetch-Site":            {"none"},
			"Sec-Fetch-User":            {"?1"},
			"Upgrade-Insecure-Requests": {"1"},
		},
	},
	// ── Chrome 133 (Windows, en-US, full hints) ──
	{
		FingerprintName: "chrome",
		Headers: http.Header{
			"User-Agent":                {"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"},
			"Accept":                    {"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"},
			"Accept-Language":           {"en-US,en;q=0.9"},
			"Accept-Encoding":           {"gzip, deflate, br"},
			"Cache-Control":             {"no-cache"},
			"Pragma":                    {"no-cache"},
			"Sec-Ch-Ua":                 {`"Not/A)Brand";v="99", "Chromium";v="133", "Google Chrome";v="133"`},
			"Sec-Ch-Ua-Arch":            {`"x86"`},
			"Sec-Ch-Ua-Bitness":         {`"64"`},
			"Sec-Ch-Ua-Full-Version-List": {`"Not/A)Brand";v="99.0.0.0", "Chromium";v="133.0.6943.127", "Google Chrome";v="133.0.6943.127"`},
			"Sec-Ch-Ua-Mobile":          {"?0"},
			"Sec-Ch-Ua-Platform":        {`"Windows"`},
			"Sec-Ch-Ua-Platform-Version": {`"15.0.0"`},
			"Sec-Fetch-Dest":            {"document"},
			"Sec-Fetch-Mode":            {"navigate"},
			"Sec-Fetch-Site":            {"none"},
			"Sec-Fetch-User":            {"?1"},
			"Upgrade-Insecure-Requests": {"1"},
		},
	},

	// ── Firefox 120 (Windows) ──
	{
		FingerprintName: "firefox",
		Headers: http.Header{
			"User-Agent":                {"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0"},
			"Accept":                    {"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"},
			"Accept-Language":           {"en-US,en;q=0.9"},
			"Accept-Encoding":           {"gzip, deflate, br"},
			"Cache-Control":             {"no-cache"},
			"Pragma":                    {"no-cache"},
			"Sec-Fetch-Dest":            {"document"},
			"Sec-Fetch-Mode":            {"navigate"},
			"Sec-Fetch-Site":            {"none"},
			"Upgrade-Insecure-Requests": {"1"},
			"TE":                        {"trailers"},
		},
	},

	// ── Safari 16.0 (macOS Ventura) ──
	{
		FingerprintName: "safari",
		Headers: http.Header{
			"User-Agent":                {"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15"},
			"Accept":                    {"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"},
			"Accept-Language":           {"en-US,en;q=0.9"},
			"Accept-Encoding":           {"gzip, deflate, br"},
			"Cache-Control":             {"no-cache"},
			"Pragma":                    {"no-cache"},
			"Sec-Fetch-Dest":            {"document"},
			"Sec-Fetch-Mode":            {"navigate"},
			"Sec-Fetch-Site":            {"none"},
		},
	},
}

func RandBrowserPreset() BrowserPreset {
	return browserPresets[randv2.IntN(len(browserPresets))]
}
