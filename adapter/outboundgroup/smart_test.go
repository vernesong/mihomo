package outboundgroup

import (
	"testing"

	C "github.com/metacubex/mihomo/constant"
)

func TestIsBlackholeConnection(t *testing.T) {
	const kb = 1.0 / 1024.0 // uploadTotal/downloadTotal are in MB

	cases := []struct {
		name     string
		isUDP    bool
		connType C.Type
		duration int64
		upload   float64
		download float64
		want     bool
	}{
		// hit: upload-only TCP connection that lived past the threshold, regardless of port/Host
		{"tcp upload only timeout", false, C.TUN, 15000, 0.4 * kb, 0, true},
		{"tcp upload only at threshold", false, C.HTTP, blackholeMinDuration, 0.3 * kb, 0, true},
		{"tcp upload only socks long", false, C.SOCKS5, 120000, 0.5 * kb, 0, true},

		// miss
		{"server replied", false, C.TUN, 15000, 0.4 * kb, 0.1 * kb, false},
		{"udp", true, C.TUN, 15000, 0.4 * kb, 0, false},
		{"client cancelled early", false, C.TUN, blackholeMinDuration - 1, 0.4 * kb, 0, false},
		{"very short", false, C.TUN, 100, 0.4 * kb, 0, false},
		{"no upload (zero-traffic rule territory)", false, C.TUN, 15000, 0, 0, false},
		{"inner connection", false, C.INNER, 15000, 0.4 * kb, 0, false},
	}

	for _, tc := range cases {
		if got := isBlackholeConnection(tc.isUDP, tc.connType, tc.duration, tc.upload, tc.download); got != tc.want {
			t.Errorf("%s: isBlackholeConnection() = %v, want %v", tc.name, got, tc.want)
		}
	}
}
