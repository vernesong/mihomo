package outboundgroup

import (
	"errors"
	"testing"
)

func TestIsStatusTestFailure(t *testing.T) {
	tests := []struct {
		name string
		ok   bool
		err  error
		want bool
	}{
		{name: "successful response", ok: true, want: false},
		{name: "unexpected status", ok: false, want: true},
		{name: "request error", err: errors.New("unexpected EOF"), want: true},
		{name: "request error overrides status", ok: true, err: errors.New("timeout"), want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isStatusTestFailure(tt.ok, tt.err); got != tt.want {
				t.Fatalf("isStatusTestFailure(%v, %v) = %v, want %v", tt.ok, tt.err, got, tt.want)
			}
		})
	}
}
