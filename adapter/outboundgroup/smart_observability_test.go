package outboundgroup

import (
	"errors"
	"testing"
	"time"
)

func TestSmartConnectionTimingSeparatesProxyWriteAndReadPhases(t *testing.T) {
	base := time.Unix(1_000, 0)
	trace := newSmartConnectionTiming(
		"smart-group",
		"node-a",
		"GeoSite [github]",
		"*.github.com",
		321*time.Millisecond,
		base,
	)

	write := trace.firstWriteEvent(base.Add(25*time.Millisecond), nil)
	if write.phase != "first-write" {
		t.Fatalf("write phase = %q, want first-write", write.phase)
	}
	if write.streamReadyElapsed != 25*time.Millisecond {
		t.Fatalf("write elapsed = %s, want 25ms", write.streamReadyElapsed)
	}
	if write.firstWriteToReadObserved {
		t.Fatal("first-write event must not claim a write-to-read duration")
	}

	read := trace.firstReadEvent(base.Add(425*time.Millisecond), nil)
	if read.phase != "first-read" {
		t.Fatalf("read phase = %q, want first-read", read.phase)
	}
	if read.streamReadyElapsed != 425*time.Millisecond {
		t.Fatalf("read elapsed = %s, want 425ms", read.streamReadyElapsed)
	}
	if !read.firstWriteToReadObserved {
		t.Fatal("first-read event must observe the preceding first write")
	}
	if read.firstWriteToRead != 400*time.Millisecond {
		t.Fatalf("write-to-read = %s, want 400ms", read.firstWriteToRead)
	}
	if trace.traceID == "" {
		t.Fatal("trace id must be available before the tracker id")
	}
}

func TestSmartConnectionTimingReportsMissingFirstWriteExplicitly(t *testing.T) {
	base := time.Unix(2_000, 0)
	trace := newSmartConnectionTiming("group", "node", "target", "wildcard", 50*time.Millisecond, base)
	wantErr := errors.New("read failed")

	read := trace.firstReadEvent(base.Add(100*time.Millisecond), wantErr)
	if read.firstWriteToReadObserved {
		t.Fatal("read without first write must remain explicitly unobserved")
	}
	if !errors.Is(read.err, wantErr) {
		t.Fatalf("read error = %v, want %v", read.err, wantErr)
	}
	if got := formatSmartTimingError(nil); got != "none" {
		t.Fatalf("nil error label = %q, want none", got)
	}
	if got := formatSmartTimingError(wantErr); got != wantErr.Error() {
		t.Fatalf("error label = %q, want %q", got, wantErr.Error())
	}
}
