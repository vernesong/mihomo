package tcpstats

import (
	"io"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestGetTCPStats_Loopback(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer ln.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 4096)
		for {
			n, err := conn.Read(buf)
			if err != nil {
				return
			}
			_, err = conn.Write(buf[:n])
			if err != nil {
				return
			}
		}
	}()

	// Dial
	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer client.Close()

	// Transfer some data to populate TCP counters
	payload := make([]byte, 1024*1024) // 1MB
	for i := range payload {
		payload[i] = byte(i % 256)
	}
	if _, err := client.Write(payload); err != nil {
		t.Fatalf("failed to write: %v", err)
	}
	if _, err := io.ReadFull(client, payload); err != nil {
		t.Fatalf("failed to read: %v", err)
	}

	// Small delay for kernel counters to settle
	time.Sleep(50 * time.Millisecond)

	stats := GetTCPStats(client)
	if stats == nil {
		switch runtime.GOOS {
		case "linux", "darwin", "windows", "freebsd":
			t.Logf("GetTCPStats returned nil on %s (may be expected if connection is already closed or kernel too old)", runtime.GOOS)
		default:
			t.Logf("GetTCPStats not supported on %s", runtime.GOOS)
		}
		return
	}

	lossRate := stats.LossRate()
	t.Logf("Platform: %s, SegsOut: %d, RetransSegs: %d, BytesSent: %d, BytesRetrans: %d, LossRate: %.4f",
		runtime.GOOS, stats.SegsOut, stats.RetransSegs, stats.BytesSent, stats.BytesRetrans, lossRate)

	// On loopback, loss rate should be 0
	if lossRate != 0 {
		t.Errorf("expected 0 loss rate on loopback, got %.4f", lossRate)
	}

	// At least one of SegsOut or BytesSent should be populated
	// (FreeBSD uses TCP_PERF_INFO which fills BytesSent; Linux uses TCP_INFO which fills SegsOut)
	if stats.SegsOut == 0 && stats.BytesSent == 0 {
		t.Error("expected non-zero sent statistics (SegsOut or BytesSent)")
	}

	// If BytesSent is populated, ensure BytesRetrans is also meaningful
	if stats.BytesSent > 0 && stats.BytesRetrans > stats.BytesSent {
		t.Errorf("BytesRetrans (%d) exceeds BytesSent (%d)", stats.BytesRetrans, stats.BytesSent)
	}
}

func TestGetTCPStats_NilConn(t *testing.T) {
	if stats := GetTCPStats(nil); stats != nil {
		t.Error("expected nil stats for nil connection")
	}
}

func TestLossRate_Nil(t *testing.T) {
	var s *Stats
	if rate := s.LossRate(); rate != 0 {
		t.Errorf("expected 0 for nil Stats, got %.4f", rate)
	}
}

func TestLossRate_NoData(t *testing.T) {
	s := &Stats{}
	if rate := s.LossRate(); rate != 0 {
		t.Errorf("expected 0 for empty Stats, got %.4f", rate)
	}
}

func TestLossRate_NoLoss(t *testing.T) {
	s := &Stats{SegsOut: 1000, RetransSegs: 0}
	if rate := s.LossRate(); rate != 0 {
		t.Errorf("expected 0 for no retrans, got %.4f", rate)
	}
}

func TestLossRate_WithLoss(t *testing.T) {
	s := &Stats{SegsOut: 1000, RetransSegs: 50}
	expected := 0.05
	if rate := s.LossRate(); rate != expected {
		t.Errorf("expected %.4f, got %.4f", expected, rate)
	}
}

func TestLossRate_BytesBased(t *testing.T) {
	s := &Stats{BytesSent: 1000000, BytesRetrans: 50000, SegsOut: 0, RetransSegs: 0}
	expected := 0.05
	if rate := s.LossRate(); rate != expected {
		t.Errorf("expected %.4f for bytes-based, got %.4f", expected, rate)
	}
}
