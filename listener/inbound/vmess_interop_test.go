package inbound_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	N "github.com/metacubex/mihomo/common/net"
	C "github.com/metacubex/mihomo/constant"

	"github.com/stretchr/testify/require"
)

const vmessInteropV2RayRef = "v5.51.2"
const vmessInteropV2RayXNetRef = "bd5f1dcf71cf0d6d2424021d0a04f191396a46a7" // http2: initialize Transport on NewClientConn

func vmessInteropSkip(t *testing.T) {
	t.Helper()
	if skip, _ := strconv.ParseBool(os.Getenv("SKIP_INTEROP_TEST")); skip {
		t.Skip("SKIP_INTEROP_TEST is set")
	}
}

func vmessInteropV2RayBinary(t *testing.T) string {
	t.Helper()
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go toolchain not found, skip real v2ray interop test")
	}

	root := filepath.Join(os.TempDir(), "mihomo-vmess-interop", vmessInteropV2RayRef)
	binDir := filepath.Join(root, "bin")
	exe := ""
	if runtime.GOOS == "windows" {
		exe = ".exe"
	}
	v2rayBin := filepath.Join(binDir, "v2ray"+exe)
	if _, err := os.Stat(v2rayBin); err == nil {
		return v2rayBin
	}
	goVersion := vmessInteropGoVersion(t, goBin)
	goMajor, goMinor, ok := vmessInteropGoVersionMajorMinor(goVersion)
	if ok && goMajor == 1 && goMinor < 21 {
		t.Skipf("%s does not support GOTOOLCHAIN toolchain download, skip real v2ray interop test", goVersion)
	}

	require.NoError(t, os.RemoveAll(root))
	require.NoError(t, os.MkdirAll(binDir, 0o755))

	vmessInteropGo(t, goBin, root, "mod", "init", "mihomo-vmess-interop")
	vmessInteropGo(t, goBin, root, "get", "github.com/v2fly/v2ray-core/v5@"+vmessInteropV2RayRef)
	if ok && (goMajor > 1 || goMajor == 1 && goMinor > 26) {
		vmessInteropGo(t, goBin, root, "get", "golang.org/x/net@"+vmessInteropV2RayXNetRef)
	}
	vmessInteropGo(t, goBin, root, "build", "-mod=mod", "-trimpath", "-o", v2rayBin, "github.com/v2fly/v2ray-core/v5/main")
	return v2rayBin
}

func vmessInteropGoVersion(t *testing.T, goBin string) string {
	t.Helper()
	cmd := exec.Command(goBin, "version")
	output, err := cmd.Output()
	require.NoError(t, err, "go version")
	return vmessInteropParseGoVersion(string(output))
}

func vmessInteropParseGoVersion(output string) string {
	for _, field := range strings.Fields(output) {
		if strings.HasPrefix(field, "go1.") {
			return field
		}
	}
	return ""
}

func vmessInteropGoVersionMajorMinor(version string) (int, int, bool) {
	version = strings.TrimPrefix(version, "go")
	parts := strings.SplitN(version, ".", 3)
	if len(parts) < 2 {
		return 0, 0, false
	}
	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, false
	}
	minorText := parts[1]
	for i, r := range minorText {
		if r < '0' || r > '9' {
			minorText = minorText[:i]
			break
		}
	}
	if minorText == "" {
		return 0, 0, false
	}
	minor, err := strconv.Atoi(minorText)
	if err != nil {
		return 0, 0, false
	}
	return major, minor, true
}

func vmessInteropGo(t *testing.T, goBin, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command(goBin, args...)
	cmd.Dir = dir
	cmd.Env = vmessInteropGoEnv()
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "go %s\n%s", strings.Join(args, " "), string(output))
}

func vmessInteropGoEnv() []string {
	env := os.Environ()
	hasGoToolchain := false
	for i, value := range env {
		if strings.HasPrefix(value, "GOTOOLCHAIN=") {
			env[i] = "GOTOOLCHAIN=auto"
			hasGoToolchain = true
		}
	}
	if !hasGoToolchain {
		env = append(env, "GOTOOLCHAIN=auto")
	}
	return env
}

func startVMessInteropV2Ray(t *testing.T, v2rayBin string, config []byte, release func(), waitAddr string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, v2rayBin, "run", "-format=jsonv5")
	var output bytes.Buffer
	cmd.Stdin = bytes.NewReader(config)
	cmd.Stdout = &output
	cmd.Stderr = &output
	if release != nil {
		release()
	}
	require.NoError(t, cmd.Start())
	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()
	waited := false
	t.Cleanup(func() {
		cancel()
		if !waited {
			<-done
		}
		if t.Failed() {
			t.Log(output.String())
		}
	})

	if waitAddr == "" {
		select {
		case err := <-done:
			waited = true
			require.NoError(t, err, output.String())
			t.Fatalf("v2ray exited before interop test started\n%s", output.String())
		case <-time.After(300 * time.Millisecond):
		}
		return
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-done:
			waited = true
			require.NoError(t, err, output.String())
			t.Fatalf("v2ray exited before listening on %s\n%s", waitAddr, output.String())
		default:
		}
		conn, err := net.DialTimeout("tcp", waitAddr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("v2ray did not listen on %s\n%s", waitAddr, output.String())
}

func vmessInteropBaseConfig() map[string]any {
	return map[string]any{
		"log": map[string]any{
			"error": map[string]any{
				"type":  "Console",
				"level": "Debug",
			},
		},
	}
}

func vmessInteropDirectOutbound() map[string]any {
	return map[string]any{
		"protocol": "freedom",
		"tag":      "direct",
	}
}

func vmessInteropMarshalJSONConfig(t *testing.T, config map[string]any) []byte {
	t.Helper()
	data, err := json.MarshalIndent(config, "", "  ")
	require.NoError(t, err)
	data = append(data, '\n')
	return data
}

func vmessInteropParsePort(t *testing.T, port string) int {
	t.Helper()
	value, err := strconv.Atoi(port)
	require.NoError(t, err)
	return value
}

func startVMessInteropEcho(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()
	return ln.Addr().String()
}

func vmessInteropDirectTunnel(t *testing.T) *TestTunnel {
	t.Helper()
	return &TestTunnel{
		HandleTCPConnFn: func(conn net.Conn, metadata *C.Metadata) {
			target, err := net.Dial("tcp", metadata.RemoteAddress())
			if err != nil {
				_ = conn.Close()
				return
			}
			N.Relay(target, conn)
		},
		HandleUDPPacketFn: func(packet C.UDPPacket, metadata *C.Metadata) {
			packet.Drop()
		},
		NatTableFn: func() C.NatTable {
			return nil
		},
		CloseFn: func() error {
			return nil
		},
		NewDialerFn: func() C.Dialer {
			return nil
		},
	}
}

func vmessInteropMetadata(t *testing.T, addr string) *C.Metadata {
	t.Helper()
	host, port, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	ip, err := netip.ParseAddr(host)
	require.NoError(t, err)
	portNum, err := net.LookupPort("tcp", port)
	require.NoError(t, err)
	return &C.Metadata{
		NetWork: C.TCP,
		DstIP:   ip,
		DstPort: uint16(portNum),
	}
}

func vmessInteropRoundTripWithRetry(t *testing.T, dial func() (net.Conn, error), payloadSize int) {
	t.Helper()
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		conn, err := dial()
		if err == nil {
			err = vmessInteropRoundTripConn(conn, payloadSize)
		} else {
			err = fmt.Errorf("dial: %w", err)
		}
		if err == nil {
			return
		}
		lastErr = err
		var netErr net.Error
		if !errors.As(err, &netErr) || !netErr.Timeout() {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	require.NoError(t, lastErr)
}

func vmessInteropRoundTripConn(conn net.Conn, payloadSize int) error {
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return fmt.Errorf("set deadline: %w", err)
	}
	if payloadSize == 0 {
		payloadSize = len("vmess-interop-") * 256
	}
	for round, payload := range vmessInteropRoundTripPayloads(payloadSize) {
		if err := vmessInteropWritePayload(conn, payload, round); err != nil {
			return fmt.Errorf("write round %d size %d: %w", round, len(payload), err)
		}
		got := make([]byte, len(payload))
		if _, err := io.ReadFull(conn, got); err != nil {
			return fmt.Errorf("read full round %d size %d: %w", round, len(payload), err)
		}
		if !bytes.Equal(payload, got) {
			return fmt.Errorf("unexpected payload round %d size %d", round, len(payload))
		}
	}
	return nil
}

func vmessInteropRoundTripPayloads(payloadSize int) [][]byte {
	sizes := []int{1, 7, 64, 1024, payloadSize / 2, payloadSize}
	payloads := make([][]byte, 0, len(sizes))
	seen := make(map[int]struct{}, len(sizes))
	for round, size := range sizes {
		if size > payloadSize {
			size = payloadSize
		}
		if size <= 0 {
			continue
		}
		if _, ok := seen[size]; ok {
			continue
		}
		seen[size] = struct{}{}
		payloads = append(payloads, vmessInteropPayload(size, round))
	}
	return payloads
}

func vmessInteropPayload(size, round int) []byte {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte((i*31 + round*17 + i>>8) % 251)
	}
	return payload
}

func vmessInteropWritePayload(conn net.Conn, payload []byte, round int) error {
	if len(payload) <= 1 {
		return vmessInteropWrite(conn, payload)
	}
	split := 1 + (round*37)%(len(payload)-1)
	if err := vmessInteropWrite(conn, payload[:split]); err != nil {
		return err
	}
	return vmessInteropWrite(conn, payload[split:])
}

func vmessInteropWrite(conn net.Conn, payload []byte) error {
	n, err := conn.Write(payload)
	if err != nil {
		return err
	}
	if n != len(payload) {
		return io.ErrShortWrite
	}
	return nil
}

type vmessInteropReservedTCPPort struct {
	ln   net.Listener
	port int
	once sync.Once
}

func vmessInteropReserveTCPPort(t *testing.T) *vmessInteropReservedTCPPort {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := &vmessInteropReservedTCPPort{
		ln:   ln,
		port: ln.Addr().(*net.TCPAddr).Port,
	}
	t.Cleanup(port.Release)
	return port
}

func (p *vmessInteropReservedTCPPort) Port() int {
	return p.port
}

func (p *vmessInteropReservedTCPPort) Release() {
	p.once.Do(func() {
		_ = p.ln.Close()
	})
}

type vmessInteropReservedUDPPort struct {
	pc   net.PacketConn
	port int
	once sync.Once
}

func vmessInteropReserveUDPPort(t *testing.T) *vmessInteropReservedUDPPort {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	port := &vmessInteropReservedUDPPort{
		pc:   pc,
		port: pc.LocalAddr().(*net.UDPAddr).Port,
	}
	t.Cleanup(port.Release)
	return port
}

func (p *vmessInteropReservedUDPPort) Port() int {
	return p.port
}

func (p *vmessInteropReservedUDPPort) Release() {
	p.once.Do(func() {
		_ = p.pc.Close()
	})
}

func vmessInteropPort(addr string) string {
	_, port, _ := net.SplitHostPort(addr)
	return port
}

func vmessInteropCertChainHash(certContent []byte) string {
	var hashValue []byte
	for {
		block, remain := pem.Decode(certContent)
		if block == nil {
			break
		}
		certHash := sha256.Sum256(block.Bytes)
		if hashValue == nil {
			hashValue = certHash[:]
		} else {
			chainHash := sha256.Sum256(append(hashValue, certHash[:]...))
			hashValue = chainHash[:]
		}
		certContent = remain
	}
	return base64.StdEncoding.EncodeToString(hashValue)
}
