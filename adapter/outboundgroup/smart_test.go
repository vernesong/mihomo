package outboundgroup

import (
	"context"
	"encoding/json"
	"errors"
	"net/netip"
	"testing"

	"github.com/metacubex/mihomo/common/lru"
	"github.com/metacubex/mihomo/common/utils"
	"github.com/metacubex/mihomo/component/profile/cachefile"
	C "github.com/metacubex/mihomo/constant"
)

func TestNewSmartRejectsUnsupportedStrategy(t *testing.T) {
	_, err := NewSmart(GroupCommonOption{Name: "smart", URL: "https://example.com/generate_204"}, SmartOption{Strategy: "round-robin"}, nil, nil)
	if err == nil {
		t.Fatal("expected unsupported smart strategy error")
	}
}

func TestNewSmartKeepsEmptyStrategyNonSticky(t *testing.T) {
	s, err := NewSmart(GroupCommonOption{Name: "smart", URL: "https://example.com/generate_204"}, SmartOption{}, nil, nil)
	if err != nil {
		t.Fatalf("expected empty smart strategy to be accepted: %v", err)
	}
	t.Cleanup(func() {
		_ = s.Close()
	})

	if s.strategy != "" {
		t.Fatalf("expected empty strategy, got %q", s.strategy)
	}
	if s.stickyCache != nil {
		t.Fatal("empty strategy should not initialize sticky cache")
	}
}

func TestSmartStickyKeyUsesSourceAndEffectiveDestination(t *testing.T) {
	metadata := &C.Metadata{
		SrcIP:       netip.MustParseAddr("192.0.2.10"),
		DstIP:       netip.MustParseAddr("203.0.113.8"),
		Host:        "chat.openai.com",
		SmartTarget: "RuleSet [AI Suite]",
	}

	sameEffectiveTarget := metadata.Clone()
	sameEffectiveTarget.SmartTarget = "RuleSet [Other Label]"

	differentSource := metadata.Clone()
	differentSource.SrcIP = netip.MustParseAddr("192.0.2.11")

	differentHost := metadata.Clone()
	differentHost.Host = "api.another-example.com"

	key := getSmartStickyKey(metadata)
	if key != getSmartStickyKey(sameEffectiveTarget) {
		t.Fatal("expected sticky key to ignore broad SmartTarget labels when host/IP is available")
	}
	if key == getSmartStickyKey(differentSource) {
		t.Fatal("expected sticky key to include source IP")
	}
	if key == getSmartStickyKey(differentHost) {
		t.Fatal("expected sticky key to include effective destination")
	}
}

func TestSmartStickySelectionReusesHealthyProxy(t *testing.T) {
	s := newSmartStickyTestGroup()
	metadata := smartStickyTestMetadata()
	proxies := []C.Proxy{
		newSmartTestProxy("proxy-a", true, true, 30),
		newSmartTestProxy("proxy-b", true, true, 20),
		newSmartTestProxy("proxy-c", true, true, 10),
	}

	s.stickyCache.Set(getSmartStickyKey(metadata), "proxy-b")

	selected, _ := s.selectProxies(metadata, proxies)
	if len(selected) != 1 {
		t.Fatalf("expected one sticky proxy, got %d", len(selected))
	}
	if selected[0].Name() != "proxy-b" {
		t.Fatalf("expected proxy-b, got %s", selected[0].Name())
	}
}

func TestSmartStickySelectionDropsUnavailableProxy(t *testing.T) {
	s := newSmartStickyTestGroup()
	metadata := smartStickyTestMetadata()
	proxies := []C.Proxy{
		newSmartTestProxy("proxy-a", true, true, 30),
		newSmartTestProxy("proxy-c", true, true, 10),
	}

	s.stickyCache.Set(getSmartStickyKey(metadata), "proxy-b")

	selected, _ := s.selectProxies(metadata, proxies)
	if len(selected) != len(proxies) {
		t.Fatalf("expected fallback Smart candidates, got %d proxies", len(selected))
	}
	for _, proxy := range selected {
		if proxy.Name() == "proxy-b" {
			t.Fatal("unavailable sticky proxy should not be returned")
		}
	}
}

func TestSmartStoreStickyProxyWritesCache(t *testing.T) {
	s := newSmartStickyTestGroup()
	metadata := smartStickyTestMetadata()
	proxy := newSmartTestProxy("proxy-b", true, true, 20)

	s.storeStickyProxy(metadata, proxy)

	proxyName, ok := s.stickyCache.Get(getSmartStickyKey(metadata))
	if !ok {
		t.Fatal("expected sticky proxy to be cached")
	}
	if proxyName != "proxy-b" {
		t.Fatalf("expected proxy-b in sticky cache, got %s", proxyName)
	}
}

func TestSmartClearStickyProxyDeletesCache(t *testing.T) {
	s := newSmartStickyTestGroup()
	metadata := smartStickyTestMetadata()

	s.stickyCache.Set(getSmartStickyKey(metadata), "proxy-b")
	s.clearStickyProxy(metadata)

	if proxyName, ok := s.stickyCache.Get(getSmartStickyKey(metadata)); ok {
		t.Fatalf("expected sticky cache entry to be cleared, got %s", proxyName)
	}
}

func TestSmartDialContextClearsStickyOnSingleProxyFailure(t *testing.T) {
	s := newSmartStickyTestGroup()
	metadata := smartStickyTestMetadata()
	proxy := newSmartTestProxy("proxy-b", true, true, 20)
	proxy.dialErr = errors.New("dial failed")
	s.providerProxies = []C.Proxy{proxy}

	s.stickyCache.Set(getSmartStickyKey(metadata), proxy.Name())

	_, err := s.DialContext(context.Background(), metadata)
	if err == nil {
		t.Fatal("expected dial failure")
	}
	if proxyName, ok := s.stickyCache.Get(getSmartStickyKey(metadata)); ok {
		t.Fatalf("expected sticky cache entry to be cleared after failed single-proxy dial, got %s", proxyName)
	}
}

func newSmartStickyTestGroup() *Smart {
	return &Smart{
		GroupBase: NewGroupBase(GroupBaseOption{
			Name: "smart-test",
			Type: C.Smart,
		}),
		store:       cachefile.GetSmartStore(),
		configName:  "smart-test-config",
		testUrl:     "https://example.com/generate_204",
		strategy:    smartStrategyStickySessions,
		stickyCache: lru.New[uint64, string](lru.WithAge[uint64, string](600), lru.WithSize[uint64, string](1000)),
	}
}

func smartStickyTestMetadata() *C.Metadata {
	return &C.Metadata{
		NetWork:     C.TCP,
		SrcIP:       netip.MustParseAddr("192.0.2.10"),
		DstIP:       netip.MustParseAddr("203.0.113.8"),
		Host:        "api.example.com",
		SmartTarget: "RuleSet [AI Suite]",
	}
}

type smartTestProxy struct {
	name    string
	alive   bool
	udp     bool
	delay   uint16
	dialErr error
}

func newSmartTestProxy(name string, alive bool, udp bool, delay uint16) *smartTestProxy {
	return &smartTestProxy{name: name, alive: alive, udp: udp, delay: delay}
}

func (p *smartTestProxy) Name() string { return p.name }

func (p *smartTestProxy) Type() C.AdapterType { return C.Direct }

func (p *smartTestProxy) Addr() string { return p.name }

func (p *smartTestProxy) SupportUDP() bool { return p.udp }

func (p *smartTestProxy) ProxyInfo() C.ProxyInfo { return C.ProxyInfo{} }

func (p *smartTestProxy) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]string{"name": p.name})
}

func (p *smartTestProxy) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	if p.dialErr != nil {
		return nil, p.dialErr
	}
	return nil, C.ErrNotSupport
}

func (p *smartTestProxy) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
	return nil, C.ErrNotSupport
}

func (p *smartTestProxy) SupportUOT() bool { return false }

func (p *smartTestProxy) IsL3Protocol(metadata *C.Metadata) bool { return false }

func (p *smartTestProxy) Unwrap(metadata *C.Metadata, touch bool) C.Proxy { return p }

func (p *smartTestProxy) Close() error { return nil }

func (p *smartTestProxy) Adapter() C.ProxyAdapter { return p }

func (p *smartTestProxy) AliveForTestUrl(url string) bool { return p.alive }

func (p *smartTestProxy) DelayHistory() []C.DelayHistory { return nil }

func (p *smartTestProxy) DelayHistoryForTestUrl(url string) []C.DelayHistory { return nil }

func (p *smartTestProxy) ExtraDelayHistories() map[string]C.ProxyState { return nil }

func (p *smartTestProxy) LastDelayForTestUrl(url string) uint16 {
	if !p.alive {
		return 0xffff
	}
	if p.delay == 0 {
		return 1
	}
	return p.delay
}

func (p *smartTestProxy) URLTest(ctx context.Context, url string, expectedStatus utils.IntRanges[uint16]) (uint16, error) {
	return p.LastDelayForTestUrl(url), nil
}

func (p *smartTestProxy) StatusTest(ctx context.Context, url string) (uint16, bool, error) {
	return 204, p.alive, nil
}
