package outboundgroup

import (
	"bytes"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/adapter/provider"
	"github.com/metacubex/mihomo/component/smart"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

// countTimedTasks counts the running Smart timed-task goroutines (group and global ones).
func countTimedTasks() int {
	var buf bytes.Buffer
	_ = pprof.Lookup("goroutine").WriteTo(&buf, 2)
	s := buf.String()
	return strings.Count(s, "outboundgroup.(*Smart).startTimedTask.func1(") +
		strings.Count(s, "outboundgroup.runTimedTask.func1(")
}

func waitTasks(want int) int {
	deadline := time.Now().Add(2 * time.Second)
	for {
		n := countTimedTasks()
		if n == want || time.Now().After(deadline) {
			return n
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func newNamedSmart(t *testing.T, nodes ...*fakeNode) *Smart {
	t.Helper()
	proxies := make([]C.Proxy, 0, len(nodes))
	for _, n := range nodes {
		proxies = append(proxies, adapter.NewProxy(n))
	}
	pd, err := provider.NewCompatibleProvider("smart-lc", proxies, provider.NewHealthCheck(proxies, "", 0, 0, true, nil))
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewSmart(GroupCommonOption{Name: "smart-lc-" + t.Name() + "-" + time.Now().Format("150405.000000000")},
		SmartOption{}, adapter.NewProxy(outbound.NewCompatible()), []P.ProxyProvider{pd})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// A config reload parses the new config (NewSmart -> InitSmart) before ApplyConfig closes the
// old groups (closeSmart). The process-wide tasks (queue flush, cache tuning, orphan clean-up)
// must keep running after the old group that started them is closed.
func TestSmartGlobalTasksSurviveConfigReload(t *testing.T) {
	base := countTimedTasks()
	old := newNamedSmart(t, newFakeNode("a"))
	withGlobals := countTimedTasks() - base
	fresh := newNamedSmart(t, newFakeNode("a")) // the new config, parsed before the old one closes
	defer fresh.Close()
	groupOnly := countTimedTasks() - base - withGlobals
	_ = old.Close() // ApplyConfig -> closeSmart on the old groups

	remaining := waitTasks(base+withGlobals) - base
	if withGlobals <= groupOnly {
		t.Fatalf("setup: expected the first group to also own the global tasks (%d vs %d)", withGlobals, groupOnly)
	}
	if remaining == groupOnly {
		t.Fatalf("after the reload only the new group's own tasks run (%d); the %d global tasks are gone",
			remaining, withGlobals-groupOnly)
	}
}

// Every Close must persist the queued learning, not only the first Close in the process.
func TestSmartEveryCloseFlushesQueuedLearning(t *testing.T) {
	first := newNamedSmart(t, newFakeNode("a"))
	_ = first.Close() // the first close in the process

	// as in a reload, another group stays alive while this one closes, so only Close's own
	// flush can persist the queue here
	keep := newNamedSmart(t, newFakeNode("a"))
	defer keep.Close()
	s := newNamedSmart(t, newFakeNode("a"))
	// start from an empty queue so the append below cannot hit the batch threshold
	s.store.FlushQueue(true)
	s.store.AppendToGlobalQueue(smart.StoreOperation{
		Type: smart.OpSaveRanking, Group: s.Name(), Config: s.configName, Data: []byte(`{}`),
	})
	if smart.QueuedOperations() != 1 {
		t.Fatalf("setup: expected exactly 1 queued operation, got %d", smart.QueuedOperations())
	}
	_ = s.Close()
	if n := smart.QueuedOperations(); n > 0 {
		t.Fatalf("the queue still holds %d operation(s) after Close", n)
	}
}
