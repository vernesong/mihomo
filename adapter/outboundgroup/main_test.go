package outboundgroup

import (
	"os"
	"testing"

	C "github.com/metacubex/mihomo/constant"
)

// TestMain gives the package a real, throw-away bbolt cache file. Without one the Smart store
// runs with a nil DB, and a threshold flush of the process-global write queue silently drops
// queued learning, which makes tests that read learned weights depend on test order.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "smart-test-home-")
	if err != nil {
		panic(err)
	}
	C.SetHomeDir(dir)
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}
