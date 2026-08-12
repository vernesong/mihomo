package route

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/common/utils"
	"github.com/metacubex/mihomo/component/modules"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/hub/executor"

	"github.com/metacubex/http"
	"github.com/metacubex/http/httptest"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestModulesFileOverrideAndControlAPI(t *testing.T) {
	homeDir, configPath, server := newModulesAPITestServer(t)
	modulesDir := filepath.Join(homeDir, "modules")
	require.NoError(t, os.MkdirAll(modulesDir, 0o755))

	module1Path := filepath.Join(modulesDir, "module1.yaml")
	module0Path := filepath.Join(modulesDir, "module0.yaml")
	require.NoError(t, os.WriteFile(module1Path, []byte(`
log-level: warning
+rules:
  - DOMAIN,first.example,DIRECT
dns:
  enable: true
  ipv6: true
`), 0o644))
	require.NoError(t, os.WriteFile(module0Path, []byte(`
log-level: debug
rules+:
  - MATCH,DIRECT
dns!:
  enable: false
`), 0o644))
	require.NoError(t, os.WriteFile(configPath, []byte(`
mixed-port: 0
log-level: info
rules:
  - DOMAIN,base.example,DIRECT
modules:
  module1:
    enable: true
    type: file
    path: ./modules/module1.yaml
  module0:
    enable: true
    type: file
    path: ./modules/module0.yaml
  broken:
    enable: false
    type: file
    path: ./modules/missing.yaml
`), 0o644))

	response := modulesAPIRequest(t, server.Client(), http.MethodPut, server.URL+"/configs?force=true", map[string]any{
		"path": configPath,
	})
	require.Equal(t, http.StatusNoContent, response.StatusCode)
	require.NoError(t, response.Body.Close())

	response = modulesAPIRequest(t, server.Client(), http.MethodGet, server.URL+"/modules", nil)
	require.Equal(t, http.StatusOK, response.StatusCode)
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Less(t, bytes.Index(body, []byte(`"module1"`)), bytes.Index(body, []byte(`"module0"`)))
	var moduleList struct {
		Modules map[string]modules.Info `json:"modules"`
	}
	require.NoError(t, json.Unmarshal(body, &moduleList))
	require.True(t, moduleList.Modules["module1"].Enable)
	require.Equal(t, module1Path, moduleList.Modules["module1"].Path)
	require.True(t, moduleList.Modules["module0"].Enable)
	require.False(t, moduleList.Modules["broken"].Enable)
	require.Nil(t, moduleList.Modules["broken"].UpdatedAt)

	require.Equal(t, "debug", modulesAPILogLevel(t, server))
	rules := modulesAPIRules(t, server)
	require.Len(t, rules, 3)
	require.Equal(t, "first.example", rules[0].Payload)
	require.Equal(t, "base.example", rules[1].Payload)
	require.Equal(t, "Match", rules[2].Type)
	effective := modulesAPIEffectiveConfig(t, server)
	require.Equal(t, "debug", effective.LogLevel)
	require.Equal(t, []string{
		"DOMAIN,first.example,DIRECT",
		"DOMAIN,base.example,DIRECT",
		"MATCH,DIRECT",
	}, effective.Rules)
	require.False(t, effective.DNS.Enable)

	response = modulesAPIRequest(t, server.Client(), http.MethodPatch, server.URL+"/modules/module0", map[string]any{
		"enable": false,
	})
	require.Equal(t, http.StatusNoContent, response.StatusCode)
	require.NoError(t, response.Body.Close())
	require.Equal(t, "warning", modulesAPILogLevel(t, server))

	persisted, err := os.ReadFile(configPath)
	require.NoError(t, err)
	var persistedConfig struct {
		Modules map[string]struct {
			Enable bool `yaml:"enable"`
		} `yaml:"modules"`
	}
	require.NoError(t, yaml.Unmarshal(persisted, &persistedConfig))
	require.False(t, persistedConfig.Modules["module0"].Enable)
	require.True(t, persistedConfig.Modules["module1"].Enable)

	rules = modulesAPIRules(t, server)
	require.Len(t, rules, 2)
	require.Equal(t, "first.example", rules[0].Payload)
	require.Equal(t, "base.example", rules[1].Payload)
	effective = modulesAPIEffectiveConfig(t, server)
	require.Equal(t, "warning", effective.LogLevel)
	require.Equal(t, []string{
		"DOMAIN,first.example,DIRECT",
		"DOMAIN,base.example,DIRECT",
	}, effective.Rules)
	require.True(t, effective.DNS.Enable)
	require.True(t, effective.DNS.IPv6)

	require.NoError(t, os.WriteFile(module1Path, []byte(`
log-level: error
+rules:
  - DOMAIN,changed.example,DIRECT
`), 0o644))
	require.Eventually(t, func() bool {
		return modulesAPILogLevel(t, server) == "error"
	}, 5*time.Second, 50*time.Millisecond)

	response = modulesAPIRequest(t, server.Client(), http.MethodPatch, server.URL+"/modules/module0", map[string]any{
		"enable": true,
	})
	require.Equal(t, http.StatusNoContent, response.StatusCode)
	require.NoError(t, response.Body.Close())
	require.Equal(t, "debug", modulesAPILogLevel(t, server))

	response = modulesAPIRequest(t, server.Client(), http.MethodGet, server.URL+"/modules/module0", nil)
	require.Equal(t, http.StatusOK, response.StatusCode)
	var module0 modules.Info
	require.NoError(t, json.NewDecoder(response.Body).Decode(&module0))
	require.NoError(t, response.Body.Close())
	require.True(t, module0.Enable)

	response = modulesAPIRequest(t, server.Client(), http.MethodPatch, server.URL+"/modules/module0", map[string]any{})
	require.Equal(t, http.StatusBadRequest, response.StatusCode)
	require.NoError(t, response.Body.Close())
	response = modulesAPIRequest(t, server.Client(), http.MethodPatch, server.URL+"/modules/not-found", map[string]any{
		"enable": true,
	})
	require.Equal(t, http.StatusNotFound, response.StatusCode)
	require.NoError(t, response.Body.Close())

	response = modulesAPIRequest(t, server.Client(), http.MethodPatch, server.URL+"/modules/broken", map[string]any{
		"enable": true,
	})
	require.Equal(t, http.StatusBadRequest, response.StatusCode)
	require.NoError(t, response.Body.Close())
	persisted, err = os.ReadFile(configPath)
	require.NoError(t, err)
	require.NoError(t, yaml.Unmarshal(persisted, &persistedConfig))
	require.False(t, persistedConfig.Modules["broken"].Enable)
	require.Equal(t, "debug", modulesAPILogLevel(t, server))
}

func TestModulesOrderAPI(t *testing.T) {
	homeDir, configPath, server := newModulesAPITestServer(t)
	modulesDir := filepath.Join(homeDir, "modules")
	require.NoError(t, os.MkdirAll(modulesDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(modulesDir, "first.yaml"), []byte("log-level: warning\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(modulesDir, "second.yaml"), []byte("log-level: error\n"), 0o644))
	require.NoError(t, os.WriteFile(configPath, []byte(`
mixed-port: 0
log-level: info
modules:
  first-module:
    enable: true
    type: file
    path: ./modules/first.yaml
  second-module:
    enable: true
    type: file
    path: ./modules/second.yaml
  disabled-module:
    enable: false
    type: file
    path: ./modules/missing.yaml
`), 0o644))

	response := modulesAPIRequest(t, server.Client(), http.MethodPut, server.URL+"/configs?force=true", map[string]any{
		"path": configPath,
	})
	require.Equal(t, http.StatusNoContent, response.StatusCode)
	require.NoError(t, response.Body.Close())
	require.Equal(t, "error", modulesAPILogLevel(t, server))
	require.Equal(t, []string{"first-module", "second-module", "disabled-module"}, modulesAPIOrder(t, server))

	order := []string{"second-module", "disabled-module", "first-module"}
	response = modulesAPIRequest(t, server.Client(), http.MethodPatch, server.URL+"/modules", map[string]any{
		"order": order,
	})
	require.Equal(t, http.StatusNoContent, response.StatusCode)
	require.NoError(t, response.Body.Close())
	require.Equal(t, "warning", modulesAPILogLevel(t, server))
	require.Equal(t, order, modulesAPIOrder(t, server))

	persisted, err := os.ReadFile(configPath)
	require.NoError(t, err)
	secondIndex := bytes.Index(persisted, []byte("second-module:"))
	disabledIndex := bytes.Index(persisted, []byte("disabled-module:"))
	firstIndex := bytes.Index(persisted, []byte("first-module:"))
	require.NotEqual(t, -1, secondIndex)
	require.Less(t, secondIndex, disabledIndex)
	require.Less(t, disabledIndex, firstIndex)

	unchanged := append([]byte(nil), persisted...)
	response = modulesAPIRequest(t, server.Client(), http.MethodPatch, server.URL+"/modules", map[string]any{
		"order": order,
	})
	require.Equal(t, http.StatusNoContent, response.StatusCode)
	require.NoError(t, response.Body.Close())
	persisted, err = os.ReadFile(configPath)
	require.NoError(t, err)
	require.Equal(t, unchanged, persisted)

	invalidOrders := []struct {
		name    string
		body    map[string]any
		message string
	}{
		{name: "missing module", body: map[string]any{"order": []string{"second-module", "first-module"}}, message: "missing modules"},
		{name: "duplicate module", body: map[string]any{"order": []string{"second-module", "second-module", "first-module"}}, message: "duplicate module"},
		{name: "unknown module", body: map[string]any{"order": []string{"second-module", "unknown-module", "first-module"}}, message: "unknown module"},
		{name: "missing order field", body: map[string]any{}, message: "Body invalid"},
	}
	for _, testCase := range invalidOrders {
		t.Run(testCase.name, func(t *testing.T) {
			response := modulesAPIRequest(t, server.Client(), http.MethodPatch, server.URL+"/modules", testCase.body)
			require.Equal(t, http.StatusBadRequest, response.StatusCode)
			body, err := io.ReadAll(response.Body)
			require.NoError(t, err)
			require.NoError(t, response.Body.Close())
			require.Contains(t, string(body), testCase.message)

			current, err := os.ReadFile(configPath)
			require.NoError(t, err)
			require.Equal(t, unchanged, current)
			require.Equal(t, order, modulesAPIOrder(t, server))
			require.Equal(t, "warning", modulesAPILogLevel(t, server))
		})
	}
}

func TestModulesOrderRejectsInvalidEffectiveConfigAPI(t *testing.T) {
	homeDir, configPath, server := newModulesAPITestServer(t)
	modulesDir := filepath.Join(homeDir, "modules")
	require.NoError(t, os.MkdirAll(modulesDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(modulesDir, "invalid.yaml"), []byte("log-level: invalid\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(modulesDir, "valid.yaml"), []byte("log-level: warning\n"), 0o644))
	require.NoError(t, os.WriteFile(configPath, []byte(`
mixed-port: 0
modules:
  invalid-first:
    enable: true
    type: file
    path: ./modules/invalid.yaml
  valid-last:
    enable: true
    type: file
    path: ./modules/valid.yaml
`), 0o644))

	response := modulesAPIRequest(t, server.Client(), http.MethodPut, server.URL+"/configs?force=true", map[string]any{
		"path": configPath,
	})
	require.Equal(t, http.StatusNoContent, response.StatusCode)
	require.NoError(t, response.Body.Close())
	require.Equal(t, "warning", modulesAPILogLevel(t, server))

	before, err := os.ReadFile(configPath)
	require.NoError(t, err)
	response = modulesAPIRequest(t, server.Client(), http.MethodPatch, server.URL+"/modules", map[string]any{
		"order": []string{"valid-last", "invalid-first"},
	})
	require.Equal(t, http.StatusBadRequest, response.StatusCode)
	require.NoError(t, response.Body.Close())

	after, err := os.ReadFile(configPath)
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.Equal(t, []string{"invalid-first", "valid-last"}, modulesAPIOrder(t, server))
	require.Equal(t, "warning", modulesAPILogLevel(t, server))
}

func TestModulesHTTPUpdateAndValidationAPI(t *testing.T) {
	var remoteContent atomic.Value
	remoteContent.Store("log-level: warning\n")
	var requestCount atomic.Int32
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requestCount.Add(1)
		_, _ = io.WriteString(w, remoteContent.Load().(string))
	}))
	defer remote.Close()

	homeDir, configPath, server := newModulesAPITestServer(t)
	require.NoError(t, os.WriteFile(configPath, []byte("mixed-port: 0\nmodules:\n  remote:\n    enable: true\n    type: http\n    url: \""+remote.URL+"\"\n    interval: 1\n"), 0o644))

	response := modulesAPIRequest(t, server.Client(), http.MethodPut, server.URL+"/configs?force=true", map[string]any{
		"path": configPath,
	})
	require.Equal(t, http.StatusNoContent, response.StatusCode)
	require.NoError(t, response.Body.Close())
	require.Equal(t, "warning", modulesAPILogLevel(t, server))

	response = modulesAPIRequest(t, server.Client(), http.MethodGet, server.URL+"/modules/remote", nil)
	require.Equal(t, http.StatusOK, response.StatusCode)
	var remoteInfo modules.Info
	require.NoError(t, json.NewDecoder(response.Body).Decode(&remoteInfo))
	require.NoError(t, response.Body.Close())
	expectedPath := filepath.Join(homeDir, "modules", utils.MakeHash([]byte(remote.URL)).String())
	require.Equal(t, expectedPath, remoteInfo.Path)
	cache, err := os.ReadFile(expectedPath)
	require.NoError(t, err)
	require.Equal(t, "log-level: warning\n", string(cache))

	invalidRequestCount := requestCount.Load()
	remoteContent.Store("log-level: invalid\n")
	require.Eventually(t, func() bool {
		return requestCount.Load() > invalidRequestCount
	}, 4*time.Second, 50*time.Millisecond)
	require.Eventually(t, func() bool {
		cache, readErr := os.ReadFile(expectedPath)
		return readErr == nil && string(cache) == "log-level: warning\n"
	}, 2*time.Second, 50*time.Millisecond)
	require.Equal(t, "warning", modulesAPILogLevel(t, server))

	validRequestCount := requestCount.Load()
	remoteContent.Store("log-level: error\n")
	require.Eventually(t, func() bool {
		return requestCount.Load() > validRequestCount && modulesAPILogLevel(t, server) == "error"
	}, 5*time.Second, 50*time.Millisecond)

	response = modulesAPIRequest(t, server.Client(), http.MethodPut, server.URL+"/configs", map[string]any{
		"payload": "modules:\n  missing-enable:\n    type: file\n    path: ./modules/missing.yaml\n",
	})
	require.Equal(t, http.StatusBadRequest, response.StatusCode)
	errorBody, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Contains(t, string(errorBody), "enable is required")

	response = modulesAPIRequest(t, server.Client(), http.MethodPut, server.URL+"/configs", map[string]any{
		"payload": "modules:\n  first:\n    enable: false\n    type: file\n    path: ./modules/same.yaml\n  second:\n    enable: false\n    type: file\n    path: ./modules/same.yaml\n",
	})
	require.Equal(t, http.StatusBadRequest, response.StatusCode)
	errorBody, err = io.ReadAll(response.Body)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Contains(t, string(errorBody), "path duplicates")
	require.Equal(t, "error", modulesAPILogLevel(t, server))
}

func newModulesAPITestServer(t *testing.T) (string, string, *httptest.Server) {
	t.Helper()
	originalHome := C.Path.HomeDir()
	originalConfig := C.Path.Config()
	homeDir := t.TempDir()
	configPath := filepath.Join(homeDir, "config.yaml")
	C.SetHomeDir(homeDir)
	C.SetConfig(configPath)

	server := httptest.NewServer(router(false, "", "", Cors{}))
	t.Cleanup(func() {
		if cfg, err := executor.ParseWithBytes([]byte("mixed-port: 0\nlog-level: silent\n")); err == nil {
			executor.ApplyConfig(cfg, true)
		}
		server.Close()
		C.SetHomeDir(originalHome)
		C.SetConfig(originalConfig)
	})
	return homeDir, configPath, server
}

func modulesAPIRequest(t *testing.T, client *http.Client, method, url string, body any) *http.Response {
	t.Helper()
	var reader io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		require.NoError(t, err)
		reader = bytes.NewReader(payload)
	}
	request, err := http.NewRequest(method, url, reader)
	require.NoError(t, err)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := client.Do(request)
	require.NoError(t, err)
	return response
}

func modulesAPILogLevel(t *testing.T, server *httptest.Server) string {
	t.Helper()
	response := modulesAPIRequest(t, server.Client(), http.MethodGet, server.URL+"/configs", nil)
	require.Equal(t, http.StatusOK, response.StatusCode)
	defer response.Body.Close()
	var config struct {
		LogLevel string `json:"log-level"`
	}
	require.NoError(t, json.NewDecoder(response.Body).Decode(&config))
	return config.LogLevel
}

func modulesAPIRules(t *testing.T, server *httptest.Server) []Rule {
	t.Helper()
	response := modulesAPIRequest(t, server.Client(), http.MethodGet, server.URL+"/rules", nil)
	require.Equal(t, http.StatusOK, response.StatusCode)
	defer response.Body.Close()
	var result struct {
		Rules []Rule `json:"rules"`
	}
	require.NoError(t, json.NewDecoder(response.Body).Decode(&result))
	return result.Rules
}

func modulesAPIOrder(t *testing.T, server *httptest.Server) []string {
	t.Helper()
	response := modulesAPIRequest(t, server.Client(), http.MethodGet, server.URL+"/modules", nil)
	require.Equal(t, http.StatusOK, response.StatusCode)
	defer response.Body.Close()
	var result struct {
		Order []string `json:"order"`
	}
	require.NoError(t, json.NewDecoder(response.Body).Decode(&result))
	return result.Order
}

type modulesAPIEffective struct {
	LogLevel string   `yaml:"log-level"`
	Rules    []string `yaml:"rules"`
	DNS      struct {
		Enable bool `yaml:"enable"`
		IPv6   bool `yaml:"ipv6"`
	} `yaml:"dns"`
}

func modulesAPIEffectiveConfig(t *testing.T, server *httptest.Server) modulesAPIEffective {
	t.Helper()
	response := modulesAPIRequest(t, server.Client(), http.MethodGet, server.URL+"/configs/modules", nil)
	require.Equal(t, http.StatusOK, response.StatusCode)
	defer response.Body.Close()
	require.Equal(t, "application/yaml; charset=utf-8", response.Header.Get("Content-Type"))
	var effective modulesAPIEffective
	require.NoError(t, yaml.NewDecoder(response.Body).Decode(&effective))
	return effective
}

func TestModulesEscapedNameAPI(t *testing.T) {
	_, configPath, server := newModulesAPITestServer(t)
	modulePath := filepath.Join(filepath.Dir(configPath), "escaped.yaml")
	require.NoError(t, os.WriteFile(modulePath, []byte("log-level: warning\n"), 0o644))
	require.NoError(t, os.WriteFile(configPath, []byte(`
mixed-port: 0
modules:
  module/name:
    enable: true
    type: file
    path: ./escaped.yaml
`), 0o644))

	response := modulesAPIRequest(t, server.Client(), http.MethodPut, server.URL+"/configs", map[string]any{"path": configPath})
	require.Equal(t, http.StatusNoContent, response.StatusCode)
	require.NoError(t, response.Body.Close())

	response = modulesAPIRequest(t, server.Client(), http.MethodGet, server.URL+"/modules/"+strings.ReplaceAll("module/name", "/", "%2F"), nil)
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.NoError(t, response.Body.Close())
}
