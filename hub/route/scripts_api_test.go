package route

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	S "github.com/metacubex/mihomo/component/script"

	"github.com/metacubex/http"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestScriptsControlAPI(t *testing.T) {
	homeDir, configPath, server := newModulesAPITestServer(t)
	scriptsDir := filepath.Join(homeDir, "scripts")
	require.NoError(t, os.MkdirAll(scriptsDir, 0o755))

	requestScriptPath := filepath.Join(scriptsDir, "request.js")
	cronScriptPath := filepath.Join(scriptsDir, "cron.js")
	require.NoError(t, os.WriteFile(requestScriptPath, []byte("$done();\n"), 0o644))
	require.NoError(t, os.WriteFile(cronScriptPath, []byte("$done();\n"), 0o644))
	require.NoError(t, os.WriteFile(configPath, []byte(`
mixed-port: 0
scripts:
  request/script:
    enable: true
    debug: true
    type: http-request
    match: '^https://api\.example\.com/'
    path: ./scripts/request.js
    interval: 9
    options:
      timeout: 7
      binary-body-mode: true
      requires-body: true
      max-body-size: 2
      indirect-eval: true
    argument: api-argument
  cron-script:
    enable: false
    type: cron
    cron: '0 * * * * *'
    path: ./scripts/cron.js
  broken:
    enable: false
    type: cron
    cron: '0 * * * * *'
    path: ./scripts/missing.js
`), 0o644))

	response := scriptsAPIRequest(t, server.Client(), http.MethodPut, server.URL+"/configs?force=true", map[string]any{
		"path": configPath,
	})
	require.Equal(t, http.StatusNoContent, response.StatusCode)
	require.NoError(t, response.Body.Close())

	response = scriptsAPIRequest(t, server.Client(), http.MethodGet, server.URL+"/scripts", nil)
	require.Equal(t, http.StatusOK, response.StatusCode)
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Less(t, bytes.Index(body, []byte(`"request/script"`)), bytes.Index(body, []byte(`"cron-script"`)))
	var scriptList struct {
		Scripts map[string]S.Info `json:"scripts"`
	}
	require.NoError(t, json.Unmarshal(body, &scriptList))
	requestInfo := scriptList.Scripts["request/script"]
	require.True(t, requestInfo.Enable)
	require.True(t, requestInfo.Debug)
	require.Equal(t, S.TypeHTTPRequest, requestInfo.Type)
	require.Equal(t, `^https://api\.example\.com/`, requestInfo.Match)
	require.Equal(t, requestScriptPath, requestInfo.Path)
	require.EqualValues(t, 9, requestInfo.Interval)
	require.EqualValues(t, 7, requestInfo.Options.Timeout)
	require.True(t, requestInfo.Options.BinaryBodyMode)
	require.True(t, requestInfo.Options.RequiresBody)
	require.EqualValues(t, 2, requestInfo.Options.MaxBodySize)
	require.True(t, requestInfo.Options.IndirectEval)
	require.Equal(t, "api-argument", requestInfo.Argument)
	require.NotNil(t, requestInfo.UpdatedAt)
	cronInfo := scriptList.Scripts["cron-script"]
	require.False(t, cronInfo.Enable)
	require.Equal(t, S.TypeCron, cronInfo.Type)
	require.Equal(t, "0 * * * * *", cronInfo.Cron)
	require.Equal(t, cronScriptPath, cronInfo.Path)
	require.Nil(t, cronInfo.UpdatedAt)

	escapedName := strings.ReplaceAll("request/script", "/", "%2F")
	response = scriptsAPIRequest(t, server.Client(), http.MethodPatch, server.URL+"/scripts/"+escapedName, map[string]any{
		"enable": false,
	})
	require.Equal(t, http.StatusNoContent, response.StatusCode)
	require.NoError(t, response.Body.Close())

	response = scriptsAPIRequest(t, server.Client(), http.MethodGet, server.URL+"/scripts/"+escapedName, nil)
	require.Equal(t, http.StatusOK, response.StatusCode)
	var disabledInfo S.Info
	require.NoError(t, json.NewDecoder(response.Body).Decode(&disabledInfo))
	require.NoError(t, response.Body.Close())
	require.False(t, disabledInfo.Enable)
	require.Nil(t, disabledInfo.UpdatedAt)

	persisted, err := os.ReadFile(configPath)
	require.NoError(t, err)
	var persistedConfig struct {
		Scripts map[string]struct {
			Enable bool `yaml:"enable"`
		} `yaml:"scripts"`
	}
	require.NoError(t, yaml.Unmarshal(persisted, &persistedConfig))
	require.False(t, persistedConfig.Scripts["request/script"].Enable)
	require.False(t, persistedConfig.Scripts["broken"].Enable)

	response = scriptsAPIRequest(t, server.Client(), http.MethodPatch, server.URL+"/scripts/"+escapedName, map[string]any{
		"enable": true,
	})
	require.Equal(t, http.StatusNoContent, response.StatusCode)
	require.NoError(t, response.Body.Close())

	response = scriptsAPIRequest(t, server.Client(), http.MethodPatch, server.URL+"/scripts/"+escapedName, map[string]any{})
	require.Equal(t, http.StatusBadRequest, response.StatusCode)
	require.NoError(t, response.Body.Close())

	response = scriptsAPIRequest(t, server.Client(), http.MethodPatch, server.URL+"/scripts/not-found", map[string]any{
		"enable": true,
	})
	require.Equal(t, http.StatusNotFound, response.StatusCode)
	require.NoError(t, response.Body.Close())

	response = scriptsAPIRequest(t, server.Client(), http.MethodPatch, server.URL+"/scripts/broken", map[string]any{
		"enable": true,
	})
	require.Equal(t, http.StatusBadRequest, response.StatusCode)
	require.NoError(t, response.Body.Close())
	persisted, err = os.ReadFile(configPath)
	require.NoError(t, err)
	require.NoError(t, yaml.Unmarshal(persisted, &persistedConfig))
	require.False(t, persistedConfig.Scripts["broken"].Enable)
}

func scriptsAPIRequest(t *testing.T, client *http.Client, method, url string, body any) *http.Response {
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
