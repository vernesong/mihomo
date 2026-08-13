package route

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	N "github.com/metacubex/mihomo/component/notification"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/metacubex/http"
	"github.com/stretchr/testify/require"
)

func TestScriptNotificationsHTTPAPI(t *testing.T) {
	N.Clear()
	t.Cleanup(N.Clear)
	homeDir, configPath, server := newModulesAPITestServer(t)
	scriptsDir := filepath.Join(homeDir, "scripts")
	require.NoError(t, os.MkdirAll(scriptsDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(scriptsDir, "notification.js"), []byte(`
if (!$environment["surge-version"] ||
    $environment["surge-version"] !== $environment["mihomo-version"]) {
  throw new Error("unexpected environment");
}
$notification.post("Script notification", "Surge compatibility", "Forwarded by mihomo", {
  action: "open-url",
  url: "https://example.com/result",
  "media-url": "https://example.com/image.png",
  "auto-dismiss": false,
  sound: true,
});
$done();
`), 0o644))

	response := modulesAPIRequest(t, server.Client(), http.MethodGet, server.URL+"/notification", nil)
	require.Equal(t, http.StatusOK, response.StatusCode)
	var initial N.Snapshot
	require.NoError(t, json.NewDecoder(response.Body).Decode(&initial))
	require.NoError(t, response.Body.Close())
	require.Equal(t, N.HistoryLimit, initial.Limit)
	require.Empty(t, initial.Notifications)

	websocketURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/notification"
	connection, bufferedReader, _, err := ws.Dial(context.Background(), websocketURL)
	require.NoError(t, err)
	defer connection.Close()
	require.NoError(t, connection.SetReadDeadline(time.Now().Add(4*time.Second)))
	var eventReader io.Reader = connection
	if bufferedReader != nil {
		eventReader = bufferedReader
	}
	eventStream := struct {
		io.Reader
		io.Writer
	}{Reader: eventReader, Writer: connection}
	payload, err := wsutil.ReadServerText(eventStream)
	require.NoError(t, err)
	var event N.Event
	require.NoError(t, json.Unmarshal(payload, &event))
	require.Equal(t, "snapshot", event.Type)
	require.Equal(t, N.HistoryLimit, event.Limit)
	require.Empty(t, event.Notifications)

	require.NoError(t, os.WriteFile(configPath, []byte(`
mixed-port: 0
scripts:
  forwarded:
    enable: true
    type: cron
    cron: '* * * * * *'
    path: ./scripts/notification.js
`), 0o644))
	response = modulesAPIRequest(t, server.Client(), http.MethodPut, server.URL+"/configs?force=true", map[string]any{
		"path": configPath,
	})
	require.Equal(t, http.StatusNoContent, response.StatusCode)
	require.NoError(t, response.Body.Close())

	payload, err = wsutil.ReadServerText(eventStream)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(payload, &event))
	require.Equal(t, "notification", event.Type)
	require.NotNil(t, event.Notification)
	notification := *event.Notification
	require.NotZero(t, notification.ID)
	require.NotZero(t, notification.Time)
	require.Equal(t, "Script notification", notification.Title)
	require.Equal(t, "Surge compatibility", notification.Subtitle)
	require.Equal(t, "Forwarded by mihomo", notification.Body)
	require.Equal(t, "forwarded", notification.Script)
	require.Equal(t, "open-url", notification.Options["action"])
	require.Equal(t, "https://example.com/result", notification.Options["url"])
	require.Equal(t, "https://example.com/image.png", notification.Options["media-url"])
	require.Equal(t, false, notification.Options["auto-dismiss"])
	require.Equal(t, true, notification.Options["sound"])

	response = modulesAPIRequest(t, server.Client(), http.MethodGet, server.URL+"/notification", nil)
	require.Equal(t, http.StatusOK, response.StatusCode)
	var snapshot N.Snapshot
	require.NoError(t, json.NewDecoder(response.Body).Decode(&snapshot))
	require.NoError(t, response.Body.Close())
	require.NotEmpty(t, snapshot.Notifications)
	require.Equal(t, notification.ID, snapshot.Notifications[len(snapshot.Notifications)-1].ID)
}
