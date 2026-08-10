package route

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	M "github.com/metacubex/mihomo/component/mitm"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/metacubex/http"
	"github.com/metacubex/http/httptest"
	"github.com/stretchr/testify/require"
)

func TestMitmCaptureHTTPAPI(t *testing.T) {
	M.SetCaptureEnabled(false)
	M.ClearCapturedSessions()
	t.Cleanup(func() {
		M.SetCaptureEnabled(false)
		M.ClearCapturedSessions()
	})

	server := httptest.NewServer(router(false, "", "", Cors{}))
	defer server.Close()
	client := server.Client()
	mitmURL := server.URL + "/mitm"

	response, err := client.Get(mitmURL + "/capture")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	var captureState struct {
		Capture bool `json:"capture"`
	}
	require.NoError(t, json.NewDecoder(response.Body).Decode(&captureState))
	require.NoError(t, response.Body.Close())
	require.False(t, captureState.Capture)

	websocketURL := "ws" + strings.TrimPrefix(mitmURL, "http")
	connection, bufferedReader, _, err := ws.Dial(context.Background(), websocketURL)
	require.NoError(t, err)
	defer connection.Close()
	require.NoError(t, connection.SetReadDeadline(time.Now().Add(3*time.Second)))
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
	var event M.CaptureEvent
	require.NoError(t, json.Unmarshal(payload, &event))
	require.Equal(t, "snapshot", event.Type)
	require.False(t, event.Capture)
	require.Equal(t, M.CaptureHistoryLimit, event.Limit)
	require.NotNil(t, event.Sessions)

	request, err := http.NewRequest(http.MethodPut, mitmURL+"/capture", strings.NewReader(`{"capture":true}`))
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/json")
	response, err = client.Do(request)
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, response.StatusCode)
	require.NoError(t, response.Body.Close())
	payload, err = wsutil.ReadServerText(eventStream)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(payload, &event))
	require.Equal(t, "capture", event.Type)
	require.True(t, event.Capture)

	response, err = client.Get(mitmURL)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	var snapshot M.CaptureSnapshot
	require.NoError(t, json.NewDecoder(response.Body).Decode(&snapshot))
	require.NoError(t, response.Body.Close())
	require.True(t, snapshot.Capture)
	require.Equal(t, M.CaptureHistoryLimit, snapshot.Limit)
	require.Empty(t, snapshot.Sessions)

	request, err = http.NewRequest(http.MethodPut, mitmURL+"/capture", strings.NewReader(`{}`))
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/json")
	response, err = client.Do(request)
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, response.StatusCode)
	require.NoError(t, response.Body.Close())

	request, err = http.NewRequest(http.MethodPatch, mitmURL+"/capture", strings.NewReader(`{"capture":false}`))
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/json")
	response, err = client.Do(request)
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, response.StatusCode)
	require.NoError(t, response.Body.Close())

	request, err = http.NewRequest(http.MethodDelete, mitmURL, nil)
	require.NoError(t, err)
	response, err = client.Do(request)
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, response.StatusCode)
	require.NoError(t, response.Body.Close())

	response, err = client.Get(mitmURL + "/capture")
	require.NoError(t, err)
	require.NoError(t, json.NewDecoder(response.Body).Decode(&captureState))
	require.NoError(t, response.Body.Close())
	require.False(t, captureState.Capture)
}
