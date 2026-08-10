package route

import (
	"encoding/json"
	"net"
	"time"

	M "github.com/metacubex/mihomo/component/mitm"
	"github.com/metacubex/mihomo/log"

	"github.com/metacubex/chi"
	"github.com/metacubex/chi/render"
	"github.com/metacubex/http"
)

func mitmRouter() http.Handler {
	r := chi.NewRouter()
	r.Get("/", getMitmSessions)
	r.Delete("/", clearMitmSessions)
	r.Get("/capture", getMitmCapture)
	r.Put("/capture", updateMitmCapture)
	r.Patch("/capture", updateMitmCapture)
	return r
}

func getMitmSessions(writer http.ResponseWriter, request *http.Request) {
	if request.Header.Get("Upgrade") != "websocket" {
		render.JSON(writer, request, M.CapturedSessionsSnapshot())
		return
	}

	events, unsubscribe := M.SubscribeCaptureEvents()
	defer unsubscribe()
	connection, _, err := wsUpgrade(request, writer)
	if err != nil {
		return
	}
	defer connection.Close()

	snapshot := M.CapturedSessionsSnapshot()
	if err := writeMitmEvent(connection, mitmSnapshotEvent{
		Type:     "snapshot",
		Capture:  snapshot.Capture,
		Limit:    snapshot.Limit,
		Sessions: snapshot.Sessions,
	}); err != nil {
		return
	}
	heartbeat := time.NewTicker(30 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case event, open := <-events:
			if !open {
				return
			}
			if err := writeMitmEvent(connection, event); err != nil {
				return
			}
		case <-heartbeat.C:
			if err := writeMitmEvent(connection, M.CaptureEvent{
				Type:    "heartbeat",
				Capture: M.CaptureEnabled(),
			}); err != nil {
				return
			}
		}
	}
}

func clearMitmSessions(writer http.ResponseWriter, request *http.Request) {
	M.ClearCapturedSessions()
	render.NoContent(writer, request)
}

func getMitmCapture(writer http.ResponseWriter, request *http.Request) {
	render.JSON(writer, request, render.M{"capture": M.CaptureEnabled()})
}

func updateMitmCapture(writer http.ResponseWriter, request *http.Request) {
	payload := struct {
		Capture *bool `json:"capture"`
	}{}
	if err := render.DecodeJSON(request.Body, &payload); err != nil || payload.Capture == nil {
		render.Status(request, http.StatusBadRequest)
		render.JSON(writer, request, ErrBadRequest)
		return
	}
	if *payload.Capture && !M.CaptureEnabled() {
		log.Warnln(M.CaptureWarning)
	}
	M.SetCaptureEnabled(*payload.Capture)
	render.NoContent(writer, request)
}

type mitmSnapshotEvent struct {
	Type     string              `json:"type"`
	Capture  bool                `json:"capture"`
	Limit    int                 `json:"limit"`
	Sessions []M.CapturedSession `json:"sessions"`
}

func writeMitmEvent(connection net.Conn, event any) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return wsWriteServerText(connection, payload)
}
