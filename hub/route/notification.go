package route

import (
	"encoding/json"
	"net"
	"time"

	N "github.com/metacubex/mihomo/component/notification"

	"github.com/metacubex/chi"
	"github.com/metacubex/chi/render"
	"github.com/metacubex/http"
)

func notificationRouter() http.Handler {
	r := chi.NewRouter()
	r.Get("/", getNotifications)
	return r
}

func getNotifications(writer http.ResponseWriter, request *http.Request) {
	if request.Header.Get("Upgrade") != "websocket" {
		render.JSON(writer, request, N.NotificationsSnapshot())
		return
	}

	events, unsubscribe := N.Subscribe()
	defer unsubscribe()
	connection, _, err := wsUpgrade(request, writer)
	if err != nil {
		return
	}
	defer connection.Close()

	snapshot := N.NotificationsSnapshot()
	if err := writeNotificationEvent(connection, N.Event{
		Type:          "snapshot",
		Limit:         snapshot.Limit,
		Notifications: snapshot.Notifications,
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
			if err := writeNotificationEvent(connection, event); err != nil {
				return
			}
		case <-heartbeat.C:
			if err := writeNotificationEvent(connection, N.Event{Type: "heartbeat"}); err != nil {
				return
			}
		}
	}
}

func writeNotificationEvent(connection net.Conn, event N.Event) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return wsWriteServerText(connection, payload)
}
