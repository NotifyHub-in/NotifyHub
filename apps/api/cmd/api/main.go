package main

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"time"

	"github.com/your-org/notification-control-plane/libs/contracts/notification"
	"github.com/your-org/notification-control-plane/libs/core/app"
	"github.com/your-org/notification-control-plane/libs/core/config"
	"github.com/your-org/notification-control-plane/libs/core/httpx"
	"github.com/your-org/notification-control-plane/libs/core/serviceinfo"
	"github.com/your-org/notification-control-plane/libs/observability/logging"
)

func main() {
	cfg, err := config.LoadHTTPServiceConfig("api", 8080)
	if err != nil {
		panic(err)
	}

	logger := logging.New(cfg.ServiceName)

	err = app.RunHTTPService(cfg, logger, func(mux *http.ServeMux, info serviceinfo.Info) {
		mux.HandleFunc("POST /v1/notification-requests", func(w http.ResponseWriter, r *http.Request) {
			var req notification.NotificationRequest
			if err := httpx.DecodeJSON(r, &req); err != nil {
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request payload"})
				return
			}

			if req.EventName == "" || req.TemplateKey == "" || len(req.Channels) == 0 || req.IdempotencyKey == "" {
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "event_name, template_key, channels, and idempotency_key are required"})
				return
			}

			accepted := notification.NotificationAccepted{
				RequestID:  newID(),
				Status:     "accepted",
				AcceptedAt: time.Now().UTC(),
			}
			httpx.WriteJSON(w, http.StatusAccepted, accepted)
		})

		mux.HandleFunc("GET /v1/notification-requests/example", func(w http.ResponseWriter, _ *http.Request) {
			httpx.WriteJSON(w, http.StatusOK, notification.NotificationRequest{
				IdempotencyKey: "order-123-delay-email",
				EventName:      "order.delayed",
				TemplateKey:    "order-delayed-v1",
				Channels:       []notification.Channel{notification.ChannelEmail, notification.ChannelSMS},
				Recipient: notification.Recipient{
					UserID: "user-123",
					Email:  "customer@example.com",
					Phone:  "+15555550123",
				},
				Variables: map[string]string{
					"order_id": "123",
					"eta":      "2026-05-27T12:00:00Z",
				},
			})
		})

		mux.HandleFunc("GET /v1/status", func(w http.ResponseWriter, _ *http.Request) {
			httpx.WriteJSON(w, http.StatusOK, map[string]any{
				"service": info.Name,
				"phase":   "scaffold",
				"state":   "northbound API active",
			})
		})
	})
	if err != nil {
		panic(err)
	}
}

func newID() string {
	var bytes [12]byte
	_, _ = rand.Read(bytes[:])
	return hex.EncodeToString(bytes[:])
}
