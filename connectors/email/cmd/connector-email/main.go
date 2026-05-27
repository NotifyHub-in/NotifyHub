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
	"github.com/your-org/notification-control-plane/libs/observability/metrics"
)

func main() {
	runConnector("connector-email", 8091, notification.ConnectorCapabilities{
		Name:     "email",
		Channels: []notification.Channel{notification.ChannelEmail},
	})
}

func runConnector(serviceName string, port int, capabilities notification.ConnectorCapabilities) {
	cfg, err := config.LoadHTTPServiceConfig(serviceName, port)
	if err != nil {
		panic(err)
	}

	logger := logging.New(cfg.ServiceName)

	err = app.RunHTTPService(cfg, logger, nil, func(mux *http.ServeMux, info serviceinfo.Info, _ *metrics.Registry) {
		mux.HandleFunc("GET /v1/capabilities", func(w http.ResponseWriter, _ *http.Request) {
			httpx.WriteJSON(w, http.StatusOK, capabilities)
		})
		mux.HandleFunc("POST /v1/send", func(w http.ResponseWriter, r *http.Request) {
			var req notification.ConnectorSendRequest
			if err := httpx.DecodeJSON(r, &req); err != nil {
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid connector send payload"})
				return
			}
			httpx.WriteJSON(w, http.StatusAccepted, notification.ConnectorSendResponse{
				RequestID:         req.RequestID,
				ProviderMessageID: providerMessageID(),
				Status:            "accepted",
				AcceptedAt:        time.Now().UTC(),
			})
		})
		mux.HandleFunc("GET /v1/status", func(w http.ResponseWriter, _ *http.Request) {
			httpx.WriteJSON(w, http.StatusOK, map[string]string{"service": info.Name, "state": "ready"})
		})
	})
	if err != nil {
		panic(err)
	}
}

func providerMessageID() string {
	var bytes [8]byte
	_, _ = rand.Read(bytes[:])
	return hex.EncodeToString(bytes[:])
}
