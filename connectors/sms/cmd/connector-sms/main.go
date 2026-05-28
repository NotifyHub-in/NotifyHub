package main

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"strings"
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
	cfg, err := config.LoadHTTPServiceConfig("connector-sms", 8092)
	if err != nil {
		panic(err)
	}

	logger := logging.New(cfg.ServiceName)

	err = app.RunHTTPService(cfg, logger, nil, func(mux *http.ServeMux, info serviceinfo.Info, _ *metrics.Registry) {
		mux.HandleFunc("GET /v1/capabilities", func(w http.ResponseWriter, _ *http.Request) {
			httpx.WriteJSON(w, http.StatusOK, notification.ConnectorCapabilities{
				Name:     "sms",
				Channels: []notification.Channel{notification.ChannelSMS},
			})
		})
		mux.HandleFunc("POST /v1/send", func(w http.ResponseWriter, r *http.Request) {
			var req notification.ConnectorSendRequest
			if err := httpx.DecodeJSON(r, &req); err != nil {
				httpx.WriteJSON(w, http.StatusBadRequest, notification.ConnectorErrorResponse{
					Error:          "invalid connector send payload",
					Code:           "invalid_payload",
					Classification: notification.FailureClassInvalidRequest,
				})
				return
			}
			if req.Destination == "" || !strings.HasPrefix(req.Destination, "+") {
				httpx.WriteJSON(w, http.StatusBadRequest, notification.ConnectorErrorResponse{
					Error:          "invalid phone destination",
					Code:           "invalid_destination",
					Classification: notification.FailureClassInvalidRequest,
				})
				return
			}
			if token := req.ProviderConfig["auth_token"]; token == "unauthorized" {
				httpx.WriteJSON(w, http.StatusUnauthorized, notification.ConnectorErrorResponse{
					Error:          "provider rejected sms credentials",
					Code:           "invalid_credentials",
					Classification: notification.FailureClassUnauthorized,
				})
				return
			}
			switch req.Metadata["simulate_failure"] {
			case "rate_limit":
				httpx.WriteJSON(w, http.StatusTooManyRequests, notification.ConnectorErrorResponse{
					Error:          "sms provider rate limited the request",
					Code:           "rate_limited",
					Classification: notification.FailureClassRateLimited,
					Retryable:      true,
				})
				return
			case "provider_outage":
				httpx.WriteJSON(w, http.StatusBadGateway, notification.ConnectorErrorResponse{
					Error:          "sms provider temporary outage",
					Code:           "provider_outage",
					Classification: notification.FailureClassTransient,
					Retryable:      true,
				})
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
