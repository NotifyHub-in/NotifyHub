package main

import (
	"net/http"
	"strings"
	"time"

	"github.com/your-org/notification-control-plane/libs/core/app"
	"github.com/your-org/notification-control-plane/libs/core/config"
	"github.com/your-org/notification-control-plane/libs/core/httpx"
	"github.com/your-org/notification-control-plane/libs/core/serviceinfo"
	"github.com/your-org/notification-control-plane/libs/observability/logging"
)

type callbackPayload struct {
	ProviderMessageID string            `json:"provider_message_id"`
	Status            string            `json:"status"`
	ErrorCode         string            `json:"error_code,omitempty"`
	Metadata          map[string]string `json:"metadata,omitempty"`
}

func main() {
	cfg, err := config.LoadHTTPServiceConfig("callback-gateway", 8082)
	if err != nil {
		panic(err)
	}

	logger := logging.New(cfg.ServiceName)

	err = app.RunHTTPService(cfg, logger, func(mux *http.ServeMux, info serviceinfo.Info) {
		mux.HandleFunc("POST /v1/providers/", func(w http.ResponseWriter, r *http.Request) {
			path := strings.TrimPrefix(r.URL.Path, "/v1/providers/")
			parts := strings.Split(strings.Trim(path, "/"), "/")
			if len(parts) != 2 || parts[1] != "callbacks" || parts[0] == "" {
				http.NotFound(w, r)
				return
			}

			var payload callbackPayload
			if err := httpx.DecodeJSON(r, &payload); err != nil {
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid callback payload"})
				return
			}

			httpx.WriteJSON(w, http.StatusAccepted, map[string]any{
				"service":             info.Name,
				"provider":            parts[0],
				"normalized_status":   payload.Status,
				"provider_message_id": payload.ProviderMessageID,
				"received_at":         time.Now().UTC(),
			})
		})
	})
	if err != nil {
		panic(err)
	}
}
