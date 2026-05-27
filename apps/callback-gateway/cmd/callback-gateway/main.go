package main

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/your-org/notification-control-plane/libs/contracts/notification"
	"github.com/your-org/notification-control-plane/libs/core/app"
	"github.com/your-org/notification-control-plane/libs/core/config"
	"github.com/your-org/notification-control-plane/libs/core/httpx"
	"github.com/your-org/notification-control-plane/libs/core/serviceinfo"
	"github.com/your-org/notification-control-plane/libs/core/webhooks"
	"github.com/your-org/notification-control-plane/libs/observability/logging"
	"github.com/your-org/notification-control-plane/libs/observability/metrics"
	"github.com/your-org/notification-control-plane/libs/storage/postgres"
)

func main() {
	cfg, err := config.LoadHTTPServiceConfig("callback-gateway", 8082)
	if err != nil {
		panic(err)
	}

	logger := logging.New(cfg.ServiceName)
	ctx := context.Background()
	registry := metrics.NewRegistry(cfg.ServiceName)

	store, err := postgres.Open(ctx, config.MustGetEnv("DATABASE_URL"))
	if err != nil {
		panic(err)
	}
	defer store.Close()
	notifier := webhooks.NewNotifier(store)

	err = app.RunHTTPService(cfg, logger, registry, func(mux *http.ServeMux, info serviceinfo.Info, registry *metrics.Registry) {
		mux.HandleFunc("POST /v1/providers/", func(w http.ResponseWriter, r *http.Request) {
			path := strings.TrimPrefix(r.URL.Path, "/v1/providers/")
			parts := strings.Split(strings.Trim(path, "/"), "/")
			if len(parts) != 2 || parts[1] != "callbacks" || parts[0] == "" {
				http.NotFound(w, r)
				return
			}

			var payload notification.ProviderCallback
			if err := httpx.DecodeJSON(r, &payload); err != nil {
				registry.IncCounter("provider_callbacks_total", "Inbound provider callback outcomes.", map[string]string{"provider": parts[0], "outcome": "invalid_payload"})
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid callback payload"})
				return
			}
			if payload.ProviderMessageID == "" || payload.Status == "" {
				registry.IncCounter("provider_callbacks_total", "Inbound provider callback outcomes.", map[string]string{"provider": parts[0], "outcome": "validation_failed"})
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "provider_message_id and status are required"})
				return
			}

			attempt, err := store.GetDeliveryAttemptByProviderMessageID(r.Context(), payload.ProviderMessageID)
			if errors.Is(err, postgres.ErrNotFound) {
				registry.IncCounter("provider_callbacks_total", "Inbound provider callback outcomes.", map[string]string{"provider": parts[0], "outcome": "attempt_not_found"})
				httpx.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "delivery attempt not found for provider_message_id"})
				return
			}
			if err != nil {
				registry.IncCounter("provider_callbacks_total", "Inbound provider callback outcomes.", map[string]string{"provider": parts[0], "outcome": "lookup_failed"})
				logger.Error("load delivery attempt by provider message id failed", "error", err, "provider_message_id", payload.ProviderMessageID)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load delivery attempt"})
				return
			}

			attempt.Status = normalizeAttemptStatus(payload.Status)
			attempt.ErrorMessage = normalizedErrorMessage(payload)
			if err := store.UpdateDeliveryAttempt(r.Context(), attempt); err != nil {
				registry.IncCounter("provider_callbacks_total", "Inbound provider callback outcomes.", map[string]string{"provider": parts[0], "outcome": "update_attempt_failed"})
				logger.Error("update delivery attempt from callback failed", "error", err, "attempt_id", attempt.AttemptID)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "update delivery attempt"})
				return
			}

			requestStatus, err := recomputeRequestStatus(r.Context(), store, attempt.RequestID)
			if err != nil {
				registry.IncCounter("provider_callbacks_total", "Inbound provider callback outcomes.", map[string]string{"provider": parts[0], "outcome": "recompute_request_failed"})
				logger.Error("recompute request status failed", "error", err, "request_id", attempt.RequestID)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "recompute request status"})
				return
			}
			if err := store.UpdateNotificationRequestStatus(r.Context(), attempt.RequestID, requestStatus); err != nil {
				registry.IncCounter("provider_callbacks_total", "Inbound provider callback outcomes.", map[string]string{"provider": parts[0], "outcome": "update_request_failed"})
				logger.Error("update request status from callback failed", "error", err, "request_id", attempt.RequestID)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "update request status"})
				return
			}
			if err := notifier.NotifyRequestUpdated(r.Context(), attempt.RequestID, map[string]interface{}{"source": "callback-gateway", "provider": parts[0]}); err != nil {
				logger.Error("notify lifecycle webhook failed", "error", err, "request_id", attempt.RequestID)
			}

			registry.IncCounter("provider_callbacks_total", "Inbound provider callback outcomes.", map[string]string{
				"provider":          parts[0],
				"outcome":           "accepted",
				"normalized_status": string(attempt.Status),
			})
			httpx.WriteJSON(w, http.StatusAccepted, map[string]any{
				"service":             info.Name,
				"provider":            parts[0],
				"request_id":          attempt.RequestID,
				"attempt_id":          attempt.AttemptID,
				"normalized_status":   attempt.Status,
				"provider_message_id": payload.ProviderMessageID,
				"request_status":      requestStatus,
				"received_at":         time.Now().UTC(),
			})
		})
	})
	if err != nil {
		panic(err)
	}
}

func normalizeAttemptStatus(status string) notification.DeliveryAttemptStatus {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "delivered", "success", "sent":
		return notification.DeliveryAttemptDelivered
	case "accepted", "queued", "processing":
		return notification.DeliveryAttemptAccepted
	default:
		return notification.DeliveryAttemptFailed
	}
}

func normalizedErrorMessage(payload notification.ProviderCallback) string {
	if normalizeAttemptStatus(payload.Status) != notification.DeliveryAttemptFailed {
		return ""
	}
	if payload.ErrorCode != "" {
		return payload.ErrorCode
	}
	return payload.Status
}

func recomputeRequestStatus(ctx context.Context, store *postgres.Store, requestID string) (notification.RequestStatus, error) {
	attempts, err := store.ListDeliveryAttempts(ctx, requestID)
	if err != nil {
		return "", err
	}

	var (
		hasDelivered   bool
		hasAccepted    bool
		hasSuppressed  bool
		hasUnsupported bool
		hasFailed      bool
	)

	for _, attempt := range attempts {
		switch attempt.Status {
		case notification.DeliveryAttemptDelivered:
			hasDelivered = true
		case notification.DeliveryAttemptAccepted, notification.DeliveryAttemptPending:
			hasAccepted = true
		case notification.DeliveryAttemptSuppressed:
			hasSuppressed = true
		case notification.DeliveryAttemptUnsupported:
			hasUnsupported = true
		case notification.DeliveryAttemptFailed:
			hasFailed = true
		}
	}

	switch {
	case hasDelivered:
		return notification.RequestStatusDelivered, nil
	case hasAccepted:
		return notification.RequestStatusDispatched, nil
	case hasFailed:
		return notification.RequestStatusFailed, nil
	case hasUnsupported:
		return notification.RequestStatusUnsupported, nil
	case hasSuppressed:
		return notification.RequestStatusSuppressed, nil
	default:
		return notification.RequestStatusDispatched, nil
	}
}
