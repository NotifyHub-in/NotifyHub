package main

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/your-org/notification-control-plane/libs/contracts/notification"
	"github.com/your-org/notification-control-plane/libs/core/app"
	"github.com/your-org/notification-control-plane/libs/core/config"
	"github.com/your-org/notification-control-plane/libs/core/httpx"
	"github.com/your-org/notification-control-plane/libs/core/id"
	"github.com/your-org/notification-control-plane/libs/core/serviceinfo"
	kafkamq "github.com/your-org/notification-control-plane/libs/messaging/kafka"
	"github.com/your-org/notification-control-plane/libs/observability/logging"
	"github.com/your-org/notification-control-plane/libs/storage/postgres"
)

func main() {
	cfg, err := config.LoadHTTPServiceConfig("api", 8080)
	if err != nil {
		panic(err)
	}

	logger := logging.New(cfg.ServiceName)
	ctx := context.Background()

	store, err := postgres.Open(ctx, config.MustGetEnv("DATABASE_URL"))
	if err != nil {
		panic(err)
	}
	defer store.Close()

	brokers := config.MustGetEnv("KAFKA_BROKERS")
	topic := config.GetEnv("KAFKA_NOTIFICATION_TOPIC", "notification.requests")
	if err := kafkamq.EnsureTopic(ctx, brokers, topic, 1, 1); err != nil {
		panic(err)
	}

	publisher := kafkamq.NewPublisher(brokers, topic)
	defer publisher.Close()

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

			now := time.Now().UTC()
			record := notification.NotificationRecord{
				RequestID:      id.New(12),
				IdempotencyKey: req.IdempotencyKey,
				EventName:      req.EventName,
				TemplateKey:    req.TemplateKey,
				Channels:       req.Channels,
				Recipient:      req.Recipient,
				Variables:      req.Variables,
				Metadata:       req.Metadata,
				Priority:       req.Priority,
				Status:         notification.RequestStatusAccepted,
				RequestedAt:    now,
			}

			if err := store.CreateNotificationRequest(r.Context(), record); err != nil {
				logger.Error("create notification request failed", "error", err)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "persist notification request"})
				return
			}

			if err := publisher.PublishJSON(r.Context(), record.RequestID, notification.DeliveryPlan{Request: record}); err != nil {
				logger.Error("publish notification request failed", "error", err, "request_id", record.RequestID)
				_ = store.UpdateNotificationRequestStatus(r.Context(), record.RequestID, notification.RequestStatusFailed)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "enqueue notification request"})
				return
			}

			httpx.WriteJSON(w, http.StatusAccepted, notification.NotificationAccepted{
				RequestID:  record.RequestID,
				Status:     notification.RequestStatusAccepted,
				AcceptedAt: now,
			})
		})

		mux.HandleFunc("GET /v1/notification-requests/example", func(w http.ResponseWriter, _ *http.Request) {
			httpx.WriteJSON(w, http.StatusOK, notification.NotificationRequest{
				IdempotencyKey: "payment-789-failure-webhook",
				EventName:      "payment.failed",
				TemplateKey:    "payment-failed-v1",
				Channels:       []notification.Channel{notification.ChannelWebhook},
				Recipient: notification.Recipient{
					UserID:  "merchant-45",
					Webhook: "https://example.com/hooks/payment-failed",
				},
				Variables: map[string]string{
					"payment_id": "789",
					"reason":     "bank_timeout",
				},
			})
		})

		mux.HandleFunc("GET /v1/notification-requests/{requestID}", func(w http.ResponseWriter, r *http.Request) {
			requestID := r.PathValue("requestID")
			record, err := store.GetNotificationRequest(r.Context(), requestID)
			if errors.Is(err, postgres.ErrNotFound) {
				httpx.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "notification request not found"})
				return
			}
			if err != nil {
				logger.Error("get notification request failed", "error", err, "request_id", requestID)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load notification request"})
				return
			}

			attempts, err := store.ListDeliveryAttempts(r.Context(), requestID)
			if err != nil {
				logger.Error("list delivery attempts failed", "error", err, "request_id", requestID)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load delivery attempts"})
				return
			}

			httpx.WriteJSON(w, http.StatusOK, map[string]any{
				"request":           record,
				"delivery_attempts": attempts,
			})
		})

		mux.HandleFunc("GET /v1/status", func(w http.ResponseWriter, _ *http.Request) {
			httpx.WriteJSON(w, http.StatusOK, map[string]any{
				"service": info.Name,
				"phase":   "happy-path",
				"state":   "api persists and enqueues notification requests",
				"topic":   topic,
			})
		})
	})
	if err != nil {
		panic(err)
	}
}
