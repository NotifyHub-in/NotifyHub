package main

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"time"

	"github.com/your-org/notification-control-plane/libs/contracts/notification"
	"github.com/your-org/notification-control-plane/libs/core/app"
	"github.com/your-org/notification-control-plane/libs/core/config"
	"github.com/your-org/notification-control-plane/libs/core/httpx"
	"github.com/your-org/notification-control-plane/libs/core/id"
	"github.com/your-org/notification-control-plane/libs/core/render"
	"github.com/your-org/notification-control-plane/libs/core/serviceinfo"
	"github.com/your-org/notification-control-plane/libs/core/webhooks"
	kafkamq "github.com/your-org/notification-control-plane/libs/messaging/kafka"
	"github.com/your-org/notification-control-plane/libs/observability/logging"
	"github.com/your-org/notification-control-plane/libs/observability/metrics"
	"github.com/your-org/notification-control-plane/libs/storage/postgres"
)

func main() {
	cfg, err := config.LoadHTTPServiceConfig("api", 8080)
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
	postgres.AttachMetrics(registry)
	notifier := webhooks.NewNotifier(store)

	brokers := config.MustGetEnv("KAFKA_BROKERS")
	topic := config.GetEnv("KAFKA_NOTIFICATION_TOPIC", "notification.requests")
	if err := kafkamq.EnsureTopic(ctx, brokers, topic, 1, 1); err != nil {
		panic(err)
	}

	publisher := kafkamq.NewPublisher(brokers, topic)
	defer publisher.Close()

	err = app.RunHTTPService(cfg, logger, registry, func(mux *http.ServeMux, info serviceinfo.Info, registry *metrics.Registry) {
		mux.HandleFunc("POST /v1/notification-requests", func(w http.ResponseWriter, r *http.Request) {
			var req notification.NotificationRequest
			if err := httpx.DecodeJSON(r, &req); err != nil {
				registry.IncCounter("notification_request_api_events_total", "Notification request API outcomes.", map[string]string{"outcome": "invalid_payload"})
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request payload"})
				return
			}

			if req.EventName == "" || req.TemplateKey == "" || req.IdempotencyKey == "" {
				registry.IncCounter("notification_request_api_events_total", "Notification request API outcomes.", map[string]string{"outcome": "validation_failed"})
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "event_name, template_key, and idempotency_key are required"})
				return
			}

			now := time.Now().UTC()
			if req.ExpiresAt != nil && !req.ExpiresAt.After(now) {
				registry.IncCounter("notification_request_api_events_total", "Notification request API outcomes.", map[string]string{"outcome": "validation_failed"})
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "expires_at must be in the future"})
				return
			}
			record := notification.NotificationRecord{
				RequestID:      id.New(12),
				IdempotencyKey: req.IdempotencyKey,
				EventName:      req.EventName,
				TemplateKey:    req.TemplateKey,
				Channels:       req.Channels,
				BindingSet:     req.BindingSet,
				Recipient:      req.Recipient,
				Variables:      req.Variables,
				Metadata:       req.Metadata,
				Priority:       req.Priority,
				Status:         notification.RequestStatusAccepted,
				RequestedAt:    now,
				ExpiresAt:      req.ExpiresAt,
			}

			if err := store.CreateNotificationRequest(r.Context(), record); err != nil {
				if errors.Is(err, postgres.ErrConflict) {
					existing, lookupErr := store.GetNotificationRequestByIdempotencyKey(r.Context(), req.IdempotencyKey)
					if lookupErr != nil {
						registry.IncCounter("notification_request_api_events_total", "Notification request API outcomes.", map[string]string{"outcome": "conflict_lookup_failed"})
						logger.Error("load conflicting notification request failed", "error", lookupErr, "idempotency_key", req.IdempotencyKey)
						httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load existing notification request"})
						return
					}

					if !sameNotificationIntent(existing, req) {
						registry.IncCounter("notification_request_api_events_total", "Notification request API outcomes.", map[string]string{"outcome": "conflict"})
						httpx.WriteJSON(w, http.StatusConflict, map[string]any{
							"error":      "idempotency_key already used for a different notification request",
							"request_id": existing.RequestID,
							"status":     existing.Status,
						})
						return
					}

					registry.IncCounter("notification_request_api_events_total", "Notification request API outcomes.", map[string]string{"outcome": "idempotent_replay"})
					httpx.WriteJSON(w, http.StatusOK, notification.NotificationAccepted{
						RequestID:        existing.RequestID,
						Status:           existing.Status,
						AcceptedAt:       existing.RequestedAt,
						IdempotentReplay: true,
					})
					return
				}

				registry.IncCounter("notification_request_api_events_total", "Notification request API outcomes.", map[string]string{"outcome": "persist_failed"})
				logger.Error("create notification request failed", "error", err)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "persist notification request"})
				return
			}

			if err := publisher.PublishJSON(r.Context(), record.RequestID, notification.DeliveryPlan{Request: record}); err != nil {
				registry.IncCounter("notification_request_api_events_total", "Notification request API outcomes.", map[string]string{"outcome": "enqueue_failed"})
				logger.Error("publish notification request failed", "error", err, "request_id", record.RequestID)
				_ = store.UpdateNotificationRequestStatus(r.Context(), record.RequestID, notification.RequestStatusFailed)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "enqueue notification request"})
				return
			}

			if err := notifier.NotifyRequestUpdated(r.Context(), record.RequestID, map[string]interface{}{"source": "api"}); err != nil {
				logger.Error("notify accepted lifecycle webhook failed", "error", err, "request_id", record.RequestID)
			}

			registry.IncCounter("notification_request_api_events_total", "Notification request API outcomes.", map[string]string{
				"outcome":  "accepted",
				"priority": string(record.Priority),
			})
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

		mux.HandleFunc("GET /v1/provider-bindings", func(w http.ResponseWriter, r *http.Request) {
			bindings, err := store.ListProviderBindings(r.Context())
			if err != nil {
				logger.Error("list provider bindings failed", "error", err)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load provider bindings"})
				return
			}
			httpx.WriteJSON(w, http.StatusOK, map[string]any{"provider_bindings": bindings})
		})

		mux.HandleFunc("GET /v1/provider-bindings/{channel}", func(w http.ResponseWriter, r *http.Request) {
			channel := notification.Channel(r.PathValue("channel"))
			bindingSet := r.URL.Query().Get("binding_set")
			bindings, err := store.ListProviderBindingsByChannel(r.Context(), channel, bindingSet)
			if errors.Is(err, postgres.ErrNotFound) {
				httpx.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "provider binding not found"})
				return
			}
			if err != nil {
				logger.Error("get provider binding failed", "error", err, "channel", channel)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load provider binding"})
				return
			}
			httpx.WriteJSON(w, http.StatusOK, map[string]any{
				"channel":           channel,
				"binding_set":       bindingSet,
				"provider_bindings": bindings,
			})
		})

		mux.HandleFunc("POST /v1/provider-bindings", func(w http.ResponseWriter, r *http.Request) {
			var req notification.ProviderBindingUpsertRequest
			if err := httpx.DecodeJSON(r, &req); err != nil {
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid provider binding payload"})
				return
			}
			if req.Channel == "" || req.ConnectorName == "" || req.EndpointURL == "" {
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "channel, connector_name, and endpoint_url are required"})
				return
			}

			binding := notification.ProviderBinding{
				BindingID:     id.New(12),
				Channel:       req.Channel,
				BindingSet:    req.BindingSet,
				ConnectorName: req.ConnectorName,
				EndpointURL:   req.EndpointURL,
				ConfigRefs:    req.ConfigRefs,
				Enabled:       req.Enabled,
				Priority:      req.Priority,
			}
			if err := store.UpsertProviderBinding(r.Context(), binding); err != nil {
				logger.Error("upsert provider binding failed", "error", err, "channel", req.Channel)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "save provider binding"})
				return
			}

			saved, err := store.GetProviderBindingByChannel(r.Context(), req.Channel, req.BindingSet)
			if err != nil {
				logger.Error("reload provider binding failed", "error", err, "channel", req.Channel, "binding_set", req.BindingSet)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "reload provider binding"})
				return
			}
			httpx.WriteJSON(w, http.StatusOK, saved)
		})

		mux.HandleFunc("GET /v1/routing-policies", func(w http.ResponseWriter, r *http.Request) {
			policies, err := store.ListRoutingPolicies(r.Context())
			if err != nil {
				logger.Error("list routing policies failed", "error", err)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load routing policies"})
				return
			}
			httpx.WriteJSON(w, http.StatusOK, map[string]any{"routing_policies": policies})
		})

		mux.HandleFunc("GET /v1/routing-policies/{eventName}", func(w http.ResponseWriter, r *http.Request) {
			eventName := r.PathValue("eventName")
			policy, err := store.GetRoutingPolicyByEventName(r.Context(), eventName)
			if errors.Is(err, postgres.ErrNotFound) {
				httpx.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "routing policy not found"})
				return
			}
			if err != nil {
				logger.Error("get routing policy failed", "error", err, "event_name", eventName)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load routing policy"})
				return
			}
			httpx.WriteJSON(w, http.StatusOK, policy)
		})

		mux.HandleFunc("POST /v1/routing-policies", func(w http.ResponseWriter, r *http.Request) {
			var req notification.RoutingPolicyUpsertRequest
			if err := httpx.DecodeJSON(r, &req); err != nil {
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid routing policy payload"})
				return
			}
			if req.EventName == "" || len(req.Channels) == 0 {
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "event_name and channels are required"})
				return
			}

			policy := notification.RoutingPolicy{
				PolicyID:   id.New(12),
				EventName:  req.EventName,
				Channels:   req.Channels,
				BindingSet: req.BindingSet,
				Enabled:    req.Enabled,
				Priority:   req.Priority,
			}
			if err := store.UpsertRoutingPolicy(r.Context(), policy); err != nil {
				logger.Error("upsert routing policy failed", "error", err, "event_name", req.EventName)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "save routing policy"})
				return
			}

			saved, err := store.GetRoutingPolicyByEventName(r.Context(), req.EventName)
			if err != nil {
				logger.Error("reload routing policy failed", "error", err, "event_name", req.EventName)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "reload routing policy"})
				return
			}
			httpx.WriteJSON(w, http.StatusOK, saved)
		})

		mux.HandleFunc("GET /v1/preference-policies", func(w http.ResponseWriter, r *http.Request) {
			userID := r.URL.Query().Get("user_id")
			policies, err := store.ListPreferencePolicies(r.Context(), userID)
			if err != nil {
				logger.Error("list preference policies failed", "error", err, "user_id", userID)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load preference policies"})
				return
			}
			httpx.WriteJSON(w, http.StatusOK, map[string]any{"preference_policies": policies})
		})

		mux.HandleFunc("GET /v1/preference-policies/{userID}/{channel}", func(w http.ResponseWriter, r *http.Request) {
			userID := r.PathValue("userID")
			channel := notification.Channel(r.PathValue("channel"))
			policy, err := store.GetPreferencePolicy(r.Context(), userID, channel)
			if errors.Is(err, postgres.ErrNotFound) {
				httpx.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "preference policy not found"})
				return
			}
			if err != nil {
				logger.Error("get preference policy failed", "error", err, "user_id", userID, "channel", channel)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load preference policy"})
				return
			}
			httpx.WriteJSON(w, http.StatusOK, policy)
		})

		mux.HandleFunc("POST /v1/preference-policies", func(w http.ResponseWriter, r *http.Request) {
			var req notification.PreferencePolicyUpsertRequest
			if err := httpx.DecodeJSON(r, &req); err != nil {
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid preference policy payload"})
				return
			}
			if req.UserID == "" || req.Channel == "" {
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "user_id and channel are required"})
				return
			}

			policy := notification.PreferencePolicy{
				PolicyID:  id.New(12),
				UserID:    req.UserID,
				Channel:   req.Channel,
				IsEnabled: req.IsEnabled,
			}
			if err := store.UpsertPreferencePolicy(r.Context(), policy); err != nil {
				logger.Error("upsert preference policy failed", "error", err, "user_id", req.UserID, "channel", req.Channel)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "save preference policy"})
				return
			}

			saved, err := store.GetPreferencePolicy(r.Context(), req.UserID, req.Channel)
			if err != nil {
				logger.Error("reload preference policy failed", "error", err, "user_id", req.UserID, "channel", req.Channel)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "reload preference policy"})
				return
			}
			httpx.WriteJSON(w, http.StatusOK, saved)
		})

		mux.HandleFunc("GET /v1/templates", func(w http.ResponseWriter, r *http.Request) {
			templates, err := store.ListTemplates(r.Context())
			if err != nil {
				logger.Error("list templates failed", "error", err)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load templates"})
				return
			}
			httpx.WriteJSON(w, http.StatusOK, map[string]any{"templates": templates})
		})

		mux.HandleFunc("GET /v1/templates/{templateKey}/{channel}", func(w http.ResponseWriter, r *http.Request) {
			templateKey := r.PathValue("templateKey")
			channel := notification.Channel(r.PathValue("channel"))
			tmpl, err := store.GetTemplateByKeyAndChannel(r.Context(), templateKey, channel)
			if errors.Is(err, postgres.ErrNotFound) {
				httpx.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "template not found"})
				return
			}
			if err != nil {
				logger.Error("get template failed", "error", err, "template_key", templateKey, "channel", channel)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load template"})
				return
			}
			httpx.WriteJSON(w, http.StatusOK, tmpl)
		})

		mux.HandleFunc("POST /v1/templates", func(w http.ResponseWriter, r *http.Request) {
			var req notification.TemplateUpsertRequest
			if err := httpx.DecodeJSON(r, &req); err != nil {
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid template payload"})
				return
			}
			if req.TemplateKey == "" || req.Channel == "" || req.BodyTemplate == "" {
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "template_key, channel, and body_template are required"})
				return
			}
			if err := render.ValidateSubjectTemplate(req.SubjectTemplate); err != nil {
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
				return
			}
			if err := render.ValidateBodyTemplate(req.BodyTemplate); err != nil {
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
				return
			}

			tmpl := notification.Template{
				TemplateID:      id.New(12),
				TemplateKey:     req.TemplateKey,
				Channel:         req.Channel,
				SubjectTemplate: req.SubjectTemplate,
				BodyTemplate:    req.BodyTemplate,
				Enabled:         req.Enabled,
			}
			if err := store.UpsertTemplate(r.Context(), tmpl); err != nil {
				logger.Error("upsert template failed", "error", err, "template_key", req.TemplateKey, "channel", req.Channel)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "save template"})
				return
			}

			saved, err := store.GetTemplateByKeyAndChannel(r.Context(), req.TemplateKey, req.Channel)
			if err != nil {
				logger.Error("reload template failed", "error", err, "template_key", req.TemplateKey, "channel", req.Channel)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "reload template"})
				return
			}
			httpx.WriteJSON(w, http.StatusOK, saved)
		})

		mux.HandleFunc("GET /v1/delivery-policies", func(w http.ResponseWriter, r *http.Request) {
			policies, err := store.ListDeliveryPolicies(r.Context())
			if err != nil {
				logger.Error("list delivery policies failed", "error", err)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load delivery policies"})
				return
			}
			httpx.WriteJSON(w, http.StatusOK, map[string]any{"delivery_policies": policies})
		})

		mux.HandleFunc("GET /v1/delivery-policies/{channel}", func(w http.ResponseWriter, r *http.Request) {
			channel := notification.Channel(r.PathValue("channel"))
			policy, err := store.GetDeliveryPolicyByChannel(r.Context(), channel)
			if errors.Is(err, postgres.ErrNotFound) {
				httpx.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "delivery policy not found"})
				return
			}
			if err != nil {
				logger.Error("get delivery policy failed", "error", err, "channel", channel)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load delivery policy"})
				return
			}
			httpx.WriteJSON(w, http.StatusOK, policy)
		})

		mux.HandleFunc("POST /v1/delivery-policies", func(w http.ResponseWriter, r *http.Request) {
			var req notification.DeliveryPolicyUpsertRequest
			if err := httpx.DecodeJSON(r, &req); err != nil {
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid delivery policy payload"})
				return
			}
			if req.Channel == "" || req.MaxAttempts < 1 || req.BackoffSeconds < 0 {
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "channel is required, max_attempts must be at least 1, and backoff_seconds cannot be negative"})
				return
			}

			policy := notification.DeliveryPolicy{
				PolicyID:       id.New(12),
				Channel:        req.Channel,
				MaxAttempts:    req.MaxAttempts,
				BackoffSeconds: req.BackoffSeconds,
				Enabled:        req.Enabled,
			}
			if err := store.UpsertDeliveryPolicy(r.Context(), policy); err != nil {
				logger.Error("upsert delivery policy failed", "error", err, "channel", req.Channel)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "save delivery policy"})
				return
			}

			saved, err := store.GetDeliveryPolicyByChannel(r.Context(), req.Channel)
			if err != nil {
				logger.Error("reload delivery policy failed", "error", err, "channel", req.Channel)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "reload delivery policy"})
				return
			}
			httpx.WriteJSON(w, http.StatusOK, saved)
		})

		mux.HandleFunc("GET /v1/webhook-subscriptions", func(w http.ResponseWriter, r *http.Request) {
			subscriptions, err := store.ListWebhookSubscriptions(r.Context())
			if err != nil {
				logger.Error("list webhook subscriptions failed", "error", err)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load webhook subscriptions"})
				return
			}
			httpx.WriteJSON(w, http.StatusOK, map[string]any{"webhook_subscriptions": subscriptions})
		})

		mux.HandleFunc("GET /v1/webhook-subscriptions/{subscriptionID}", func(w http.ResponseWriter, r *http.Request) {
			subscriptionID := r.PathValue("subscriptionID")
			subscription, err := store.GetWebhookSubscriptionByID(r.Context(), subscriptionID)
			if errors.Is(err, postgres.ErrNotFound) {
				httpx.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "webhook subscription not found"})
				return
			}
			if err != nil {
				logger.Error("get webhook subscription failed", "error", err, "subscription_id", subscriptionID)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load webhook subscription"})
				return
			}
			httpx.WriteJSON(w, http.StatusOK, subscription)
		})

		mux.HandleFunc("POST /v1/webhook-subscriptions", func(w http.ResponseWriter, r *http.Request) {
			var req notification.WebhookSubscriptionUpsertRequest
			if err := httpx.DecodeJSON(r, &req); err != nil {
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid webhook subscription payload"})
				return
			}
			if req.TargetURL == "" {
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "target_url is required"})
				return
			}

			subscription := notification.WebhookSubscription{
				SubscriptionID: id.New(12),
				TargetURL:      req.TargetURL,
				Enabled:        req.Enabled,
			}
			if err := store.UpsertWebhookSubscription(r.Context(), subscription); err != nil {
				logger.Error("upsert webhook subscription failed", "error", err, "target_url", req.TargetURL)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "save webhook subscription"})
				return
			}

			subscriptions, err := store.ListWebhookSubscriptions(r.Context())
			if err != nil {
				logger.Error("list webhook subscriptions after upsert failed", "error", err)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "reload webhook subscriptions"})
				return
			}
			for _, saved := range subscriptions {
				if saved.TargetURL == req.TargetURL {
					httpx.WriteJSON(w, http.StatusOK, saved)
					return
				}
			}
			httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "reload webhook subscription"})
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

			webhookAttempts, err := store.ListWebhookDeliveryAttempts(r.Context(), requestID)
			if err != nil {
				logger.Error("list webhook delivery attempts failed", "error", err, "request_id", requestID)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load webhook delivery attempts"})
				return
			}

			httpx.WriteJSON(w, http.StatusOK, map[string]any{
				"request":                   record,
				"delivery_attempts":         attempts,
				"webhook_delivery_attempts": webhookAttempts,
			})
		})

		mux.HandleFunc("GET /v1/status", func(w http.ResponseWriter, _ *http.Request) {
			httpx.WriteJSON(w, http.StatusOK, map[string]any{
				"service": info.Name,
				"phase":   "webhook-delivery-tracking",
				"state":   "api persists requests, provider bindings, routing policies, preference policies, templates, delivery policies, webhook subscriptions, and webhook delivery attempts",
				"topic":   topic,
			})
		})
	})
	if err != nil {
		panic(err)
	}
}

func sameNotificationIntent(existing notification.NotificationRecord, incoming notification.NotificationRequest) bool {
	return existing.EventName == incoming.EventName &&
		existing.TemplateKey == incoming.TemplateKey &&
		reflect.DeepEqual(existing.Channels, incoming.Channels) &&
		existing.BindingSet == incoming.BindingSet &&
		reflect.DeepEqual(existing.Recipient, incoming.Recipient) &&
		reflect.DeepEqual(existing.Variables, incoming.Variables) &&
		reflect.DeepEqual(existing.Metadata, incoming.Metadata) &&
		existing.Priority == incoming.Priority &&
		timesEqual(existing.ExpiresAt, incoming.ExpiresAt)
}

func timesEqual(existing *time.Time, incoming *time.Time) bool {
	switch {
	case existing == nil && incoming == nil:
		return true
	case existing == nil || incoming == nil:
		return false
	default:
		return existing.Equal(*incoming)
	}
}
