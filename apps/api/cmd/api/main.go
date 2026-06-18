package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/NotifyHub-in/NotifyHub/libs/contracts/notification"
	"github.com/NotifyHub-in/NotifyHub/libs/core/app"
	"github.com/NotifyHub-in/NotifyHub/libs/core/config"
	"github.com/NotifyHub-in/NotifyHub/libs/core/httpx"
	"github.com/NotifyHub-in/NotifyHub/libs/core/id"
	"github.com/NotifyHub-in/NotifyHub/libs/core/ratelimit"
	"github.com/NotifyHub-in/NotifyHub/libs/core/render"
	"github.com/NotifyHub-in/NotifyHub/libs/core/serviceinfo"
	"github.com/NotifyHub-in/NotifyHub/libs/core/webhooks"
	kafkamq "github.com/NotifyHub-in/NotifyHub/libs/messaging/kafka"
	"github.com/NotifyHub-in/NotifyHub/libs/observability/logging"
	"github.com/NotifyHub-in/NotifyHub/libs/observability/metrics"
	"github.com/NotifyHub-in/NotifyHub/libs/storage/postgres"
	"golang.org/x/time/rate"
)

func main() {
	cfg, err := config.LoadHTTPServiceConfig("api", 8080)
	if err != nil {
		panic(err)
	}

	logger := logging.New(cfg.ServiceName)
	ctx := context.Background()
	registry := metrics.NewRegistry(cfg.ServiceName)
	rateLimitConfig := loadAPIRateLimitConfig()
	rateLimiter := ratelimit.NewManager(ratelimit.Config{
		Enabled:         rateLimitConfig.Enabled,
		CleanupInterval: rateLimitConfig.CleanupInterval,
		EntryTTL:        rateLimitConfig.EntryTTL,
	})

	store, err := postgres.Open(ctx, config.MustGetEnv("DATABASE_URL"))
	if err != nil {
		panic(err)
	}
	defer store.Close()
	postgres.AttachMetrics(registry)
	notifier := webhooks.NewNotifier(store)
	authRequired := config.GetEnv("NOTIFICATION_API_AUTH_REQUIRED", "true") == "true"
	adminAuthRequired := config.GetEnv("NOTIFICATION_ADMIN_API_AUTH_REQUIRED", "true") == "true"
	adminAPIToken := config.GetEnv("NOTIFICATION_ADMIN_API_TOKEN", "")
	readOnlyAPIToken := config.GetEnv("NOTIFICATION_READONLY_API_TOKEN", "")

	brokers := config.MustGetEnv("KAFKA_BROKERS")
	topic := config.GetEnv("KAFKA_NOTIFICATION_TOPIC", "notification.requests")
	if err := kafkamq.EnsureTopic(ctx, brokers, topic, 1, 1); err != nil {
		panic(err)
	}

	publisher := kafkamq.NewPublisher(brokers, topic)
	defer publisher.Close()

	err = app.RunHTTPService(cfg, logger, registry, func(mux *http.ServeMux, info serviceinfo.Info, registry *metrics.Registry) {
		protect := func(requiredRole authRole, resource string, action string, handler func(http.ResponseWriter, *http.Request)) http.HandlerFunc {
			return func(w http.ResponseWriter, r *http.Request) {
				if rateLimitConfig.Enabled {
					if !allowAnonymousRateLimit(w, r, registry, rateLimiter, rateLimitConfig.AnonymousRPS, rateLimitConfig.AnonymousBurst, resource, action) {
						return
					}
				}
				role, err := authenticateControlPlaneRequest(r, adminAPIToken, readOnlyAPIToken, adminAuthRequired)
				if err != nil {
					recordAdminAPIEvent(registry, resource, action, "unauthorized")
					httpx.WriteJSON(w, http.StatusUnauthorized, map[string]string{"error": err.Error()})
					return
				}
				if rateLimitConfig.Enabled {
					switch role {
					case authRoleAdmin:
						if !allowRoleRateLimit(w, r, registry, rateLimiter, "admin", rateLimitConfig.AdminRPS, rateLimitConfig.AdminBurst, resource, action) {
							return
						}
					case authRoleRead:
						if !allowRoleRateLimit(w, r, registry, rateLimiter, "read", rateLimitConfig.ReadRPS, rateLimitConfig.ReadBurst, resource, action) {
							return
						}
					}
				}
				if requiredRole == authRoleAdmin && role != authRoleAdmin {
					recordAdminAPIEvent(registry, resource, action, "forbidden")
					httpx.WriteJSON(w, http.StatusForbidden, map[string]string{"error": "admin token required"})
					return
				}
				handler(w, r)
			}
		}

		mux.HandleFunc("POST /v1/clients", protect(authRoleAdmin, "client", "create", func(w http.ResponseWriter, r *http.Request) {
			var req notification.NotificationClientCreateRequest
			if err := httpx.DecodeJSON(r, &req); err != nil {
				recordAdminAPIEvent(registry, "client", "create", "invalid_payload")
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid notification client payload"})
				return
			}
			if req.TenantID == "" || req.ClientName == "" {
				recordAdminAPIEvent(registry, "client", "create", "validation_failed")
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "tenant_id and client_name are required"})
				return
			}

			apiKey, apiKeyHash := generateNotificationClientAPIKey()
			client := notification.NotificationClient{
				ClientID:        id.New(12),
				TenantID:        req.TenantID,
				ClientName:      req.ClientName,
				Enabled:         req.Enabled,
				AllowedChannels: req.AllowedChannels,
			}
			if len(client.AllowedChannels) == 0 {
				client.AllowedChannels = []notification.Channel{
					notification.ChannelEmail,
					notification.ChannelSMS,
					notification.ChannelWhatsApp,
					notification.ChannelPush,
					notification.ChannelWebhook,
				}
			}
			created, err := store.CreateNotificationClient(r.Context(), client, apiKeyHash)
			if errors.Is(err, postgres.ErrConflict) {
				recordAdminAPIEvent(registry, "client", "create", "conflict")
				httpx.WriteJSON(w, http.StatusConflict, map[string]string{"error": "notification client already exists"})
				return
			}
			if err != nil {
				logger.Error("create notification client failed", "error", err, "tenant_id", req.TenantID, "client_name", req.ClientName)
				recordAdminAPIEvent(registry, "client", "create", "save_failed")
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "create notification client"})
				return
			}
			recordAdminAPIEvent(registry, "client", "create", "accepted")
			httpx.WriteJSON(w, http.StatusCreated, notification.NotificationClientCreateResponse{
				Client: created,
				APIKey: apiKey,
			})
		}))

		mux.HandleFunc("GET /v1/clients", protect(authRoleRead, "client", "list", func(w http.ResponseWriter, r *http.Request) {
			tenantID := r.URL.Query().Get("tenant_id")
			clients, err := store.ListNotificationClients(r.Context(), tenantID)
			if err != nil {
				logger.Error("list notification clients failed", "error", err, "tenant_id", tenantID)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load notification clients"})
				return
			}
			httpx.WriteJSON(w, http.StatusOK, map[string]any{"clients": clients})
		}))

		mux.HandleFunc("GET /v1/clients/{clientID}", protect(authRoleRead, "client", "get", func(w http.ResponseWriter, r *http.Request) {
			clientID := r.PathValue("clientID")
			client, err := store.GetNotificationClient(r.Context(), clientID)
			if errors.Is(err, postgres.ErrNotFound) {
				httpx.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "notification client not found"})
				return
			}
			if err != nil {
				logger.Error("get notification client failed", "error", err, "client_id", clientID)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load notification client"})
				return
			}
			httpx.WriteJSON(w, http.StatusOK, client)
		}))

		mux.HandleFunc("POST /v1/clients/{clientID}/disable", protect(authRoleAdmin, "client", "disable", func(w http.ResponseWriter, r *http.Request) {
			clientID := r.PathValue("clientID")
			client, err := store.GetNotificationClient(r.Context(), clientID)
			if errors.Is(err, postgres.ErrNotFound) {
				httpx.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "notification client not found"})
				return
			}
			if err != nil {
				logger.Error("load notification client failed", "error", err, "client_id", clientID)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load notification client"})
				return
			}
			client.Enabled = false
			if err := store.UpdateNotificationClient(r.Context(), client); err != nil {
				logger.Error("disable notification client failed", "error", err, "client_id", clientID)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "disable notification client"})
				return
			}
			updated, err := store.GetNotificationClient(r.Context(), clientID)
			if err != nil {
				logger.Error("reload disabled notification client failed", "error", err, "client_id", clientID)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "reload notification client"})
				return
			}
			httpx.WriteJSON(w, http.StatusOK, updated)
		}))

		mux.HandleFunc("POST /v1/notification-requests", func(w http.ResponseWriter, r *http.Request) {
			if rateLimitConfig.Enabled {
				if !allowAnonymousRateLimit(w, r, registry, rateLimiter, rateLimitConfig.AnonymousRPS, rateLimitConfig.AnonymousBurst, "notification_request", "create") {
					return
				}
			}
			var req notification.NotificationRequest
			if err := httpx.DecodeJSON(r, &req); err != nil {
				registry.IncCounter("notification_request_api_events_total", "Notification request API outcomes.", map[string]string{"outcome": "invalid_payload"})
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request payload"})
				return
			}

			caller, callerErr := authenticateNotificationRequest(r, store, authRequired)
			if callerErr != nil {
				registry.IncCounter("notification_request_api_events_total", "Notification request API outcomes.", map[string]string{"outcome": "unauthorized"})
				httpx.WriteJSON(w, http.StatusUnauthorized, map[string]string{"error": callerErr.Error()})
				return
			}
			if rateLimitConfig.Enabled {
				subject := caller.ClientID
				if subject == "" || subject == "anonymous" {
					subject = requestIP(r)
					if subject == "" {
						subject = "anonymous"
					}
				}
				if !rateLimiter.Allow(rateLimitKey("client", subject), rate.Limit(rateLimitConfig.ClientRPS), rateLimitConfig.ClientBurst) {
					registry.IncCounter("notification_request_api_events_total", "Notification request API outcomes.", map[string]string{"outcome": "rate_limited"})
					httpx.WriteJSON(w, http.StatusTooManyRequests, map[string]string{"error": "notification request rate limit exceeded"})
					return
				}
			}

			if req.EventName == "" || req.TemplateKey == "" || req.IdempotencyKey == "" {
				registry.IncCounter("notification_request_api_events_total", "Notification request API outcomes.", map[string]string{"outcome": "validation_failed"})
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "event_name, template_key, and idempotency_key are required"})
				return
			}
			if len(req.Channels) == 0 {
				registry.IncCounter("notification_request_api_events_total", "Notification request API outcomes.", map[string]string{"outcome": "validation_failed"})
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "at least one channel is required"})
				return
			}
			if !clientAllowsChannels(caller, req.Channels) {
				registry.IncCounter("notification_request_api_events_total", "Notification request API outcomes.", map[string]string{"outcome": "forbidden"})
				httpx.WriteJSON(w, http.StatusForbidden, map[string]string{"error": "client not allowed to send one or more requested channels"})
				return
			}

			now := time.Now().UTC()
			if req.ExpiresAt != nil && !req.ExpiresAt.After(now) {
				registry.IncCounter("notification_request_api_events_total", "Notification request API outcomes.", map[string]string{"outcome": "validation_failed"})
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "expires_at must be in the future"})
				return
			}
			record := notification.NotificationRecord{
				RequestID:        id.New(12),
				IdempotencyKey:   req.IdempotencyKey,
				EventName:        req.EventName,
				TemplateKey:      req.TemplateKey,
				LanguageCode:     notification.NormalizeLanguageCode(req.LanguageCode),
				Channels:         req.Channels,
				BindingSet:       req.BindingSet,
				Recipient:        req.Recipient,
				Variables:        req.Variables,
				Metadata:         req.Metadata,
				Priority:         req.Priority,
				SourceClientID:   caller.ClientID,
				SourceTenantID:   caller.TenantID,
				SourceClientName: caller.ClientName,
				Status:           notification.RequestStatusAccepted,
				RequestedAt:      now,
				ExpiresAt:        req.ExpiresAt,
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

					if !sameNotificationIntent(existing, req, caller) {
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

			if err := notifier.NotifyRequestUpdated(r.Context(), record.RequestID, map[string]interface{}{"source": "api", "source_client": caller.ClientName, "source_client_id": caller.ClientID}); err != nil {
				logger.Error("notify accepted lifecycle webhook failed", "error", err, "request_id", record.RequestID)
			}

			registry.IncCounter("notification_request_api_events_total", "Notification request API outcomes.", map[string]string{
				"outcome":       "accepted",
				"priority":      string(record.Priority),
				"source_client": record.SourceClientName,
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

		mux.HandleFunc("GET /v1/provider-bindings", protect(authRoleRead, "provider_binding", "list", func(w http.ResponseWriter, r *http.Request) {
			bindings, err := store.ListProviderBindings(r.Context())
			if err != nil {
				logger.Error("list provider bindings failed", "error", err)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load provider bindings"})
				return
			}
			httpx.WriteJSON(w, http.StatusOK, map[string]any{"provider_bindings": bindings})
		}))

		mux.HandleFunc("GET /v1/provider-definitions", protect(authRoleRead, "provider_definition", "list", func(w http.ResponseWriter, _ *http.Request) {
			httpx.WriteJSON(w, http.StatusOK, map[string]any{"provider_definitions": notification.ProviderDefinitions()})
		}))

		mux.HandleFunc("GET /v1/provider-accounts", protect(authRoleRead, "provider_account", "list", func(w http.ResponseWriter, r *http.Request) {
			tenantID := r.URL.Query().Get("tenant_id")
			accounts, err := store.ListProviderAccounts(r.Context(), tenantID)
			if err != nil {
				logger.Error("list provider accounts failed", "error", err, "tenant_id", tenantID)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load provider accounts"})
				return
			}
			httpx.WriteJSON(w, http.StatusOK, map[string]any{"provider_accounts": accounts})
		}))

		mux.HandleFunc("GET /v1/provider-accounts/{providerAccountID}", protect(authRoleRead, "provider_account", "get", func(w http.ResponseWriter, r *http.Request) {
			providerAccountID := r.PathValue("providerAccountID")
			account, err := store.GetProviderAccount(r.Context(), providerAccountID)
			if errors.Is(err, postgres.ErrNotFound) {
				httpx.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "provider account not found"})
				return
			}
			if err != nil {
				logger.Error("get provider account failed", "error", err, "provider_account_id", providerAccountID)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load provider account"})
				return
			}
			httpx.WriteJSON(w, http.StatusOK, account)
		}))

		mux.HandleFunc("GET /v1/provider-accounts/{providerAccountID}/status", protect(authRoleRead, "provider_account", "status", func(w http.ResponseWriter, r *http.Request) {
			providerAccountID := r.PathValue("providerAccountID")
			account, err := store.GetProviderAccount(r.Context(), providerAccountID)
			if errors.Is(err, postgres.ErrNotFound) {
				httpx.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "provider account not found"})
				return
			}
			if err != nil {
				logger.Error("get provider account status failed", "error", err, "provider_account_id", providerAccountID)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load provider account"})
				return
			}

			bindings, err := store.ListProviderBindings(r.Context())
			if err != nil {
				logger.Error("list provider bindings for provider account status failed", "error", err, "provider_account_id", providerAccountID)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load provider bindings"})
				return
			}

			var accountBindings []notification.ProviderBinding
			var bindingHealth []notification.ProviderBindingHealth
			for _, binding := range bindings {
				if binding.ProviderAccountID != providerAccountID {
					continue
				}
				accountBindings = append(accountBindings, binding)

				health, healthErr := store.GetProviderBindingHealth(r.Context(), binding.BindingID)
				if errors.Is(healthErr, postgres.ErrNotFound) {
					continue
				}
				if healthErr != nil {
					logger.Error("load provider binding health for provider account status failed", "error", healthErr, "provider_account_id", providerAccountID, "binding_id", binding.BindingID)
					httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load provider binding health"})
					return
				}
				bindingHealth = append(bindingHealth, health)
			}

			var callbackRoute *notification.CallbackRoute
			if route, routeErr := store.GetCallbackRouteByProviderKey(r.Context(), account.ProviderKey); routeErr == nil {
				callbackRoute = &route
			} else if !errors.Is(routeErr, postgres.ErrNotFound) {
				logger.Error("load callback route for provider account status failed", "error", routeErr, "provider_account_id", providerAccountID, "provider_key", account.ProviderKey)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load callback route"})
				return
			}

			httpx.WriteJSON(w, http.StatusOK, map[string]any{
				"provider_account":  account,
				"provider_bindings": accountBindings,
				"binding_health":    bindingHealth,
				"callback_route":    callbackRoute,
			})
		}))

		mux.HandleFunc("POST /v1/provider-accounts", protect(authRoleAdmin, "provider_account", "create", func(w http.ResponseWriter, r *http.Request) {
			var req notification.ProviderAccountUpsertRequest
			if err := httpx.DecodeJSON(r, &req); err != nil {
				recordAdminAPIEvent(registry, "provider_account", "create", "invalid_payload")
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid provider account payload"})
				return
			}
			account := notification.ProviderAccount{
				ProviderAccountID: id.New(12),
				TenantID:          req.TenantID,
				ProviderKey:       req.ProviderKey,
				DisplayName:       req.DisplayName,
				Channel:           req.Channel,
				Enabled:           req.Enabled,
				Config:            req.Config,
				SecretRefs:        req.SecretRefs,
			}
			if err := notification.ValidateProviderAccount(account); err != nil {
				recordAdminAPIEvent(registry, "provider_account", "create", "validation_failed")
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
				return
			}
			if err := store.UpsertProviderAccount(r.Context(), account); err != nil {
				logger.Error("upsert provider account failed", "error", err, "provider_account_id", account.ProviderAccountID)
				recordAdminAPIEvent(registry, "provider_account", "create", "save_failed")
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "save provider account"})
				return
			}

			saved, err := store.GetProviderAccount(r.Context(), account.ProviderAccountID)
			if err != nil {
				logger.Error("reload provider account failed", "error", err, "provider_account_id", account.ProviderAccountID)
				recordAdminAPIEvent(registry, "provider_account", "create", "reload_failed")
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "reload provider account"})
				return
			}
			recordAdminAPIEvent(registry, "provider_account", "create", "accepted")
			httpx.WriteJSON(w, http.StatusCreated, saved)
		}))

		mux.HandleFunc("PATCH /v1/provider-accounts/{providerAccountID}", protect(authRoleAdmin, "provider_account", "patch", func(w http.ResponseWriter, r *http.Request) {
			providerAccountID := r.PathValue("providerAccountID")
			existing, err := store.GetProviderAccount(r.Context(), providerAccountID)
			if errors.Is(err, postgres.ErrNotFound) {
				httpx.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "provider account not found"})
				return
			}
			if err != nil {
				logger.Error("load provider account for patch failed", "error", err, "provider_account_id", providerAccountID)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load provider account"})
				return
			}

			var req notification.ProviderAccountPatchRequest
			if err := httpx.DecodeJSON(r, &req); err != nil {
				recordAdminAPIEvent(registry, "provider_account", "patch", "invalid_payload")
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid provider account patch payload"})
				return
			}

			updated := existing
			if req.DisplayName != nil {
				updated.DisplayName = *req.DisplayName
			}
			if req.Enabled != nil {
				updated.Enabled = *req.Enabled
			}
			if req.Config != nil {
				updated.Config = *req.Config
			}
			if req.SecretRefs != nil {
				updated.SecretRefs = *req.SecretRefs
			}

			if err := notification.ValidateProviderAccount(updated); err != nil {
				recordAdminAPIEvent(registry, "provider_account", "patch", "validation_failed")
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
				return
			}
			if err := store.UpsertProviderAccount(r.Context(), updated); err != nil {
				logger.Error("patch provider account failed", "error", err, "provider_account_id", providerAccountID)
				recordAdminAPIEvent(registry, "provider_account", "patch", "save_failed")
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "save provider account"})
				return
			}

			saved, err := store.GetProviderAccount(r.Context(), providerAccountID)
			if err != nil {
				logger.Error("reload patched provider account failed", "error", err, "provider_account_id", providerAccountID)
				recordAdminAPIEvent(registry, "provider_account", "patch", "reload_failed")
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "reload provider account"})
				return
			}
			recordAdminAPIEvent(registry, "provider_account", "patch", "accepted")
			httpx.WriteJSON(w, http.StatusOK, saved)
		}))

		mux.HandleFunc("POST /v1/provider-accounts/{providerAccountID}/disable", protect(authRoleAdmin, "provider_account", "disable", func(w http.ResponseWriter, r *http.Request) {
			providerAccountID := r.PathValue("providerAccountID")
			if err := store.DisableProviderAccount(r.Context(), providerAccountID); errors.Is(err, postgres.ErrNotFound) {
				recordAdminAPIEvent(registry, "provider_account", "disable", "not_found")
				httpx.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "provider account not found"})
				return
			} else if err != nil {
				logger.Error("disable provider account failed", "error", err, "provider_account_id", providerAccountID)
				recordAdminAPIEvent(registry, "provider_account", "disable", "save_failed")
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "disable provider account"})
				return
			}

			account, err := store.GetProviderAccount(r.Context(), providerAccountID)
			if err != nil {
				logger.Error("reload disabled provider account failed", "error", err, "provider_account_id", providerAccountID)
				recordAdminAPIEvent(registry, "provider_account", "disable", "reload_failed")
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "reload provider account"})
				return
			}
			recordAdminAPIEvent(registry, "provider_account", "disable", "accepted")
			httpx.WriteJSON(w, http.StatusOK, account)
		}))

		mux.HandleFunc("GET /v1/callback-routes", protect(authRoleRead, "callback_route", "list", func(w http.ResponseWriter, r *http.Request) {
			routes, err := store.ListCallbackRoutes(r.Context())
			if err != nil {
				logger.Error("list callback routes failed", "error", err)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load callback routes"})
				return
			}
			httpx.WriteJSON(w, http.StatusOK, map[string]any{"callback_routes": routes})
		}))

		mux.HandleFunc("GET /v1/callback-routes/{providerKey}", protect(authRoleRead, "callback_route", "get", func(w http.ResponseWriter, r *http.Request) {
			providerKey := r.PathValue("providerKey")
			route, err := store.GetCallbackRouteByProviderKey(r.Context(), providerKey)
			if errors.Is(err, postgres.ErrNotFound) {
				httpx.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "callback route not found"})
				return
			}
			if err != nil {
				logger.Error("get callback route failed", "error", err, "provider_key", providerKey)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load callback route"})
				return
			}
			httpx.WriteJSON(w, http.StatusOK, route)
		}))

		mux.HandleFunc("POST /v1/callback-routes", protect(authRoleAdmin, "callback_route", "create", func(w http.ResponseWriter, r *http.Request) {
			var req notification.CallbackRouteUpsertRequest
			if err := httpx.DecodeJSON(r, &req); err != nil {
				recordAdminAPIEvent(registry, "callback_route", "create", "invalid_payload")
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid callback route payload"})
				return
			}
			if req.ProviderAccountID == "" || req.CallbackPath == "" {
				recordAdminAPIEvent(registry, "callback_route", "create", "validation_failed")
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "provider_key, provider_account_id, and callback_path are required"})
				return
			}
			if req.ProviderKey == "" {
				recordAdminAPIEvent(registry, "callback_route", "create", "validation_failed")
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "provider_key is required"})
				return
			}

			_, err := store.GetProviderAccount(r.Context(), req.ProviderAccountID)
			if errors.Is(err, postgres.ErrNotFound) {
				recordAdminAPIEvent(registry, "callback_route", "create", "provider_account_not_found")
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "provider_account_id not found"})
				return
			}
			if err != nil {
				logger.Error("load provider account for callback route failed", "error", err, "provider_account_id", req.ProviderAccountID)
				recordAdminAPIEvent(registry, "callback_route", "create", "load_failed")
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load provider account"})
				return
			}

			route := notification.CallbackRoute{
				RouteID:               id.New(12),
				ProviderKey:           req.ProviderKey,
				ProviderAccountID:     req.ProviderAccountID,
				CallbackPath:          req.CallbackPath,
				VerificationMode:      req.VerificationMode,
				VerificationSecretRef: req.VerificationSecretRef,
				Enabled:               req.Enabled,
			}
			if err := notification.ValidateCallbackRoute(route); err != nil {
				recordAdminAPIEvent(registry, "callback_route", "create", "validation_failed")
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
				return
			}
			if err := store.UpsertCallbackRoute(r.Context(), route); err != nil {
				logger.Error("upsert callback route failed", "error", err, "provider_key", route.ProviderKey)
				recordAdminAPIEvent(registry, "callback_route", "create", "save_failed")
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "save callback route"})
				return
			}

			saved, err := store.GetCallbackRouteByProviderKey(r.Context(), route.ProviderKey)
			if err != nil {
				logger.Error("reload callback route failed", "error", err, "provider_key", route.ProviderKey)
				recordAdminAPIEvent(registry, "callback_route", "create", "reload_failed")
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "reload callback route"})
				return
			}
			recordAdminAPIEvent(registry, "callback_route", "create", "accepted")
			httpx.WriteJSON(w, http.StatusCreated, saved)
		}))

		mux.HandleFunc("GET /v1/provider-bindings/{channel}", protect(authRoleRead, "provider_binding", "get", func(w http.ResponseWriter, r *http.Request) {
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
		}))

		mux.HandleFunc("POST /v1/provider-bindings", protect(authRoleAdmin, "provider_binding", "create", func(w http.ResponseWriter, r *http.Request) {
			var req notification.ProviderBindingUpsertRequest
			if err := httpx.DecodeJSON(r, &req); err != nil {
				recordAdminAPIEvent(registry, "provider_binding", "create", "invalid_payload")
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid provider binding payload"})
				return
			}
			if req.Channel == "" {
				recordAdminAPIEvent(registry, "provider_binding", "create", "validation_failed")
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "channel is required"})
				return
			}
			if req.ProviderAccountID == "" {
				recordAdminAPIEvent(registry, "provider_binding", "create", "validation_failed")
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "provider_account_id is required"})
				return
			}
			if req.ConnectorName == "" {
				account, err := store.GetProviderAccount(r.Context(), req.ProviderAccountID)
				if errors.Is(err, postgres.ErrNotFound) {
					recordAdminAPIEvent(registry, "provider_binding", "create", "provider_account_not_found")
					httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "provider_account_id not found"})
					return
				}
				if err != nil {
					logger.Error("load provider account for binding failed", "error", err, "provider_account_id", req.ProviderAccountID)
					recordAdminAPIEvent(registry, "provider_binding", "create", "load_failed")
					httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load provider account"})
					return
				}
				def, ok := notification.ProviderDefinitionByKey(account.ProviderKey)
				if !ok {
					recordAdminAPIEvent(registry, "provider_binding", "create", "provider_definition_missing")
					httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "provider account provider_key is not registered"})
					return
				}
				req.ConnectorName = def.ConnectorName
			}
			if req.EndpointURL == "" {
				recordAdminAPIEvent(registry, "provider_binding", "create", "validation_failed")
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "endpoint_url is required for the current connector deployment"})
				return
			}

			binding := notification.ProviderBinding{
				BindingID:         id.New(12),
				Channel:           req.Channel,
				BindingSet:        req.BindingSet,
				ConnectorName:     req.ConnectorName,
				EndpointURL:       req.EndpointURL,
				ProviderAccountID: req.ProviderAccountID,
				Enabled:           req.Enabled,
				Priority:          req.Priority,
			}
			if err := store.UpsertProviderBinding(r.Context(), binding); err != nil {
				logger.Error("upsert provider binding failed", "error", err, "channel", req.Channel)
				recordAdminAPIEvent(registry, "provider_binding", "create", "save_failed")
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "save provider binding"})
				return
			}

			saved, err := store.GetProviderBindingByChannel(r.Context(), req.Channel, req.BindingSet)
			if err != nil {
				logger.Error("reload provider binding failed", "error", err, "channel", req.Channel, "binding_set", req.BindingSet)
				recordAdminAPIEvent(registry, "provider_binding", "create", "reload_failed")
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "reload provider binding"})
				return
			}
			recordAdminAPIEvent(registry, "provider_binding", "create", "accepted")
			httpx.WriteJSON(w, http.StatusOK, saved)
		}))

		mux.HandleFunc("GET /v1/provider-binding-health", protect(authRoleRead, "provider_binding", "health_list", func(w http.ResponseWriter, r *http.Request) {
			healthRecords, err := store.ListProviderBindingHealth(r.Context())
			if err != nil {
				logger.Error("list provider binding health failed", "error", err)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load provider binding health"})
				return
			}
			httpx.WriteJSON(w, http.StatusOK, map[string]any{"provider_binding_health": healthRecords})
		}))

		mux.HandleFunc("GET /v1/provider-binding-health/{bindingID}", protect(authRoleRead, "provider_binding", "health_get", func(w http.ResponseWriter, r *http.Request) {
			bindingID := r.PathValue("bindingID")
			healthRecord, err := store.GetProviderBindingHealth(r.Context(), bindingID)
			if errors.Is(err, postgres.ErrNotFound) {
				httpx.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "provider binding health not found"})
				return
			}
			if err != nil {
				logger.Error("get provider binding health failed", "error", err, "binding_id", bindingID)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load provider binding health"})
				return
			}
			httpx.WriteJSON(w, http.StatusOK, healthRecord)
		}))

		mux.HandleFunc("POST /v1/provider-binding-health/{bindingID}/reset", protect(authRoleAdmin, "provider_binding", "health_reset", func(w http.ResponseWriter, r *http.Request) {
			bindingID := r.PathValue("bindingID")
			if err := store.ResetProviderBindingHealth(r.Context(), bindingID); errors.Is(err, postgres.ErrNotFound) {
				httpx.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "provider binding health not found"})
				return
			} else if err != nil {
				logger.Error("reset provider binding health failed", "error", err, "binding_id", bindingID)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "reset provider binding health"})
				return
			}

			healthRecord, err := store.GetProviderBindingHealth(r.Context(), bindingID)
			if errors.Is(err, postgres.ErrNotFound) {
				httpx.WriteJSON(w, http.StatusOK, map[string]any{
					"binding_id": bindingID,
					"status":     "reset",
				})
				return
			}
			if err != nil {
				logger.Error("reload provider binding health failed", "error", err, "binding_id", bindingID)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "reload provider binding health"})
				return
			}
			httpx.WriteJSON(w, http.StatusOK, healthRecord)
		}))

		mux.HandleFunc("GET /v1/routing-policies", protect(authRoleRead, "routing_policy", "list", func(w http.ResponseWriter, r *http.Request) {
			policies, err := store.ListRoutingPolicies(r.Context())
			if err != nil {
				logger.Error("list routing policies failed", "error", err)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load routing policies"})
				return
			}
			httpx.WriteJSON(w, http.StatusOK, map[string]any{"routing_policies": policies})
		}))

		mux.HandleFunc("GET /v1/routing-policies/{eventName}", protect(authRoleRead, "routing_policy", "get", func(w http.ResponseWriter, r *http.Request) {
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
		}))

		mux.HandleFunc("POST /v1/routing-policies", protect(authRoleAdmin, "routing_policy", "create", func(w http.ResponseWriter, r *http.Request) {
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
		}))

		mux.HandleFunc("GET /v1/preference-policies", protect(authRoleRead, "preference_policy", "list", func(w http.ResponseWriter, r *http.Request) {
			userID := r.URL.Query().Get("user_id")
			policies, err := store.ListPreferencePolicies(r.Context(), userID)
			if err != nil {
				logger.Error("list preference policies failed", "error", err, "user_id", userID)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load preference policies"})
				return
			}
			httpx.WriteJSON(w, http.StatusOK, map[string]any{"preference_policies": policies})
		}))

		mux.HandleFunc("GET /v1/preference-policies/{userID}/{channel}", protect(authRoleRead, "preference_policy", "get", func(w http.ResponseWriter, r *http.Request) {
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
		}))

		mux.HandleFunc("POST /v1/preference-policies", protect(authRoleAdmin, "preference_policy", "create", func(w http.ResponseWriter, r *http.Request) {
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
		}))

		mux.HandleFunc("GET /v1/templates", protect(authRoleRead, "template", "list", func(w http.ResponseWriter, r *http.Request) {
			templates, err := store.ListTemplates(r.Context())
			if err != nil {
				logger.Error("list templates failed", "error", err)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load templates"})
				return
			}
			httpx.WriteJSON(w, http.StatusOK, map[string]any{"templates": templates})
		}))

		mux.HandleFunc("GET /v1/templates/{templateKey}/{channel}", protect(authRoleRead, "template", "get", func(w http.ResponseWriter, r *http.Request) {
			templateKey := r.PathValue("templateKey")
			channel := notification.Channel(r.PathValue("channel"))
			languageCode := notification.NormalizeLanguageCode(r.URL.Query().Get("language_code"))
			tmpl, err := store.GetTemplateByKeyAndChannelAndLanguage(r.Context(), templateKey, channel, languageCode)
			if errors.Is(err, postgres.ErrNotFound) {
				httpx.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "template not found"})
				return
			}
			if err != nil {
				logger.Error("get template failed", "error", err, "template_key", templateKey, "channel", channel, "language_code", languageCode)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load template"})
				return
			}
			httpx.WriteJSON(w, http.StatusOK, tmpl)
		}))

		mux.HandleFunc("POST /v1/templates", protect(authRoleAdmin, "template", "create", func(w http.ResponseWriter, r *http.Request) {
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
				LanguageCode:    notification.NormalizeLanguageCode(req.LanguageCode),
				SubjectTemplate: req.SubjectTemplate,
				BodyTemplate:    req.BodyTemplate,
				Metadata:        req.Metadata,
				Enabled:         req.Enabled,
			}
			if err := store.UpsertTemplate(r.Context(), tmpl); err != nil {
				logger.Error("upsert template failed", "error", err, "template_key", req.TemplateKey, "channel", req.Channel)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "save template"})
				return
			}

			saved, err := store.GetTemplateByKeyAndChannelAndLanguage(r.Context(), req.TemplateKey, req.Channel, tmpl.LanguageCode)
			if err != nil {
				logger.Error("reload template failed", "error", err, "template_key", req.TemplateKey, "channel", req.Channel, "language_code", tmpl.LanguageCode)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "reload template"})
				return
			}
			httpx.WriteJSON(w, http.StatusOK, saved)
		}))

		mux.HandleFunc("GET /v1/delivery-policies", protect(authRoleRead, "delivery_policy", "list", func(w http.ResponseWriter, r *http.Request) {
			policies, err := store.ListDeliveryPolicies(r.Context())
			if err != nil {
				logger.Error("list delivery policies failed", "error", err)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load delivery policies"})
				return
			}
			httpx.WriteJSON(w, http.StatusOK, map[string]any{"delivery_policies": policies})
		}))

		mux.HandleFunc("GET /v1/delivery-policies/{channel}", protect(authRoleRead, "delivery_policy", "get", func(w http.ResponseWriter, r *http.Request) {
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
		}))

		mux.HandleFunc("POST /v1/delivery-policies", protect(authRoleAdmin, "delivery_policy", "create", func(w http.ResponseWriter, r *http.Request) {
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
		}))

		mux.HandleFunc("GET /v1/webhook-subscriptions", protect(authRoleRead, "webhook_subscription", "list", func(w http.ResponseWriter, r *http.Request) {
			subscriptions, err := store.ListWebhookSubscriptions(r.Context())
			if err != nil {
				logger.Error("list webhook subscriptions failed", "error", err)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load webhook subscriptions"})
				return
			}
			httpx.WriteJSON(w, http.StatusOK, map[string]any{"webhook_subscriptions": subscriptions})
		}))

		mux.HandleFunc("GET /v1/webhook-subscriptions/{subscriptionID}", protect(authRoleRead, "webhook_subscription", "get", func(w http.ResponseWriter, r *http.Request) {
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
		}))

		mux.HandleFunc("POST /v1/webhook-subscriptions", protect(authRoleAdmin, "webhook_subscription", "create", func(w http.ResponseWriter, r *http.Request) {
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
		}))

		mux.HandleFunc("GET /v1/channel-events", protect(authRoleRead, "channel_event", "list", func(w http.ResponseWriter, r *http.Request) {
			providerKey := strings.TrimSpace(r.URL.Query().Get("provider_key"))
			channel := notification.Channel(strings.TrimSpace(r.URL.Query().Get("channel")))
			limit := 100
			if rawLimit := strings.TrimSpace(r.URL.Query().Get("limit")); rawLimit != "" {
				parsed, err := strconv.Atoi(rawLimit)
				if err != nil || parsed < 1 {
					httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "limit must be a positive integer"})
					return
				}
				if parsed > 500 {
					parsed = 500
				}
				limit = parsed
			}

			events, err := store.ListChannelEvents(r.Context(), providerKey, channel, limit)
			if err != nil {
				logger.Error("list channel events failed", "error", err, "provider_key", providerKey, "channel", channel)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load channel events"})
				return
			}
			httpx.WriteJSON(w, http.StatusOK, map[string]any{"channel_events": events})
		}))

		mux.HandleFunc("GET /v1/notification-requests/{requestID}", protect(authRoleRead, "notification_request", "get", func(w http.ResponseWriter, r *http.Request) {
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

			scheduledRetries, err := store.ListScheduledRetries(r.Context(), requestID)
			if err != nil {
				logger.Error("list scheduled retries failed", "error", err, "request_id", requestID)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load scheduled retries"})
				return
			}

			deadLetters, err := store.ListDeadLetterNotifications(r.Context(), requestID)
			if err != nil {
				logger.Error("list dead letters failed", "error", err, "request_id", requestID)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load dead letters"})
				return
			}

			httpx.WriteJSON(w, http.StatusOK, map[string]any{
				"request":                   record,
				"delivery_attempts":         attempts,
				"scheduled_retries":         scheduledRetries,
				"dead_letters":              deadLetters,
				"webhook_delivery_attempts": webhookAttempts,
			})
		}))

		mux.HandleFunc("GET /v1/dead-letters", protect(authRoleRead, "dead_letter", "list", func(w http.ResponseWriter, r *http.Request) {
			deadLetters, err := store.ListDeadLetterNotifications(r.Context(), "")
			if err != nil {
				registry.IncCounter("dead_letter_api_events_total", "Dead-letter API outcomes.", map[string]string{"action": "list", "outcome": "load_failed"})
				logger.Error("list dead letters failed", "error", err)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load dead letters"})
				return
			}
			registry.IncCounter("dead_letter_api_events_total", "Dead-letter API outcomes.", map[string]string{"action": "list", "outcome": "ok"})
			httpx.WriteJSON(w, http.StatusOK, map[string]any{"dead_letters": deadLetters})
		}))

		mux.HandleFunc("GET /v1/dead-letters/{deadLetterID}", protect(authRoleRead, "dead_letter", "get", func(w http.ResponseWriter, r *http.Request) {
			deadLetterID := r.PathValue("deadLetterID")
			deadLetter, err := store.GetDeadLetterNotificationByID(r.Context(), deadLetterID)
			if errors.Is(err, postgres.ErrNotFound) {
				registry.IncCounter("dead_letter_api_events_total", "Dead-letter API outcomes.", map[string]string{"action": "get", "outcome": "not_found"})
				httpx.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "dead letter not found"})
				return
			}
			if err != nil {
				registry.IncCounter("dead_letter_api_events_total", "Dead-letter API outcomes.", map[string]string{"action": "get", "outcome": "load_failed"})
				logger.Error("get dead letter failed", "error", err, "dead_letter_id", deadLetterID)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load dead letter"})
				return
			}
			registry.IncCounter("dead_letter_api_events_total", "Dead-letter API outcomes.", map[string]string{"action": "get", "outcome": "ok"})
			httpx.WriteJSON(w, http.StatusOK, deadLetter)
		}))

		mux.HandleFunc("POST /v1/dead-letters/{deadLetterID}/replay", protect(authRoleAdmin, "dead_letter", "replay", func(w http.ResponseWriter, r *http.Request) {
			deadLetterID := r.PathValue("deadLetterID")
			deadLetter, err := store.GetDeadLetterNotificationByID(r.Context(), deadLetterID)
			if errors.Is(err, postgres.ErrNotFound) {
				registry.IncCounter("dead_letter_replay_api_events_total", "Dead-letter replay API outcomes.", map[string]string{"outcome": "dead_letter_not_found"})
				httpx.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "dead letter not found"})
				return
			}
			if err != nil {
				registry.IncCounter("dead_letter_replay_api_events_total", "Dead-letter replay API outcomes.", map[string]string{"outcome": "dead_letter_load_failed"})
				logger.Error("get dead letter for replay failed", "error", err, "dead_letter_id", deadLetterID)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load dead letter"})
				return
			}

			original, err := store.GetNotificationRequest(r.Context(), deadLetter.RequestID)
			if errors.Is(err, postgres.ErrNotFound) {
				registry.IncCounter("dead_letter_replay_api_events_total", "Dead-letter replay API outcomes.", map[string]string{"outcome": "original_request_not_found"})
				httpx.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "original notification request not found"})
				return
			}
			if err != nil {
				registry.IncCounter("dead_letter_replay_api_events_total", "Dead-letter replay API outcomes.", map[string]string{"outcome": "original_request_load_failed"})
				logger.Error("get original notification request for replay failed", "error", err, "request_id", deadLetter.RequestID)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load original notification request"})
				return
			}

			now := time.Now().UTC()
			replayRecord := original
			replayRecord.RequestID = id.New(12)
			replayRecord.IdempotencyKey = fmt.Sprintf("%s-replay-%d", original.IdempotencyKey, now.UnixNano())
			replayRecord.Status = notification.RequestStatusAccepted
			replayRecord.RequestedAt = now
			replayRecord.CreatedAt = time.Time{}
			replayRecord.UpdatedAt = time.Time{}
			if replayRecord.Metadata == nil {
				replayRecord.Metadata = map[string]string{}
			}
			replayRecord.Metadata["replay_of_request_id"] = original.RequestID
			replayRecord.Metadata["replay_of_dead_letter_id"] = deadLetter.DeadLetterID

			if err := store.CreateNotificationRequest(r.Context(), replayRecord); err != nil {
				registry.IncCounter("dead_letter_replay_api_events_total", "Dead-letter replay API outcomes.", map[string]string{"outcome": "create_failed"})
				logger.Error("create replay notification request failed", "error", err, "dead_letter_id", deadLetterID)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "create replay notification request"})
				return
			}

			if err := publisher.PublishJSON(r.Context(), replayRecord.RequestID, notification.DeliveryPlan{Request: replayRecord}); err != nil {
				registry.IncCounter("dead_letter_replay_api_events_total", "Dead-letter replay API outcomes.", map[string]string{"outcome": "enqueue_failed"})
				logger.Error("publish replay notification request failed", "error", err, "request_id", replayRecord.RequestID, "dead_letter_id", deadLetterID)
				_ = store.UpdateNotificationRequestStatus(r.Context(), replayRecord.RequestID, notification.RequestStatusFailed)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "enqueue replay notification request"})
				return
			}

			if err := store.MarkDeadLetterReplayed(r.Context(), deadLetterID, replayRecord.RequestID, now); err != nil {
				registry.IncCounter("dead_letter_replay_api_events_total", "Dead-letter replay API outcomes.", map[string]string{"outcome": "mark_replayed_failed"})
				logger.Error("mark dead letter replayed failed", "error", err, "dead_letter_id", deadLetterID, "replay_request_id", replayRecord.RequestID)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "mark dead letter replayed"})
				return
			}

			if err := notifier.NotifyRequestUpdated(r.Context(), replayRecord.RequestID, map[string]interface{}{"source": "api", "replay": true, "dead_letter_id": deadLetterID}); err != nil {
				logger.Error("notify replay accepted lifecycle webhook failed", "error", err, "request_id", replayRecord.RequestID)
			}

			registry.IncCounter("dead_letter_replay_api_events_total", "Dead-letter replay API outcomes.", map[string]string{"outcome": "accepted"})
			httpx.WriteJSON(w, http.StatusAccepted, notification.NotificationAccepted{
				RequestID:  replayRecord.RequestID,
				Status:     notification.RequestStatusAccepted,
				AcceptedAt: now,
			})
		}))

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

func sameNotificationIntent(existing notification.NotificationRecord, incoming notification.NotificationRequest, caller notification.NotificationClient) bool {
	sourceMatches := true
	if existing.SourceClientID != "" || existing.SourceTenantID != "" || existing.SourceClientName != "" {
		sourceMatches = existing.SourceClientID == caller.ClientID &&
			existing.SourceTenantID == caller.TenantID &&
			existing.SourceClientName == caller.ClientName
	}

	return existing.EventName == incoming.EventName &&
		existing.TemplateKey == incoming.TemplateKey &&
		notification.NormalizeLanguageCode(existing.LanguageCode) == notification.NormalizeLanguageCode(incoming.LanguageCode) &&
		reflect.DeepEqual(existing.Channels, incoming.Channels) &&
		existing.BindingSet == incoming.BindingSet &&
		reflect.DeepEqual(existing.Recipient, incoming.Recipient) &&
		reflect.DeepEqual(existing.Variables, incoming.Variables) &&
		reflect.DeepEqual(existing.Metadata, incoming.Metadata) &&
		existing.Priority == incoming.Priority &&
		sourceMatches &&
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

func authenticateNotificationRequest(r *http.Request, store *postgres.Store, authRequired bool) (notification.NotificationClient, error) {
	apiKey := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
	if apiKey == "" {
		apiKey = strings.TrimSpace(r.Header.Get("X-Notification-Api-Key"))
	}
	if apiKey == "" {
		if authRequired {
			return notification.NotificationClient{}, fmt.Errorf("missing notification client api key")
		}
		return notification.NotificationClient{
			ClientID:        "anonymous",
			TenantID:        "anonymous",
			ClientName:      "anonymous",
			Enabled:         true,
			AllowedChannels: []notification.Channel{notification.ChannelEmail, notification.ChannelSMS, notification.ChannelWhatsApp, notification.ChannelPush, notification.ChannelWebhook},
		}, nil
	}

	client, err := store.GetNotificationClientByAPIKeyHash(r.Context(), hashAPIKey(apiKey))
	if errors.Is(err, postgres.ErrNotFound) {
		return notification.NotificationClient{}, fmt.Errorf("invalid notification client api key")
	}
	if err != nil {
		return notification.NotificationClient{}, err
	}
	if !client.Enabled {
		return notification.NotificationClient{}, fmt.Errorf("notification client disabled")
	}
	return client, nil
}

type apiRateLimitConfig struct {
	Enabled         bool
	AnonymousRPS    float64
	AnonymousBurst  int
	ClientRPS       float64
	ClientBurst     int
	ReadRPS         float64
	ReadBurst       int
	AdminRPS        float64
	AdminBurst      int
	CleanupInterval time.Duration
	EntryTTL        time.Duration
}

func loadAPIRateLimitConfig() apiRateLimitConfig {
	return apiRateLimitConfig{
		Enabled:         config.GetEnv("NOTIFICATION_RATE_LIMIT_ENABLED", "false") == "true",
		AnonymousRPS:    parseEnvFloat("NOTIFICATION_RATE_LIMIT_ANON_RPS", 2),
		AnonymousBurst:  parseEnvInt("NOTIFICATION_RATE_LIMIT_ANON_BURST", 4),
		ClientRPS:       parseEnvFloat("NOTIFICATION_RATE_LIMIT_CLIENT_RPS", 10),
		ClientBurst:     parseEnvInt("NOTIFICATION_RATE_LIMIT_CLIENT_BURST", 20),
		ReadRPS:         parseEnvFloat("NOTIFICATION_RATE_LIMIT_READ_RPS", 15),
		ReadBurst:       parseEnvInt("NOTIFICATION_RATE_LIMIT_READ_BURST", 30),
		AdminRPS:        parseEnvFloat("NOTIFICATION_RATE_LIMIT_ADMIN_RPS", 10),
		AdminBurst:      parseEnvInt("NOTIFICATION_RATE_LIMIT_ADMIN_BURST", 20),
		CleanupInterval: parseEnvDuration("NOTIFICATION_RATE_LIMIT_CLEANUP_INTERVAL", 5*time.Minute),
		EntryTTL:        parseEnvDuration("NOTIFICATION_RATE_LIMIT_ENTRY_TTL", 15*time.Minute),
	}
}

func parseEnvFloat(key string, fallback float64) float64 {
	value := config.GetEnv(key, "")
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

func parseEnvInt(key string, fallback int) int {
	value := config.GetEnv(key, "")
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func parseEnvDuration(key string, fallback time.Duration) time.Duration {
	value := config.GetEnv(key, "")
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func rateLimitKey(prefix string, subject string) string {
	return prefix + ":" + subject
}

func requestIP(r *http.Request) string {
	if forwarded := strings.TrimSpace(r.Header.Get("X-Forwarded-For")); forwarded != "" {
		parts := strings.Split(forwarded, ",")
		if len(parts) > 0 {
			if ip := strings.TrimSpace(parts[0]); ip != "" {
				return ip
			}
		}
	}
	if realIP := strings.TrimSpace(r.Header.Get("X-Real-IP")); realIP != "" {
		return realIP
	}
	host := r.RemoteAddr
	if idx := strings.LastIndex(host, ":"); idx > -1 {
		return strings.TrimSpace(host[:idx])
	}
	return strings.TrimSpace(host)
}

func allowAnonymousRateLimit(w http.ResponseWriter, r *http.Request, registry *metrics.Registry, limiter *ratelimit.Manager, rps float64, burst int, resource string, action string) bool {
	subject := requestIP(r)
	if subject == "" {
		subject = "anonymous"
	}
	if limiter.Allow(rateLimitKey("anon", subject), rate.Limit(rps), burst) {
		return true
	}
	recordAdminAPIEvent(registry, resource, action, "rate_limited")
	w.Header().Set("Retry-After", "1")
	httpx.WriteJSON(w, http.StatusTooManyRequests, map[string]string{"error": "rate limit exceeded"})
	return false
}

func allowRoleRateLimit(w http.ResponseWriter, r *http.Request, registry *metrics.Registry, limiter *ratelimit.Manager, role string, rps float64, burst int, resource string, action string) bool {
	subject := requestIP(r)
	if subject == "" {
		subject = "anonymous"
	}
	if limiter.Allow(rateLimitKey(role, subject), rate.Limit(rps), burst) {
		return true
	}
	recordAdminAPIEvent(registry, resource, action, "rate_limited")
	w.Header().Set("Retry-After", "1")
	httpx.WriteJSON(w, http.StatusTooManyRequests, map[string]string{"error": "rate limit exceeded"})
	return false
}

type authRole string

const (
	authRoleRead  authRole = "read"
	authRoleAdmin authRole = "admin"
)

func authenticateControlPlaneRequest(r *http.Request, adminToken string, readToken string, authRequired bool) (authRole, error) {
	if adminToken != "" {
		if got := strings.TrimSpace(r.Header.Get("X-Notification-Admin-Token")); got != "" {
			if subtle.ConstantTimeCompare([]byte(got), []byte(adminToken)) == 1 {
				return authRoleAdmin, nil
			}
		}
	}
	if readToken != "" {
		if got := strings.TrimSpace(r.Header.Get("X-Notification-Read-Token")); got != "" {
			if subtle.ConstantTimeCompare([]byte(got), []byte(readToken)) == 1 {
				return authRoleRead, nil
			}
		}
	}
	if !authRequired {
		return authRoleAdmin, nil
	}
	if adminToken == "" && readToken == "" {
		return "", fmt.Errorf("admin api auth not configured")
	}
	return "", fmt.Errorf("invalid control plane api token")
}

func generateNotificationClientAPIKey() (string, string) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		panic(err)
	}
	apiKey := base64.RawURLEncoding.EncodeToString(raw[:])
	return apiKey, hashAPIKey(apiKey)
}

func hashAPIKey(apiKey string) string {
	sum := sha256.Sum256([]byte(apiKey))
	return fmt.Sprintf("%x", sum[:])
}

func clientAllowsChannels(client notification.NotificationClient, requested []notification.Channel) bool {
	if client.ClientID == "" || len(client.AllowedChannels) == 0 {
		return true
	}
	allowed := make(map[notification.Channel]struct{}, len(client.AllowedChannels))
	for _, channel := range client.AllowedChannels {
		allowed[channel] = struct{}{}
	}
	for _, channel := range requested {
		if _, ok := allowed[channel]; !ok {
			return false
		}
	}
	return true
}

func recordAdminAPIEvent(registry *metrics.Registry, resource string, action string, outcome string) {
	if registry == nil {
		return
	}
	registry.IncCounter("admin_api_events_total", "Administrative API outcomes for clients, provider accounts, callback routes, and bindings.", map[string]string{
		"resource": resource,
		"action":   action,
		"outcome":  outcome,
	})
}
