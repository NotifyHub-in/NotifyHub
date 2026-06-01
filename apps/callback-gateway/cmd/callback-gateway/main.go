package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Arunshaik2001/notification-control-plane/libs/contracts/notification"
	"github.com/Arunshaik2001/notification-control-plane/libs/core/app"
	"github.com/Arunshaik2001/notification-control-plane/libs/core/config"
	"github.com/Arunshaik2001/notification-control-plane/libs/core/httpx"
	"github.com/Arunshaik2001/notification-control-plane/libs/core/secrets"
	"github.com/Arunshaik2001/notification-control-plane/libs/core/serviceinfo"
	"github.com/Arunshaik2001/notification-control-plane/libs/core/webhooks"
	"github.com/Arunshaik2001/notification-control-plane/libs/observability/logging"
	"github.com/Arunshaik2001/notification-control-plane/libs/observability/metrics"
	"github.com/Arunshaik2001/notification-control-plane/libs/storage/postgres"
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
	postgres.AttachMetrics(registry)
	notifier := webhooks.NewNotifier(store)

	err = app.RunHTTPService(cfg, logger, registry, func(mux *http.ServeMux, info serviceinfo.Info, registry *metrics.Registry) {
		handleProviderCallback := func(w http.ResponseWriter, r *http.Request) {
			path := strings.TrimPrefix(r.URL.Path, "/v1/providers/")
			parts := strings.Split(strings.Trim(path, "/"), "/")
			if len(parts) != 2 || parts[1] != "callbacks" || parts[0] == "" {
				http.NotFound(w, r)
				return
			}

			rawBody, err := io.ReadAll(r.Body)
			if err != nil {
				registry.IncCounter("provider_callbacks_total", "Inbound provider callback outcomes.", map[string]string{"provider": parts[0], "outcome": "read_failed"})
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "read callback payload"})
				return
			}

			def, providerKnown := notification.ProviderDefinitionByKey(parts[0])
			if providerKnown && def.CallbackMode == "none" {
				registry.IncCounter("provider_callbacks_total", "Inbound provider callback outcomes.", map[string]string{"provider": parts[0], "outcome": "unsupported"})
				httpx.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "provider callbacks are not supported"})
				return
			}

			route, routeErr := store.GetCallbackRouteByProviderKey(r.Context(), parts[0])
			if providerKnown && def.CallbackMode != "none" {
				if routeErr != nil {
					registry.IncCounter("provider_callbacks_total", "Inbound provider callback outcomes.", map[string]string{"provider": parts[0], "outcome": "route_missing"})
					httpx.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "callback route not configured"})
					return
				}
				if !route.Enabled {
					registry.IncCounter("provider_callbacks_total", "Inbound provider callback outcomes.", map[string]string{"provider": parts[0], "outcome": "route_disabled"})
					httpx.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "callback route not configured"})
					return
				}
			}

			if routeErr == nil && route.Enabled {
				if ok, verifyErr := verifyCallbackRequest(r, rawBody, route); verifyErr != nil {
					registry.IncCounter("provider_callbacks_total", "Inbound provider callback outcomes.", map[string]string{"provider": parts[0], "outcome": "verification_failed"})
					logger.Error("verify provider callback failed", "error", verifyErr, "provider", parts[0], "provider_account_id", route.ProviderAccountID)
					httpx.WriteJSON(w, http.StatusUnauthorized, map[string]string{"error": "callback verification failed"})
					return
				} else if !ok {
					registry.IncCounter("provider_callbacks_total", "Inbound provider callback outcomes.", map[string]string{"provider": parts[0], "outcome": "verification_failed"})
					httpx.WriteJSON(w, http.StatusUnauthorized, map[string]string{"error": "callback verification failed"})
					return
				}
			}

			callbacks, decodeErr := decodeProviderCallbacks(parts[0], r.Method, r.URL.Query(), rawBody)
			if decodeErr != nil {
				registry.IncCounter("provider_callbacks_total", "Inbound provider callback outcomes.", map[string]string{"provider": parts[0], "outcome": "invalid_payload"})
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": decodeErr.Error()})
				return
			}
			if len(callbacks) == 0 {
				registry.IncCounter("provider_callbacks_total", "Inbound provider callback outcomes.", map[string]string{"provider": parts[0], "outcome": "validation_failed"})
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "provider_message_id and status are required"})
				return
			}

			responses := make([]map[string]any, 0, len(callbacks))
			for _, payload := range callbacks {
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

				record, err := store.GetNotificationRequest(r.Context(), attempt.RequestID)
				if err == nil {
					registry.ObserveHistogram("notification_end_to_end_seconds", "End-to-end notification latency in seconds from API acceptance to observed terminal or dispatched state.", map[string]string{
						"stage":        "callback",
						"final_status": string(requestStatus),
						"provider":     parts[0],
					}, metrics.DefaultLatencyBuckets(), time.Since(record.RequestedAt).Seconds())
				}

				if err := notifier.NotifyRequestUpdated(r.Context(), attempt.RequestID, map[string]interface{}{
					"source":              "callback-gateway",
					"provider":            parts[0],
					"provider_message_id": payload.ProviderMessageID,
				}); err != nil {
					logger.Error("notify lifecycle webhook failed", "error", err, "request_id", attempt.RequestID)
				}

				registry.IncCounter("delivery_status_updates_total", "Delivery status updates normalized from provider callbacks.", map[string]string{
					"channel":   string(attempt.Channel),
					"connector": attempt.ConnectorName,
					"provider":  parts[0],
					"status":    string(attempt.Status),
				})
				registry.IncCounter("provider_callbacks_total", "Inbound provider callback outcomes.", map[string]string{
					"provider":          parts[0],
					"outcome":           "accepted",
					"normalized_status": string(attempt.Status),
				})
				responses = append(responses, map[string]any{
					"request_id":          attempt.RequestID,
					"attempt_id":          attempt.AttemptID,
					"normalized_status":   attempt.Status,
					"provider_message_id": payload.ProviderMessageID,
					"request_status":      requestStatus,
				})
			}

			httpx.WriteJSON(w, http.StatusAccepted, map[string]any{
				"service":     info.Name,
				"provider":    parts[0],
				"received_at": time.Now().UTC(),
				"callbacks":   responses,
			})
		}

		mux.HandleFunc("GET /v1/providers/", handleProviderCallback)
		mux.HandleFunc("POST /v1/providers/", handleProviderCallback)
	})
	if err != nil {
		panic(err)
	}
}

func verifyCallbackRequest(r *http.Request, rawBody []byte, route notification.CallbackRoute) (bool, error) {
	if route.VerificationMode == "" || route.VerificationMode == notification.CallbackVerificationModeNone {
		return true, nil
	}

	secret, err := secrets.Resolve(route.VerificationSecretRef)
	if err != nil {
		return false, err
	}

	switch route.VerificationMode {
	case notification.CallbackVerificationModeSharedSecret:
		return r.Header.Get("X-Provider-Secret") == secret, nil
	case notification.CallbackVerificationModeHMACSHA256:
		signature := strings.TrimSpace(r.Header.Get("X-Provider-Signature"))
		if signature == "" {
			return false, nil
		}
		mac := hmac.New(sha256.New, []byte(secret))
		_, _ = mac.Write(rawBody)
		expected := hex.EncodeToString(mac.Sum(nil))
		return hmac.Equal([]byte(signature), []byte(expected)), nil
	default:
		return false, fmt.Errorf("unsupported verification mode %q", route.VerificationMode)
	}
}

func decodeProviderCallbacks(providerKey, method string, query url.Values, rawBody []byte) ([]notification.ProviderCallback, error) {
	switch providerKey {
	case "gupshup-whatsapp":
		if method != http.MethodPost {
			return nil, fmt.Errorf("gupshup whatsapp callbacks must use POST")
		}
		return decodeGupshupWhatsAppCallbacks(rawBody)
	case "karix-whatsapp":
		if method != http.MethodPost {
			return nil, fmt.Errorf("karix whatsapp callbacks must use POST")
		}
		return decodeKarixWhatsAppCallback(rawBody)
	case "gupshup-sms":
		if method != http.MethodPost {
			return nil, fmt.Errorf("gupshup sms callbacks must use POST")
		}
		return decodeGupshupSMSCallbacks(rawBody)
	case "karix-sms":
		if method != http.MethodGet {
			return nil, fmt.Errorf("karix sms callbacks must use GET")
		}
		return decodeKarixSMSCallback(query)
	default:
		var payload notification.ProviderCallback
		if err := json.Unmarshal(rawBody, &payload); err != nil {
			return nil, fmt.Errorf("invalid callback payload")
		}
		if payload.ProviderMessageID == "" || payload.Status == "" {
			return nil, fmt.Errorf("provider_message_id and status are required")
		}
		return []notification.ProviderCallback{payload}, nil
	}
}

func decodeGupshupWhatsAppCallbacks(rawBody []byte) ([]notification.ProviderCallback, error) {
	var batch []notification.GupshupWhatsAppCallbackRequest
	if err := json.Unmarshal(rawBody, &batch); err == nil {
		return normalizeGupshupWhatsAppCallbacks(batch)
	}

	var single notification.GupshupWhatsAppCallbackRequest
	if err := json.Unmarshal(rawBody, &single); err != nil {
		return nil, fmt.Errorf("invalid gupshup whatsapp callback payload")
	}
	return normalizeGupshupWhatsAppCallbacks([]notification.GupshupWhatsAppCallbackRequest{single})
}

func normalizeGupshupWhatsAppCallbacks(batch []notification.GupshupWhatsAppCallbackRequest) ([]notification.ProviderCallback, error) {
	callbacks := make([]notification.ProviderCallback, 0, len(batch))
	for _, item := range batch {
		if item.ExternalID == "" || item.EventType == "" {
			return nil, fmt.Errorf("externalId and eventType are required")
		}
		callbacks = append(callbacks, notification.ProviderCallback{
			ProviderMessageID: item.ExternalID,
			Status:            normalizeGupshupEventType(item.EventType),
			ErrorCode:         firstNonEmpty(item.ErrorCode, item.Cause),
			Metadata: map[string]string{
				"event_type": item.EventType,
				"cause":      item.Cause,
				"dest_addr":  item.DestAddr,
				"src_addr":   item.SrcAddr,
				"channel":    item.Channel,
			},
		})
	}
	return callbacks, nil
}

func normalizeGupshupEventType(eventType string) string {
	switch strings.ToUpper(strings.TrimSpace(eventType)) {
	case "DELIVERED", "READ":
		return "delivered"
	case "SENT":
		return "accepted"
	default:
		return "failed"
	}
}

func decodeKarixWhatsAppCallback(rawBody []byte) ([]notification.ProviderCallback, error) {
	var payload notification.KarixWhatsAppCallbackRequest
	if err := json.Unmarshal(rawBody, &payload); err != nil {
		return nil, fmt.Errorf("invalid karix whatsapp callback payload")
	}
	if payload.Events.MID == "" || payload.NotificationAttributes.Status == "" {
		return nil, fmt.Errorf("events.mid and notificationAttributes.status are required")
	}

	return []notification.ProviderCallback{{
		ProviderMessageID: payload.Events.MID,
		Status:            normalizeKarixStatus(payload.NotificationAttributes.Status),
		ErrorCode:         firstNonEmpty(payload.NotificationAttributes.Code, payload.NotificationAttributes.Reason),
		Metadata: map[string]string{
			"event_type": payload.Events.EventType,
			"reason":     payload.NotificationAttributes.Reason,
			"code":       payload.NotificationAttributes.Code,
			"channel":    payload.Channel,
		},
	}}, nil
}

func normalizeKarixStatus(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "delivered", "read":
		return "delivered"
	case "sent", "accepted", "queued", "processing":
		return "accepted"
	default:
		return "failed"
	}
}

func decodeGupshupSMSCallbacks(rawBody []byte) ([]notification.ProviderCallback, error) {
	var payload notification.SmsCallbackRequest
	if err := json.Unmarshal(rawBody, &payload); err != nil {
		return nil, fmt.Errorf("invalid gupshup sms callback payload")
	}
	if len(payload.Response) == 0 {
		return nil, fmt.Errorf("response is required")
	}

	callbacks := make([]notification.ProviderCallback, 0, len(payload.Response))
	for _, item := range payload.Response {
		if item.ExternalID == "" || item.EventType == "" {
			return nil, fmt.Errorf("externalId and eventType are required")
		}
		callbacks = append(callbacks, notification.ProviderCallback{
			ProviderMessageID: item.ExternalID,
			Status:            normalizeGupshupSMSStatus(item.EventType),
			ErrorCode:         firstNonEmpty(item.ErrorCode, item.Cause),
			Metadata: map[string]string{
				"event_type": item.EventType,
				"cause":      item.Cause,
				"dest_addr":  item.DestAddr,
				"src_addr":   item.SrcAddr,
				"channel":    item.Channel,
			},
		})
	}
	return callbacks, nil
}

func normalizeGupshupSMSStatus(eventType string) string {
	switch strings.ToUpper(strings.TrimSpace(eventType)) {
	case "DELIVERED":
		return "delivered"
	case "SENT", "SUBMITTED":
		return "accepted"
	default:
		return "failed"
	}
}

func decodeKarixSMSCallback(query url.Values) ([]notification.ProviderCallback, error) {
	messageID := strings.TrimSpace(query.Get("sid"))
	dest := strings.TrimSpace(query.Get("dest"))
	stime := strings.TrimSpace(query.Get("stime"))
	status := strings.TrimSpace(query.Get("status"))
	reason := strings.TrimSpace(query.Get("reason"))
	if messageID == "" || dest == "" || stime == "" || status == "" || reason == "" {
		return nil, fmt.Errorf("sid, dest, stime, status, and reason are required")
	}

	normalizedStatus := "failed"
	switch strings.ToLower(reason) {
	case "delivrd":
		normalizedStatus = "delivered"
	case "sent", "submitted", "acceptd":
		normalizedStatus = "accepted"
	}

	return []notification.ProviderCallback{{
		ProviderMessageID: messageID,
		Status:            normalizedStatus,
		ErrorCode:         status,
		Metadata: map[string]string{
			"dest":   dest,
			"stime":  stime,
			"dtime":  strings.TrimSpace(query.Get("dtime")),
			"reason": reason,
		},
	}}, nil
}

func normalizeAttemptStatus(status string) notification.DeliveryAttemptStatus {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "delivered", "success":
		return notification.DeliveryAttemptDelivered
	case "accepted", "queued", "processing", "sent":
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

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func recomputeRequestStatus(ctx context.Context, store *postgres.Store, requestID string) (notification.RequestStatus, error) {
	attempts, err := store.ListDeliveryAttempts(ctx, requestID)
	if err != nil {
		return "", err
	}

	var (
		hasDelivered   bool
		hasAccepted    bool
		hasExpired     bool
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
		case notification.DeliveryAttemptExpired:
			hasExpired = true
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
	case hasExpired:
		return notification.RequestStatusExpired, nil
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
