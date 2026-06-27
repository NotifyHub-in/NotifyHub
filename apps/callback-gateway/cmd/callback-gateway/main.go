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
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/NotifyHub-in/NotifyHub/libs/contracts/notification"
	"github.com/NotifyHub-in/NotifyHub/libs/core/app"
	"github.com/NotifyHub-in/NotifyHub/libs/core/config"
	"github.com/NotifyHub-in/NotifyHub/libs/core/httpx"
	"github.com/NotifyHub-in/NotifyHub/libs/core/id"
	"github.com/NotifyHub-in/NotifyHub/libs/core/secrets"
	"github.com/NotifyHub-in/NotifyHub/libs/core/serviceinfo"
	"github.com/NotifyHub-in/NotifyHub/libs/core/webhooks"
	"github.com/NotifyHub-in/NotifyHub/libs/observability/logging"
	"github.com/NotifyHub-in/NotifyHub/libs/observability/metrics"
	"github.com/NotifyHub-in/NotifyHub/libs/storage/postgres"
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
			if len(parts) < 2 || parts[len(parts)-1] != "callbacks" || parts[0] == "" {
				http.NotFound(w, r)
				return
			}
			providerKey := parts[0]
			providerAccountID := ""
			if len(parts) == 3 {
				providerAccountID = parts[1]
			} else if len(parts) != 2 {
				http.NotFound(w, r)
				return
			}

			rawBody, err := io.ReadAll(r.Body)
			if err != nil {
				registry.IncCounter("provider_callbacks_total", "Inbound provider callback outcomes.", map[string]string{"provider": providerKey, "outcome": "read_failed"})
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "read callback payload"})
				return
			}

			def, providerKnown := notification.ProviderDefinitionByKey(providerKey)
			var (
				route    notification.CallbackRoute
				routeErr error
			)
			if providerAccountID != "" {
				route, routeErr = store.GetCallbackRouteByProviderKeyAndAccountID(r.Context(), providerKey, providerAccountID)
			} else {
				route, routeErr = store.GetCallbackRouteByProviderKey(r.Context(), providerKey)
			}
			if errors.Is(routeErr, postgres.ErrNotFound) {
				registry.IncCounter("provider_callbacks_total", "Inbound provider callback outcomes.", map[string]string{"provider": providerKey, "outcome": "route_missing"})
				httpx.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "callback route not configured"})
				return
			}
			if errors.Is(routeErr, postgres.ErrConflict) {
				registry.IncCounter("provider_callbacks_total", "Inbound provider callback outcomes.", map[string]string{"provider": providerKey, "outcome": "route_ambiguous"})
				httpx.WriteJSON(w, http.StatusConflict, map[string]string{"error": "multiple callback routes exist for provider_key; use an account-specific callback path"})
				return
			}
			if routeErr != nil {
				registry.IncCounter("provider_callbacks_total", "Inbound provider callback outcomes.", map[string]string{"provider": providerKey, "outcome": "lookup_failed"})
				logger.Error("load callback route failed", "error", routeErr, "provider", providerKey, "provider_account_id", providerAccountID)
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "load callback route"})
				return
			}
			if !route.Enabled {
				registry.IncCounter("provider_callbacks_total", "Inbound provider callback outcomes.", map[string]string{"provider": providerKey, "outcome": "route_disabled"})
				httpx.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "callback route not configured"})
				return
			}

			if ok, verifyErr := verifyCallbackRequest(r, rawBody, route, callbackVerificationSpecFor(def, providerKnown)); verifyErr != nil {
				registry.IncCounter("provider_callbacks_total", "Inbound provider callback outcomes.", map[string]string{"provider": providerKey, "outcome": "verification_failed"})
				logger.Error("verify provider callback failed", "error", verifyErr, "provider", providerKey, "provider_account_id", route.ProviderAccountID)
				httpx.WriteJSON(w, http.StatusUnauthorized, map[string]string{"error": "callback verification failed"})
				return
			} else if !ok {
				registry.IncCounter("provider_callbacks_total", "Inbound provider callback outcomes.", map[string]string{"provider": providerKey, "outcome": "verification_failed"})
				httpx.WriteJSON(w, http.StatusUnauthorized, map[string]string{"error": "callback verification failed"})
				return
			}

			statusCallbacks, statusErr := decodeProviderCallbacks(def, providerKnown, r.Method, r.URL.Query(), rawBody)
			if statusErr == nil && len(statusCallbacks) > 0 {
				if err := handleProviderStatusCallbacks(r.Context(), store, notifier, registry, logger, providerKey, statusCallbacks); err != nil {
					statusCode, message := providerCallbackErrorResponse(err)
					httpx.WriteJSON(w, statusCode, map[string]string{"error": message})
					return
				}
				httpx.WriteJSON(w, http.StatusAccepted, map[string]any{
					"service":     info.Name,
					"provider":    providerKey,
					"received_at": time.Now().UTC(),
					"callbacks":   statusCallbacks,
				})
				return
			}

			events, inboundErr := decodeProviderInboundEvents(def, providerKnown, r.Method, r.URL.Query(), rawBody)
			if inboundErr != nil || len(events) == 0 {
				message := ""
				switch {
				case statusErr != nil:
					message = statusErr.Error()
				case inboundErr != nil:
					message = inboundErr.Error()
				default:
					message = "invalid payload"
				}
				registry.IncCounter("provider_callbacks_total", "Inbound provider callback outcomes.", map[string]string{"provider": providerKey, "outcome": "invalid_payload"})
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": message})
				return
			}
			if err := handleProviderInboundEvents(r.Context(), store, notifier, registry, logger, providerKey, events); err != nil {
				statusCode, message := providerChannelEventErrorResponse(err)
				httpx.WriteJSON(w, statusCode, map[string]string{"error": message})
				return
			}
			httpx.WriteJSON(w, http.StatusAccepted, map[string]any{
				"service":     info.Name,
				"provider":    providerKey,
				"received_at": time.Now().UTC(),
				"events":      events,
			})
		}

		mux.HandleFunc("GET /v1/providers/", handleProviderCallback)
		mux.HandleFunc("POST /v1/providers/", handleProviderCallback)
	})
	if err != nil {
		panic(err)
	}
}

type providerCallbackHTTPError struct {
	statusCode int
	message    string
}

func (e *providerCallbackHTTPError) Error() string {
	return e.message
}

func providerCallbackErrorResponse(err error) (int, string) {
	var httpErr *providerCallbackHTTPError
	if errors.As(err, &httpErr) {
		return httpErr.statusCode, httpErr.message
	}
	return http.StatusInternalServerError, "process provider callback"
}

func providerChannelEventErrorResponse(err error) (int, string) {
	var httpErr *providerCallbackHTTPError
	if errors.As(err, &httpErr) {
		return httpErr.statusCode, httpErr.message
	}
	return http.StatusInternalServerError, "process inbound channel event"
}

func handleProviderStatusCallbacks(ctx context.Context, store *postgres.Store, notifier *webhooks.Notifier, registry *metrics.Registry, logger *slog.Logger, provider string, callbacks []notification.ProviderCallback) error {
	for _, payload := range callbacks {
		if payload.ProviderMessageID == "" || payload.Status == "" {
			registry.IncCounter("provider_callbacks_total", "Inbound provider callback outcomes.", map[string]string{"provider": provider, "outcome": "validation_failed"})
			return &providerCallbackHTTPError{statusCode: http.StatusBadRequest, message: "provider_message_id and status are required"}
		}

		attempt, err := store.GetDeliveryAttemptByProviderMessageID(ctx, payload.ProviderMessageID)
		if errors.Is(err, postgres.ErrNotFound) {
			registry.IncCounter("provider_callbacks_total", "Inbound provider callback outcomes.", map[string]string{"provider": provider, "outcome": "attempt_not_found"})
			return &providerCallbackHTTPError{statusCode: http.StatusNotFound, message: "delivery attempt not found for provider_message_id"}
		}
		if err != nil {
			registry.IncCounter("provider_callbacks_total", "Inbound provider callback outcomes.", map[string]string{"provider": provider, "outcome": "lookup_failed"})
			logger.Error("load delivery attempt by provider message id failed", "error", err, "provider_message_id", payload.ProviderMessageID)
			return &providerCallbackHTTPError{statusCode: http.StatusInternalServerError, message: "load delivery attempt"}
		}

		attempt.Status = normalizeAttemptStatus(payload.Status)
		attempt.ErrorMessage = normalizedErrorMessage(payload)
		if err := store.UpdateDeliveryAttempt(ctx, attempt); err != nil {
			registry.IncCounter("provider_callbacks_total", "Inbound provider callback outcomes.", map[string]string{"provider": provider, "outcome": "update_attempt_failed"})
			logger.Error("update delivery attempt from callback failed", "error", err, "attempt_id", attempt.AttemptID)
			return &providerCallbackHTTPError{statusCode: http.StatusInternalServerError, message: "update delivery attempt"}
		}

		requestStatus, err := recomputeRequestStatus(ctx, store, attempt.RequestID)
		if err != nil {
			registry.IncCounter("provider_callbacks_total", "Inbound provider callback outcomes.", map[string]string{"provider": provider, "outcome": "recompute_request_failed"})
			logger.Error("recompute request status failed", "error", err, "request_id", attempt.RequestID)
			return &providerCallbackHTTPError{statusCode: http.StatusInternalServerError, message: "recompute request status"}
		}
		if err := store.UpdateNotificationRequestStatus(ctx, attempt.RequestID, requestStatus); err != nil {
			registry.IncCounter("provider_callbacks_total", "Inbound provider callback outcomes.", map[string]string{"provider": provider, "outcome": "update_request_failed"})
			logger.Error("update request status from callback failed", "error", err, "request_id", attempt.RequestID)
			return &providerCallbackHTTPError{statusCode: http.StatusInternalServerError, message: "update request status"}
		}

		record, err := store.GetNotificationRequest(ctx, attempt.RequestID)
		if err == nil {
			registry.ObserveHistogram("notification_end_to_end_seconds", "End-to-end notification latency in seconds from API acceptance to observed terminal or dispatched state.", map[string]string{
				"stage":        "callback",
				"final_status": string(requestStatus),
				"provider":     provider,
			}, metrics.DefaultLatencyBuckets(), time.Since(record.RequestedAt).Seconds())
		}

		notifier.NotifyRequestUpdatedAsync(logger, attempt.RequestID, map[string]interface{}{
			"source":              "callback-gateway",
			"provider":            provider,
			"provider_message_id": payload.ProviderMessageID,
		})

		registry.IncCounter("delivery_status_updates_total", "Delivery status updates normalized from provider callbacks.", map[string]string{
			"channel":   string(attempt.Channel),
			"connector": attempt.ConnectorName,
			"provider":  provider,
			"status":    string(attempt.Status),
		})
		registry.IncCounter("provider_callbacks_total", "Inbound provider callback outcomes.", map[string]string{
			"provider":          provider,
			"outcome":           "accepted",
			"normalized_status": string(attempt.Status),
		})
	}
	return nil
}

func handleProviderInboundEvents(ctx context.Context, store *postgres.Store, notifier *webhooks.Notifier, registry *metrics.Registry, logger *slog.Logger, provider string, events []notification.ChannelEvent) error {
	for _, event := range events {
		event.ProviderKey = provider
		if event.Channel == "" {
			event.Channel = notification.ChannelWhatsApp
		}
		event.Direction = notification.ChannelEventDirectionInbound
		if event.Status == "" {
			event.Status = notification.ChannelEventStatusReceived
		}
		if event.EventID == "" {
			event.EventID = id.New(12)
		}
		if event.ReceivedAt.IsZero() {
			event.ReceivedAt = time.Now().UTC()
		}
		if err := store.UpsertChannelEvent(ctx, event); err != nil {
			registry.IncCounter("channel_events_total", "Inbound channel event outcomes.", map[string]string{"provider": provider, "outcome": "persist_failed"})
			logger.Error("persist inbound channel event failed", "error", err, "provider", provider, "event_type", event.EventType)
			return &providerCallbackHTTPError{statusCode: http.StatusInternalServerError, message: "store inbound channel event"}
		}
		registry.IncCounter("channel_events_total", "Inbound channel event outcomes.", map[string]string{
			"provider":   provider,
			"channel":    string(event.Channel),
			"event_type": event.EventType,
			"status":     string(event.Status),
			"outcome":    "stored",
		})
		notifier.NotifyChannelEventAsync(logger, event, map[string]any{
			"source":      "callback-gateway",
			"provider":    provider,
			"channel":     event.Channel,
			"event_type":  event.EventType,
			"external_id": event.ExternalMessageID,
			"reply_to_id": event.ReplyToMessageID,
		})
	}
	return nil
}

type callbackVerifier interface {
	Verify(r *http.Request, rawBody []byte, route notification.CallbackRoute, spec notification.CallbackVerificationSpec) (bool, error)
}

type callbackVerifierFunc func(r *http.Request, rawBody []byte, route notification.CallbackRoute, spec notification.CallbackVerificationSpec) (bool, error)

func (fn callbackVerifierFunc) Verify(r *http.Request, rawBody []byte, route notification.CallbackRoute, spec notification.CallbackVerificationSpec) (bool, error) {
	return fn(r, rawBody, route, spec)
}

var callbackVerifiers = map[notification.CallbackVerificationMode]callbackVerifier{
	notification.CallbackVerificationModeNone: callbackVerifierFunc(func(_ *http.Request, _ []byte, _ notification.CallbackRoute, _ notification.CallbackVerificationSpec) (bool, error) {
		return true, nil
	}),
	notification.CallbackVerificationModeSharedSecret: callbackVerifierFunc(func(r *http.Request, _ []byte, route notification.CallbackRoute, spec notification.CallbackVerificationSpec) (bool, error) {
		secret, err := secrets.Resolve(route.VerificationSecretRef)
		if err != nil {
			return false, err
		}
		headerName := spec.SecretHeader
		if headerName == "" {
			headerName = "X-Provider-Secret"
		}
		return r.Header.Get(headerName) == secret, nil
	}),
	notification.CallbackVerificationModeHMACSHA256: callbackVerifierFunc(func(r *http.Request, rawBody []byte, route notification.CallbackRoute, spec notification.CallbackVerificationSpec) (bool, error) {
		secret, err := secrets.Resolve(route.VerificationSecretRef)
		if err != nil {
			return false, err
		}
		headerName := spec.SignatureHeader
		if headerName == "" {
			headerName = "X-Provider-Signature"
		}
		signature := strings.TrimSpace(r.Header.Get(headerName))
		if signature == "" {
			return false, nil
		}
		mac := hmac.New(sha256.New, []byte(secret))
		_, _ = mac.Write(rawBody)
		expected := hex.EncodeToString(mac.Sum(nil))
		return hmac.Equal([]byte(signature), []byte(expected)), nil
	}),
}

func callbackVerificationSpecFor(def notification.ProviderDefinition, ok bool) notification.CallbackVerificationSpec {
	if ok && def.CallbackVerification != nil {
		return *def.CallbackVerification
	}
	return notification.CallbackVerificationSpec{
		SecretHeader:    "X-Provider-Secret",
		SignatureHeader: "X-Provider-Signature",
	}
}

func verifyCallbackRequest(r *http.Request, rawBody []byte, route notification.CallbackRoute, spec notification.CallbackVerificationSpec) (bool, error) {
	mode := route.VerificationMode
	if mode == "" {
		mode = notification.CallbackVerificationModeNone
	}

	verifier, ok := callbackVerifiers[mode]
	if !ok {
		return false, fmt.Errorf("unsupported verification mode %q", mode)
	}
	return verifier.Verify(r, rawBody, route, spec)
}

type callbackDecoder interface {
	Decode(method string, query url.Values, rawBody []byte) ([]notification.ProviderCallback, error)
}

type callbackDecoderFunc func(method string, query url.Values, rawBody []byte) ([]notification.ProviderCallback, error)

func (fn callbackDecoderFunc) Decode(method string, query url.Values, rawBody []byte) ([]notification.ProviderCallback, error) {
	return fn(method, query, rawBody)
}

var callbackDecoders = map[string]callbackDecoder{
	"generic-json": callbackDecoderFunc(func(_ string, _ url.Values, rawBody []byte) ([]notification.ProviderCallback, error) {
		var payload notification.ProviderCallback
		if err := json.Unmarshal(rawBody, &payload); err != nil {
			return nil, fmt.Errorf("invalid callback payload")
		}
		if payload.ProviderMessageID == "" || payload.Status == "" {
			return nil, fmt.Errorf("provider_message_id and status are required")
		}
		return []notification.ProviderCallback{payload}, nil
	}),
	"gupshup-whatsapp": callbackDecoderFunc(func(method string, _ url.Values, rawBody []byte) ([]notification.ProviderCallback, error) {
		if method != http.MethodPost {
			return nil, fmt.Errorf("gupshup whatsapp callbacks must use POST")
		}
		return decodeGupshupWhatsAppCallbacks(rawBody)
	}),
	"karix-whatsapp": callbackDecoderFunc(func(method string, _ url.Values, rawBody []byte) ([]notification.ProviderCallback, error) {
		if method != http.MethodPost {
			return nil, fmt.Errorf("karix whatsapp callbacks must use POST")
		}
		return decodeKarixWhatsAppCallback(rawBody)
	}),
	"gupshup-sms": callbackDecoderFunc(func(method string, _ url.Values, rawBody []byte) ([]notification.ProviderCallback, error) {
		if method != http.MethodPost {
			return nil, fmt.Errorf("gupshup sms callbacks must use POST")
		}
		return decodeGupshupSMSCallbacks(rawBody)
	}),
	"karix-sms": callbackDecoderFunc(func(method string, query url.Values, _ []byte) ([]notification.ProviderCallback, error) {
		if method != http.MethodGet {
			return nil, fmt.Errorf("karix sms callbacks must use GET")
		}
		return decodeKarixSMSCallback(query)
	}),
}

type channelEventDecoder interface {
	Decode(method string, query url.Values, rawBody []byte) ([]notification.ChannelEvent, error)
}

type channelEventDecoderFunc func(method string, query url.Values, rawBody []byte) ([]notification.ChannelEvent, error)

func (fn channelEventDecoderFunc) Decode(method string, query url.Values, rawBody []byte) ([]notification.ChannelEvent, error) {
	return fn(method, query, rawBody)
}

var inboundEventDecoders = map[string]channelEventDecoder{
	"gupshup-whatsapp": channelEventDecoderFunc(decodeGupshupWhatsAppInboundEvents),
	"meta-whatsapp":    channelEventDecoderFunc(decodeMetaWhatsAppInboundEvents),
}

func decodeProviderCallbacks(def notification.ProviderDefinition, providerKnown bool, method string, query url.Values, rawBody []byte) ([]notification.ProviderCallback, error) {
	decoderKey := "generic-json"
	if providerKnown && def.CallbackDecoder != "" {
		decoderKey = def.CallbackDecoder
	}

	decoder, ok := callbackDecoders[decoderKey]
	if !ok {
		return nil, fmt.Errorf("unsupported callback decoder %q", decoderKey)
	}
	return decoder.Decode(method, query, rawBody)
}

func decodeProviderInboundEvents(def notification.ProviderDefinition, providerKnown bool, method string, query url.Values, rawBody []byte) ([]notification.ChannelEvent, error) {
	decoderKey := strings.TrimSpace(def.InboundDecoder)
	if !providerKnown || decoderKey == "" {
		return nil, fmt.Errorf("unsupported inbound decoder")
	}
	decoder, ok := inboundEventDecoders[decoderKey]
	if !ok {
		return nil, fmt.Errorf("unsupported inbound decoder %q", decoderKey)
	}
	return decoder.Decode(method, query, rawBody)
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

func decodeGupshupWhatsAppInboundEvents(_ string, _ url.Values, rawBody []byte) ([]notification.ChannelEvent, error) {
	var payload notification.GupshupWhatsAppInboundMessage
	if err := json.Unmarshal(rawBody, &payload); err != nil {
		return nil, fmt.Errorf("invalid gupshup whatsapp inbound payload")
	}
	if strings.ToLower(strings.TrimSpace(payload.Type)) != "message" {
		return nil, fmt.Errorf("not a gupshup inbound message payload")
	}
	event, err := channelEventFromGupshupInbound(payload)
	if err != nil {
		return nil, err
	}
	return []notification.ChannelEvent{event}, nil
}

func channelEventFromGupshupInbound(payload notification.GupshupWhatsAppInboundMessage) (notification.ChannelEvent, error) {
	inbound := payload.Payload
	body, mediaType, mediaURL, mediaName := extractGupshupInboundContent(inbound)
	eventType := "message_received"
	if inbound.Context != nil && (strings.TrimSpace(inbound.Context.ID) != "" || strings.TrimSpace(inbound.Context.GsID) != "") {
		eventType = "reply_received"
	}
	conversationID := firstNonEmpty(gupshupInboundContextID(inbound.Context), gupshupInboundContextGsID(inbound.Context))
	replyTo := firstNonEmpty(strings.TrimSpace(gupshupInboundContextGsID(inbound.Context)), strings.TrimSpace(gupshupInboundContextID(inbound.Context)))
	return notification.ChannelEvent{
		EventID:           id.New(12),
		ProviderKey:       "gupshup-whatsapp",
		Channel:           notification.ChannelWhatsApp,
		Direction:         notification.ChannelEventDirectionInbound,
		EventType:         eventType,
		Status:            notification.ChannelEventStatusReceived,
		ExternalMessageID: strings.TrimSpace(inbound.ID),
		ReplyToMessageID:  replyTo,
		ConversationID:    conversationID,
		FromAddress:       strings.TrimSpace(inbound.Source),
		Body:              body,
		MediaType:         mediaType,
		MediaURL:          mediaURL,
		MediaName:         mediaName,
		Payload: map[string]any{
			"raw": payload,
		},
		Metadata: map[string]string{
			"app":          payload.App,
			"sender_name":  inbound.Sender.Name,
			"message_type": inbound.Type,
		},
		ReceivedAt: time.UnixMilli(payload.Timestamp).UTC(),
	}, nil
}

func extractGupshupInboundContent(payload notification.GupshupWhatsAppInboundPayload) (body, mediaType, mediaURL, mediaName string) {
	switch strings.ToLower(strings.TrimSpace(payload.Type)) {
	case "text":
		body = strings.TrimSpace(payload.Payload.Text)
	case "image", "video", "audio", "document", "sticker":
		body = strings.TrimSpace(firstNonEmpty(payload.Payload.Caption, payload.Payload.Text))
		mediaType = strings.ToLower(strings.TrimSpace(payload.Type))
		mediaURL = strings.TrimSpace(payload.Payload.URL)
		mediaName = strings.TrimSpace(payload.Payload.Name)
	default:
		body = strings.TrimSpace(firstNonEmpty(payload.Payload.Text, payload.Payload.Caption))
		mediaType = strings.ToLower(strings.TrimSpace(payload.Type))
	}
	return body, mediaType, mediaURL, mediaName
}

func gupshupInboundContextID(ctx *notification.GupshupWhatsAppInboundContext) string {
	if ctx == nil {
		return ""
	}
	return strings.TrimSpace(ctx.ID)
}

func gupshupInboundContextGsID(ctx *notification.GupshupWhatsAppInboundContext) string {
	if ctx == nil {
		return ""
	}
	return strings.TrimSpace(ctx.GsID)
}

func decodeMetaWhatsAppInboundEvents(_ string, _ url.Values, rawBody []byte) ([]notification.ChannelEvent, error) {
	var payload notification.MetaWhatsAppWebhookPayload
	if err := json.Unmarshal(rawBody, &payload); err != nil {
		return nil, fmt.Errorf("invalid meta whatsapp inbound payload")
	}
	var events []notification.ChannelEvent
	for _, entry := range payload.Entry {
		for _, change := range entry.Changes {
			for _, msg := range change.Value.Messages {
				event, err := channelEventFromMetaMessage(msg, change.Value)
				if err != nil {
					return nil, err
				}
				events = append(events, event)
			}
		}
	}
	if len(events) == 0 {
		return nil, fmt.Errorf("no inbound messages found")
	}
	return events, nil
}

func channelEventFromMetaMessage(msg notification.MetaWhatsAppMessage, value notification.MetaWhatsAppWebhookValue) (notification.ChannelEvent, error) {
	body, mediaType, mediaURL, mediaName := extractMetaMessageContent(msg)
	eventType := "message_received"
	if msg.Context != nil && strings.TrimSpace(msg.Context.ID) != "" {
		eventType = "reply_received"
	}
	replyTo := ""
	if msg.Context != nil {
		replyTo = strings.TrimSpace(msg.Context.ID)
	}
	return notification.ChannelEvent{
		EventID:           id.New(12),
		ProviderKey:       "karix-whatsapp",
		Channel:           notification.ChannelWhatsApp,
		Direction:         notification.ChannelEventDirectionInbound,
		EventType:         eventType,
		Status:            notification.ChannelEventStatusReceived,
		ExternalMessageID: strings.TrimSpace(msg.ID),
		ReplyToMessageID:  replyTo,
		FromAddress:       strings.TrimSpace(msg.From),
		ToAddress:         strings.TrimSpace(value.Metadata.DisplayPhoneNumber),
		Body:              body,
		MediaType:         mediaType,
		MediaURL:          mediaURL,
		MediaName:         mediaName,
		Payload: map[string]any{
			"raw": msg,
		},
		Metadata: map[string]string{
			"phone_number_id": value.Metadata.PhoneNumberID,
			"message_type":    msg.Type,
			"contact_name":    firstMetaContactName(value.Contacts),
		},
		ReceivedAt: parseMetaTimestamp(msg.Timestamp),
	}, nil
}

func extractMetaMessageContent(msg notification.MetaWhatsAppMessage) (body, mediaType, mediaURL, mediaName string) {
	switch strings.ToLower(strings.TrimSpace(msg.Type)) {
	case "text":
		if msg.Text != nil {
			body = strings.TrimSpace(msg.Text.Body)
		}
	case "image":
		mediaType = "image"
		if msg.Image != nil {
			mediaURL = strings.TrimSpace(msg.Image.ID)
			mediaName = strings.TrimSpace(msg.Image.Caption)
			body = strings.TrimSpace(msg.Image.Caption)
		}
	case "video":
		mediaType = "video"
		if msg.Video != nil {
			mediaURL = strings.TrimSpace(msg.Video.ID)
			mediaName = strings.TrimSpace(msg.Video.Caption)
			body = strings.TrimSpace(msg.Video.Caption)
		}
	case "document":
		mediaType = "document"
		if msg.Document != nil {
			mediaURL = strings.TrimSpace(msg.Document.ID)
			mediaName = strings.TrimSpace(msg.Document.Caption)
			body = strings.TrimSpace(msg.Document.Caption)
		}
	case "audio":
		mediaType = "audio"
		if msg.Audio != nil {
			mediaURL = strings.TrimSpace(msg.Audio.ID)
		}
	case "sticker":
		mediaType = "sticker"
		if msg.Sticker != nil {
			mediaURL = strings.TrimSpace(msg.Sticker.ID)
		}
	}
	return body, mediaType, mediaURL, mediaName
}

func firstMetaContactName(contacts []notification.MetaWhatsAppContact) string {
	if len(contacts) == 0 {
		return ""
	}
	return strings.TrimSpace(contacts[0].Profile.Name)
}

func parseMetaTimestamp(raw string) time.Time {
	if raw == "" {
		return time.Now().UTC()
	}
	seconds, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return time.Now().UTC()
	}
	return time.Unix(seconds, 0).UTC()
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
