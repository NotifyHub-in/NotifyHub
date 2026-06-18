package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/NotifyHub-in/NotifyHub/libs/contracts/notification"
	"github.com/NotifyHub-in/NotifyHub/libs/core/app"
	"github.com/NotifyHub-in/NotifyHub/libs/core/config"
	"github.com/NotifyHub-in/NotifyHub/libs/core/httpx"
	"github.com/NotifyHub-in/NotifyHub/libs/core/secrets"
	"github.com/NotifyHub-in/NotifyHub/libs/core/serviceinfo"
	"github.com/NotifyHub-in/NotifyHub/libs/observability/logging"
	"github.com/NotifyHub-in/NotifyHub/libs/observability/metrics"
)

func main() {
	cfg, err := config.LoadHTTPServiceConfig("connector-webhook", 8093)
	if err != nil {
		panic(err)
	}

	logger := logging.New(cfg.ServiceName)
	registry := metrics.NewRegistry(cfg.ServiceName)
	providerClient := &http.Client{Timeout: 5 * time.Second}

	err = app.RunHTTPService(cfg, logger, registry, func(mux *http.ServeMux, info serviceinfo.Info, registry *metrics.Registry) {
		mux.HandleFunc("GET /v1/capabilities", func(w http.ResponseWriter, _ *http.Request) {
			httpx.WriteJSON(w, http.StatusOK, notification.ConnectorCapabilities{
				Name:     "webhook",
				Channels: []notification.Channel{notification.ChannelWebhook},
			})
		})
		mux.HandleFunc("POST /v1/send", func(w http.ResponseWriter, r *http.Request) {
			var req notification.ConnectorSendRequest
			if err := httpx.DecodeJSON(r, &req); err != nil {
				registry.IncCounter("connector_send_requests_total", "Outbound connector send outcomes.", map[string]string{"connector": info.Name, "provider": "unknown", "outcome": "invalid_payload"})
				httpx.WriteJSON(w, http.StatusBadRequest, notification.ConnectorErrorResponse{
					Error:          "invalid connector send payload",
					Code:           "invalid_payload",
					Classification: notification.FailureClassInvalidRequest,
				})
				return
			}

			adapter := webhookAdapter{}
			if err := resolveWebhookProviderConfig(&req); err != nil {
				registry.IncCounter("connector_send_requests_total", "Outbound connector send outcomes.", map[string]string{"connector": info.Name, "provider": req.ProviderKey, "outcome": "provider_config_resolution_failed"})
				httpx.WriteJSON(w, err.statusCode, notification.ConnectorErrorResponse{
					Error:          err.message,
					Code:           err.code,
					Classification: err.classification,
					Retryable:      err.retryable,
				})
				return
			}
			if err := adapter.validate(req); err != nil {
				registry.IncCounter("connector_send_requests_total", "Outbound connector send outcomes.", map[string]string{"connector": info.Name, "provider": req.ProviderKey, "outcome": "validation_failed"})
				httpx.WriteJSON(w, err.statusCode, notification.ConnectorErrorResponse{
					Error:          err.message,
					Code:           err.code,
					Classification: err.classification,
					Retryable:      err.retryable,
				})
				return
			}

			outbound, buildErr := adapter.build(req)
			if buildErr != nil {
				registry.IncCounter("connector_send_requests_total", "Outbound connector send outcomes.", map[string]string{"connector": info.Name, "provider": req.ProviderKey, "outcome": "build_failed"})
				httpx.WriteJSON(w, buildErr.statusCode, notification.ConnectorErrorResponse{
					Error:          buildErr.message,
					Code:           buildErr.code,
					Classification: buildErr.classification,
					Retryable:      buildErr.retryable,
				})
				return
			}

			sendStarted := time.Now()
			messageID, sendErr := adapter.send(r.Context(), providerClient, outbound)
			if sendErr != nil {
				registry.IncCounter("connector_send_requests_total", "Outbound connector send outcomes.", map[string]string{"connector": info.Name, "provider": req.ProviderKey, "outcome": "send_failed", "classification": string(sendErr.classification)})
				registry.ObserveHistogram("connector_provider_request_duration_seconds", "Outbound provider request duration in seconds.", map[string]string{"connector": info.Name, "provider": req.ProviderKey, "outcome": "send_failed"}, metrics.DefaultLatencyBuckets(), time.Since(sendStarted).Seconds())
				httpx.WriteJSON(w, sendErr.statusCode, notification.ConnectorErrorResponse{
					Error:          sendErr.message,
					Code:           sendErr.code,
					Classification: sendErr.classification,
					Retryable:      sendErr.retryable,
				})
				return
			}
			registry.IncCounter("connector_send_requests_total", "Outbound connector send outcomes.", map[string]string{"connector": info.Name, "provider": req.ProviderKey, "outcome": "accepted"})
			registry.ObserveHistogram("connector_provider_request_duration_seconds", "Outbound provider request duration in seconds.", map[string]string{"connector": info.Name, "provider": req.ProviderKey, "outcome": "accepted"}, metrics.DefaultLatencyBuckets(), time.Since(sendStarted).Seconds())

			logger.Info("sent webhook provider request", "request_id", req.RequestID, "endpoint", outbound.URL)
			httpx.WriteJSON(w, http.StatusAccepted, notification.ConnectorSendResponse{
				RequestID:         req.RequestID,
				ProviderMessageID: messageID,
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

type webhookAdapter struct{}

type webhookAdapterError struct {
	statusCode     int
	message        string
	code           string
	classification notification.FailureClass
	retryable      bool
}

type webhookOutboundRequest struct {
	URL     string
	Method  string
	Headers map[string]string
	Body    []byte
}

type webhookOutboundPayload struct {
	RequestID string               `json:"request_id"`
	Subject   string               `json:"subject,omitempty"`
	Body      string               `json:"body"`
	Metadata  map[string]string    `json:"metadata,omitempty"`
	Channel   notification.Channel `json:"channel"`
}

func resolveWebhookProviderConfig(req *notification.ConnectorSendRequest) *webhookAdapterError {
	resolved, err := secrets.ResolveConfig(req.ProviderConfig, req.ProviderSecretRefs)
	if err != nil {
		return &webhookAdapterError{
			statusCode:     http.StatusBadRequest,
			message:        "failed to resolve provider secrets: " + err.Error(),
			code:           "provider_secret_resolution_failed",
			classification: notification.FailureClassMisconfigured,
		}
	}
	req.ProviderConfig = resolved
	return nil
}

func (webhookAdapter) validate(req notification.ConnectorSendRequest) *webhookAdapterError {
	if req.Destination == "" {
		return &webhookAdapterError{
			statusCode:     http.StatusBadRequest,
			message:        "missing webhook destination",
			code:           "invalid_destination",
			classification: notification.FailureClassInvalidRequest,
		}
	}
	parsed, err := url.Parse(req.Destination)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return &webhookAdapterError{
			statusCode:     http.StatusBadRequest,
			message:        "invalid webhook destination",
			code:           "invalid_destination",
			classification: notification.FailureClassInvalidRequest,
		}
	}
	if secret := req.ProviderConfig["shared_secret"]; secret == "unauthorized" {
		return &webhookAdapterError{
			statusCode:     http.StatusUnauthorized,
			message:        "provider rejected webhook credentials",
			code:           "invalid_credentials",
			classification: notification.FailureClassUnauthorized,
		}
	}
	return nil
}

func (webhookAdapter) build(req notification.ConnectorSendRequest) (webhookOutboundRequest, *webhookAdapterError) {
	payload := webhookOutboundPayload{
		RequestID: req.RequestID,
		Subject:   req.Subject,
		Body:      req.Body,
		Metadata:  req.Metadata,
		Channel:   req.Channel,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return webhookOutboundRequest{}, &webhookAdapterError{
			statusCode:     http.StatusInternalServerError,
			message:        "failed to encode webhook payload",
			code:           "encode_failed",
			classification: notification.FailureClassTransient,
		}
	}
	return webhookOutboundRequest{
		URL:    req.Destination,
		Method: http.MethodPost,
		Headers: map[string]string{
			"Content-Type":           "application/json",
			"X-Connector-Request-ID": req.RequestID,
			"X-Webhook-Secret":       req.ProviderConfig["shared_secret"],
		},
		Body: body,
	}, nil
}

func (webhookAdapter) send(ctx context.Context, client *http.Client, outbound webhookOutboundRequest) (string, *webhookAdapterError) {
	if client == nil {
		client = http.DefaultClient
	}
	req, err := http.NewRequestWithContext(ctx, outbound.Method, outbound.URL, bytes.NewReader(outbound.Body))
	if err != nil {
		return "", &webhookAdapterError{
			statusCode:     http.StatusInternalServerError,
			message:        "failed to build webhook request",
			code:           "request_build_failed",
			classification: notification.FailureClassTransient,
			retryable:      true,
		}
	}
	for key, value := range outbound.Headers {
		req.Header.Set(key, value)
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", &webhookAdapterError{
			statusCode:     http.StatusBadGateway,
			message:        "webhook provider request failed: " + err.Error(),
			code:           "provider_request_failed",
			classification: notification.FailureClassTransient,
			retryable:      true,
		}
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return "", webhookProviderErrorFromHTTPResponse(resp.StatusCode, string(body))
	}
	if value := resp.Header.Get("X-Provider-Message-ID"); value != "" {
		return value, nil
	}
	if value := resp.Header.Get("X-Message-Id"); value != "" {
		return value, nil
	}
	return providerMessageID(), nil
}

func webhookProviderErrorFromHTTPResponse(statusCode int, responseBody string) *webhookAdapterError {
	switch statusCode {
	case http.StatusUnauthorized, http.StatusForbidden:
		return &webhookAdapterError{
			statusCode:     statusCode,
			message:        "webhook provider rejected credentials",
			code:           "invalid_credentials",
			classification: notification.FailureClassUnauthorized,
		}
	case http.StatusTooManyRequests:
		return &webhookAdapterError{
			statusCode:     statusCode,
			message:        "webhook provider rate limited the request",
			code:           "rate_limited",
			classification: notification.FailureClassRateLimited,
			retryable:      true,
		}
	case http.StatusBadRequest, http.StatusUnprocessableEntity:
		return &webhookAdapterError{
			statusCode:     statusCode,
			message:        "webhook provider rejected the request: " + trimResponse(responseBody),
			code:           "invalid_provider_request",
			classification: notification.FailureClassInvalidRequest,
		}
	default:
		return &webhookAdapterError{
			statusCode:     statusCode,
			message:        "webhook provider temporary outage",
			code:           "provider_outage",
			classification: notification.FailureClassTransient,
			retryable:      true,
		}
	}
}

func trimResponse(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 160 {
		return value[:160]
	}
	return value
}
