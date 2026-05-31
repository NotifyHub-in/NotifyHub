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

	"github.com/your-org/notification-control-plane/libs/contracts/notification"
	"github.com/your-org/notification-control-plane/libs/core/app"
	"github.com/your-org/notification-control-plane/libs/core/config"
	"github.com/your-org/notification-control-plane/libs/core/httpx"
	"github.com/your-org/notification-control-plane/libs/core/serviceinfo"
	"github.com/your-org/notification-control-plane/libs/observability/logging"
	"github.com/your-org/notification-control-plane/libs/observability/metrics"
)

func main() {
	cfg, err := config.LoadHTTPServiceConfig("connector-push", 8094)
	if err != nil {
		panic(err)
	}

	logger := logging.New(cfg.ServiceName)
	providerClient := &http.Client{Timeout: 5 * time.Second}

	err = app.RunHTTPService(cfg, logger, nil, func(mux *http.ServeMux, info serviceinfo.Info, _ *metrics.Registry) {
		mux.HandleFunc("GET /v1/capabilities", func(w http.ResponseWriter, _ *http.Request) {
			httpx.WriteJSON(w, http.StatusOK, notification.ConnectorCapabilities{
				Name:     "push",
				Channels: []notification.Channel{notification.ChannelPush},
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

			adapter := fcmAdapter{}
			if err := adapter.validate(req); err != nil {
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
				httpx.WriteJSON(w, buildErr.statusCode, notification.ConnectorErrorResponse{
					Error:          buildErr.message,
					Code:           buildErr.code,
					Classification: buildErr.classification,
					Retryable:      buildErr.retryable,
				})
				return
			}

			messageID, sendErr := adapter.send(r.Context(), providerClient, outbound)
			if sendErr != nil {
				httpx.WriteJSON(w, sendErr.statusCode, notification.ConnectorErrorResponse{
					Error:          sendErr.message,
					Code:           sendErr.code,
					Classification: sendErr.classification,
					Retryable:      sendErr.retryable,
				})
				return
			}

			logger.Info("sent push provider request", "request_id", req.RequestID, "provider", req.ProviderKey, "endpoint", outbound.URL)
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

type fcmAdapter struct{}

type fcmAdapterError struct {
	statusCode     int
	message        string
	code           string
	classification notification.FailureClass
	retryable      bool
}

type fcmOutboundRequest struct {
	URL     string
	Method  string
	Headers map[string]string
	Body    []byte
}

type fcmOutboundPayload struct {
	Message fcmMessage `json:"message"`
}

type fcmMessage struct {
	Token        string            `json:"token,omitempty"`
	Topic        string            `json:"topic,omitempty"`
	Notification fcmNotification   `json:"notification"`
	Data         map[string]string `json:"data,omitempty"`
}

type fcmNotification struct {
	Title string `json:"title,omitempty"`
	Body  string `json:"body,omitempty"`
}

func (fcmAdapter) validate(req notification.ConnectorSendRequest) *fcmAdapterError {
	if req.Destination == "" {
		return &fcmAdapterError{
			statusCode:     http.StatusBadRequest,
			message:        "missing push destination",
			code:           "invalid_destination",
			classification: notification.FailureClassInvalidRequest,
		}
	}
	if req.ProviderConfig["service_account_json"] == "" {
		return &fcmAdapterError{
			statusCode:     http.StatusBadRequest,
			message:        "missing fcm service_account_json",
			code:           "missing_provider_config",
			classification: notification.FailureClassMisconfigured,
		}
	}
	if token := req.ProviderConfig["access_token"]; token == "unauthorized" {
		return &fcmAdapterError{
			statusCode:     http.StatusUnauthorized,
			message:        "provider rejected push credentials",
			code:           "invalid_credentials",
			classification: notification.FailureClassUnauthorized,
		}
	}
	switch req.Metadata["simulate_failure"] {
	case "rate_limit":
		return &fcmAdapterError{
			statusCode:     http.StatusTooManyRequests,
			message:        "push provider rate limited the request",
			code:           "rate_limited",
			classification: notification.FailureClassRateLimited,
			retryable:      true,
		}
	case "provider_outage":
		return &fcmAdapterError{
			statusCode:     http.StatusBadGateway,
			message:        "push provider temporary outage",
			code:           "provider_outage",
			classification: notification.FailureClassTransient,
			retryable:      true,
		}
	}
	return nil
}

func (fcmAdapter) build(req notification.ConnectorSendRequest) (fcmOutboundRequest, *fcmAdapterError) {
	projectID := req.ProviderConfig["project_id"]
	if projectID == "" {
		return fcmOutboundRequest{}, &fcmAdapterError{
			statusCode:     http.StatusBadRequest,
			message:        "missing fcm project_id",
			code:           "missing_provider_config",
			classification: notification.FailureClassMisconfigured,
		}
	}
	baseURL := req.ProviderConfig["base_url"]
	if baseURL == "" {
		baseURL = "https://fcm.googleapis.com"
	}
	endpoint, err := joinProviderURL(baseURL, "/v1/projects/"+projectID+"/messages:send")
	if err != nil {
		return fcmOutboundRequest{}, &fcmAdapterError{
			statusCode:     http.StatusBadRequest,
			message:        "invalid fcm base_url",
			code:           "invalid_provider_config",
			classification: notification.FailureClassMisconfigured,
		}
	}
	accessToken := req.ProviderConfig["access_token"]
	if accessToken == "" {
		accessToken = extractAccessToken(req.ProviderConfig["service_account_json"])
	}
	if accessToken == "" {
		return fcmOutboundRequest{}, &fcmAdapterError{
			statusCode:     http.StatusBadRequest,
			message:        "service_account_json does not contain an access token",
			code:           "missing_provider_config",
			classification: notification.FailureClassMisconfigured,
		}
	}

	message := fcmMessage{
		Notification: fcmNotification{
			Title: req.Subject,
			Body:  req.Body,
		},
		Data: req.Metadata,
	}
	if strings.HasPrefix(req.Destination, "/topics/") {
		message.Topic = strings.TrimPrefix(req.Destination, "/topics/")
	} else if strings.HasPrefix(req.Destination, "topics/") {
		message.Topic = strings.TrimPrefix(req.Destination, "topics/")
	} else {
		message.Token = req.Destination
	}
	if message.Notification.Title == "" {
		message.Notification.Title = "Notification"
	}
	body, err := json.Marshal(fcmOutboundPayload{Message: message})
	if err != nil {
		return fcmOutboundRequest{}, &fcmAdapterError{
			statusCode:     http.StatusInternalServerError,
			message:        "failed to encode fcm payload",
			code:           "encode_failed",
			classification: notification.FailureClassTransient,
		}
	}

	return fcmOutboundRequest{
		URL:    endpoint,
		Method: http.MethodPost,
		Headers: map[string]string{
			"Authorization": "Bearer " + accessToken,
			"Content-Type":  "application/json",
		},
		Body: body,
	}, nil
}

func (fcmAdapter) send(ctx context.Context, client *http.Client, outbound fcmOutboundRequest) (string, *fcmAdapterError) {
	if client == nil {
		client = http.DefaultClient
	}
	req, err := http.NewRequestWithContext(ctx, outbound.Method, outbound.URL, bytes.NewReader(outbound.Body))
	if err != nil {
		return "", &fcmAdapterError{
			statusCode:     http.StatusInternalServerError,
			message:        "failed to build push request",
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
		return "", &fcmAdapterError{
			statusCode:     http.StatusBadGateway,
			message:        "push provider request failed: " + err.Error(),
			code:           "provider_request_failed",
			classification: notification.FailureClassTransient,
			retryable:      true,
		}
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return "", fcmProviderErrorFromHTTPResponse(resp.StatusCode, string(body))
	}
	if value := resp.Header.Get("X-Provider-Message-ID"); value != "" {
		return value, nil
	}
	if value := resp.Header.Get("X-Message-Id"); value != "" {
		return value, nil
	}
	var parsed map[string]any
	if err := json.Unmarshal(body, &parsed); err == nil {
		for _, key := range []string{"name", "message_id", "id"} {
			if value, ok := parsed[key].(string); ok && value != "" {
				return value, nil
			}
		}
	}
	return providerMessageID(), nil
}

func fcmProviderErrorFromHTTPResponse(statusCode int, responseBody string) *fcmAdapterError {
	switch statusCode {
	case http.StatusUnauthorized, http.StatusForbidden:
		return &fcmAdapterError{
			statusCode:     statusCode,
			message:        "push provider rejected credentials",
			code:           "invalid_credentials",
			classification: notification.FailureClassUnauthorized,
		}
	case http.StatusTooManyRequests:
		return &fcmAdapterError{
			statusCode:     statusCode,
			message:        "push provider rate limited the request",
			code:           "rate_limited",
			classification: notification.FailureClassRateLimited,
			retryable:      true,
		}
	case http.StatusBadRequest, http.StatusUnprocessableEntity:
		return &fcmAdapterError{
			statusCode:     statusCode,
			message:        "push provider rejected the request: " + trimResponse(responseBody),
			code:           "invalid_provider_request",
			classification: notification.FailureClassInvalidRequest,
		}
	default:
		return &fcmAdapterError{
			statusCode:     statusCode,
			message:        "push provider temporary outage",
			code:           "provider_outage",
			classification: notification.FailureClassTransient,
			retryable:      true,
		}
	}
}

func extractAccessToken(serviceAccountJSON string) string {
	if serviceAccountJSON == "" {
		return ""
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(serviceAccountJSON), &parsed); err != nil {
		return ""
	}
	if value, ok := parsed["access_token"].(string); ok && value != "" {
		return value
	}
	if value, ok := parsed["server_key"].(string); ok && value != "" {
		return value
	}
	return ""
}

func joinProviderURL(baseURL string, path string) (string, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return "", err
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/") + path
	return parsed.String(), nil
}

func trimResponse(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 160 {
		return value[:160]
	}
	return value
}
