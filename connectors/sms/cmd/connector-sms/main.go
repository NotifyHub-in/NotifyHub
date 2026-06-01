package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
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
	"github.com/Arunshaik2001/notification-control-plane/libs/observability/logging"
	"github.com/Arunshaik2001/notification-control-plane/libs/observability/metrics"
)

func main() {
	cfg, err := config.LoadHTTPServiceConfig("connector-sms", 8092)
	if err != nil {
		panic(err)
	}

	logger := logging.New(cfg.ServiceName)
	registry := metrics.NewRegistry(cfg.ServiceName)
	providerClient := &http.Client{Timeout: 5 * time.Second}

	err = app.RunHTTPService(cfg, logger, registry, func(mux *http.ServeMux, info serviceinfo.Info, registry *metrics.Registry) {
		mux.HandleFunc("GET /v1/capabilities", func(w http.ResponseWriter, _ *http.Request) {
			httpx.WriteJSON(w, http.StatusOK, notification.ConnectorCapabilities{
				Name:     "sms",
				Channels: []notification.Channel{notification.ChannelSMS},
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
			adapter, err := selectSMSAdapter(req.ProviderKey)
			if err != nil {
				registry.IncCounter("connector_send_requests_total", "Outbound connector send outcomes.", map[string]string{"connector": info.Name, "provider": req.ProviderKey, "outcome": "unsupported_provider"})
				httpx.WriteJSON(w, http.StatusBadRequest, notification.ConnectorErrorResponse{
					Error:          err.Error(),
					Code:           "unsupported_provider",
					Classification: notification.FailureClassMisconfigured,
				})
				return
			}
			if err := resolveSMSProviderConfig(&req); err != nil {
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
			providerMessageID, sendErr := adapter.send(r.Context(), providerClient, outbound)
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
			logger.Info("sent sms provider request", "request_id", req.RequestID, "provider", req.ProviderKey, "endpoint", outbound.URL)
			httpx.WriteJSON(w, http.StatusAccepted, notification.ConnectorSendResponse{
				RequestID:         req.RequestID,
				ProviderMessageID: providerMessageID,
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

type smsAdapter interface {
	validate(req notification.ConnectorSendRequest) *smsAdapterError
	build(req notification.ConnectorSendRequest) (providerOutboundRequest, *smsAdapterError)
	send(ctx context.Context, client *http.Client, outbound providerOutboundRequest) (string, *smsAdapterError)
}

type twilioSMSAdapter struct{}

type gupshupSMSAdapter struct{}

type karixSMSAdapter struct{}

type providerOutboundRequest struct {
	URL     string
	Method  string
	Headers map[string]string
	Body    []byte
}

type twilioOutboundPayload struct {
	To   string `json:"To"`
	From string `json:"From"`
	Body string `json:"Body"`
}

type gupshupOutboundPayload struct {
	Channel     string `json:"channel"`
	Source      string `json:"source"`
	Destination string `json:"destination"`
	Message     string `json:"message"`
}

type karixSmsRequest struct {
	Ver      string            `json:"ver"`
	Key      string            `json:"key"`
	Messages []karixSmsMessage `json:"messages"`
}

type karixSmsMessage struct {
	Dest []string `json:"dest"`
	Text string   `json:"text"`
	Send string   `json:"send"`
	Type string   `json:"type"`
}

type smsAdapterError struct {
	statusCode     int
	message        string
	code           string
	classification notification.FailureClass
	retryable      bool
}

func resolveSMSProviderConfig(req *notification.ConnectorSendRequest) *smsAdapterError {
	resolved, err := secrets.ResolveConfig(req.ProviderConfig, req.ProviderSecretRefs)
	if err != nil {
		return &smsAdapterError{
			statusCode:     http.StatusBadRequest,
			message:        "failed to resolve provider secrets: " + err.Error(),
			code:           "provider_secret_resolution_failed",
			classification: notification.FailureClassMisconfigured,
		}
	}
	req.ProviderConfig = resolved
	return nil
}

func selectSMSAdapter(providerKey string) (smsAdapter, error) {
	switch providerKey {
	case "", "twilio-sms":
		return twilioSMSAdapter{}, nil
	case "gupshup-sms":
		return gupshupSMSAdapter{}, nil
	case "karix-sms":
		return karixSMSAdapter{}, nil
	default:
		return nil, fmt.Errorf("unsupported sms provider %q", providerKey)
	}
}

func (twilioSMSAdapter) validate(req notification.ConnectorSendRequest) *smsAdapterError {
	if req.Destination == "" || !strings.HasPrefix(req.Destination, "+") {
		return &smsAdapterError{
			statusCode:     http.StatusBadRequest,
			message:        "invalid phone destination",
			code:           "invalid_destination",
			classification: notification.FailureClassInvalidRequest,
		}
	}
	if token := req.ProviderConfig["auth_token"]; token == "unauthorized" {
		return &smsAdapterError{
			statusCode:     http.StatusUnauthorized,
			message:        "provider rejected sms credentials",
			code:           "invalid_credentials",
			classification: notification.FailureClassUnauthorized,
		}
	}
	return nil
}

func (twilioSMSAdapter) build(req notification.ConnectorSendRequest) (providerOutboundRequest, *smsAdapterError) {
	accountSID := req.ProviderConfig["account_sid"]
	authToken := req.ProviderConfig["auth_token"]
	fromNumber := req.ProviderConfig["from_number"]
	baseURL := req.ProviderConfig["base_url"]
	if accountSID == "" || authToken == "" || fromNumber == "" {
		return providerOutboundRequest{}, &smsAdapterError{
			statusCode:     http.StatusBadRequest,
			message:        "missing twilio provider config",
			code:           "missing_provider_config",
			classification: notification.FailureClassMisconfigured,
		}
	}
	if baseURL == "" {
		baseURL = "https://api.twilio.com"
	}
	endpoint, err := joinProviderURL(baseURL, "/2010-04-01/Accounts/"+accountSID+"/Messages.json")
	if err != nil {
		return providerOutboundRequest{}, &smsAdapterError{
			statusCode:     http.StatusBadRequest,
			message:        "invalid twilio base_url",
			code:           "invalid_provider_config",
			classification: notification.FailureClassMisconfigured,
		}
	}
	body := url.Values{}
	body.Set("To", req.Destination)
	body.Set("From", fromNumber)
	body.Set("Body", req.Body)
	return providerOutboundRequest{
		URL:    endpoint,
		Method: http.MethodPost,
		Headers: map[string]string{
			"Authorization": "Basic " + base64.StdEncoding.EncodeToString([]byte(accountSID+":"+authToken)),
			"Content-Type":  "application/x-www-form-urlencoded",
		},
		Body: []byte(body.Encode()),
	}, nil
}

func (twilioSMSAdapter) send(ctx context.Context, client *http.Client, outbound providerOutboundRequest) (string, *smsAdapterError) {
	return executeSMSProviderRequest(ctx, client, outbound, "sms")
}

func (gupshupSMSAdapter) validate(req notification.ConnectorSendRequest) *smsAdapterError {
	if strings.TrimSpace(req.Destination) == "" {
		return &smsAdapterError{
			statusCode:     http.StatusBadRequest,
			message:        "invalid phone destination",
			code:           "invalid_destination",
			classification: notification.FailureClassInvalidRequest,
		}
	}
	if token := req.ProviderConfig["api_key"]; token == "unauthorized" || req.ProviderConfig["password"] == "unauthorized" {
		return &smsAdapterError{
			statusCode:     http.StatusUnauthorized,
			message:        "provider rejected sms credentials",
			code:           "invalid_credentials",
			classification: notification.FailureClassUnauthorized,
		}
	}
	return nil
}

func (gupshupSMSAdapter) build(req notification.ConnectorSendRequest) (providerOutboundRequest, *smsAdapterError) {
	apiKey := req.ProviderConfig["api_key"]
	senderID := req.ProviderConfig["sender_id"]
	baseURL := req.ProviderConfig["base_url"]
	if apiKey != "" && senderID != "" {
		if baseURL == "" {
			baseURL = "https://api.gupshup.io"
		}
		endpoint, err := joinProviderURL(baseURL, "/sms/1/send")
		if err != nil {
			return providerOutboundRequest{}, &smsAdapterError{
				statusCode:     http.StatusBadRequest,
				message:        "invalid gupshup base_url",
				code:           "invalid_provider_config",
				classification: notification.FailureClassMisconfigured,
			}
		}
		body, err := json.Marshal(gupshupOutboundPayload{
			Channel:     "sms",
			Source:      senderID,
			Destination: normalizeSMSDestination(req.Destination),
			Message:     req.Body,
		})
		if err != nil {
			return providerOutboundRequest{}, &smsAdapterError{
				statusCode:     http.StatusInternalServerError,
				message:        "failed to encode gupshup payload",
				code:           "encode_failed",
				classification: notification.FailureClassTransient,
			}
		}
		return providerOutboundRequest{
			URL:    endpoint,
			Method: http.MethodPost,
			Headers: map[string]string{
				"apikey":       apiKey,
				"Content-Type": "application/json",
			},
			Body: body,
		}, nil
	}

	username := req.ProviderConfig["username"]
	password := req.ProviderConfig["password"]
	senderID = req.ProviderConfig["send"]
	if senderID == "" {
		senderID = req.ProviderConfig["sender_id"]
	}
	version := req.ProviderConfig["version"]
	if version == "" {
		version = "1.1"
	}
	if username == "" || password == "" || senderID == "" {
		return providerOutboundRequest{}, &smsAdapterError{
			statusCode:     http.StatusBadRequest,
			message:        "missing gupshup provider config",
			code:           "missing_provider_config",
			classification: notification.FailureClassMisconfigured,
		}
	}
	if baseURL == "" {
		baseURL = "https://enterprise.smsgupshup.com/GatewayAPI/rest"
	}
	endpoint, err := joinProviderURL(baseURL, "")
	if err != nil {
		return providerOutboundRequest{}, &smsAdapterError{
			statusCode:     http.StatusBadRequest,
			message:        "invalid gupshup base_url",
			code:           "invalid_provider_config",
			classification: notification.FailureClassMisconfigured,
		}
	}
	query := url.Values{}
	query.Set("method", "sendMessage")
	query.Set("msg", req.Body)
	query.Set("priority", "8")
	query.Set("v", version)
	query.Set("userid", username)
	query.Set("password", password)
	query.Set("send_to", normalizeSMSDestination(req.Destination))
	query.Set("format", "text")
	query.Set("channel", "sms")
	parsed, _ := url.Parse(endpoint)
	parsed.RawQuery = query.Encode()
	return providerOutboundRequest{
		URL:    parsed.String(),
		Method: http.MethodGet,
		Headers: map[string]string{
			"Accept": "*/*",
		},
	}, nil
}

func (gupshupSMSAdapter) send(ctx context.Context, client *http.Client, outbound providerOutboundRequest) (string, *smsAdapterError) {
	return executeSMSProviderRequest(ctx, client, outbound, "sms")
}

func (karixSMSAdapter) validate(req notification.ConnectorSendRequest) *smsAdapterError {
	if strings.TrimSpace(req.Destination) == "" {
		return &smsAdapterError{
			statusCode:     http.StatusBadRequest,
			message:        "invalid phone destination",
			code:           "invalid_destination",
			classification: notification.FailureClassInvalidRequest,
		}
	}
	if token := req.ProviderConfig["api_key"]; token == "unauthorized" || req.ProviderConfig["key"] == "unauthorized" {
		return &smsAdapterError{
			statusCode:     http.StatusUnauthorized,
			message:        "provider rejected sms credentials",
			code:           "invalid_credentials",
			classification: notification.FailureClassUnauthorized,
		}
	}
	return nil
}

func (karixSMSAdapter) build(req notification.ConnectorSendRequest) (providerOutboundRequest, *smsAdapterError) {
	apiKey := req.ProviderConfig["api_key"]
	senderID := req.ProviderConfig["sender_id"]
	baseURL := req.ProviderConfig["base_url"]
	if apiKey != "" && senderID != "" {
		if baseURL == "" {
			baseURL = "https://api.karix.io"
		}
		version := req.ProviderConfig["ver"]
		if version == "" {
			version = "1.0"
		}
		endpoint, err := joinProviderURL(baseURL, "/v1/messages")
		if err != nil {
			return providerOutboundRequest{}, &smsAdapterError{
				statusCode:     http.StatusBadRequest,
				message:        "invalid karix base_url",
				code:           "invalid_provider_config",
				classification: notification.FailureClassMisconfigured,
			}
		}
		body, err := json.Marshal(karixSmsRequest{
			Ver: version,
			Key: apiKey,
			Messages: []karixSmsMessage{
				{
					Dest: []string{normalizeSMSDestination(req.Destination)},
					Text: req.Body,
					Send: senderID,
					Type: "PM",
				},
			},
		})
		if err != nil {
			return providerOutboundRequest{}, &smsAdapterError{
				statusCode:     http.StatusInternalServerError,
				message:        "failed to encode karix payload",
				code:           "encode_failed",
				classification: notification.FailureClassTransient,
			}
		}
		return providerOutboundRequest{
			URL:    endpoint,
			Method: http.MethodPost,
			Headers: map[string]string{
				"api-key":      apiKey,
				"Content-Type": "application/json",
			},
			Body: body,
		}, nil
	}

	stageKey := req.ProviderConfig["key"]
	stageVersion := req.ProviderConfig["ver"]
	stageSend := req.ProviderConfig["send"]
	if baseURL == "" {
		baseURL = req.ProviderConfig["url"]
	}
	if baseURL == "" {
		baseURL = "https://japi.instaalerts.zone/httpapi/JsonReceiver"
	}
	if stageKey == "" || stageVersion == "" || stageSend == "" {
		return providerOutboundRequest{}, &smsAdapterError{
			statusCode:     http.StatusBadRequest,
			message:        "missing karix provider config",
			code:           "missing_provider_config",
			classification: notification.FailureClassMisconfigured,
		}
	}
	payload := karixSmsRequest{
		Ver: stageVersion,
		Key: stageKey,
		Messages: []karixSmsMessage{
			{
				Dest: []string{normalizeSMSDestination(req.Destination)},
				Text: req.Body,
				Send: stageSend,
				Type: "PM",
			},
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return providerOutboundRequest{}, &smsAdapterError{
			statusCode:     http.StatusInternalServerError,
			message:        "failed to encode karix payload",
			code:           "encode_failed",
			classification: notification.FailureClassTransient,
		}
	}
	return providerOutboundRequest{
		URL:    baseURL,
		Method: http.MethodPost,
		Headers: map[string]string{
			"Content-Type": "application/json",
			"Accept":       "*/*",
		},
		Body: body,
	}, nil
}

func (karixSMSAdapter) send(ctx context.Context, client *http.Client, outbound providerOutboundRequest) (string, *smsAdapterError) {
	return executeSMSProviderRequest(ctx, client, outbound, "sms")
}

func executeSMSProviderRequest(ctx context.Context, client *http.Client, outbound providerOutboundRequest, channelLabel string) (string, *smsAdapterError) {
	if client == nil {
		client = http.DefaultClient
	}
	req, err := http.NewRequestWithContext(ctx, outbound.Method, outbound.URL, bytes.NewReader(outbound.Body))
	if err != nil {
		return "", &smsAdapterError{
			statusCode:     http.StatusInternalServerError,
			message:        "failed to build provider request",
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
		return "", &smsAdapterError{
			statusCode:     http.StatusBadGateway,
			message:        channelLabel + " provider request failed: " + err.Error(),
			code:           "provider_request_failed",
			classification: notification.FailureClassTransient,
			retryable:      true,
		}
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return "", smsProviderErrorFromHTTPResponse(resp.StatusCode, string(body), channelLabel)
	}
	return providerMessageIDFromResponse(resp.Header, body), nil
}

func smsProviderErrorFromHTTPResponse(statusCode int, responseBody string, channelLabel string) *smsAdapterError {
	switch statusCode {
	case http.StatusUnauthorized, http.StatusForbidden:
		return &smsAdapterError{
			statusCode:     statusCode,
			message:        channelLabel + " provider rejected credentials",
			code:           "invalid_credentials",
			classification: notification.FailureClassUnauthorized,
		}
	case http.StatusTooManyRequests:
		return &smsAdapterError{
			statusCode:     statusCode,
			message:        channelLabel + " provider rate limited the request",
			code:           "rate_limited",
			classification: notification.FailureClassRateLimited,
			retryable:      true,
		}
	case http.StatusBadRequest, http.StatusUnprocessableEntity:
		return &smsAdapterError{
			statusCode:     statusCode,
			message:        channelLabel + " provider rejected the request: " + trimResponse(responseBody),
			code:           "invalid_provider_request",
			classification: notification.FailureClassInvalidRequest,
		}
	default:
		return &smsAdapterError{
			statusCode:     statusCode,
			message:        channelLabel + " provider temporary outage",
			code:           "provider_outage",
			classification: notification.FailureClassTransient,
			retryable:      true,
		}
	}
}

func providerMessageIDFromResponse(headers http.Header, body []byte) string {
	if value := headers.Get("X-Provider-Message-ID"); value != "" {
		return value
	}
	if value := headers.Get("X-Message-Id"); value != "" {
		return value
	}
	var parsed map[string]any
	if err := json.Unmarshal(body, &parsed); err == nil {
		for _, key := range []string{"message_id", "id", "messageId", "ackid"} {
			if value, ok := parsed[key].(string); ok && value != "" {
				return value
			}
		}
	}
	text := strings.TrimSpace(string(body))
	if idx := strings.LastIndex(text, "|"); idx >= 0 && idx < len(text)-1 {
		return strings.TrimSpace(text[idx+1:])
	}
	return providerMessageID()
}

func normalizeSMSDestination(value string) string {
	value = strings.TrimSpace(value)
	value = strings.TrimPrefix(value, "+")
	return value
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
