package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/smtp"
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
	runConnector("connector-email", 8091, notification.ConnectorCapabilities{
		Name:     "email",
		Channels: []notification.Channel{notification.ChannelEmail},
	})
}

func runConnector(serviceName string, port int, capabilities notification.ConnectorCapabilities) {
	cfg, err := config.LoadHTTPServiceConfig(serviceName, port)
	if err != nil {
		panic(err)
	}

	logger := logging.New(cfg.ServiceName)
	registry := metrics.NewRegistry(cfg.ServiceName)
	providerClient := &http.Client{Timeout: 5 * time.Second}

	err = app.RunHTTPService(cfg, logger, registry, func(mux *http.ServeMux, info serviceinfo.Info, registry *metrics.Registry) {
		mux.HandleFunc("GET /v1/capabilities", func(w http.ResponseWriter, _ *http.Request) {
			httpx.WriteJSON(w, http.StatusOK, capabilities)
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
			adapter, err := selectEmailAdapter(req.ProviderKey)
			if err != nil {
				registry.IncCounter("connector_send_requests_total", "Outbound connector send outcomes.", map[string]string{"connector": info.Name, "provider": req.ProviderKey, "outcome": "unsupported_provider"})
				httpx.WriteJSON(w, http.StatusBadRequest, notification.ConnectorErrorResponse{
					Error:          err.Error(),
					Code:           "unsupported_provider",
					Classification: notification.FailureClassMisconfigured,
				})
				return
			}
			if err := resolveEmailProviderConfig(&req); err != nil {
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
			outcome := "accepted"
			if sendErr != nil {
				outcome = "send_failed"
				registry.IncCounter("connector_send_requests_total", "Outbound connector send outcomes.", map[string]string{"connector": info.Name, "provider": req.ProviderKey, "outcome": outcome, "classification": string(sendErr.classification)})
				registry.ObserveHistogram("connector_provider_request_duration_seconds", "Outbound provider request duration in seconds.", map[string]string{"connector": info.Name, "provider": req.ProviderKey, "outcome": outcome}, metrics.DefaultLatencyBuckets(), time.Since(sendStarted).Seconds())
				httpx.WriteJSON(w, sendErr.statusCode, notification.ConnectorErrorResponse{
					Error:          sendErr.message,
					Code:           sendErr.code,
					Classification: sendErr.classification,
					Retryable:      sendErr.retryable,
				})
				return
			}
			registry.IncCounter("connector_send_requests_total", "Outbound connector send outcomes.", map[string]string{"connector": info.Name, "provider": req.ProviderKey, "outcome": outcome})
			registry.ObserveHistogram("connector_provider_request_duration_seconds", "Outbound provider request duration in seconds.", map[string]string{"connector": info.Name, "provider": req.ProviderKey, "outcome": outcome}, metrics.DefaultLatencyBuckets(), time.Since(sendStarted).Seconds())
			logger.Info("sent email provider request", "request_id", req.RequestID, "provider", req.ProviderKey, "endpoint", outbound.URL)
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

type emailAdapter interface {
	validate(req notification.ConnectorSendRequest) *emailAdapterError
	build(req notification.ConnectorSendRequest) (providerOutboundRequest, *emailAdapterError)
	send(ctx context.Context, client *http.Client, outbound providerOutboundRequest) (string, *emailAdapterError)
}

type sendgridEmailAdapter struct{}

type smtpEmailAdapter struct{}

type providerOutboundRequest struct {
	Transport   string
	URL         string
	Method      string
	Headers     map[string]string
	Body        []byte
	SMTPHost    string
	SMTPAuth    smtp.Auth
	SMTPFrom    string
	SMTPTo      string
	SMTPSubject string
}

type sendgridOutboundPayload struct {
	Personalizations []sendgridPersonalization `json:"personalizations"`
	From             sendgridAddress           `json:"from"`
	Subject          string                    `json:"subject,omitempty"`
	Content          []sendgridContent         `json:"content"`
}

type sendgridPersonalization struct {
	To []sendgridAddress `json:"to"`
}

type sendgridAddress struct {
	Email string `json:"email"`
}

type sendgridContent struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

type emailAdapterError struct {
	statusCode     int
	message        string
	code           string
	classification notification.FailureClass
	retryable      bool
}

func resolveEmailProviderConfig(req *notification.ConnectorSendRequest) *emailAdapterError {
	resolved, err := secrets.ResolveConfig(req.ProviderConfig, req.ProviderSecretRefs)
	if err != nil {
		return &emailAdapterError{
			statusCode:     http.StatusBadRequest,
			message:        "failed to resolve provider secrets: " + err.Error(),
			code:           "provider_secret_resolution_failed",
			classification: notification.FailureClassMisconfigured,
		}
	}
	req.ProviderConfig = resolved
	return nil
}

func selectEmailAdapter(providerKey string) (emailAdapter, error) {
	switch providerKey {
	case "", "sendgrid-email":
		return sendgridEmailAdapter{}, nil
	case "smtp-email":
		return smtpEmailAdapter{}, nil
	default:
		return nil, fmt.Errorf("unsupported email provider %q", providerKey)
	}
}

func (sendgridEmailAdapter) validate(req notification.ConnectorSendRequest) *emailAdapterError {
	if req.Destination == "" || !strings.Contains(req.Destination, "@") {
		return &emailAdapterError{
			statusCode:     http.StatusBadRequest,
			message:        "invalid email destination",
			code:           "invalid_destination",
			classification: notification.FailureClassInvalidRequest,
		}
	}
	if token := req.ProviderConfig["api_key"]; token == "unauthorized" {
		return &emailAdapterError{
			statusCode:     http.StatusUnauthorized,
			message:        "provider rejected email credentials",
			code:           "invalid_credentials",
			classification: notification.FailureClassUnauthorized,
		}
	}
	return nil
}

func (sendgridEmailAdapter) build(req notification.ConnectorSendRequest) (providerOutboundRequest, *emailAdapterError) {
	fromEmail := req.ProviderConfig["from_email"]
	apiKey := req.ProviderConfig["api_key"]
	baseURL := req.ProviderConfig["base_url"]
	if fromEmail == "" {
		return providerOutboundRequest{}, &emailAdapterError{
			statusCode:     http.StatusBadRequest,
			message:        "missing sendgrid from_email",
			code:           "missing_provider_config",
			classification: notification.FailureClassMisconfigured,
		}
	}
	if apiKey == "" {
		return providerOutboundRequest{}, &emailAdapterError{
			statusCode:     http.StatusBadRequest,
			message:        "missing sendgrid api_key",
			code:           "missing_provider_config",
			classification: notification.FailureClassMisconfigured,
		}
	}
	if baseURL == "" {
		baseURL = "https://api.sendgrid.com"
	}
	endpoint, err := joinProviderURL(baseURL, "/v3/mail/send")
	if err != nil {
		return providerOutboundRequest{}, &emailAdapterError{
			statusCode:     http.StatusBadRequest,
			message:        "invalid sendgrid base_url",
			code:           "invalid_provider_config",
			classification: notification.FailureClassMisconfigured,
		}
	}

	subject := req.Subject
	if subject == "" {
		subject = "Notification"
	}

	payload := sendgridOutboundPayload{
		Personalizations: []sendgridPersonalization{
			{
				To: []sendgridAddress{{Email: req.Destination}},
			},
		},
		From:    sendgridAddress{Email: fromEmail},
		Subject: subject,
		Content: []sendgridContent{
			{Type: "text/plain", Value: req.Body},
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return providerOutboundRequest{}, &emailAdapterError{
			statusCode:     http.StatusInternalServerError,
			message:        "failed to encode sendgrid payload",
			code:           "encode_failed",
			classification: notification.FailureClassTransient,
		}
	}

	return providerOutboundRequest{
		Transport: "http",
		URL:       endpoint,
		Method:    http.MethodPost,
		Headers: map[string]string{
			"Authorization": "Bearer " + apiKey,
			"Content-Type":  "application/json",
		},
		Body: body,
	}, nil
}

func (sendgridEmailAdapter) send(ctx context.Context, client *http.Client, outbound providerOutboundRequest) (string, *emailAdapterError) {
	messageID, err := executeProviderRequest(ctx, client, outbound, "email")
	if err != nil {
		return "", err
	}
	return messageID, nil
}

func (smtpEmailAdapter) validate(req notification.ConnectorSendRequest) *emailAdapterError {
	if req.Destination == "" || !strings.Contains(req.Destination, "@") {
		return &emailAdapterError{
			statusCode:     http.StatusBadRequest,
			message:        "invalid email destination",
			code:           "invalid_destination",
			classification: notification.FailureClassInvalidRequest,
		}
	}
	return nil
}

func (smtpEmailAdapter) build(req notification.ConnectorSendRequest) (providerOutboundRequest, *emailAdapterError) {
	host := req.ProviderConfig["host"]
	user := req.ProviderConfig["user"]
	password := req.ProviderConfig["password"]
	fromEmail := req.ProviderConfig["from_email"]
	port := req.ProviderConfig["port"]
	if host == "" || user == "" || password == "" {
		return providerOutboundRequest{}, &emailAdapterError{
			statusCode:     http.StatusBadRequest,
			message:        "missing smtp provider config",
			code:           "missing_provider_config",
			classification: notification.FailureClassMisconfigured,
		}
	}
	if fromEmail == "" {
		fromEmail = user
	}
	if port == "" {
		port = "587"
	}
	subject := req.Subject
	if subject == "" {
		subject = "Notification"
	}
	var body bytes.Buffer
	body.WriteString("From: " + fromEmail + "\r\n")
	body.WriteString("To: " + req.Destination + "\r\n")
	body.WriteString("Subject: " + subject + "\r\n")
	body.WriteString("MIME-Version: 1.0\r\n")
	body.WriteString("Content-Type: text/plain; charset=UTF-8\r\n")
	body.WriteString("\r\n")
	body.WriteString(req.Body)

	return providerOutboundRequest{
		Transport:   "smtp",
		SMTPHost:    net.JoinHostPort(host, port),
		SMTPAuth:    smtp.PlainAuth("", user, password, host),
		SMTPFrom:    fromEmail,
		SMTPTo:      req.Destination,
		SMTPSubject: subject,
		Body:        body.Bytes(),
	}, nil
}

func (smtpEmailAdapter) send(ctx context.Context, client *http.Client, outbound providerOutboundRequest) (string, *emailAdapterError) {
	messageID, err := executeProviderRequest(ctx, client, outbound, "email")
	if err != nil {
		return "", err
	}
	return messageID, nil
}

func executeProviderRequest(ctx context.Context, client *http.Client, outbound providerOutboundRequest, channelLabel string) (string, *emailAdapterError) {
	if outbound.Transport == "smtp" {
		return executeSMTPProviderRequest(ctx, outbound, channelLabel)
	}
	if client == nil {
		client = http.DefaultClient
	}
	req, err := http.NewRequestWithContext(ctx, outbound.Method, outbound.URL, bytes.NewReader(outbound.Body))
	if err != nil {
		return "", &emailAdapterError{
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
		return "", &emailAdapterError{
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
		return "", providerErrorFromHTTPResponse(resp.StatusCode, string(body), channelLabel)
	}

	return providerMessageIDFromResponse(resp.Header, body), nil
}

func executeSMTPProviderRequest(_ context.Context, outbound providerOutboundRequest, channelLabel string) (string, *emailAdapterError) {
	if outbound.SMTPHost == "" || outbound.SMTPFrom == "" || outbound.SMTPTo == "" {
		return "", &emailAdapterError{
			statusCode:     http.StatusBadRequest,
			message:        "missing smtp transport details",
			code:           "missing_provider_config",
			classification: notification.FailureClassMisconfigured,
		}
	}
	if err := smtp.SendMail(outbound.SMTPHost, outbound.SMTPAuth, outbound.SMTPFrom, []string{outbound.SMTPTo}, outbound.Body); err != nil {
		return "", &emailAdapterError{
			statusCode:     http.StatusBadGateway,
			message:        channelLabel + " provider request failed: " + err.Error(),
			code:           "provider_request_failed",
			classification: notification.FailureClassTransient,
			retryable:      true,
		}
	}
	return providerMessageID(), nil
}

func providerErrorFromHTTPResponse(statusCode int, responseBody string, channelLabel string) *emailAdapterError {
	switch statusCode {
	case http.StatusUnauthorized, http.StatusForbidden:
		return &emailAdapterError{
			statusCode:     statusCode,
			message:        channelLabel + " provider rejected credentials",
			code:           "invalid_credentials",
			classification: notification.FailureClassUnauthorized,
		}
	case http.StatusTooManyRequests:
		return &emailAdapterError{
			statusCode:     statusCode,
			message:        channelLabel + " provider rate limited the request",
			code:           "rate_limited",
			classification: notification.FailureClassRateLimited,
			retryable:      true,
		}
	case http.StatusBadRequest, http.StatusUnprocessableEntity:
		return &emailAdapterError{
			statusCode:     statusCode,
			message:        channelLabel + " provider rejected the request: " + trimResponse(responseBody),
			code:           "invalid_provider_request",
			classification: notification.FailureClassInvalidRequest,
		}
	default:
		return &emailAdapterError{
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
		for _, key := range []string{"message_id", "id", "messageId"} {
			if value, ok := parsed[key].(string); ok && value != "" {
				return value
			}
		}
	}
	return providerMessageID()
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
