package main

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
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
	cfg, err := config.LoadHTTPServiceConfig("connector-push", 8094)
	if err != nil {
		panic(err)
	}

	logger := logging.New(cfg.ServiceName)
	registry := metrics.NewRegistry(cfg.ServiceName)
	providerClient := &http.Client{Timeout: 5 * time.Second}

	err = app.RunHTTPService(cfg, logger, registry, func(mux *http.ServeMux, info serviceinfo.Info, registry *metrics.Registry) {
		mux.HandleFunc("GET /v1/capabilities", func(w http.ResponseWriter, _ *http.Request) {
			httpx.WriteJSON(w, http.StatusOK, notification.ConnectorCapabilities{
				Name:     "push",
				Channels: []notification.Channel{notification.ChannelPush},
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

			adapter := fcmAdapter{}
			if err := resolvePushProviderConfig(&req); err != nil {
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

func resolvePushProviderConfig(req *notification.ConnectorSendRequest) *fcmAdapterError {
	resolved, err := secrets.ResolveConfig(req.ProviderConfig, req.ProviderSecretRefs)
	if err != nil {
		return &fcmAdapterError{
			statusCode:     http.StatusBadRequest,
			message:        "failed to resolve provider secrets: " + err.Error(),
			code:           "provider_secret_resolution_failed",
			classification: notification.FailureClassMisconfigured,
		}
	}
	req.ProviderConfig = resolved
	return nil
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
			message:        "push provider temporary outage: " + trimResponse(responseBody),
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
	return accessTokenFromServiceAccount(parsed)
}

func accessTokenFromServiceAccount(serviceAccount map[string]any) string {
	clientEmail, _ := serviceAccount["client_email"].(string)
	privateKey, _ := serviceAccount["private_key"].(string)
	tokenURI, _ := serviceAccount["token_uri"].(string)
	if tokenURI == "" {
		tokenURI = "https://oauth2.googleapis.com/token"
	}
	if clientEmail == "" || privateKey == "" {
		return ""
	}
	signedJWT, err := signServiceAccountJWT(clientEmail, privateKey, tokenURI)
	if err != nil {
		return ""
	}
	accessToken, err := exchangeJWTForAccessToken(tokenURI, signedJWT)
	if err != nil {
		return ""
	}
	return accessToken
}

func signServiceAccountJWT(clientEmail, privateKey, tokenURI string) (string, error) {
	block, _ := pem.Decode([]byte(privateKey))
	if block == nil {
		return "", errors.New("invalid private key pem")
	}
	key, err := parsePrivateKey(block.Bytes)
	if err != nil {
		return "", err
	}
	now := time.Now().UTC().Unix()
	claims := map[string]any{
		"iss":   clientEmail,
		"scope": "https://www.googleapis.com/auth/firebase.messaging",
		"aud":   tokenURI,
		"iat":   now,
		"exp":   now + 3600,
	}
	header := map[string]any{
		"alg": "RS256",
		"typ": "JWT",
	}
	headerJSON, err := json.Marshal(header)
	if err != nil {
		return "", err
	}
	claimsJSON, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	encodedHeader := base64.RawURLEncoding.EncodeToString(headerJSON)
	encodedClaims := base64.RawURLEncoding.EncodeToString(claimsJSON)
	signingInput := encodedHeader + "." + encodedClaims
	digest := sha256.Sum256([]byte(signingInput))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		return "", err
	}
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}

func parsePrivateKey(keyPEM []byte) (*rsa.PrivateKey, error) {
	if parsed, err := x509.ParsePKCS1PrivateKey(keyPEM); err == nil {
		return parsed, nil
	}
	parsedKey, err := x509.ParsePKCS8PrivateKey(keyPEM)
	if err != nil {
		return nil, err
	}
	privateKey, ok := parsedKey.(*rsa.PrivateKey)
	if !ok {
		return nil, errors.New("private key is not rsa")
	}
	return privateKey, nil
}

func exchangeJWTForAccessToken(tokenURI, assertion string) (string, error) {
	values := url.Values{}
	values.Set("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer")
	values.Set("assertion", assertion)
	req, err := http.NewRequest(http.MethodPost, tokenURI, strings.NewReader(values.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return "", errors.New(trimResponse(string(body)))
	}
	var parsed map[string]any
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "", err
	}
	accessToken, _ := parsed["access_token"].(string)
	if accessToken == "" {
		return "", errors.New("missing access_token")
	}
	return accessToken, nil
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
