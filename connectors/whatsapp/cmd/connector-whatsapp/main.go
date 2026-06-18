package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
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
	cfg, err := config.LoadHTTPServiceConfig("connector-whatsapp", 8095)
	if err != nil {
		panic(err)
	}

	logger := logging.New(cfg.ServiceName)
	registry := metrics.NewRegistry(cfg.ServiceName)
	providerClient := &http.Client{Timeout: 5 * time.Second}

	err = app.RunHTTPService(cfg, logger, registry, func(mux *http.ServeMux, info serviceinfo.Info, registry *metrics.Registry) {
		mux.HandleFunc("GET /v1/capabilities", func(w http.ResponseWriter, _ *http.Request) {
			httpx.WriteJSON(w, http.StatusOK, notification.ConnectorCapabilities{
				Name:     "whatsapp",
				Channels: []notification.Channel{notification.ChannelWhatsApp},
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

			adapter, err := selectWhatsAppAdapter(req.ProviderKey)
			if err != nil {
				registry.IncCounter("connector_send_requests_total", "Outbound connector send outcomes.", map[string]string{"connector": info.Name, "provider": req.ProviderKey, "outcome": "unsupported_provider"})
				httpx.WriteJSON(w, http.StatusBadRequest, notification.ConnectorErrorResponse{
					Error:          err.Error(),
					Code:           "unsupported_provider",
					Classification: notification.FailureClassMisconfigured,
				})
				return
			}
			if err := resolveWhatsAppProviderConfig(&req); err != nil {
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

			logger.Info("sent whatsapp provider request", "request_id", req.RequestID, "provider", req.ProviderKey, "endpoint", outbound.URL)
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

type whatsappAdapter interface {
	validate(req notification.ConnectorSendRequest) *whatsappAdapterError
	build(req notification.ConnectorSendRequest) (providerOutboundRequest, *whatsappAdapterError)
	send(ctx context.Context, client *http.Client, outbound providerOutboundRequest) (string, *whatsappAdapterError)
}

type gupshupWhatsAppAdapter struct{}

type karixWhatsAppAdapter struct{}

var whatsappAdapters = map[string]whatsappAdapter{
	"":                 gupshupWhatsAppAdapter{},
	"gupshup-whatsapp": gupshupWhatsAppAdapter{},
	"karix-whatsapp":   karixWhatsAppAdapter{},
}

type providerOutboundRequest struct {
	URL     string
	Method  string
	Headers map[string]string
	Body    []byte
}

type whatsappMediaSpec struct {
	Type     string
	URL      string
	FileName string
	Title    string
}

func whatsappMediaSpecFromRequest(req notification.ConnectorSendRequest) whatsappMediaSpec {
	return whatsappMediaSpec{
		Type:     normalizeWhatsAppMediaType(firstNonEmpty(req.Metadata["media_type"], req.Metadata["media_content_type"], req.Metadata["content_type"])),
		URL:      firstNonEmpty(req.Metadata["media_url"], req.Metadata["media_link"]),
		FileName: firstNonEmpty(req.Metadata["media_file_name"], req.Metadata["media_name"], req.Metadata["filename"]),
		Title:    firstNonEmpty(req.Metadata["media_title"], req.Metadata["title"]),
	}
}

func (m whatsappMediaSpec) present() bool {
	return m.Type != "" || m.URL != "" || m.FileName != "" || m.Title != ""
}

func (m whatsappMediaSpec) isText() bool {
	return m.Type == "" || m.Type == "text"
}

func normalizeWhatsAppMediaType(mediaType string) string {
	mediaType = strings.ToLower(strings.TrimSpace(mediaType))
	switch mediaType {
	case "photo":
		return "image"
	case "file", "doc":
		return "document"
	default:
		return mediaType
	}
}

type whatsappAdapterError struct {
	statusCode     int
	message        string
	code           string
	classification notification.FailureClass
	retryable      bool
}

func resolveWhatsAppProviderConfig(req *notification.ConnectorSendRequest) *whatsappAdapterError {
	resolved, err := secrets.ResolveConfig(req.ProviderConfig, req.ProviderSecretRefs)
	if err != nil {
		return &whatsappAdapterError{
			statusCode:     http.StatusBadRequest,
			message:        "failed to resolve provider secrets: " + err.Error(),
			code:           "provider_secret_resolution_failed",
			classification: notification.FailureClassMisconfigured,
		}
	}
	req.ProviderConfig = resolved
	return nil
}

func selectWhatsAppAdapter(providerKey string) (whatsappAdapter, error) {
	adapter, ok := whatsappAdapters[providerKey]
	if !ok {
		return nil, fmt.Errorf("unsupported whatsapp provider %q", providerKey)
	}
	return adapter, nil
}

func (gupshupWhatsAppAdapter) validate(req notification.ConnectorSendRequest) *whatsappAdapterError {
	if normalizeDestination(req.Destination) == "" {
		return &whatsappAdapterError{
			statusCode:     http.StatusBadRequest,
			message:        "invalid whatsapp destination",
			code:           "invalid_destination",
			classification: notification.FailureClassInvalidRequest,
		}
	}
	if token := req.ProviderConfig["password"]; token == "unauthorized" {
		return &whatsappAdapterError{
			statusCode:     http.StatusUnauthorized,
			message:        "provider rejected whatsapp credentials",
			code:           "invalid_credentials",
			classification: notification.FailureClassUnauthorized,
		}
	}
	return whatsappAdapterErrorIfSimulated(req)
}

func (gupshupWhatsAppAdapter) build(req notification.ConnectorSendRequest) (providerOutboundRequest, *whatsappAdapterError) {
	username := req.ProviderConfig["username"]
	password := req.ProviderConfig["password"]
	version := req.ProviderConfig["version"]
	baseURL := req.ProviderConfig["base_url"]
	if username == "" || password == "" || version == "" {
		return providerOutboundRequest{}, &whatsappAdapterError{
			statusCode:     http.StatusBadRequest,
			message:        "missing gupshup whatsapp provider config",
			code:           "missing_provider_config",
			classification: notification.FailureClassMisconfigured,
		}
	}
	if baseURL == "" {
		baseURL = "https://media.smsgupshup.com/GatewayAPI/rest"
	}
	media := whatsappMediaSpecFromRequest(req)
	if isGupshupMediaTemplate(req.Metadata) {
		return buildGupshupWhatsAppTemplateRequest(req, baseURL)
	}

	body := url.Values{}
	interactive := parseInteractiveAttributes(req.Metadata["interactive_attributes"])
	isTemplate := false
	if media.present() && !media.isText() {
		body.Set("method", "SendMediaMessage")
		body.Set("msg_type", strings.ToUpper(media.Type))
		body.Set("caption", req.Body)
		if media.URL != "" {
			body.Set("media_url", media.URL)
		}
		if strings.EqualFold(media.Type, "document") && media.FileName != "" {
			body.Set("filename", media.FileName)
		}
	} else {
		body.Set("method", "SendMessage")
		body.Set("format", "text")
		body.Set("msg", req.Body)
	}
	if interactive != nil {
		if strings.EqualFold(interactive.ButtonCategory, "CallToAction") || strings.EqualFold(interactive.ButtonCategory, "QuickReply") {
			isTemplate = true
		}
		if interactive.Headers != "" {
			body.Set("header", substituteHeaderExample(interactive.Headers, interactive.HeaderExamples, req.TemplateVariables))
			isTemplate = true
		}
		if interactive.Footer != "" {
			body.Set("footer", interactive.Footer)
			isTemplate = true
		}
	}
	body.Set("userid", username)
	body.Set("password", password)
	body.Set("send_to", normalizeDestination(req.Destination))
	body.Set("v", version)
	body.Set("auth_scheme", "plain")
	if isTemplate {
		body.Set("isTemplate", "true")
	}
	body.Set("isHSM", "true")

	return providerOutboundRequest{
		URL:    baseURL,
		Method: http.MethodPost,
		Headers: map[string]string{
			"Content-Type": "application/x-www-form-urlencoded",
			"Accept":       "*/*",
		},
		Body: []byte(body.Encode()),
	}, nil
}

func isGupshupMediaTemplate(metadata map[string]string) bool {
	mediaType := normalizeWhatsAppMediaType(firstNonEmpty(metadata["media_type"], metadata["media_content_type"], metadata["content_type"]))
	if mediaType == "" || mediaType == "text" {
		return false
	}
	return firstNonEmpty(metadata["gupshup_template_id"], metadata["gupshup_template_name"], metadata["template_id"], metadata["template_name"]) != ""
}

type gupshupTemplateSendRequest struct {
	Source      string                      `json:"source"`
	Destination string                      `json:"destination"`
	Channel     string                      `json:"channel,omitempty"`
	Template    gupshupTemplateDescriptor   `json:"template"`
	Message     gupshupTemplateMediaMessage `json:"message"`
}

type gupshupTemplateDescriptor struct {
	ID     string   `json:"id"`
	Params []string `json:"params,omitempty"`
}

type gupshupTemplateMediaMessage struct {
	Type     string                          `json:"type"`
	Image    *gupshupTemplateMediaReference  `json:"image,omitempty"`
	Video    *gupshupTemplateMediaReference  `json:"video,omitempty"`
	Document *gupshupTemplateDocumentMessage `json:"document,omitempty"`
	Audio    *gupshupTemplateMediaReference  `json:"audio,omitempty"`
	Sticker  *gupshupTemplateMediaReference  `json:"sticker,omitempty"`
}

type gupshupTemplateMediaReference struct {
	Link string `json:"link"`
}

type gupshupTemplateDocumentMessage struct {
	Link     string `json:"link"`
	Filename string `json:"filename,omitempty"`
}

func buildGupshupWhatsAppTemplateRequest(req notification.ConnectorSendRequest, baseURL string) (providerOutboundRequest, *whatsappAdapterError) {
	templateID := firstNonEmpty(req.Metadata["gupshup_template_id"], req.Metadata["gupshup_template_name"], req.Metadata["template_id"], req.Metadata["template_name"])
	source := firstNonEmpty(req.ProviderConfig["source"], req.ProviderConfig["sender"], req.ProviderConfig["username"])
	if templateID == "" || source == "" {
		return providerOutboundRequest{}, &whatsappAdapterError{
			statusCode:     http.StatusBadRequest,
			message:        "missing gupshup whatsapp template config",
			code:           "missing_provider_config",
			classification: notification.FailureClassMisconfigured,
		}
	}

	media := whatsappMediaSpecFromRequest(req)
	if media.isText() {
		return providerOutboundRequest{}, &whatsappAdapterError{
			statusCode:     http.StatusBadRequest,
			message:        "gupshup whatsapp template send requires media_type",
			code:           "missing_media_type",
			classification: notification.FailureClassMisconfigured,
		}
	}
	if media.URL == "" {
		return providerOutboundRequest{}, &whatsappAdapterError{
			statusCode:     http.StatusBadRequest,
			message:        "gupshup whatsapp template send requires media_url",
			code:           "missing_media_url",
			classification: notification.FailureClassMisconfigured,
		}
	}

	params := gupshupTemplateParams(req)
	message := gupshupTemplateMediaMessage{Type: media.Type}
	switch media.Type {
	case "image":
		message.Image = &gupshupTemplateMediaReference{Link: media.URL}
	case "video":
		message.Video = &gupshupTemplateMediaReference{Link: media.URL}
	case "document":
		message.Document = &gupshupTemplateDocumentMessage{
			Link:     media.URL,
			Filename: media.FileName,
		}
	case "audio":
		message.Audio = &gupshupTemplateMediaReference{Link: media.URL}
	case "sticker":
		message.Sticker = &gupshupTemplateMediaReference{Link: media.URL}
	default:
		return providerOutboundRequest{}, &whatsappAdapterError{
			statusCode:     http.StatusBadRequest,
			message:        "unsupported gupshup whatsapp media type",
			code:           "unsupported_media_type",
			classification: notification.FailureClassInvalidRequest,
		}
	}

	payload := gupshupTemplateSendRequest{
		Source:      source,
		Destination: normalizeDestination(req.Destination),
		Channel:     "whatsapp",
		Template: gupshupTemplateDescriptor{
			ID:     templateID,
			Params: params,
		},
		Message: message,
	}
	endpoint := req.ProviderConfig["template_url"]
	if endpoint == "" {
		endpoint = resolveGupshupTemplateEndpoint(baseURL)
	}

	return providerOutboundRequest{
		URL:    endpoint,
		Method: http.MethodPost,
		Headers: map[string]string{
			"Content-Type": "application/x-www-form-urlencoded",
			"Accept":       "*/*",
		},
		Body: []byte(url.Values{
			"source":      []string{payload.Source},
			"destination": []string{payload.Destination},
			"template":    []string{string(bodyFieldJSON(payload.Template))},
			"message":     []string{string(bodyFieldJSON(payload.Message))},
			"channel":     []string{payload.Channel},
		}.Encode()),
	}, nil
}

func bodyFieldJSON(value any) []byte {
	body, _ := json.Marshal(value)
	return body
}

func resolveGupshupTemplateEndpoint(baseURL string) string {
	baseURL = strings.TrimSpace(baseURL)
	if baseURL == "" {
		return "https://api.gupshup.io/wa/api/v1/template/msg"
	}
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return strings.TrimRight(baseURL, "/") + "/wa/api/v1/template/msg"
	}
	if strings.Contains(strings.ToLower(parsed.Path), "/gatewayapi/rest") {
		parsed.Path = "/wa/api/v1/template/msg"
	} else {
		parsed.Path = strings.TrimRight(parsed.Path, "/") + "/wa/api/v1/template/msg"
	}
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String()
}

func gupshupTemplateParams(req notification.ConnectorSendRequest) []string {
	if len(req.Metadata) == 0 && len(req.TemplateVariables) == 0 {
		if strings.TrimSpace(req.Body) == "" {
			return nil
		}
		return []string{req.Body}
	}

	indexedKeys := make([]int, 0, len(req.Metadata))
	indexedValues := make(map[int]string, len(req.Metadata))
	for key, variableName := range req.Metadata {
		idx, err := strconv.Atoi(strings.TrimSpace(key))
		if err != nil {
			continue
		}
		indexedKeys = append(indexedKeys, idx)
		if value := req.TemplateVariables[variableName]; value != "" {
			indexedValues[idx] = value
			continue
		}
		if strings.TrimSpace(req.Body) != "" && len(req.TemplateVariables) == 0 {
			indexedValues[idx] = req.Body
			continue
		}
		indexedValues[idx] = variableName
	}
	if len(indexedKeys) > 0 {
		sort.Ints(indexedKeys)
		params := make([]string, 0, len(indexedKeys))
		for _, idx := range indexedKeys {
			if value := indexedValues[idx]; value != "" {
				params = append(params, value)
			}
		}
		if len(params) > 0 {
			return params
		}
	}

	if len(req.TemplateVariables) == 1 {
		for _, value := range req.TemplateVariables {
			if value != "" {
				return []string{value}
			}
		}
	}

	keys := make([]string, 0, len(req.TemplateVariables))
	for key := range req.TemplateVariables {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	params := make([]string, 0, len(keys))
	for _, key := range keys {
		if value := req.TemplateVariables[key]; value != "" {
			params = append(params, value)
		}
	}
	if len(params) > 0 {
		return params
	}
	if strings.TrimSpace(req.Body) != "" {
		return []string{req.Body}
	}
	return nil
}

func (gupshupWhatsAppAdapter) send(ctx context.Context, client *http.Client, outbound providerOutboundRequest) (string, *whatsappAdapterError) {
	if client == nil {
		client = http.DefaultClient
	}
	req, err := http.NewRequestWithContext(ctx, outbound.Method, outbound.URL, bytes.NewReader(outbound.Body))
	if err != nil {
		return "", &whatsappAdapterError{
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
		return "", &whatsappAdapterError{
			statusCode:     http.StatusBadGateway,
			message:        "whatsapp provider request failed: " + err.Error(),
			code:           "provider_request_failed",
			classification: notification.FailureClassTransient,
			retryable:      true,
		}
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return "", whatsappProviderErrorFromHTTPResponse(resp.StatusCode, string(body))
	}
	return providerMessageIDFromResponse(resp.Header, body), nil
}

type karixWhatsAppRequest struct {
	Message  karixWhatsAppMessage  `json:"message"`
	MetaData karixWhatsAppMetaData `json:"metaData"`
}

type karixWhatsAppMetaData struct {
	Version string `json:"version"`
}

type karixWhatsAppMessage struct {
	Channel     string                   `json:"channel"`
	Content     karixWhatsAppContent     `json:"content"`
	Recipient   karixWhatsAppRecipient   `json:"recipient"`
	Sender      karixWhatsAppSender      `json:"sender"`
	Preferences karixWhatsAppPreferences `json:"preferences"`
}

type karixWhatsAppRecipient struct {
	To            string `json:"to"`
	RecipientType string `json:"recipient_type"`
}

type karixWhatsAppSender struct {
	From string `json:"from"`
}

type karixWhatsAppPreferences struct {
	WebHookDNID string `json:"webHookDNId"`
}

type karixWhatsAppContent struct {
	PreviewURL    bool                          `json:"preview_url"`
	Type          string                        `json:"type"`
	Template      *karixWhatsAppTemplateContent `json:"template,omitempty"`
	MediaTemplate *karixWhatsAppMediaTemplate   `json:"mediaTemplate,omitempty"`
}

type karixWhatsAppTemplateContent struct {
	TemplateID      string            `json:"templateId"`
	ParameterValues map[string]string `json:"parameterValues"`
}

type karixWhatsAppMediaTemplate struct {
	TemplateID          string                `json:"templateId"`
	BodyParameterValues map[string]string     `json:"bodyParameterValues"`
	Media               *karixWhatsAppMedia   `json:"media,omitempty"`
	Buttons             *karixWhatsAppButtons `json:"buttons,omitempty"`
}

type karixWhatsAppMedia struct {
	Type     string `json:"type"`
	URL      string `json:"url,omitempty"`
	FileName string `json:"fileName,omitempty"`
	Title    string `json:"title,omitempty"`
}

type karixWhatsAppButtons struct {
	QuickReplies []karixWhatsAppQuickReply `json:"quickReplies,omitempty"`
	Actions      []karixWhatsAppAction     `json:"actions,omitempty"`
}

type karixWhatsAppQuickReply struct {
	Index   string `json:"index"`
	Payload string `json:"payload"`
}

type karixWhatsAppAction struct {
	Index   string `json:"index"`
	Payload string `json:"payload"`
	Type    string `json:"type"`
}

func (karixWhatsAppAdapter) validate(req notification.ConnectorSendRequest) *whatsappAdapterError {
	if normalizeDestination(req.Destination) == "" {
		return &whatsappAdapterError{
			statusCode:     http.StatusBadRequest,
			message:        "invalid whatsapp destination",
			code:           "invalid_destination",
			classification: notification.FailureClassInvalidRequest,
		}
	}
	if token := req.ProviderConfig["key"]; token == "unauthorized" {
		return &whatsappAdapterError{
			statusCode:     http.StatusUnauthorized,
			message:        "provider rejected whatsapp credentials",
			code:           "invalid_credentials",
			classification: notification.FailureClassUnauthorized,
		}
	}
	return whatsappAdapterErrorIfSimulated(req)
}

func (karixWhatsAppAdapter) build(req notification.ConnectorSendRequest) (providerOutboundRequest, *whatsappAdapterError) {
	key := req.ProviderConfig["key"]
	sender := req.ProviderConfig["sender"]
	if sender == "" {
		sender = req.ProviderConfig["from"]
	}
	baseURL := req.ProviderConfig["base_url"]
	if baseURL == "" {
		baseURL = "https://rcmapi.instaalerts.zone/services/rcm/sendMessage"
	}
	version := req.ProviderConfig["version"]
	if version == "" {
		version = "v1.0.9"
	}
	templateName := req.ProviderConfig["template_name"]
	if templateName == "" {
		templateName = req.Metadata["karix_template_name"]
	}
	if templateName == "" {
		templateName = req.ProviderConfig["template_id"]
	}
	if templateName == "" {
		templateName = req.Metadata["template_id"]
	}
	if key == "" || sender == "" || templateName == "" {
		return providerOutboundRequest{}, &whatsappAdapterError{
			statusCode:     http.StatusBadRequest,
			message:        "missing karix whatsapp provider config",
			code:           "missing_provider_config",
			classification: notification.FailureClassMisconfigured,
		}
	}
	media := whatsappMediaSpecFromRequest(req)

	bodyParameterValues := map[string]string{}
	for key, value := range req.TemplateVariables {
		if value != "" {
			bodyParameterValues[key] = value
		}
	}
	for key, value := range req.Metadata {
		switch key {
		case "template_id", "karix_template_name", "gupshup_template_id", "gupshup_template_name", "media_type", "media_url", "media_title", "media_file_name", "button_category", "interactive_attributes":
			continue
		default:
			if value != "" {
				bodyParameterValues[key] = value
			}
		}
	}
	if len(bodyParameterValues) == 0 {
		bodyParameterValues["otp"] = req.Body
	}

	content := karixWhatsAppContent{
		PreviewURL: false,
		Type:       "TEMPLATE",
		Template: &karixWhatsAppTemplateContent{
			TemplateID:      templateName,
			ParameterValues: bodyParameterValues,
		},
	}
	if media.present() {
		content.Template = nil
		content.Type = "MEDIA_TEMPLATE"
		buttons := parseKarixButtons(req.Metadata["interactive_attributes"])
		content.MediaTemplate = &karixWhatsAppMediaTemplate{
			TemplateID:          templateName,
			BodyParameterValues: bodyParameterValues,
			Media: &karixWhatsAppMedia{
				Type:     firstNonEmpty(media.Type, "image"),
				URL:      media.URL,
				FileName: media.FileName,
				Title:    media.Title,
			},
			Buttons: buttons,
		}
	}

	payload := karixWhatsAppRequest{
		MetaData: karixWhatsAppMetaData{Version: version},
		Message: karixWhatsAppMessage{
			Channel: "WABA",
			Content: content,
			Recipient: karixWhatsAppRecipient{
				To:            normalizeDestination(req.Destination),
				RecipientType: "individual",
			},
			Sender:      karixWhatsAppSender{From: sender},
			Preferences: karixWhatsAppPreferences{WebHookDNID: "1001"},
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return providerOutboundRequest{}, &whatsappAdapterError{
			statusCode:     http.StatusInternalServerError,
			message:        "failed to encode karix whatsapp payload",
			code:           "encode_failed",
			classification: notification.FailureClassTransient,
			retryable:      true,
		}
	}

	return providerOutboundRequest{
		URL:    baseURL,
		Method: http.MethodPost,
		Headers: map[string]string{
			"Authentication": "Bearer " + key,
			"Content-Type":   "application/json",
			"Accept":         "*/*",
		},
		Body: body,
	}, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

type karixInteractiveAttributes struct {
	ButtonCategory string `json:"button_category"`
	Headers        string `json:"headers"`
	Footer         string `json:"footer"`
	Buttons        []struct {
		Type     string `json:"type"`
		URLType  string `json:"urlType"`
		URL      string `json:"url"`
		Text     string `json:"text"`
		PhoneNum string `json:"phone_number"`
	} `json:"buttons"`
}

func parseKarixButtons(raw string) *karixWhatsAppButtons {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	var attrs karixInteractiveAttributes
	if err := json.Unmarshal([]byte(raw), &attrs); err != nil {
		return nil
	}
	if !strings.EqualFold(attrs.ButtonCategory, "CallToAction") || len(attrs.Buttons) == 0 {
		return nil
	}
	buttons := &karixWhatsAppButtons{}
	for idx, button := range attrs.Buttons {
		index := fmt.Sprintf("%d", idx)
		switch strings.ToLower(strings.TrimSpace(button.Type)) {
		case "url":
			buttons.Actions = append(buttons.Actions, karixWhatsAppAction{
				Index:   index,
				Payload: firstNonEmpty(button.URL, button.Text),
				Type:    "url",
			})
		case "phone_number":
			buttons.Actions = append(buttons.Actions, karixWhatsAppAction{
				Index:   index,
				Payload: firstNonEmpty(button.PhoneNum, button.Text),
				Type:    "phone_number",
			})
		default:
			buttons.QuickReplies = append(buttons.QuickReplies, karixWhatsAppQuickReply{
				Index:   index,
				Payload: firstNonEmpty(button.Text, button.URL),
			})
		}
	}
	if len(buttons.Actions) == 0 && len(buttons.QuickReplies) == 0 {
		return nil
	}
	return buttons
}

func (karixWhatsAppAdapter) send(ctx context.Context, client *http.Client, outbound providerOutboundRequest) (string, *whatsappAdapterError) {
	if client == nil {
		client = http.DefaultClient
	}
	req, err := http.NewRequestWithContext(ctx, outbound.Method, outbound.URL, bytes.NewReader(outbound.Body))
	if err != nil {
		return "", &whatsappAdapterError{
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
		return "", &whatsappAdapterError{
			statusCode:     http.StatusBadGateway,
			message:        "whatsapp provider request failed: " + err.Error(),
			code:           "provider_request_failed",
			classification: notification.FailureClassTransient,
			retryable:      true,
		}
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return "", whatsappProviderErrorFromHTTPResponse(resp.StatusCode, string(body))
	}
	return providerMessageIDFromResponse(resp.Header, body), nil
}

func whatsappAdapterErrorIfSimulated(req notification.ConnectorSendRequest) *whatsappAdapterError {
	return nil
}

type gupshupInteractiveAttributes struct {
	ButtonCategory string `json:"button_category"`
	Headers        string `json:"headers"`
	HeaderExamples string `json:"header_examples"`
	Footer         string `json:"footer"`
}

func parseInteractiveAttributes(raw string) *gupshupInteractiveAttributes {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	var attrs gupshupInteractiveAttributes
	if err := json.Unmarshal([]byte(raw), &attrs); err != nil {
		return nil
	}
	if attrs.ButtonCategory == "" && attrs.Headers == "" && attrs.HeaderExamples == "" && attrs.Footer == "" {
		return nil
	}
	return &attrs
}

var interactiveHeaderPlaceholderPattern = regexp.MustCompile(`\{\{.*?\}\}`)

func substituteHeaderExample(header, headerExampleKey string, templateVariables map[string]string) string {
	header = strings.TrimSpace(header)
	if header == "" {
		return ""
	}
	headerExampleKey = strings.TrimSpace(headerExampleKey)
	if headerExampleKey == "" {
		return header
	}
	headerExample := templateVariables[headerExampleKey]
	if headerExample == "" {
		headerExample = headerExampleKey
	}
	return interactiveHeaderPlaceholderPattern.ReplaceAllString(header, headerExample)
}

func whatsappProviderErrorFromHTTPResponse(statusCode int, responseBody string) *whatsappAdapterError {
	switch statusCode {
	case http.StatusUnauthorized, http.StatusForbidden:
		return &whatsappAdapterError{
			statusCode:     statusCode,
			message:        "whatsapp provider rejected credentials",
			code:           "invalid_credentials",
			classification: notification.FailureClassUnauthorized,
		}
	case http.StatusTooManyRequests:
		return &whatsappAdapterError{
			statusCode:     statusCode,
			message:        "whatsapp provider rate limited the request",
			code:           "rate_limited",
			classification: notification.FailureClassRateLimited,
			retryable:      true,
		}
	case http.StatusBadRequest, http.StatusUnprocessableEntity:
		return &whatsappAdapterError{
			statusCode:     statusCode,
			message:        "whatsapp provider rejected the request: " + trimResponse(responseBody),
			code:           "invalid_provider_request",
			classification: notification.FailureClassInvalidRequest,
		}
	default:
		return &whatsappAdapterError{
			statusCode:     statusCode,
			message:        "whatsapp provider temporary outage",
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

func normalizeDestination(value string) string {
	value = strings.TrimSpace(value)
	value = strings.TrimPrefix(value, "+")
	return value
}

func trimResponse(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 160 {
		return value[:160]
	}
	return value
}
