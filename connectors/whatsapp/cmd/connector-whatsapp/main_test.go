package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/NotifyHub-in/NotifyHub/libs/contracts/notification"
)

func TestGupshupWhatsAppAdapterSend(t *testing.T) {
	var gotBody url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %q, want POST", r.Method)
		}
		if r.URL.Path != "/" {
			t.Fatalf("path = %q, want /", r.URL.Path)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		gotBody = r.PostForm
		w.Header().Set("X-Provider-Message-ID", "wa-123")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	adapter := gupshupWhatsAppAdapter{}
	outbound, err := adapter.build(notification.ConnectorSendRequest{
		Destination: "+918700491033",
		Body:        "Hello WhatsApp",
		ProviderConfig: map[string]string{
			"username": "demo-user",
			"password": "demo-pass",
			"version":  "1.1",
			"base_url": server.URL,
		},
	})
	if err != nil {
		t.Fatalf("build returned error: %v", err)
	}

	messageID, sendErr := adapter.send(context.Background(), server.Client(), outbound)
	if sendErr != nil {
		t.Fatalf("send returned error: %v", sendErr)
	}
	if messageID != "wa-123" {
		t.Fatalf("messageID = %q, want %q", messageID, "wa-123")
	}
	if gotBody.Get("method") != "SendMessage" {
		t.Fatalf("method = %q, want SendMessage", gotBody.Get("method"))
	}
	if gotBody.Get("send_to") != "918700491033" {
		t.Fatalf("send_to = %q, want %q", gotBody.Get("send_to"), "918700491033")
	}
	if gotBody.Get("msg") != "Hello WhatsApp" {
		t.Fatalf("msg = %q, want %q", gotBody.Get("msg"), "Hello WhatsApp")
	}
	if gotBody.Get("v") != "1.1" {
		t.Fatalf("version = %q, want 1.1", gotBody.Get("v"))
	}
	if !strings.EqualFold(gotBody.Get("auth_scheme"), "plain") {
		t.Fatalf("auth_scheme = %q, want plain", gotBody.Get("auth_scheme"))
	}
}

func TestGupshupWhatsAppAdapterMediaPayloadMatchesCE(t *testing.T) {
	var gotBody url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		gotBody = r.PostForm
		w.Header().Set("X-Provider-Message-ID", "wa-media-123")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	adapter := gupshupWhatsAppAdapter{}
	outbound, err := adapter.build(notification.ConnectorSendRequest{
		Destination: "+918700491033",
		Body:        "AWD Program: Important Update",
		ProviderConfig: map[string]string{
			"username": "demo-user",
			"password": "demo-pass",
			"version":  "1.1",
			"base_url": server.URL,
		},
		Metadata: map[string]string{
			"media_type": "image",
			"media_url":  "https://example.com/image.jpg",
		},
	})
	if err != nil {
		t.Fatalf("build returned error: %v", err)
	}

	messageID, sendErr := adapter.send(context.Background(), server.Client(), outbound)
	if sendErr != nil {
		t.Fatalf("send returned error: %v", sendErr)
	}
	if messageID != "wa-media-123" {
		t.Fatalf("messageID = %q, want %q", messageID, "wa-media-123")
	}
	if got := gotBody.Get("method"); got != "SendMediaMessage" {
		t.Fatalf("method = %q, want SendMediaMessage", got)
	}
	if got := gotBody.Get("isTemplate"); got != "" {
		t.Fatalf("isTemplate = %q, want empty for plain media payload", got)
	}
	if got := gotBody.Get("channel"); got != "" {
		t.Fatalf("channel = %q, want empty", got)
	}
	if got := gotBody.Get("media_url"); got != "https://example.com/image.jpg" {
		t.Fatalf("media_url = %q, want media URL", got)
	}
	if got := gotBody.Get("caption"); got != "AWD Program: Important Update" {
		t.Fatalf("caption = %q, want request body", got)
	}
}

func TestGupshupWhatsAppAdapterTemplateMediaPayloadUsesTemplateAPI(t *testing.T) {
	var gotBody url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %q, want POST", r.Method)
		}
		if r.URL.Path != "/wa/api/v1/template/msg" {
			t.Fatalf("path = %q, want /wa/api/v1/template/msg", r.URL.Path)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		gotBody = r.PostForm
		w.Header().Set("X-Provider-Message-ID", "wa-template-123")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	adapter := gupshupWhatsAppAdapter{}
	outbound, err := adapter.build(notification.ConnectorSendRequest{
		Destination: "+918700491033",
		Body:        "AWD Program: Important Update",
		ProviderConfig: map[string]string{
			"username": "917208898844",
			"password": "demo-pass",
			"version":  "1.1",
			"base_url": server.URL,
		},
		Metadata: map[string]string{
			"0":                      "placeholder1",
			"gupshup_template_id":    "249910",
			"gupshup_template_name":  "awd_scale_down_partners_english",
			"media_type":             "image",
			"media_url":              "https://example.com/image.jpg",
			"interactive_attributes": `{"button_category":"CallToAction","buttons":[{"type":"url","urlType":"static","url":"https://nrf.page.link/paHospicash","text":"Claim Offer"}]}`,
		},
		TemplateVariables: map[string]string{
			"placeholder1": "Pratik",
		},
	})
	if err != nil {
		t.Fatalf("build returned error: %v", err)
	}

	messageID, sendErr := adapter.send(context.Background(), server.Client(), outbound)
	if sendErr != nil {
		t.Fatalf("send returned error: %v", sendErr)
	}
	if messageID != "wa-template-123" {
		t.Fatalf("messageID = %q, want %q", messageID, "wa-template-123")
	}

	if got := gotBody.Get("source"); got != "917208898844" {
		t.Fatalf("source = %q, want %q", got, "917208898844")
	}
	if got := gotBody.Get("destination"); got != "918700491033" {
		t.Fatalf("destination = %q, want %q", got, "918700491033")
	}
	if got := gotBody.Get("channel"); got != "whatsapp" {
		t.Fatalf("channel = %q, want whatsapp", got)
	}

	var templatePayload map[string]any
	if err := json.Unmarshal([]byte(gotBody.Get("template")), &templatePayload); err != nil {
		t.Fatalf("decode template: %v", err)
	}
	if got, _ := templatePayload["id"].(string); got != "249910" {
		t.Fatalf("template.id = %q, want 249910", got)
	}
	params, ok := templatePayload["params"].([]any)
	if !ok {
		t.Fatalf("template.params missing: %#v", templatePayload)
	}
	if len(params) != 1 {
		t.Fatalf("template.params len = %d, want 1", len(params))
	}
	if got, _ := params[0].(string); got != "Pratik" {
		t.Fatalf("template.params[0] = %q, want Pratik", got)
	}

	var messagePayload map[string]any
	if err := json.Unmarshal([]byte(gotBody.Get("message")), &messagePayload); err != nil {
		t.Fatalf("decode message: %v", err)
	}
	if got, _ := messagePayload["type"].(string); got != "image" {
		t.Fatalf("message.type = %q, want image", got)
	}
	image, ok := messagePayload["image"].(map[string]any)
	if !ok {
		t.Fatalf("message.image missing: %#v", messagePayload)
	}
	if got, _ := image["link"].(string); got != "https://example.com/image.jpg" {
		t.Fatalf("message.image.link = %q, want image url", got)
	}
}

func TestGupshupWhatsAppAdapterTemplateMediaPayloadSupportsGenericMediaAliases(t *testing.T) {
	var gotBody url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		gotBody = r.PostForm
		w.Header().Set("X-Provider-Message-ID", "wa-audio-123")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	adapter := gupshupWhatsAppAdapter{}
	outbound, err := adapter.build(notification.ConnectorSendRequest{
		Destination: "+918700491033",
		Body:        "Listen to this update",
		ProviderConfig: map[string]string{
			"username": "demo-user",
			"password": "demo-pass",
			"version":  "1.1",
			"base_url": server.URL,
		},
		Metadata: map[string]string{
			"0":                      "placeholder1",
			"gupshup_template_id":    "249910",
			"gupshup_template_name":  "awd_scale_down_partners_english",
			"media_content_type":     "audio",
			"media_link":             "https://example.com/audio.mp3",
			"interactive_attributes": `{"footer":"audio footer"}`,
		},
		TemplateVariables: map[string]string{
			"placeholder1": "Pratik",
		},
	})
	if err != nil {
		t.Fatalf("build returned error: %v", err)
	}

	messageID, sendErr := adapter.send(context.Background(), server.Client(), outbound)
	if sendErr != nil {
		t.Fatalf("send returned error: %v", sendErr)
	}
	if messageID != "wa-audio-123" {
		t.Fatalf("messageID = %q, want %q", messageID, "wa-audio-123")
	}

	var messagePayload map[string]any
	if err := json.Unmarshal([]byte(gotBody.Get("message")), &messagePayload); err != nil {
		t.Fatalf("decode message: %v", err)
	}
	if got, _ := messagePayload["type"].(string); got != "audio" {
		t.Fatalf("message.type = %q, want audio", got)
	}
	audio, ok := messagePayload["audio"].(map[string]any)
	if !ok {
		t.Fatalf("message.audio missing: %#v", messagePayload)
	}
	if got, _ := audio["link"].(string); got != "https://example.com/audio.mp3" {
		t.Fatalf("message.audio.link = %q, want audio url", got)
	}
}

func TestSubstituteHeaderExample(t *testing.T) {
	got := substituteHeaderExample("Hi {{name}}", "customer_name", map[string]string{
		"customer_name": "Pratik",
	})
	if got != "Hi Pratik" {
		t.Fatalf("substituteHeaderExample() = %q, want %q", got, "Hi Pratik")
	}

	got = substituteHeaderExample("Hi {{name}}", "customer_name", nil)
	if got != "Hi customer_name" {
		t.Fatalf("fallback substituteHeaderExample() = %q, want %q", got, "Hi customer_name")
	}
}

func TestKarixWhatsAppAdapterSend(t *testing.T) {
	var gotBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %q, want POST", r.Method)
		}
		if r.URL.Path != "/" {
			t.Fatalf("path = %q, want /", r.URL.Path)
		}
		if auth := r.Header.Get("Authentication"); auth != "Bearer demo-key" {
			t.Fatalf("Authentication = %q, want Bearer demo-key", auth)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.Header().Set("X-Provider-Message-ID", "karix-123")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	adapter := karixWhatsAppAdapter{}
	outbound, err := adapter.build(notification.ConnectorSendRequest{
		Destination: "+918700491033",
		Body:        "123456",
		ProviderConfig: map[string]string{
			"key":         "demo-key",
			"sender":      "917208898844",
			"template_id": "farm_login_otp_wa_v2",
			"version":     "v1.0.9",
			"base_url":    server.URL,
		},
		Metadata: map[string]string{
			"otp": "123456",
		},
	})
	if err != nil {
		t.Fatalf("build returned error: %v", err)
	}

	messageID, sendErr := adapter.send(context.Background(), server.Client(), outbound)
	if sendErr != nil {
		t.Fatalf("send returned error: %v", sendErr)
	}
	if messageID != "karix-123" {
		t.Fatalf("messageID = %q, want %q", messageID, "karix-123")
	}

	msg, ok := gotBody["message"].(map[string]any)
	if !ok {
		t.Fatalf("message missing in body: %#v", gotBody)
	}
	if got, _ := msg["channel"].(string); got != "WABA" {
		t.Fatalf("channel = %q, want WABA", got)
	}
	content, ok := msg["content"].(map[string]any)
	if !ok {
		t.Fatalf("content missing in body: %#v", gotBody)
	}
	if got, _ := content["type"].(string); got != "TEMPLATE" {
		t.Fatalf("content.type = %q, want TEMPLATE", got)
	}
	template, ok := content["template"].(map[string]any)
	if !ok {
		t.Fatalf("template missing in body: %#v", gotBody)
	}
	if got, _ := template["templateId"].(string); got != "farm_login_otp_wa_v2" {
		t.Fatalf("templateId = %q, want farm_login_otp_wa_v2", got)
	}
	params, ok := template["parameterValues"].(map[string]any)
	if !ok {
		t.Fatalf("parameterValues missing in body: %#v", gotBody)
	}
	if got, _ := params["otp"].(string); got != "123456" {
		t.Fatalf("otp param = %q, want 123456", got)
	}
}

func TestKarixWhatsAppAdapterMediaPayloadUsesGenericMediaType(t *testing.T) {
	var gotBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.Header().Set("X-Provider-Message-ID", "karix-media-123")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	adapter := karixWhatsAppAdapter{}
	outbound, err := adapter.build(notification.ConnectorSendRequest{
		Destination: "+918700491033",
		Body:        "Media test",
		ProviderConfig: map[string]string{
			"key":      "demo-key",
			"sender":   "917208898844",
			"version":  "v1.0.9",
			"base_url": server.URL,
		},
		Metadata: map[string]string{
			"karix_template_name":    "test_template_pratik_1",
			"media_type":             "sticker",
			"media_url":              "https://example.com/sticker.webp",
			"media_title":            "Sticker title",
			"interactive_attributes": `{"button_category":"CallToAction","buttons":[{"type":"url","urlType":"static","url":"https://example.com","text":"Open"}]}`,
		},
	})
	if err != nil {
		t.Fatalf("build returned error: %v", err)
	}

	messageID, sendErr := adapter.send(context.Background(), server.Client(), outbound)
	if sendErr != nil {
		t.Fatalf("send returned error: %v", sendErr)
	}
	if messageID != "karix-media-123" {
		t.Fatalf("messageID = %q, want %q", messageID, "karix-media-123")
	}

	message, ok := gotBody["message"].(map[string]any)
	if !ok {
		t.Fatalf("message missing in payload: %#v", gotBody)
	}
	content, ok := message["content"].(map[string]any)
	if !ok {
		t.Fatalf("content missing in payload: %#v", gotBody)
	}
	if got, _ := content["type"].(string); got != "MEDIA_TEMPLATE" {
		t.Fatalf("content.type = %q, want MEDIA_TEMPLATE", got)
	}
	mediaTemplate, ok := content["mediaTemplate"].(map[string]any)
	if !ok {
		t.Fatalf("mediaTemplate missing in payload: %#v", gotBody)
	}
	media, ok := mediaTemplate["media"].(map[string]any)
	if !ok {
		t.Fatalf("media missing in payload: %#v", gotBody)
	}
	if got, _ := media["type"].(string); got != "sticker" {
		t.Fatalf("media.type = %q, want sticker", got)
	}
	if got, _ := media["url"].(string); got != "https://example.com/sticker.webp" {
		t.Fatalf("media.url = %q, want sticker url", got)
	}
}
