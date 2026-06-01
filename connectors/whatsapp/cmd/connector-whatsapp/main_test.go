package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/Arunshaik2001/notification-control-plane/libs/contracts/notification"
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
