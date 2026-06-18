package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/NotifyHub-in/NotifyHub/libs/contracts/notification"
)

func TestWebhookAdapterSend(t *testing.T) {
	var gotSecret string
	var gotPayload webhookOutboundPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %q, want POST", r.Method)
		}
		if r.URL.Path != "/deliver" {
			t.Fatalf("path = %q, want /deliver", r.URL.Path)
		}
		gotSecret = r.Header.Get("X-Webhook-Secret")
		if err := json.NewDecoder(r.Body).Decode(&gotPayload); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		w.Header().Set("X-Provider-Message-ID", "wh-123")
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	adapter := webhookAdapter{}
	outbound, err := adapter.build(notification.ConnectorSendRequest{
		RequestID:   "req-123",
		Channel:     notification.ChannelWebhook,
		Destination: server.URL + "/deliver",
		Subject:     "Subject",
		Body:        "Webhook body",
		Metadata:    map[string]string{"source": "unit-test"},
		ProviderConfig: map[string]string{
			"shared_secret": "webhook-secret",
		},
	})
	if err != nil {
		t.Fatalf("build returned error: %v", err)
	}

	messageID, sendErr := adapter.send(context.Background(), server.Client(), outbound)
	if sendErr != nil {
		t.Fatalf("send returned error: %v", sendErr)
	}
	if messageID != "wh-123" {
		t.Fatalf("messageID = %q, want %q", messageID, "wh-123")
	}
	if gotSecret != "webhook-secret" {
		t.Fatalf("secret header = %q, want %q", gotSecret, "webhook-secret")
	}
	if gotPayload.RequestID != "req-123" || gotPayload.Subject != "Subject" || gotPayload.Body != "Webhook body" {
		t.Fatalf("payload = %#v, want request/subject/body", gotPayload)
	}
	if gotPayload.Metadata["source"] != "unit-test" {
		t.Fatalf("metadata = %#v, want source unit-test", gotPayload.Metadata)
	}
}
