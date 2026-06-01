package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Arunshaik2001/notification-control-plane/libs/contracts/notification"
)

func TestFCMAdapterSend(t *testing.T) {
	var gotAuth string
	var gotPayload fcmOutboundPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %q, want POST", r.Method)
		}
		if r.URL.Path != "/v1/projects/demo-project/messages:send" {
			t.Fatalf("path = %q, want fcm path", r.URL.Path)
		}
		gotAuth = r.Header.Get("Authorization")
		if err := json.NewDecoder(r.Body).Decode(&gotPayload); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		w.Header().Set("X-Provider-Message-ID", "fcm-123")
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	adapter := fcmAdapter{}
	outbound, err := adapter.build(notification.ConnectorSendRequest{
		Destination: "device-token-123",
		Subject:     "Welcome",
		Body:        "Hello from push",
		ProviderConfig: map[string]string{
			"project_id":           "demo-project",
			"service_account_json": `{"access_token":"fcm-secret"}`,
			"base_url":             server.URL,
		},
	})
	if err != nil {
		t.Fatalf("build returned error: %v", err)
	}

	messageID, sendErr := adapter.send(context.Background(), server.Client(), outbound)
	if sendErr != nil {
		t.Fatalf("send returned error: %v", sendErr)
	}
	if messageID != "fcm-123" {
		t.Fatalf("messageID = %q, want %q", messageID, "fcm-123")
	}
	if gotAuth != "Bearer fcm-secret" {
		t.Fatalf("authorization header = %q, want %q", gotAuth, "Bearer fcm-secret")
	}
	if gotPayload.Message.Token != "device-token-123" {
		t.Fatalf("token = %q, want %q", gotPayload.Message.Token, "device-token-123")
	}
	if gotPayload.Message.Notification.Title != "Welcome" || gotPayload.Message.Notification.Body != "Hello from push" {
		t.Fatalf("notification = %#v, want title/body", gotPayload.Message.Notification)
	}
}
