package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/your-org/notification-control-plane/libs/contracts/notification"
)

func TestSendgridEmailAdapterSend(t *testing.T) {
	var gotAuth string
	var gotPayload sendgridOutboundPayload

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %q, want POST", r.Method)
		}
		if r.URL.Path != "/v3/mail/send" {
			t.Fatalf("path = %q, want /v3/mail/send", r.URL.Path)
		}
		gotAuth = r.Header.Get("Authorization")
		if err := json.NewDecoder(r.Body).Decode(&gotPayload); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		w.Header().Set("X-Provider-Message-ID", "sg-123")
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	adapter := sendgridEmailAdapter{}
	outbound, err := adapter.build(notification.ConnectorSendRequest{
		Destination: "person@example.com",
		Subject:     "Welcome",
		Body:        "Hello there",
		ProviderConfig: map[string]string{
			"from_email": "noreply@example.com",
			"api_key":    "sendgrid-secret",
			"base_url":   server.URL,
		},
	})
	if err != nil {
		t.Fatalf("build returned error: %v", err)
	}

	messageID, sendErr := adapter.send(context.Background(), server.Client(), outbound)
	if sendErr != nil {
		t.Fatalf("send returned error: %v", sendErr)
	}
	if messageID != "sg-123" {
		t.Fatalf("messageID = %q, want %q", messageID, "sg-123")
	}
	if gotAuth != "Bearer sendgrid-secret" {
		t.Fatalf("authorization header = %q, want %q", gotAuth, "Bearer sendgrid-secret")
	}
	if gotPayload.From.Email != "noreply@example.com" {
		t.Fatalf("from email = %q, want %q", gotPayload.From.Email, "noreply@example.com")
	}
	if gotPayload.Personalizations[0].To[0].Email != "person@example.com" {
		t.Fatalf("recipient email = %q, want %q", gotPayload.Personalizations[0].To[0].Email, "person@example.com")
	}
	if gotPayload.Subject != "Welcome" {
		t.Fatalf("subject = %q, want %q", gotPayload.Subject, "Welcome")
	}
}
