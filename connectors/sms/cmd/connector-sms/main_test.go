package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/your-org/notification-control-plane/libs/contracts/notification"
)

func TestTwilioSMSAdapterSend(t *testing.T) {
	var gotAuth string
	var gotForm url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %q, want POST", r.Method)
		}
		if r.URL.Path != "/2010-04-01/Accounts/AC123/Messages.json" {
			t.Fatalf("path = %q, want twilio messages path", r.URL.Path)
		}
		gotAuth = r.Header.Get("Authorization")
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		gotForm = r.PostForm
		w.Header().Set("X-Provider-Message-ID", "twilio-123")
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	adapter := twilioSMSAdapter{}
	outbound, err := adapter.build(notification.ConnectorSendRequest{
		Destination: "+15555550123",
		Body:        "Verify your login",
		ProviderConfig: map[string]string{
			"account_sid": "AC123",
			"auth_token":  "twilio-secret",
			"from_number": "+15555550199",
			"base_url":    server.URL,
		},
	})
	if err != nil {
		t.Fatalf("build returned error: %v", err)
	}

	messageID, sendErr := adapter.send(context.Background(), server.Client(), outbound)
	if sendErr != nil {
		t.Fatalf("send returned error: %v", sendErr)
	}
	if messageID != "twilio-123" {
		t.Fatalf("messageID = %q, want %q", messageID, "twilio-123")
	}
	wantAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte("AC123:twilio-secret"))
	if gotAuth != wantAuth {
		t.Fatalf("authorization header = %q, want %q", gotAuth, wantAuth)
	}
	if gotForm.Get("To") != "+15555550123" || gotForm.Get("From") != "+15555550199" || gotForm.Get("Body") != "Verify your login" {
		t.Fatalf("form = %#v, want destination/from/body", gotForm)
	}
}

func TestGupshupSMSAdapterSend(t *testing.T) {
	var gotAuth string
	var gotPayload gupshupOutboundPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %q, want POST", r.Method)
		}
		if r.URL.Path != "/sms/1/send" {
			t.Fatalf("path = %q, want gupshup path", r.URL.Path)
		}
		gotAuth = r.Header.Get("apikey")
		if err := json.NewDecoder(r.Body).Decode(&gotPayload); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		w.Header().Set("X-Provider-Message-ID", "gupshup-123")
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	adapter := gupshupSMSAdapter{}
	outbound, err := adapter.build(notification.ConnectorSendRequest{
		Destination: "+15555550123",
		Body:        "Your code is 1234",
		ProviderConfig: map[string]string{
			"api_key":   "gupshup-secret",
			"sender_id": "EXAMPLE",
			"base_url":  server.URL,
		},
	})
	if err != nil {
		t.Fatalf("build returned error: %v", err)
	}

	messageID, sendErr := adapter.send(context.Background(), server.Client(), outbound)
	if sendErr != nil {
		t.Fatalf("send returned error: %v", sendErr)
	}
	if messageID != "gupshup-123" {
		t.Fatalf("messageID = %q, want %q", messageID, "gupshup-123")
	}
	if gotAuth != "gupshup-secret" {
		t.Fatalf("apikey header = %q, want %q", gotAuth, "gupshup-secret")
	}
	if gotPayload.Source != "EXAMPLE" || gotPayload.Destination != "+15555550123" || gotPayload.Message != "Your code is 1234" {
		t.Fatalf("payload = %#v, want sender/destination/message", gotPayload)
	}
}

func TestKarixSMSAdapterSend(t *testing.T) {
	var gotAuth string
	var gotPayload karixOutboundPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %q, want POST", r.Method)
		}
		if r.URL.Path != "/v1/messages" {
			t.Fatalf("path = %q, want karix path", r.URL.Path)
		}
		gotAuth = r.Header.Get("api-key")
		if err := json.NewDecoder(r.Body).Decode(&gotPayload); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		w.Header().Set("X-Provider-Message-ID", "karix-123")
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	adapter := karixSMSAdapter{}
	outbound, err := adapter.build(notification.ConnectorSendRequest{
		Destination: "+15555550123",
		Body:        "Your package is out for delivery",
		ProviderConfig: map[string]string{
			"api_key":   "karix-secret",
			"sender_id": "EXAMPLE",
			"base_url":  server.URL,
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
	if gotAuth != "karix-secret" {
		t.Fatalf("api-key header = %q, want %q", gotAuth, "karix-secret")
	}
	if gotPayload.From != "EXAMPLE" || gotPayload.To != "+15555550123" || gotPayload.Message != "Your package is out for delivery" {
		t.Fatalf("payload = %#v, want sender/destination/message", gotPayload)
	}
}
