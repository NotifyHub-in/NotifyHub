package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/NotifyHub-in/NotifyHub/libs/contracts/notification"
)

func TestVerifyCallbackRequestNoneAllowsCallbacksWithoutHeaders(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "http://example.com/v1/providers/test/callbacks", nil)

	ok, err := verifyCallbackRequest(req, []byte(`{"hello":"world"}`), notification.CallbackRoute{
		VerificationMode: notification.CallbackVerificationModeNone,
	}, notification.CallbackVerificationSpec{})
	if err != nil {
		t.Fatalf("verifyCallbackRequest returned error: %v", err)
	}
	if !ok {
		t.Fatal("expected verification mode none to allow the callback")
	}
}

func TestVerifyCallbackRequestSharedSecret(t *testing.T) {
	t.Setenv("CALLBACK_SHARED_SECRET_TEST", "expected-secret")
	req := httptest.NewRequest(http.MethodPost, "http://example.com/v1/providers/test/callbacks", nil)
	req.Header.Set("X-Provider-Secret", "expected-secret")

	ok, err := verifyCallbackRequest(req, []byte(`{"event":"delivered"}`), notification.CallbackRoute{
		VerificationMode: notification.CallbackVerificationModeSharedSecret,
		VerificationSecretRef: notification.SecretReference{
			Ref:          "CALLBACK_SHARED_SECRET_TEST",
			MaterialType: notification.MaterialTypeSecretString,
		},
	}, notification.CallbackVerificationSpec{})
	if err != nil {
		t.Fatalf("verifyCallbackRequest returned error: %v", err)
	}
	if !ok {
		t.Fatal("expected shared-secret verification to pass")
	}
}

func TestVerifyCallbackRequestHMACSHA256(t *testing.T) {
	body := []byte(`{"event":"delivered"}`)
	t.Setenv("CALLBACK_HMAC_SECRET_TEST", "super-secret")
	secret := "super-secret"
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(body)
	signature := hex.EncodeToString(mac.Sum(nil))

	req := httptest.NewRequest(http.MethodPost, "http://example.com/v1/providers/test/callbacks", nil)
	req.Header.Set("X-Provider-Signature", signature)

	ok, err := verifyCallbackRequest(req, body, notification.CallbackRoute{
		VerificationMode: notification.CallbackVerificationModeHMACSHA256,
		VerificationSecretRef: notification.SecretReference{
			Ref:          "CALLBACK_HMAC_SECRET_TEST",
			MaterialType: notification.MaterialTypeSecretString,
		},
	}, notification.CallbackVerificationSpec{})
	if err != nil {
		t.Fatalf("verifyCallbackRequest returned error: %v", err)
	}
	if !ok {
		t.Fatal("expected hmac verification to pass")
	}
}

func TestVerifyCallbackRequestRejectsUnsupportedMode(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "http://example.com/v1/providers/test/callbacks", nil)

	ok, err := verifyCallbackRequest(req, nil, notification.CallbackRoute{
		VerificationMode: notification.CallbackVerificationMode("totally_unknown"),
	}, notification.CallbackVerificationSpec{})
	if err == nil {
		t.Fatal("expected unsupported mode to return an error")
	}
	if ok {
		t.Fatal("expected unsupported mode to fail verification")
	}
}

func TestVerifyCallbackRequestUsesProviderSpecificHeaders(t *testing.T) {
	t.Setenv("CALLBACK_SPEC_SECRET_TEST", "expected-secret")
	req := httptest.NewRequest(http.MethodPost, "http://example.com/v1/providers/custom/callbacks", nil)
	req.Header.Set("X-Custom-Secret", "expected-secret")

	ok, err := verifyCallbackRequest(req, nil, notification.CallbackRoute{
		VerificationMode: notification.CallbackVerificationModeSharedSecret,
		VerificationSecretRef: notification.SecretReference{
			Ref:          "CALLBACK_SPEC_SECRET_TEST",
			MaterialType: notification.MaterialTypeSecretString,
		},
	}, notification.CallbackVerificationSpec{
		SecretHeader: "X-Custom-Secret",
	})
	if err != nil {
		t.Fatalf("verifyCallbackRequest returned error: %v", err)
	}
	if !ok {
		t.Fatal("expected provider-specific secret header to pass")
	}
}

func TestDecodeProviderCallbacksGenericJSON(t *testing.T) {
	def := notification.ProviderDefinition{}
	callbacks, err := decodeProviderCallbacks(def, false, http.MethodPost, url.Values{}, []byte(`{"provider_message_id":"msg-1","status":"delivered"}`))
	if err != nil {
		t.Fatalf("decodeProviderCallbacks returned error: %v", err)
	}
	if len(callbacks) != 1 {
		t.Fatalf("callback count = %d, want 1", len(callbacks))
	}
	if callbacks[0].ProviderMessageID != "msg-1" || callbacks[0].Status != "delivered" {
		t.Fatalf("decoded callback = %+v", callbacks[0])
	}
}

func TestDecodeProviderCallbacksGupshupWhatsApp(t *testing.T) {
	def, ok := notification.ProviderDefinitionByKey("gupshup-whatsapp")
	if !ok {
		t.Fatal("expected gupshup-whatsapp provider definition")
	}

	callbacks, err := decodeProviderCallbacks(def, true, http.MethodPost, url.Values{}, []byte(`[{"externalId":"msg-1","eventType":"DELIVERED","eventTs":1,"destAddr":"918700491033","srcAddr":"917123456789","cause":"SUCCESS","errorCode":"","channel":"WHATSAPP"}]`))
	if err != nil {
		t.Fatalf("decodeProviderCallbacks returned error: %v", err)
	}
	if len(callbacks) != 1 {
		t.Fatalf("callback count = %d, want 1", len(callbacks))
	}
	if callbacks[0].ProviderMessageID != "msg-1" || callbacks[0].Status != "delivered" {
		t.Fatalf("decoded callback = %+v", callbacks[0])
	}
}

func TestDecodeProviderInboundEventsGupshupWhatsApp(t *testing.T) {
	def, ok := notification.ProviderDefinitionByKey("gupshup-whatsapp")
	if !ok {
		t.Fatal("expected gupshup-whatsapp provider definition")
	}

	events, err := decodeProviderInboundEvents(def, true, http.MethodPost, url.Values{}, []byte(`{"app":"docdeck","timestamp":1718007189549,"version":2,"type":"message","payload":{"id":"ABEGkZUTIXZ0Ago6jWqOZm-Sz0WD","source":"918700491033","type":"text","payload":{"text":"Hi"},"sender":{"phone":"918700491033","name":"SJ","country_code":"91","dial_code":"91"},"context":{"id":"gBEGkYaYVSEEAgnPFrOLcjkFjL8","gsId":"9b71295f-f7af-4c1f-b2b4-31b4a4867bad"}}}`))
	if err != nil {
		t.Fatalf("decodeProviderInboundEvents returned error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("event count = %d, want 1", len(events))
	}
	if events[0].EventType != "reply_received" || events[0].Status != notification.ChannelEventStatusReceived {
		t.Fatalf("decoded event = %+v", events[0])
	}
	if events[0].Body != "Hi" {
		t.Fatalf("body = %q, want Hi", events[0].Body)
	}
}

func TestDecodeProviderInboundEventsMetaWhatsApp(t *testing.T) {
	def, ok := notification.ProviderDefinitionByKey("karix-whatsapp")
	if !ok {
		t.Fatal("expected karix-whatsapp provider definition")
	}

	events, err := decodeProviderInboundEvents(def, true, http.MethodPost, url.Values{}, []byte(`{"object":"whatsapp_business_account","entry":[{"id":"123","changes":[{"field":"messages","value":{"messages":[{"id":"wamid.inbound-1","from":"918700491033","timestamp":"1718007189","type":"text","text":{"body":"Need help"},"context":{"id":"wamid.original-1","from":"917208898844"}}],"contacts":[{"profile":{"name":"Arun"},"wa_id":"918700491033"}],"metadata":{"display_phone_number":"917208898844","phone_number_id":"phone-number-id-123"}}}]}]}`))
	if err != nil {
		t.Fatalf("decodeProviderInboundEvents returned error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("event count = %d, want 1", len(events))
	}
	if events[0].EventType != "reply_received" || events[0].Body != "Need help" {
		t.Fatalf("decoded event = %+v", events[0])
	}
	if events[0].FromAddress != "918700491033" {
		t.Fatalf("from_address = %q, want 918700491033", events[0].FromAddress)
	}
}
