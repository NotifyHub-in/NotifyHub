package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Arunshaik2001/notification-control-plane/libs/contracts/notification"
)

type testClient struct {
	baseURL         string
	callbackBaseURL string
	apiKey          string
	adminToken      string
	readToken       string
	client          *http.Client
}

type requestDetailsResponse struct {
	Request          notification.NotificationRecord       `json:"request"`
	DeliveryAttempts []notification.DeliveryAttempt        `json:"delivery_attempts"`
	ScheduledRetries []notification.ScheduledRetry         `json:"scheduled_retries"`
	DeadLetters      []notification.DeadLetterNotification `json:"dead_letters"`
}

type providerBindingHealthResponse struct {
	BindingID           string                            `json:"binding_id"`
	Channel             notification.Channel              `json:"channel"`
	BindingSet          string                            `json:"binding_set,omitempty"`
	ConnectorName       string                            `json:"connector_name"`
	CircuitState        notification.ProviderCircuitState `json:"circuit_state"`
	ConsecutiveFailures int                               `json:"consecutive_failures"`
}

type providerAccountsResponse struct {
	ProviderAccounts []notification.ProviderAccount `json:"provider_accounts"`
}

type providerAccountStatusResponse struct {
	ProviderAccount  notification.ProviderAccount         `json:"provider_account"`
	ProviderBindings []notification.ProviderBinding       `json:"provider_bindings"`
	BindingHealth    []notification.ProviderBindingHealth `json:"binding_health"`
	CallbackRoute    *notification.CallbackRoute          `json:"callback_route,omitempty"`
}

func TestNotificationRequestAcceptedFlow(t *testing.T) {
	client := requireIntegrationClient(t)

	req := notification.NotificationRequest{
		IdempotencyKey: uniqueKey("it-accepted"),
		EventName:      "order.delayed",
		TemplateKey:    "order-delayed-v1",
		Channels:       []notification.Channel{notification.ChannelEmail},
		Recipient: notification.Recipient{
			UserID: "it-user-accepted",
			Email:  "integration-accepted@example.com",
		},
		Variables: map[string]string{
			"order_id": "IT-ORD-ACCEPTED",
			"reason":   "carrier_delay",
		},
	}

	accepted := postNotificationRequest(t, client, req)
	details := waitForTerminalRequest(t, client, accepted.RequestID, 15, 1*time.Second)

	if details.Request.Status != notification.RequestStatusDispatched {
		t.Fatalf("request status = %q, want %q", details.Request.Status, notification.RequestStatusDispatched)
	}
	if len(details.DeliveryAttempts) == 0 {
		t.Fatal("expected at least one delivery attempt")
	}
	if details.DeliveryAttempts[0].Status != notification.DeliveryAttemptAccepted {
		t.Fatalf("delivery attempt status = %q, want %q", details.DeliveryAttempts[0].Status, notification.DeliveryAttemptAccepted)
	}
}

func TestAdminAPIsRequireToken(t *testing.T) {
	client := requireIntegrationClient(t)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, client.baseURL+"/v1/provider-definitions", nil)
	if err != nil {
		t.Fatalf("build unauthenticated admin request: %v", err)
	}
	req.Header.Del("X-Notification-Admin-Token")

	resp, err := client.client.Do(req)
	if err != nil {
		t.Fatalf("perform unauthenticated admin request: %v", err)
	}
	defer resp.Body.Close()

	body, err := ioReadAll(resp)
	if err != nil {
		t.Fatalf("read unauthenticated admin response: %v", err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("admin endpoint status = %d, want %d, body=%s", resp.StatusCode, http.StatusUnauthorized, string(body))
	}
}

func TestReadOnlyTokenCanReadButCannotWriteAdminAPIs(t *testing.T) {
	client := requireIntegrationClient(t).readOnlyClient()

	status, body := client.mustRequest(t, http.MethodGet, "/v1/provider-definitions", nil)
	if status != http.StatusOK {
		t.Fatalf("read-only provider definitions status = %d, want %d, body=%s", status, http.StatusOK, string(body))
	}

	writeStatus, writeBody := client.mustJSON(t, http.MethodPost, "/v1/provider-accounts", notification.ProviderAccountUpsertRequest{
		TenantID:    "tenant-read-only",
		ProviderKey: "twilio-sms",
		DisplayName: "Read-only should not write",
		Channel:     notification.ChannelSMS,
		Enabled:     true,
		Config: map[string]string{
			"from_number": "+14155550123",
		},
		SecretRefs: map[string]notification.SecretReference{
			"account_sid": {
				Ref:          "secret://tenant/tenant-read-only/twilio/account-sid",
				MaterialType: notification.MaterialTypeSecretString,
				Source:       "vault",
			},
			"auth_token": {
				Ref:          "secret://tenant/tenant-read-only/twilio/auth-token",
				MaterialType: notification.MaterialTypeSecretString,
				Source:       "vault",
			},
		},
	})
	if writeStatus != http.StatusForbidden {
		t.Fatalf("read-only write status = %d, want %d, body=%s", writeStatus, http.StatusForbidden, string(writeBody))
	}
}

func TestTemplateLanguageSelectionFallsBackToEnglish(t *testing.T) {
	client := requireIntegrationClient(t)

	upsertTemplate(t, client, notification.TemplateUpsertRequest{
		TemplateKey:     "multilang-welcome",
		Channel:         notification.ChannelEmail,
		LanguageCode:    "en",
		SubjectTemplate: "Welcome {{name}}",
		BodyTemplate:    "Hello {{name}}, your order {{order_id}} is confirmed.",
		Enabled:         true,
	})
	upsertTemplate(t, client, notification.TemplateUpsertRequest{
		TemplateKey:     "multilang-welcome",
		Channel:         notification.ChannelEmail,
		LanguageCode:    "hi-in",
		SubjectTemplate: "स्वागत है {{name}}",
		BodyTemplate:    "नमस्ते {{name}}, आपका ऑर्डर {{order_id}} कन्फर्म हो गया है।",
		Enabled:         true,
	})

	english := getTemplate(t, client, "multilang-welcome", notification.ChannelEmail, "")
	if english.LanguageCode != "en" {
		t.Fatalf("default template language = %q, want %q", english.LanguageCode, "en")
	}
	if english.BodyTemplate != "Hello {{name}}, your order {{order_id}} is confirmed." {
		t.Fatalf("default template body = %q, want english body", english.BodyTemplate)
	}

	hindi := getTemplate(t, client, "multilang-welcome", notification.ChannelEmail, "hi-in")
	if hindi.LanguageCode != "hi-in" {
		t.Fatalf("template language = %q, want %q", hindi.LanguageCode, "hi-in")
	}
	if hindi.BodyTemplate != "नमस्ते {{name}}, आपका ऑर्डर {{order_id}} कन्फर्म हो गया है।" {
		t.Fatalf("template body = %q, want hindi body", hindi.BodyTemplate)
	}

	fallback := getTemplate(t, client, "multilang-welcome", notification.ChannelEmail, "fr")
	if fallback.LanguageCode != "en" {
		t.Fatalf("fallback language = %q, want %q", fallback.LanguageCode, "en")
	}
}

func TestProviderCallbackMarksDelivered(t *testing.T) {
	client := requireIntegrationClient(t)

	req := notification.NotificationRequest{
		IdempotencyKey: uniqueKey("it-callback"),
		EventName:      "order.delayed",
		TemplateKey:    "order-delayed-v1",
		Channels:       []notification.Channel{notification.ChannelEmail},
		Recipient: notification.Recipient{
			UserID: "it-user-callback",
			Email:  "integration-callback@example.com",
		},
		Variables: map[string]string{
			"order_id": "IT-ORD-CALLBACK",
			"reason":   "carrier_delay",
		},
	}

	accepted := postNotificationRequest(t, client, req)
	details := waitForTerminalRequest(t, client, accepted.RequestID, 15, 1*time.Second)
	if len(details.DeliveryAttempts) == 0 {
		t.Fatal("expected a delivery attempt before callback")
	}
	providerMessageID := details.DeliveryAttempts[0].ProviderMessageID
	if providerMessageID == "" {
		t.Fatal("expected provider_message_id before callback")
	}

	postProviderCallback(t, client, "email", notification.ProviderCallback{
		ProviderMessageID: providerMessageID,
		Status:            "delivered",
	})

	delivered := waitForRequestCondition(t, client, accepted.RequestID, 15, 1*time.Second, func(details requestDetailsResponse) bool {
		return details.Request.Status == notification.RequestStatusDelivered
	})
	if delivered.DeliveryAttempts[0].Status != notification.DeliveryAttemptDelivered {
		t.Fatalf("delivery attempt status = %q, want %q", delivered.DeliveryAttempts[0].Status, notification.DeliveryAttemptDelivered)
	}
}

func TestSendgridEmailCallbacksAreRejected(t *testing.T) {
	client := requireIntegrationClient(t)

	status, body := postRawProviderCallbackJSON(t, client, "sendgrid-email", notification.ProviderCallback{
		ProviderMessageID: "msg-123",
		Status:            "delivered",
	})
	if status != http.StatusNotFound {
		t.Fatalf("sendgrid callback status = %d, want %d, body=%s", status, http.StatusNotFound, string(body))
	}
}

func TestProviderCallbackVerificationWithRoute(t *testing.T) {
	client := requireIntegrationClient(t)
	account := createProviderAccount(t, client, notification.ProviderAccountUpsertRequest{
		TenantID:    "communication-engine",
		ProviderKey: "sendgrid-email",
		DisplayName: "SendGrid Email Callback Demo",
		Channel:     notification.ChannelEmail,
		Enabled:     true,
		Config: map[string]string{
			"from_email": "noreply@example.com",
		},
		SecretRefs: map[string]notification.SecretReference{
			"api_key": {
				Ref:          "EMAIL_PROVIDER_API_KEY_DEMO",
				MaterialType: notification.MaterialTypeSecretString,
				Source:       "env",
			},
		},
	})
	route := createCallbackRoute(t, client, notification.CallbackRouteUpsertRequest{
		ProviderKey:       "email",
		ProviderAccountID: account.ProviderAccountID,
		CallbackPath:      "/v1/providers/email/callbacks",
		VerificationMode:  notification.CallbackVerificationModeSharedSecret,
		VerificationSecretRef: notification.SecretReference{
			Ref:          "CALLBACK_PROVIDER_SECRET_DEMO",
			MaterialType: notification.MaterialTypeSecretString,
			Source:       "env",
		},
		Enabled: true,
	})
	if route.ProviderKey != "email" {
		t.Fatalf("route provider_key = %q, want %q", route.ProviderKey, "email")
	}

	req := notification.NotificationRequest{
		IdempotencyKey: uniqueKey("it-callback-managed"),
		EventName:      "order.delayed",
		TemplateKey:    "order-delayed-v1",
		Channels:       []notification.Channel{notification.ChannelEmail},
		Recipient: notification.Recipient{
			UserID: "it-user-callback-managed",
			Email:  "integration-callback-managed@example.com",
		},
		Variables: map[string]string{
			"order_id": "IT-ORD-CB-MANAGED",
			"reason":   "route_verification",
		},
	}

	accepted := postNotificationRequest(t, client, req)
	details := waitForTerminalRequest(t, client, accepted.RequestID, 15, 1*time.Second)
	if len(details.DeliveryAttempts) == 0 {
		t.Fatal("expected at least one delivery attempt before verified callback")
	}

	postProviderCallbackWithHeaders(t, client, "email", notification.ProviderCallback{
		ProviderMessageID: details.DeliveryAttempts[0].ProviderMessageID,
		Status:            "delivered",
	}, map[string]string{
		"X-Provider-Secret": "demo-callback-secret",
	})

	delivered := waitForRequestCondition(t, client, accepted.RequestID, 15, 1*time.Second, func(details requestDetailsResponse) bool {
		return details.Request.Status == notification.RequestStatusDelivered
	})
	if delivered.DeliveryAttempts[0].Status != notification.DeliveryAttemptDelivered {
		t.Fatalf("delivery attempt status = %q, want %q", delivered.DeliveryAttempts[0].Status, notification.DeliveryAttemptDelivered)
	}
}

func TestWhatsAppProviderCallbacksUseRealPayloadShapes(t *testing.T) {
	client := requireIntegrationClient(t)
	capture := startHostCaptureServer(t)

	upsertTemplate(t, client, notification.TemplateUpsertRequest{
		TemplateKey:  "test-template-pratik",
		Channel:      notification.ChannelWhatsApp,
		BodyTemplate: "Hi {{placeholder1}},\n\nHow are you. Welcome to nurture.",
		Metadata: map[string]string{
			"media_type":             "image",
			"gupshup_template_name":  "test_template_pratik",
			"karix_template_name":    "test_template_pratik_1",
			"interactive_attributes": `{"button_category":"CallToAction","buttons":[{"type":"url","urlType":"static","url":"https://nrf.page.link/paHospicash","text":"Claim Offer"}]}`,
		},
		Enabled: true,
	})

	eventName := uniqueKey("it-whatsapp-callback")
	bindingSet := uniqueKey("it-whatsapp-callback-binding")
	upsertRoutingPolicy(t, client, notification.RoutingPolicyUpsertRequest{
		EventName:  eventName,
		Channels:   []notification.Channel{notification.ChannelWhatsApp},
		BindingSet: bindingSet,
		Enabled:    true,
		Priority:   100,
	})

	t.Run("gupshup", func(t *testing.T) {
		account := createProviderAccount(t, client, notification.ProviderAccountUpsertRequest{
			TenantID:    "communication-engine",
			ProviderKey: "gupshup-whatsapp",
			DisplayName: "Gupshup WhatsApp Callback Demo",
			Channel:     notification.ChannelWhatsApp,
			Enabled:     true,
			Config: map[string]string{
				"username": "demo-user",
				"password": "demo-pass",
				"version":  "1.1",
				"base_url": capture.baseURL(t),
			},
			SecretRefs: map[string]notification.SecretReference{
				"password": {
					Ref:          "WHATSAPP_GUPSHUP_PASSWORD_DEMO",
					MaterialType: notification.MaterialTypeSecretString,
					Source:       "env",
				},
			},
		})
		createCallbackRoute(t, client, notification.CallbackRouteUpsertRequest{
			ProviderKey:       "gupshup-whatsapp",
			ProviderAccountID: account.ProviderAccountID,
			CallbackPath:      "/v1/providers/gupshup-whatsapp/callbacks",
			VerificationMode:  notification.CallbackVerificationModeNone,
			Enabled:           true,
		})
		createProviderBinding(t, client, notification.ProviderBindingUpsertRequest{
			Channel:           notification.ChannelWhatsApp,
			BindingSet:        bindingSet,
			ProviderAccountID: account.ProviderAccountID,
			EndpointURL:       "http://connector-whatsapp:8095",
			Enabled:           true,
			Priority:          100,
		})

		req := notification.NotificationRequest{
			IdempotencyKey: uniqueKey("it-whatsapp-callback-gupshup"),
			EventName:      eventName,
			TemplateKey:    "test-template-pratik",
			Channels:       []notification.Channel{notification.ChannelWhatsApp},
			BindingSet:     bindingSet,
			Recipient: notification.Recipient{
				UserID: "it-user-whatsapp-callback-gupshup",
				Phone:  "918700491033",
			},
			Variables: map[string]string{
				"placeholder1": "Pratik",
			},
		}

		accepted := postNotificationRequest(t, client, req)
		details := waitForTerminalRequest(t, client, accepted.RequestID, 20, 1*time.Second)
		if len(details.DeliveryAttempts) == 0 || details.DeliveryAttempts[0].ProviderMessageID == "" {
			t.Fatal("expected a provider message id before callback")
		}

		status, body := postRawProviderCallback(t, client, "gupshup-whatsapp", []notification.GupshupWhatsAppCallbackRequest{{
			ExternalID: details.DeliveryAttempts[0].ProviderMessageID,
			EventType:  "DELIVERED",
			EventTS:    time.Now().UnixMilli(),
			DestAddr:   "918700491033",
			SrcAddr:    "917123456789",
			Cause:      "SUCCESS",
			ErrorCode:  "",
			Channel:    "WHATSAPP",
		}})
		if status != http.StatusAccepted {
			t.Fatalf("provider callback status = %d, want %d, body=%s", status, http.StatusAccepted, string(body))
		}

		delivered := waitForRequestCondition(t, client, accepted.RequestID, 15, 1*time.Second, func(details requestDetailsResponse) bool {
			return details.Request.Status == notification.RequestStatusDelivered
		})
		if delivered.DeliveryAttempts[0].Status != notification.DeliveryAttemptDelivered {
			t.Fatalf("delivery attempt status = %q, want %q", delivered.DeliveryAttempts[0].Status, notification.DeliveryAttemptDelivered)
		}
	})

	t.Run("karix", func(t *testing.T) {
		account := createProviderAccount(t, client, notification.ProviderAccountUpsertRequest{
			TenantID:    "communication-engine",
			ProviderKey: "karix-whatsapp",
			DisplayName: "Karix WhatsApp Callback Demo",
			Channel:     notification.ChannelWhatsApp,
			Enabled:     true,
			Config: map[string]string{
				"key":      "demo-karix-key",
				"sender":   "917208898844",
				"version":  "v1.0.9",
				"base_url": capture.baseURL(t),
			},
			SecretRefs: map[string]notification.SecretReference{
				"key": {
					Ref:          "WHATSAPP_KARIX_KEY_DEMO",
					MaterialType: notification.MaterialTypeSecretString,
					Source:       "env",
				},
			},
		})
		createCallbackRoute(t, client, notification.CallbackRouteUpsertRequest{
			ProviderKey:       "karix-whatsapp",
			ProviderAccountID: account.ProviderAccountID,
			CallbackPath:      "/v1/providers/karix-whatsapp/callbacks",
			VerificationMode:  notification.CallbackVerificationModeNone,
			Enabled:           true,
		})
		createProviderBinding(t, client, notification.ProviderBindingUpsertRequest{
			Channel:           notification.ChannelWhatsApp,
			BindingSet:        bindingSet,
			ProviderAccountID: account.ProviderAccountID,
			EndpointURL:       "http://connector-whatsapp:8095",
			Enabled:           true,
			Priority:          100,
		})

		req := notification.NotificationRequest{
			IdempotencyKey: uniqueKey("it-whatsapp-callback-karix"),
			EventName:      eventName,
			TemplateKey:    "test-template-pratik",
			Channels:       []notification.Channel{notification.ChannelWhatsApp},
			BindingSet:     bindingSet,
			Recipient: notification.Recipient{
				UserID: "it-user-whatsapp-callback-karix",
				Phone:  "918700491033",
			},
			Variables: map[string]string{
				"placeholder1": "Pratik",
			},
		}

		accepted := postNotificationRequest(t, client, req)
		details := waitForTerminalRequest(t, client, accepted.RequestID, 20, 1*time.Second)
		if len(details.DeliveryAttempts) == 0 || details.DeliveryAttempts[0].ProviderMessageID == "" {
			t.Fatal("expected a provider message id before callback")
		}

		callback := notification.KarixWhatsAppCallbackRequest{
			Channel: "WABA",
			Recipient: notification.KarixWhatsAppCallbackRecipient{
				To:            "918700491033",
				RecipientType: "individual",
				Reference:     map[string]any{"cust_ref": "integration"},
			},
			Events: notification.KarixWhatsAppCallbackEvents{
				EventType: "message",
				Timestamp: time.Now().UnixMilli(),
				MID:       details.DeliveryAttempts[0].ProviderMessageID,
			},
			NotificationAttributes: notification.KarixWhatsAppCallbackStatus{
				Status: "delivered",
				Reason: "delivered",
				Code:   "0",
			},
		}
		status, body := postRawProviderCallbackJSON(t, client, "karix-whatsapp", callback)
		if status != http.StatusAccepted {
			t.Fatalf("provider callback status = %d, want %d, body=%s", status, http.StatusAccepted, string(body))
		}

		delivered := waitForRequestCondition(t, client, accepted.RequestID, 15, 1*time.Second, func(details requestDetailsResponse) bool {
			return details.Request.Status == notification.RequestStatusDelivered
		})
		if delivered.DeliveryAttempts[0].Status != notification.DeliveryAttemptDelivered {
			t.Fatalf("delivery attempt status = %q, want %q", delivered.DeliveryAttempts[0].Status, notification.DeliveryAttemptDelivered)
		}
	})
}

func TestManagedPushDeliveryAccepted(t *testing.T) {
	client := requireIntegrationClient(t)

	upsertTemplate(t, client, notification.TemplateUpsertRequest{
		TemplateKey:     "push-notification-v1",
		Channel:         notification.ChannelPush,
		SubjectTemplate: "Push notice for {{user_id}}",
		BodyTemplate:    "Push body for {{user_id}} and {{event_id}}.",
		Enabled:         true,
	})
	upsertRoutingPolicy(t, client, notification.RoutingPolicyUpsertRequest{
		EventName: "push.event.requested",
		Channels:  []notification.Channel{notification.ChannelPush},
		Enabled:   true,
		Priority:  100,
	})

	account := createProviderAccount(t, client, notification.ProviderAccountUpsertRequest{
		TenantID:    "communication-engine",
		ProviderKey: "fcm-push",
		DisplayName: "FCM Push Demo",
		Channel:     notification.ChannelPush,
		Enabled:     true,
		Config: map[string]string{
			"project_id": "demo-project",
			"base_url":   "http://webhook-sink:8080",
		},
		SecretRefs: map[string]notification.SecretReference{
			"service_account_json": {
				Ref:          "PUSH_PROVIDER_SERVICE_ACCOUNT_JSON_DEMO",
				MaterialType: notification.MaterialTypeSecretJSON,
				Source:       "env",
			},
		},
	})
	createdBinding := createProviderBinding(t, client, notification.ProviderBindingUpsertRequest{
		Channel:           notification.ChannelPush,
		ProviderAccountID: account.ProviderAccountID,
		EndpointURL:       "http://connector-push:8094",
		Enabled:           true,
		Priority:          100,
	})
	if createdBinding.ConnectorName != "connector-push" {
		t.Fatalf("connector_name = %q, want %q", createdBinding.ConnectorName, "connector-push")
	}

	req := notification.NotificationRequest{
		IdempotencyKey: uniqueKey("it-push"),
		EventName:      "push.event.requested",
		TemplateKey:    "push-notification-v1",
		Channels:       []notification.Channel{notification.ChannelPush},
		Recipient: notification.Recipient{
			UserID:    "it-user-push",
			PushToken: "device-token-123",
		},
		Variables: map[string]string{
			"user_id":  "it-user-push",
			"event_id": "push-001",
		},
	}

	accepted := postNotificationRequest(t, client, req)
	details := waitForTerminalRequest(t, client, accepted.RequestID, 20, 1*time.Second)
	if details.Request.Status != notification.RequestStatusDispatched {
		t.Fatalf("request status = %q, want %q", details.Request.Status, notification.RequestStatusDispatched)
	}
	if len(details.DeliveryAttempts) == 0 {
		t.Fatal("expected a delivery attempt for push")
	}
	if details.DeliveryAttempts[0].ConnectorName != "connector-push" {
		t.Fatalf("connector = %q, want %q", details.DeliveryAttempts[0].ConnectorName, "connector-push")
	}
	if details.DeliveryAttempts[0].Status != notification.DeliveryAttemptAccepted {
		t.Fatalf("delivery attempt status = %q, want %q", details.DeliveryAttempts[0].Status, notification.DeliveryAttemptAccepted)
	}
	if details.DeliveryAttempts[0].ProviderMessageID == "" {
		t.Fatal("expected provider message id for push delivery")
	}
	if details.DeliveryAttempts[0].Destination != "device-token-123" {
		t.Fatalf("push destination = %q, want %q", details.DeliveryAttempts[0].Destination, "device-token-123")
	}
}

func TestManagedWhatsAppTemplateMetadataFlowsToConnector(t *testing.T) {
	client := requireIntegrationClient(t)
	capture := startHostCaptureServer(t)

	upsertTemplate(t, client, notification.TemplateUpsertRequest{
		TemplateKey:  "test-template-pratik",
		Channel:      notification.ChannelWhatsApp,
		BodyTemplate: "Hi {{placeholder1}},\n\nHow are you. Welcome to nurture.",
		Metadata: map[string]string{
			"media_type":             "image",
			"gupshup_template_name":  "test_template_pratik",
			"karix_template_name":    "test_template_pratik_1",
			"gupshup_template_id":    "387618",
			"karix_template_id":      "318391747228607",
			"interactive_attributes": `{"button_category":"CallToAction","buttons":[{"type":"url","urlType":"static","url":"https://nrf.page.link/paHospicash","text":"Claim Offer"}]}`,
		},
		Enabled: true,
	})

	eventName := uniqueKey("it-whatsapp-template")
	bindingSet := uniqueKey("it-whatsapp-binding-set")
	upsertRoutingPolicy(t, client, notification.RoutingPolicyUpsertRequest{
		EventName:  eventName,
		Channels:   []notification.Channel{notification.ChannelWhatsApp},
		BindingSet: bindingSet,
		Enabled:    true,
		Priority:   100,
	})

	t.Run("gupshup", func(t *testing.T) {
		reqURL := capture.baseURL(t)
		account := createProviderAccount(t, client, notification.ProviderAccountUpsertRequest{
			TenantID:    "communication-engine",
			ProviderKey: "gupshup-whatsapp",
			DisplayName: "Gupshup WhatsApp Template Test",
			Channel:     notification.ChannelWhatsApp,
			Enabled:     true,
			Config: map[string]string{
				"username": "demo-user",
				"password": "demo-pass",
				"version":  "1.1",
				"base_url": reqURL,
			},
			SecretRefs: map[string]notification.SecretReference{
				"password": {
					Ref:          "WHATSAPP_GUPSHUP_PASSWORD_DEMO",
					MaterialType: notification.MaterialTypeSecretString,
					Source:       "env",
				},
			},
		})
		createProviderBinding(t, client, notification.ProviderBindingUpsertRequest{
			Channel:           notification.ChannelWhatsApp,
			BindingSet:        bindingSet,
			ProviderAccountID: account.ProviderAccountID,
			EndpointURL:       "http://connector-whatsapp:8095",
			Enabled:           true,
			Priority:          100,
		})

		req := notification.NotificationRequest{
			IdempotencyKey: uniqueKey("it-whatsapp-gupshup"),
			EventName:      eventName,
			TemplateKey:    "test-template-pratik",
			Channels:       []notification.Channel{notification.ChannelWhatsApp},
			BindingSet:     bindingSet,
			Recipient: notification.Recipient{
				UserID: "it-user-whatsapp-gupshup",
				Phone:  "918700491033",
			},
			Variables: map[string]string{
				"placeholder1": "Pratik",
			},
		}

		accepted := postNotificationRequest(t, client, req)
		details := waitForTerminalRequest(t, client, accepted.RequestID, 20, 1*time.Second)
		if details.Request.Status != notification.RequestStatusDispatched {
			t.Fatalf("request status = %q, want %q", details.Request.Status, notification.RequestStatusDispatched)
		}

		captured := capture.waitFor(t, 20, 1*time.Second)
		form, err := url.ParseQuery(string(captured.Body))
		if err != nil {
			t.Fatalf("parse gupshup body: %v", err)
		}
		if got := form.Get("method"); got != "SendMediaMessage" {
			t.Fatalf("method = %q, want SendMediaMessage", got)
		}
		if got := form.Get("isTemplate"); got != "" {
			t.Fatalf("isTemplate = %q, want empty for non-interactive media payload", got)
		}
		if got := form.Get("media_url"); got != "https://www.gstatic.com/webp/gallery3/2.png" {
			t.Fatalf("media_url = %q, want media url from metadata", got)
		}
		if got := form.Get("format"); got != "" {
			t.Fatalf("format = %q, want empty for media payload", got)
		}
		if got := form.Get("channel"); got != "" {
			t.Fatalf("channel = %q, want empty to match CE gupshup payload", got)
		}
		if got := form.Get("caption"); !strings.Contains(got, "Hi Pratik") {
			t.Fatalf("caption = %q, want rendered body", got)
		}
	})

	t.Run("karix", func(t *testing.T) {
		reqURL := capture.baseURL(t)
		account := createProviderAccount(t, client, notification.ProviderAccountUpsertRequest{
			TenantID:    "communication-engine",
			ProviderKey: "karix-whatsapp",
			DisplayName: "Karix WhatsApp Template Test",
			Channel:     notification.ChannelWhatsApp,
			Enabled:     true,
			Config: map[string]string{
				"key":           "demo-karix-key",
				"sender":        "917208898844",
				"version":       "v1.0.9",
				"base_url":      reqURL,
				"template_name": "test_template_pratik_1",
			},
			SecretRefs: map[string]notification.SecretReference{
				"key": {
					Ref:          "WHATSAPP_KARIX_KEY_DEMO",
					MaterialType: notification.MaterialTypeSecretString,
					Source:       "env",
				},
			},
		})
		createProviderBinding(t, client, notification.ProviderBindingUpsertRequest{
			Channel:           notification.ChannelWhatsApp,
			BindingSet:        bindingSet,
			ProviderAccountID: account.ProviderAccountID,
			EndpointURL:       "http://connector-whatsapp:8095",
			Enabled:           true,
			Priority:          100,
		})

		req := notification.NotificationRequest{
			IdempotencyKey: uniqueKey("it-whatsapp-karix"),
			EventName:      eventName,
			TemplateKey:    "test-template-pratik",
			Channels:       []notification.Channel{notification.ChannelWhatsApp},
			BindingSet:     bindingSet,
			Recipient: notification.Recipient{
				UserID: "it-user-whatsapp-karix",
				Phone:  "918700491033",
			},
			Variables: map[string]string{
				"placeholder1": "Pratik",
			},
		}

		accepted := postNotificationRequest(t, client, req)
		details := waitForTerminalRequest(t, client, accepted.RequestID, 20, 1*time.Second)
		if details.Request.Status != notification.RequestStatusDispatched {
			t.Fatalf("request status = %q, want %q", details.Request.Status, notification.RequestStatusDispatched)
		}

		captured := capture.waitFor(t, 20, 1*time.Second)
		var payload map[string]any
		if err := json.Unmarshal(captured.Body, &payload); err != nil {
			t.Fatalf("parse karix body: %v", err)
		}
		message, ok := payload["message"].(map[string]any)
		if !ok {
			t.Fatalf("message missing in payload: %#v", payload)
		}
		content, ok := message["content"].(map[string]any)
		if !ok {
			t.Fatalf("content missing in payload: %#v", payload)
		}
		if got, _ := content["type"].(string); got != "MEDIA_TEMPLATE" {
			t.Fatalf("content.type = %q, want MEDIA_TEMPLATE", got)
		}
		mediaTemplate, ok := content["mediaTemplate"].(map[string]any)
		if !ok {
			t.Fatalf("mediaTemplate missing in payload: %#v", payload)
		}
		if got, _ := mediaTemplate["templateId"].(string); got != "test_template_pratik_1" {
			t.Fatalf("templateId = %q, want test_template_pratik_1", got)
		}
		bodyParams, ok := mediaTemplate["bodyParameterValues"].(map[string]any)
		if !ok {
			t.Fatalf("bodyParameterValues missing in payload: %#v", payload)
		}
		if got, _ := bodyParams["placeholder1"].(string); got != "Pratik" {
			t.Fatalf("placeholder1 = %q, want Pratik", got)
		}
	})
}

func TestBindingSetFailoverAcceptsOnBackupConnector(t *testing.T) {
	client := requireIntegrationClient(t)
	bindingSet := uniqueKey("it-failover")

	upsertProviderBinding(t, client, notification.ProviderBindingUpsertRequest{
		Channel:       notification.ChannelEmail,
		BindingSet:    bindingSet,
		ConnectorName: "connector-email-bad",
		EndpointURL:   "http://127.0.0.1:65535",
		Enabled:       true,
		Priority:      10,
	})
	upsertProviderBinding(t, client, notification.ProviderBindingUpsertRequest{
		Channel:       notification.ChannelEmail,
		BindingSet:    bindingSet,
		ConnectorName: "connector-email",
		EndpointURL:   "http://connector-email:8091",
		Enabled:       true,
		Priority:      20,
	})

	req := notification.NotificationRequest{
		IdempotencyKey: uniqueKey("it-failover-request"),
		EventName:      "order.delayed",
		TemplateKey:    "order-delayed-v1",
		Channels:       []notification.Channel{notification.ChannelEmail},
		BindingSet:     bindingSet,
		Recipient: notification.Recipient{
			UserID: "it-user-failover",
			Email:  "integration-failover@example.com",
		},
		Variables: map[string]string{
			"order_id": "IT-ORD-FAILOVER",
			"reason":   "connector_failover",
		},
	}

	accepted := postNotificationRequest(t, client, req)
	details := waitForTerminalRequest(t, client, accepted.RequestID, 20, 1*time.Second)

	if details.Request.Status != notification.RequestStatusDispatched {
		t.Fatalf("request status = %q, want %q", details.Request.Status, notification.RequestStatusDispatched)
	}
	if len(details.DeliveryAttempts) < 2 {
		t.Fatalf("expected at least two delivery attempts for failover, got %d", len(details.DeliveryAttempts))
	}
	if details.DeliveryAttempts[0].ConnectorName != "connector-email-bad" || details.DeliveryAttempts[0].Status != notification.DeliveryAttemptFailed {
		t.Fatalf("first failover attempt = %+v, want failed connector-email-bad", details.DeliveryAttempts[0])
	}
	last := details.DeliveryAttempts[len(details.DeliveryAttempts)-1]
	if last.ConnectorName != "connector-email" || last.Status != notification.DeliveryAttemptAccepted {
		t.Fatalf("last failover attempt = %+v, want accepted connector-email", last)
	}
}

func TestProviderAccountLifecycle(t *testing.T) {
	client := requireIntegrationClient(t)

	defs := getProviderDefinitions(t, client)
	if !hasProviderDefinition(defs, "twilio-sms") {
		t.Fatal("expected provider definitions to include twilio-sms")
	}

	tenantID := uniqueKey("tenant-provider-account")
	created := createProviderAccount(t, client, notification.ProviderAccountUpsertRequest{
		TenantID:    tenantID,
		ProviderKey: "twilio-sms",
		DisplayName: "Twilio SMS Production",
		Channel:     notification.ChannelSMS,
		Enabled:     true,
		Config: map[string]string{
			"from_number": "+14155550123",
		},
		SecretRefs: map[string]notification.SecretReference{
			"account_sid": {
				Ref:          "secret://tenant/" + tenantID + "/twilio/account-sid",
				MaterialType: notification.MaterialTypeSecretString,
				Version:      "current",
				Source:       "vault",
			},
			"auth_token": {
				Ref:          "secret://tenant/" + tenantID + "/twilio/auth-token",
				MaterialType: notification.MaterialTypeSecretString,
				Version:      "current",
				Source:       "vault",
			},
		},
	})
	if created.ProviderAccountID == "" {
		t.Fatal("expected provider_account_id to be populated")
	}
	if created.DisplayName != "Twilio SMS Production" {
		t.Fatalf("display_name = %q, want %q", created.DisplayName, "Twilio SMS Production")
	}
	if created.ProviderKey != "twilio-sms" {
		t.Fatalf("provider_key = %q, want %q", created.ProviderKey, "twilio-sms")
	}
	if created.Config["from_number"] != "+14155550123" {
		t.Fatalf("from_number = %q, want %q", created.Config["from_number"], "+14155550123")
	}
	if created.SecretRefs["account_sid"].MaterialType != notification.MaterialTypeSecretString {
		t.Fatalf("secret material_type = %q, want %q", created.SecretRefs["account_sid"].MaterialType, notification.MaterialTypeSecretString)
	}

	listed := listProviderAccounts(t, client, tenantID)
	if len(listed) != 1 {
		t.Fatalf("provider account list length = %d, want 1", len(listed))
	}
	if listed[0].ProviderAccountID != created.ProviderAccountID {
		t.Fatalf("listed provider_account_id = %q, want %q", listed[0].ProviderAccountID, created.ProviderAccountID)
	}

	loaded := getProviderAccount(t, client, created.ProviderAccountID)
	if loaded.ProviderAccountID != created.ProviderAccountID {
		t.Fatalf("loaded provider_account_id = %q, want %q", loaded.ProviderAccountID, created.ProviderAccountID)
	}

	updatedName := "Twilio SMS Production v2"
	patched := patchProviderAccount(t, client, created.ProviderAccountID, notification.ProviderAccountPatchRequest{
		DisplayName: &updatedName,
	})
	if patched.DisplayName != updatedName {
		t.Fatalf("patched display_name = %q, want %q", patched.DisplayName, updatedName)
	}

	disabled := disableProviderAccount(t, client, created.ProviderAccountID)
	if disabled.Enabled {
		t.Fatal("expected provider account to be disabled")
	}
}

func TestManagedProviderAccountDeliversThroughBinding(t *testing.T) {
	client := requireIntegrationClient(t)
	bindingSet := uniqueKey("it-managed-provider")

	account := createProviderAccount(t, client, notification.ProviderAccountUpsertRequest{
		TenantID:    "communication-engine",
		ProviderKey: "sendgrid-email",
		DisplayName: "SendGrid Email Demo",
		Channel:     notification.ChannelEmail,
		Enabled:     true,
		Config: map[string]string{
			"from_email": "noreply@example.com",
		},
		SecretRefs: map[string]notification.SecretReference{
			"api_key": {
				Ref:          "EMAIL_PROVIDER_API_KEY_DEMO",
				MaterialType: notification.MaterialTypeSecretString,
				Source:       "env",
			},
		},
	})

	upsertProviderBinding(t, client, notification.ProviderBindingUpsertRequest{
		Channel:           notification.ChannelEmail,
		BindingSet:        bindingSet,
		ConnectorName:     "connector-email",
		EndpointURL:       "http://connector-email:8091",
		ProviderAccountID: account.ProviderAccountID,
		Enabled:           true,
		Priority:          10,
	})

	req := notification.NotificationRequest{
		IdempotencyKey: uniqueKey("it-managed-provider-send"),
		EventName:      "order.delayed",
		TemplateKey:    "order-delayed-v1",
		Channels:       []notification.Channel{notification.ChannelEmail},
		BindingSet:     bindingSet,
		Recipient: notification.Recipient{
			UserID: "it-user-managed-provider",
			Email:  "integration-managed-provider@example.com",
		},
		Variables: map[string]string{
			"order_id": "IT-ORD-MANAGED",
			"reason":   "provider_account_routing",
		},
	}

	accepted := postNotificationRequest(t, client, req)
	details := waitForTerminalRequest(t, client, accepted.RequestID, 15, 1*time.Second)
	if details.Request.Status != notification.RequestStatusDispatched {
		t.Fatalf("request status = %q, want %q", details.Request.Status, notification.RequestStatusDispatched)
	}
	if len(details.DeliveryAttempts) == 0 {
		t.Fatal("expected a delivery attempt")
	}
	if details.DeliveryAttempts[0].Status != notification.DeliveryAttemptAccepted {
		t.Fatalf("delivery attempt status = %q, want %q", details.DeliveryAttempts[0].Status, notification.DeliveryAttemptAccepted)
	}
}

func TestProviderAccountStatusEndpoint(t *testing.T) {
	client := requireIntegrationClient(t)
	bindingSet := uniqueKey("it-provider-status")

	account := createProviderAccount(t, client, notification.ProviderAccountUpsertRequest{
		TenantID:    "communication-engine",
		ProviderKey: "sendgrid-email",
		DisplayName: "SendGrid Status Demo",
		Channel:     notification.ChannelEmail,
		Enabled:     true,
		Config: map[string]string{
			"from_email": "noreply@example.com",
		},
		SecretRefs: map[string]notification.SecretReference{
			"api_key": {
				Ref:          "EMAIL_PROVIDER_API_KEY_DEMO",
				MaterialType: notification.MaterialTypeSecretString,
				Source:       "env",
			},
		},
	})

	upsertProviderBinding(t, client, notification.ProviderBindingUpsertRequest{
		Channel:           notification.ChannelEmail,
		BindingSet:        bindingSet,
		ConnectorName:     "connector-email",
		EndpointURL:       "http://connector-email:8091",
		ProviderAccountID: account.ProviderAccountID,
		Enabled:           true,
		Priority:          10,
	})

	createCallbackRoute(t, client, notification.CallbackRouteUpsertRequest{
		ProviderKey:       "email",
		ProviderAccountID: account.ProviderAccountID,
		CallbackPath:      "/v1/providers/email/callbacks",
		VerificationMode:  notification.CallbackVerificationModeSharedSecret,
		VerificationSecretRef: notification.SecretReference{
			Ref:          "CALLBACK_PROVIDER_SECRET_DEMO",
			MaterialType: notification.MaterialTypeSecretString,
			Source:       "env",
		},
		Enabled: true,
	})

	req := notification.NotificationRequest{
		IdempotencyKey: uniqueKey("it-provider-status-request"),
		EventName:      "order.delayed",
		TemplateKey:    "order-delayed-v1",
		Channels:       []notification.Channel{notification.ChannelEmail},
		BindingSet:     bindingSet,
		Recipient: notification.Recipient{
			UserID: "it-user-provider-status",
			Email:  "integration-provider-status@example.com",
		},
		Variables: map[string]string{
			"order_id": "IT-ORD-STATUS",
			"reason":   "status_endpoint",
		},
	}

	accepted := postNotificationRequest(t, client, req)
	_ = waitForTerminalRequest(t, client, accepted.RequestID, 15, 1*time.Second)

	status := getProviderAccountStatus(t, client, account.ProviderAccountID)
	if status.ProviderAccount.ProviderAccountID != account.ProviderAccountID {
		t.Fatalf("provider_account_id = %q, want %q", status.ProviderAccount.ProviderAccountID, account.ProviderAccountID)
	}
	if len(status.ProviderBindings) != 1 {
		t.Fatalf("provider bindings length = %d, want 1", len(status.ProviderBindings))
	}
	if len(status.BindingHealth) == 0 {
		t.Fatal("expected binding health to be populated")
	}
	if status.CallbackRoute == nil {
		t.Fatal("expected callback route to be present")
	}
}

func TestOpenCircuitSkipsProviderAttempt(t *testing.T) {
	client := requireIntegrationClient(t)
	resetProviderBindingHealth(t, client, "binding-email-default")

	failing := notification.NotificationRequest{
		IdempotencyKey: uniqueKey("it-circuit-open"),
		EventName:      "order.delayed",
		TemplateKey:    "order-delayed-v1",
		Channels:       []notification.Channel{notification.ChannelEmail},
		Recipient: notification.Recipient{
			UserID: "it-user-circuit-open",
			Email:  "integration-circuit-open@example.com",
		},
		Variables: map[string]string{
			"order_id": "IT-ORD-CIRCUIT-OPEN",
			"reason":   "provider_outage",
		},
	}

	accepted := postNotificationRequest(t, client, failing)
	_ = waitForRequestCondition(t, client, accepted.RequestID, 20, 1*time.Second, func(details requestDetailsResponse) bool {
		health := getProviderBindingHealth(t, client, "binding-email-default")
		return health.CircuitState == notification.ProviderCircuitStateOpen && len(details.ScheduledRetries) > 0
	})

	healthy := notification.NotificationRequest{
		IdempotencyKey: uniqueKey("it-circuit-skip"),
		EventName:      "order.delayed",
		TemplateKey:    "order-delayed-v1",
		Channels:       []notification.Channel{notification.ChannelEmail},
		Recipient: notification.Recipient{
			UserID: "it-user-circuit-skip",
			Email:  "integration-circuit-skip@example.com",
		},
		Variables: map[string]string{
			"order_id": "IT-ORD-CIRCUIT-SKIP",
			"reason":   "healthy_request",
		},
	}

	skipped := postNotificationRequest(t, client, healthy)
	details := waitForRequestCondition(t, client, skipped.RequestID, 10, 1*time.Second, func(details requestDetailsResponse) bool {
		return len(details.ScheduledRetries) > 0
	})
	if len(details.DeliveryAttempts) != 0 {
		t.Fatalf("expected zero delivery attempts while circuit is open, got %d", len(details.DeliveryAttempts))
	}
}

func requireIntegrationClient(t *testing.T) *testClient {
	t.Helper()

	if os.Getenv("RUN_INTEGRATION") != "1" {
		t.Skip("set RUN_INTEGRATION=1 to run live integration tests")
	}

	baseURL := os.Getenv("NOTIFICATION_API_BASE_URL")
	if baseURL == "" {
		baseURL = "http://localhost:8080"
	}
	callbackBaseURL := os.Getenv("CALLBACK_API_BASE_URL")
	if callbackBaseURL == "" {
		callbackBaseURL = "http://localhost:8082"
	}

	client := &testClient{
		baseURL:         baseURL,
		callbackBaseURL: callbackBaseURL,
		adminToken:      envOrDefault("NOTIFICATION_ADMIN_API_TOKEN", "integration-admin-token"),
		readToken:       envOrDefault("NOTIFICATION_READONLY_API_TOKEN", "integration-read-token"),
		client:          &http.Client{Timeout: 5 * time.Second},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, client.baseURL+"/v1/status", nil)
	if err != nil {
		t.Fatalf("build status request: %v", err)
	}
	resp, err := client.client.Do(req)
	if err != nil {
		t.Skipf("integration API unavailable at %s: %v", client.baseURL, err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Skipf("integration API returned status %d from /v1/status", resp.StatusCode)
	}

	bootstrapTenant := uniqueKey("tenant-client")
	status, body := client.mustJSON(t, http.MethodPost, "/v1/clients", notification.NotificationClientCreateRequest{
		TenantID:   bootstrapTenant,
		ClientName: "integration-test-client",
		Enabled:    true,
		AllowedChannels: []notification.Channel{
			notification.ChannelEmail,
			notification.ChannelSMS,
			notification.ChannelWhatsApp,
			notification.ChannelPush,
			notification.ChannelWebhook,
		},
	})
	if status != http.StatusCreated {
		t.Skipf("integration API client registration failed with status %d: %s", status, string(body))
	}
	var clientResponse notification.NotificationClientCreateResponse
	if err := json.Unmarshal(body, &clientResponse); err != nil {
		t.Fatalf("decode notification client response: %v", err)
	}
	client.apiKey = clientResponse.APIKey

	return client
}

func postNotificationRequest(t *testing.T, client *testClient, req notification.NotificationRequest) notification.NotificationAccepted {
	t.Helper()

	status, body := client.mustJSON(t, http.MethodPost, "/v1/notification-requests", req)
	if status != http.StatusAccepted {
		t.Fatalf("submit request status = %d, want %d, body=%s", status, http.StatusAccepted, string(body))
	}

	var accepted notification.NotificationAccepted
	if err := json.Unmarshal(body, &accepted); err != nil {
		t.Fatalf("decode accepted response: %v", err)
	}
	return accepted
}

func replayDeadLetter(t *testing.T, client *testClient, deadLetterID string) notification.NotificationAccepted {
	t.Helper()

	status, body := client.mustJSON(t, http.MethodPost, "/v1/dead-letters/"+deadLetterID+"/replay", map[string]any{})
	if status != http.StatusAccepted {
		t.Fatalf("replay dead letter status = %d, want %d, body=%s", status, http.StatusAccepted, string(body))
	}

	var accepted notification.NotificationAccepted
	if err := json.Unmarshal(body, &accepted); err != nil {
		t.Fatalf("decode replay accepted response: %v", err)
	}
	return accepted
}

func resetProviderBindingHealth(t *testing.T, client *testClient, bindingID string) {
	t.Helper()
	status, body := client.mustJSON(t, http.MethodPost, "/v1/provider-binding-health/"+bindingID+"/reset", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("reset provider binding health status = %d, want %d, body=%s", status, http.StatusOK, string(body))
	}
}

func getProviderBindingHealth(t *testing.T, client *testClient, bindingID string) providerBindingHealthResponse {
	t.Helper()

	status, body := client.mustRequest(t, http.MethodGet, "/v1/provider-binding-health/"+bindingID, nil)
	if status != http.StatusOK {
		t.Fatalf("get provider binding health status = %d, want %d, body=%s", status, http.StatusOK, string(body))
	}

	var health providerBindingHealthResponse
	if err := json.Unmarshal(body, &health); err != nil {
		t.Fatalf("decode provider binding health response: %v", err)
	}
	return health
}

func upsertProviderBinding(t *testing.T, client *testClient, req notification.ProviderBindingUpsertRequest) {
	t.Helper()
	status, body := client.mustJSON(t, http.MethodPost, "/v1/provider-bindings", req)
	if status != http.StatusOK {
		t.Fatalf("upsert provider binding status = %d, want %d, body=%s", status, http.StatusOK, string(body))
	}
}

func createProviderBinding(t *testing.T, client *testClient, req notification.ProviderBindingUpsertRequest) notification.ProviderBinding {
	t.Helper()
	status, body := client.mustJSON(t, http.MethodPost, "/v1/provider-bindings", req)
	if status != http.StatusOK {
		t.Fatalf("create provider binding status = %d, want %d, body=%s", status, http.StatusOK, string(body))
	}

	var binding notification.ProviderBinding
	if err := json.Unmarshal(body, &binding); err != nil {
		t.Fatalf("decode provider binding response: %v", err)
	}
	return binding
}

func createCallbackRoute(t *testing.T, client *testClient, req notification.CallbackRouteUpsertRequest) notification.CallbackRoute {
	t.Helper()

	status, body := client.mustJSON(t, http.MethodPost, "/v1/callback-routes", req)
	if status != http.StatusCreated {
		t.Fatalf("create callback route status = %d, want %d, body=%s", status, http.StatusCreated, string(body))
	}

	var route notification.CallbackRoute
	if err := json.Unmarshal(body, &route); err != nil {
		t.Fatalf("decode callback route response: %v", err)
	}
	return route
}

func upsertRoutingPolicy(t *testing.T, client *testClient, req notification.RoutingPolicyUpsertRequest) notification.RoutingPolicy {
	t.Helper()

	status, body := client.mustJSON(t, http.MethodPost, "/v1/routing-policies", req)
	if status != http.StatusOK {
		t.Fatalf("upsert routing policy status = %d, want %d, body=%s", status, http.StatusOK, string(body))
	}

	var policy notification.RoutingPolicy
	if err := json.Unmarshal(body, &policy); err != nil {
		t.Fatalf("decode routing policy response: %v", err)
	}
	return policy
}

func upsertTemplate(t *testing.T, client *testClient, req notification.TemplateUpsertRequest) notification.Template {
	t.Helper()

	status, body := client.mustJSON(t, http.MethodPost, "/v1/templates", req)
	if status != http.StatusOK {
		t.Fatalf("upsert template status = %d, want %d, body=%s", status, http.StatusOK, string(body))
	}

	var tmpl notification.Template
	if err := json.Unmarshal(body, &tmpl); err != nil {
		t.Fatalf("decode template response: %v", err)
	}
	return tmpl
}

func getProviderAccountStatus(t *testing.T, client *testClient, providerAccountID string) providerAccountStatusResponse {
	t.Helper()

	status, body := client.mustRequest(t, http.MethodGet, "/v1/provider-accounts/"+providerAccountID+"/status", nil)
	if status != http.StatusOK {
		t.Fatalf("get provider account status code = %d, want %d, body=%s", status, http.StatusOK, string(body))
	}

	var response providerAccountStatusResponse
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("decode provider account status response: %v", err)
	}
	return response
}

func getProviderDefinitions(t *testing.T, client *testClient) []notification.ProviderDefinition {
	t.Helper()

	status, body := client.mustRequest(t, http.MethodGet, "/v1/provider-definitions", nil)
	if status != http.StatusOK {
		t.Fatalf("get provider definitions status = %d, want %d, body=%s", status, http.StatusOK, string(body))
	}

	var response struct {
		ProviderDefinitions []notification.ProviderDefinition `json:"provider_definitions"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("decode provider definitions response: %v", err)
	}
	return response.ProviderDefinitions
}

func getTemplate(t *testing.T, client *testClient, templateKey string, channel notification.Channel, languageCode string) notification.Template {
	t.Helper()

	path := "/v1/templates/" + templateKey + "/" + string(channel)
	if languageCode != "" {
		path += "?language_code=" + url.QueryEscape(languageCode)
	}
	status, body := client.mustRequest(t, http.MethodGet, path, nil)
	if status != http.StatusOK {
		t.Fatalf("get template status = %d, want %d, body=%s", status, http.StatusOK, string(body))
	}

	var tmpl notification.Template
	if err := json.Unmarshal(body, &tmpl); err != nil {
		t.Fatalf("decode template response: %v", err)
	}
	return tmpl
}

type capturedProviderRequest struct {
	Method string
	Path   string
	Body   []byte
	Header http.Header
}

type hostCaptureServer struct {
	url   string
	mu    sync.Mutex
	reqCh chan capturedProviderRequest
}

func startHostCaptureServer(t *testing.T) *hostCaptureServer {
	t.Helper()

	listener, err := net.Listen("tcp4", "0.0.0.0:0")
	if err != nil {
		t.Fatalf("start capture server listener: %v", err)
	}
	server := &http.Server{}
	capture := &hostCaptureServer{
		url:   fmt.Sprintf("http://host.docker.internal:%d", listener.Addr().(*net.TCPAddr).Port),
		reqCh: make(chan capturedProviderRequest, 8),
	}
	server.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = r.Body.Close()
		capture.reqCh <- capturedProviderRequest{
			Method: r.Method,
			Path:   r.URL.Path,
			Body:   body,
			Header: r.Header.Clone(),
		}
		w.Header().Set("X-Provider-Message-ID", "capture-"+uniqueKey("msg"))
		w.WriteHeader(http.StatusOK)
	})
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() {
		_ = server.Close()
	})
	return capture
}

func (c *hostCaptureServer) baseURL(t *testing.T) string {
	t.Helper()
	return c.url
}

func (c *hostCaptureServer) waitFor(t *testing.T, attempts int, delay time.Duration) capturedProviderRequest {
	t.Helper()
	deadline := time.After(time.Duration(attempts) * delay)
	select {
	case req := <-c.reqCh:
		return req
	case <-deadline:
		t.Fatalf("timed out waiting for captured provider request")
	}
	return capturedProviderRequest{}
}

func hasProviderDefinition(definitions []notification.ProviderDefinition, providerKey string) bool {
	for _, definition := range definitions {
		if definition.ProviderKey == providerKey {
			return true
		}
	}
	return false
}

func createProviderAccount(t *testing.T, client *testClient, req notification.ProviderAccountUpsertRequest) notification.ProviderAccount {
	t.Helper()

	status, body := client.mustJSON(t, http.MethodPost, "/v1/provider-accounts", req)
	if status != http.StatusCreated {
		t.Fatalf("create provider account status = %d, want %d, body=%s", status, http.StatusCreated, string(body))
	}

	var account notification.ProviderAccount
	if err := json.Unmarshal(body, &account); err != nil {
		t.Fatalf("decode provider account response: %v", err)
	}
	return account
}

func listProviderAccounts(t *testing.T, client *testClient, tenantID string) []notification.ProviderAccount {
	t.Helper()

	status, body := client.mustRequest(t, http.MethodGet, "/v1/provider-accounts?tenant_id="+tenantID, nil)
	if status != http.StatusOK {
		t.Fatalf("list provider accounts status = %d, want %d, body=%s", status, http.StatusOK, string(body))
	}

	var response providerAccountsResponse
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("decode provider accounts response: %v", err)
	}
	return response.ProviderAccounts
}

func getProviderAccount(t *testing.T, client *testClient, providerAccountID string) notification.ProviderAccount {
	t.Helper()

	status, body := client.mustRequest(t, http.MethodGet, "/v1/provider-accounts/"+providerAccountID, nil)
	if status != http.StatusOK {
		t.Fatalf("get provider account status = %d, want %d, body=%s", status, http.StatusOK, string(body))
	}

	var account notification.ProviderAccount
	if err := json.Unmarshal(body, &account); err != nil {
		t.Fatalf("decode provider account response: %v", err)
	}
	return account
}

func patchProviderAccount(t *testing.T, client *testClient, providerAccountID string, req notification.ProviderAccountPatchRequest) notification.ProviderAccount {
	t.Helper()

	status, body := client.mustJSON(t, http.MethodPatch, "/v1/provider-accounts/"+providerAccountID, req)
	if status != http.StatusOK {
		t.Fatalf("patch provider account status = %d, want %d, body=%s", status, http.StatusOK, string(body))
	}

	var account notification.ProviderAccount
	if err := json.Unmarshal(body, &account); err != nil {
		t.Fatalf("decode patched provider account response: %v", err)
	}
	return account
}

func disableProviderAccount(t *testing.T, client *testClient, providerAccountID string) notification.ProviderAccount {
	t.Helper()

	status, body := client.mustJSON(t, http.MethodPost, "/v1/provider-accounts/"+providerAccountID+"/disable", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("disable provider account status = %d, want %d, body=%s", status, http.StatusOK, string(body))
	}

	var account notification.ProviderAccount
	if err := json.Unmarshal(body, &account); err != nil {
		t.Fatalf("decode disabled provider account response: %v", err)
	}
	return account
}

func postProviderCallback(t *testing.T, client *testClient, provider string, payload notification.ProviderCallback) {
	t.Helper()

	postProviderCallbackWithHeaders(t, client, provider, payload, nil)
}

func postProviderCallbackWithHeaders(t *testing.T, client *testClient, provider string, payload notification.ProviderCallback, headers map[string]string) {
	t.Helper()

	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal provider callback payload: %v", err)
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, client.callbackBaseURL+"/v1/providers/"+provider+"/callbacks", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build provider callback request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	resp, err := client.client.Do(req)
	if err != nil {
		t.Fatalf("perform provider callback request: %v", err)
	}
	defer resp.Body.Close()
	responseBody, err := ioReadAll(resp)
	if err != nil {
		t.Fatalf("read provider callback response: %v", err)
	}
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("provider callback status = %d, want %d, body=%s", resp.StatusCode, http.StatusAccepted, string(responseBody))
	}
}

func postRawProviderCallback[T any](t *testing.T, client *testClient, provider string, payload T) (int, []byte) {
	t.Helper()

	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal provider callback payload: %v", err)
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, client.callbackBaseURL+"/v1/providers/"+provider+"/callbacks", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build provider callback request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.client.Do(req)
	if err != nil {
		t.Fatalf("perform provider callback request: %v", err)
	}
	defer resp.Body.Close()
	responseBody, err := ioReadAll(resp)
	if err != nil {
		t.Fatalf("read provider callback response: %v", err)
	}
	return resp.StatusCode, responseBody
}

func postRawProviderCallbackJSON(t *testing.T, client *testClient, provider string, payload any) (int, []byte) {
	t.Helper()
	return postRawProviderCallback(t, client, provider, payload)
}

func waitForTerminalRequest(t *testing.T, client *testClient, requestID string, attempts int, interval time.Duration) requestDetailsResponse {
	t.Helper()
	return waitForRequestCondition(t, client, requestID, attempts, interval, func(details requestDetailsResponse) bool {
		switch details.Request.Status {
		case notification.RequestStatusDispatched, notification.RequestStatusDelivered, notification.RequestStatusFailed, notification.RequestStatusSuppressed, notification.RequestStatusUnsupported, notification.RequestStatusExpired:
			return true
		default:
			return false
		}
	})
}

func waitForRequestCondition(t *testing.T, client *testClient, requestID string, attempts int, interval time.Duration, ready func(requestDetailsResponse) bool) requestDetailsResponse {
	t.Helper()

	var last requestDetailsResponse
	for i := 0; i < attempts; i++ {
		last = getRequestDetails(t, client, requestID)
		if ready(last) {
			return last
		}
		time.Sleep(interval)
	}
	t.Fatalf("request %s did not reach expected condition, last status=%q", requestID, last.Request.Status)
	return requestDetailsResponse{}
}

func getRequestDetails(t *testing.T, client *testClient, requestID string) requestDetailsResponse {
	t.Helper()

	status, body := client.mustRequest(t, http.MethodGet, "/v1/notification-requests/"+requestID, nil)
	if status != http.StatusOK {
		t.Fatalf("get request details status = %d, want %d, body=%s", status, http.StatusOK, string(body))
	}

	var details requestDetailsResponse
	if err := json.Unmarshal(body, &details); err != nil {
		t.Fatalf("decode request details: %v", err)
	}
	return details
}

func (c *testClient) mustJSON(t *testing.T, method string, path string, payload any) (int, []byte) {
	t.Helper()

	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return c.mustRequest(t, method, path, body)
}

func (c *testClient) mustRequest(t *testing.T, method string, path string, payload []byte) (int, []byte) {
	t.Helper()

	req, err := http.NewRequestWithContext(context.Background(), method, c.baseURL+path, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.adminToken != "" {
		req.Header.Set("X-Notification-Admin-Token", c.adminToken)
	}
	if c.readToken != "" && c.adminToken == "" {
		req.Header.Set("X-Notification-Read-Token", c.readToken)
	}
	if strings.HasPrefix(path, "/v1/notification-requests") && c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		t.Fatalf("perform request: %v", err)
	}
	defer resp.Body.Close()

	body, err := ioReadAll(resp)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	return resp.StatusCode, body
}

func (c *testClient) readOnlyClient() *testClient {
	clone := *c
	clone.adminToken = ""
	clone.readToken = c.readToken
	clone.apiKey = ""
	return &clone
}

func uniqueKey(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UTC().UnixNano())
}

func envOrDefault(key string, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func ioReadAll(resp *http.Response) ([]byte, error) {
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(resp.Body); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
