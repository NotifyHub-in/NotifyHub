package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/your-org/notification-control-plane/libs/contracts/notification"
)

type testClient struct {
	baseURL         string
	callbackBaseURL string
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

func TestRetryableFailureSchedulesRetry(t *testing.T) {
	client := requireIntegrationClient(t)
	resetProviderBindingHealth(t, client, "binding-email-default")

	req := notification.NotificationRequest{
		IdempotencyKey: uniqueKey("it-retry"),
		EventName:      "order.delayed",
		TemplateKey:    "order-delayed-v1",
		Channels:       []notification.Channel{notification.ChannelEmail},
		Recipient: notification.Recipient{
			UserID: "it-user-retry",
			Email:  "integration-retry@example.com",
		},
		Variables: map[string]string{
			"order_id": "IT-ORD-RETRY",
			"reason":   "provider_outage",
		},
		Metadata: map[string]string{
			"simulate_failure": "provider_outage",
		},
	}

	accepted := postNotificationRequest(t, client, req)
	details := waitForRequestCondition(t, client, accepted.RequestID, 20, 1*time.Second, func(details requestDetailsResponse) bool {
		return len(details.ScheduledRetries) > 0
	})

	if details.Request.Status != notification.RequestStatusProcessing {
		t.Fatalf("request status = %q, want %q while retry is pending", details.Request.Status, notification.RequestStatusProcessing)
	}
	if len(details.ScheduledRetries) == 0 {
		t.Fatal("expected at least one scheduled retry")
	}
}

func TestNonRetryableFailureCreatesDeadLetterAndReplay(t *testing.T) {
	client := requireIntegrationClient(t)

	req := notification.NotificationRequest{
		IdempotencyKey: uniqueKey("it-dead-letter"),
		EventName:      "order.delayed",
		TemplateKey:    "order-delayed-v1",
		Channels:       []notification.Channel{notification.ChannelEmail},
		Recipient: notification.Recipient{
			UserID: "it-user-dead-letter",
			Email:  "not-an-email",
		},
		Variables: map[string]string{
			"order_id": "IT-ORD-DLQ",
			"reason":   "invalid_destination",
		},
	}

	accepted := postNotificationRequest(t, client, req)
	details := waitForRequestCondition(t, client, accepted.RequestID, 15, 1*time.Second, func(details requestDetailsResponse) bool {
		return details.Request.Status == notification.RequestStatusFailed && len(details.DeadLetters) > 0
	})

	if len(details.ScheduledRetries) != 0 {
		t.Fatalf("expected no scheduled retries for non-retryable failure, got %d", len(details.ScheduledRetries))
	}
	if len(details.DeadLetters) == 0 {
		t.Fatal("expected a dead letter")
	}

	replayAccepted := replayDeadLetter(t, client, details.DeadLetters[0].DeadLetterID)
	if replayAccepted.RequestID == "" {
		t.Fatal("expected replay request id to be set")
	}

	reloaded := getRequestDetails(t, client, accepted.RequestID)
	if len(reloaded.DeadLetters) == 0 || reloaded.DeadLetters[0].ReplayRequestID == "" {
		t.Fatal("expected dead letter to record replay_request_id after replay")
	}
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
		Metadata: map[string]string{
			"simulate_failure": "provider_outage",
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

func postProviderCallback(t *testing.T, client *testClient, provider string, payload notification.ProviderCallback) {
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
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("provider callback status = %d, want %d, body=%s", resp.StatusCode, http.StatusAccepted, string(responseBody))
	}
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

func uniqueKey(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UTC().UnixNano())
}

func ioReadAll(resp *http.Response) ([]byte, error) {
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(resp.Body); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
