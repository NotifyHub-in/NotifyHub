package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/your-org/notification-control-plane/libs/contracts/notification"
)

type config struct {
	apiBaseURL        string
	callbackBaseURL   string
	totalRequests     int
	concurrency       int
	timeout           time.Duration
	callbackFraction  float64
	pollAttempts      int
	pollInterval      time.Duration
	webhookTargetURL  string
	suppressedUserID  string
	suppressedAddress string
}

type runner struct {
	cfg        config
	client     *http.Client
	random     *rand.Rand
	randomMu   sync.Mutex
	runID      string
	sequence   atomic.Uint64
	requests   atomic.Uint64
	setupOnce  sync.Once
	setupError error
}

type scenario struct {
	name   string
	weight int
	run    func(context.Context, *runner, int) result
}

type result struct {
	scenario   string
	outcome    string
	statusCode int
	duration   time.Duration
	err        error
}

type requestDetailsResponse struct {
	Request          notification.NotificationRecord       `json:"request"`
	DeliveryAttempts []notification.DeliveryAttempt        `json:"delivery_attempts"`
	ScheduledRetries []notification.ScheduledRetry         `json:"scheduled_retries"`
	DeadLetters      []notification.DeadLetterNotification `json:"dead_letters"`
	WebhookAttempts  []notification.WebhookDeliveryAttempt `json:"webhook_delivery_attempts"`
}

type summary struct {
	mu                sync.Mutex
	total             int
	failures          int
	scenarioCounts    map[string]int
	outcomeCounts     map[string]int
	statusCodeCounts  map[int]int
	scenarioDurations map[string]time.Duration
}

func main() {
	cfg := config{}
	flag.StringVar(&cfg.apiBaseURL, "api-base-url", envOrDefault("LOADTEST_API_BASE_URL", "http://localhost:8080"), "Base URL for the notification API")
	flag.StringVar(&cfg.callbackBaseURL, "callback-base-url", envOrDefault("LOADTEST_CALLBACK_BASE_URL", "http://localhost:8082"), "Base URL for the callback gateway")
	flag.IntVar(&cfg.totalRequests, "requests", envInt("LOADTEST_REQUESTS", 240), "Total scenario executions")
	flag.IntVar(&cfg.concurrency, "concurrency", envInt("LOADTEST_CONCURRENCY", 24), "Number of concurrent workers")
	flag.DurationVar(&cfg.timeout, "timeout", envDuration("LOADTEST_TIMEOUT", 8*time.Second), "Per-request HTTP timeout")
	flag.Float64Var(&cfg.callbackFraction, "callback-fraction", envFloat("LOADTEST_CALLBACK_FRACTION", 0.7), "Fraction of successful accepted requests that should receive provider callbacks")
	flag.IntVar(&cfg.pollAttempts, "poll-attempts", envInt("LOADTEST_POLL_ATTEMPTS", 10), "How many times to poll request details while waiting for attempts")
	flag.DurationVar(&cfg.pollInterval, "poll-interval", envDuration("LOADTEST_POLL_INTERVAL", 300*time.Millisecond), "Interval between request-detail polls")
	flag.StringVar(&cfg.webhookTargetURL, "webhook-target-url", envOrDefault("LOADTEST_WEBHOOK_TARGET_URL", "http://webhook-sink:8080/lifecycle"), "Lifecycle webhook target URL to ensure exists before the run")
	flag.StringVar(&cfg.suppressedUserID, "suppressed-user-id", envOrDefault("LOADTEST_SUPPRESSED_USER_ID", "loadtest-suppressed-user"), "User ID used for suppressed email scenarios")
	flag.StringVar(&cfg.suppressedAddress, "suppressed-address", envOrDefault("LOADTEST_SUPPRESSED_ADDRESS", "suppressed@example.com"), "Email address used for suppressed email scenarios")
	flag.Parse()

	if cfg.totalRequests < 1 {
		log.Fatal("requests must be at least 1")
	}
	if cfg.concurrency < 1 {
		log.Fatal("concurrency must be at least 1")
	}

	r := &runner{
		cfg: cfg,
		client: &http.Client{
			Timeout: cfg.timeout,
		},
		random: rand.New(rand.NewSource(time.Now().UnixNano())),
		runID:  time.Now().UTC().Format("20060102T150405.000000000"),
	}

	ctx := context.Background()
	if err := r.setup(ctx); err != nil {
		log.Fatalf("setup failed: %v", err)
	}

	scenarios := weightedScenarios()
	jobs := make(chan int)
	results := make(chan result, cfg.totalRequests)
	var wg sync.WaitGroup

	for worker := 0; worker < cfg.concurrency; worker++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for jobID := range jobs {
				sc := scenarios[jobID%len(scenarios)]
				results <- sc.run(ctx, r, workerID)
			}
		}(worker)
	}

	start := time.Now()
	for i := 0; i < cfg.totalRequests; i++ {
		jobs <- i
	}
	close(jobs)

	go func() {
		wg.Wait()
		close(results)
	}()

	var stats summary
	stats.scenarioCounts = make(map[string]int)
	stats.outcomeCounts = make(map[string]int)
	stats.statusCodeCounts = make(map[int]int)
	stats.scenarioDurations = make(map[string]time.Duration)

	for res := range results {
		stats.add(res)
	}

	fmt.Printf("load test completed in %s\n", time.Since(start).Round(time.Millisecond))
	stats.print()
}

func (r *runner) setup(ctx context.Context) error {
	r.setupOnce.Do(func() {
		subscriptionPayload := map[string]any{
			"target_url": r.cfg.webhookTargetURL,
			"enabled":    true,
		}
		if _, _, err := r.postJSON(ctx, http.MethodPost, r.cfg.apiBaseURL+"/v1/webhook-subscriptions", subscriptionPayload); err != nil {
			r.setupError = fmt.Errorf("ensure webhook subscription: %w", err)
			return
		}

		preferencePayload := map[string]any{
			"user_id":    r.cfg.suppressedUserID,
			"channel":    "email",
			"is_enabled": false,
		}
		if _, _, err := r.postJSON(ctx, http.MethodPost, r.cfg.apiBaseURL+"/v1/preference-policies", preferencePayload); err != nil {
			r.setupError = fmt.Errorf("ensure suppressed preference: %w", err)
			return
		}
	})
	return r.setupError
}

func weightedScenarios() []scenario {
	base := []scenario{
		{name: "email_happy", weight: 28, run: runEmailHappy},
		{name: "sms_happy", weight: 18, run: runSMSHappy},
		{name: "routing_webhook_happy", weight: 14, run: runRoutedWebhookHappy},
		{name: "suppressed_email", weight: 10, run: runSuppressedEmail},
		{name: "retry_dlq_replay", weight: 6, run: runRetryDLQReplay},
		{name: "missing_variable", weight: 8, run: runMissingVariable},
		{name: "unsupported_channel", weight: 7, run: runUnsupportedChannel},
		{name: "idempotent_replay", weight: 7, run: runIdempotentReplay},
		{name: "conflict_replay", weight: 5, run: runConflictReplay},
		{name: "validation_failed", weight: 2, run: runValidationFailed},
		{name: "invalid_json", weight: 1, run: runInvalidJSON},
	}

	var expanded []scenario
	for _, sc := range base {
		for i := 0; i < sc.weight; i++ {
			expanded = append(expanded, sc)
		}
	}
	return expanded
}

func runEmailHappy(ctx context.Context, r *runner, workerID int) result {
	req := notification.NotificationRequest{
		IdempotencyKey: r.newKey("load-email"),
		EventName:      "order.delayed",
		TemplateKey:    "order-delayed-v1",
		Channels:       []notification.Channel{notification.ChannelEmail},
		Recipient: notification.Recipient{
			UserID: fmt.Sprintf("user-email-%d-%d", workerID, r.nextID()),
			Email:  fmt.Sprintf("email-%d@example.com", r.nextID()),
		},
		Variables: map[string]string{
			"order_id": fmt.Sprintf("ORD-%d", r.nextID()),
			"reason":   "carrier_delay",
		},
		Metadata: map[string]string{
			"scenario": "email_happy",
		},
		Priority: "high",
	}
	return r.submitAndMaybeCallback(ctx, "email_happy", req, true)
}

func runSMSHappy(ctx context.Context, r *runner, workerID int) result {
	req := notification.NotificationRequest{
		IdempotencyKey: r.newKey("load-sms"),
		EventName:      "otp.requested",
		TemplateKey:    "otp-requested-v1",
		Channels:       []notification.Channel{notification.ChannelSMS},
		Recipient: notification.Recipient{
			UserID: fmt.Sprintf("user-sms-%d-%d", workerID, r.nextID()),
			Phone:  fmt.Sprintf("+1555%06d", r.nextID()%1000000),
		},
		Variables: map[string]string{
			"otp": fmt.Sprintf("%06d", r.nextID()%1000000),
		},
		Metadata: map[string]string{
			"scenario": "sms_happy",
		},
		Priority: "high",
	}
	return r.submitAndMaybeCallback(ctx, "sms_happy", req, true)
}

func runRoutedWebhookHappy(ctx context.Context, r *runner, workerID int) result {
	req := notification.NotificationRequest{
		IdempotencyKey: r.newKey("load-webhook"),
		EventName:      "payment.failed",
		TemplateKey:    "payment-failed-v1",
		Recipient: notification.Recipient{
			UserID:  fmt.Sprintf("merchant-%d-%d", workerID, r.nextID()),
			Webhook: fmt.Sprintf("https://example.com/hooks/%d", r.nextID()),
		},
		Variables: map[string]string{
			"payment_id": fmt.Sprintf("PAY-%d", r.nextID()),
			"reason":     "bank_timeout",
		},
		Metadata: map[string]string{
			"scenario": "routing_webhook_happy",
		},
		Priority: "medium",
	}
	return r.submitAndMaybeCallback(ctx, "routing_webhook_happy", req, true)
}

func runSuppressedEmail(ctx context.Context, r *runner, _ int) result {
	req := notification.NotificationRequest{
		IdempotencyKey: r.newKey("load-suppressed"),
		EventName:      "order.delayed",
		TemplateKey:    "order-delayed-v1",
		Channels:       []notification.Channel{notification.ChannelEmail},
		Recipient: notification.Recipient{
			UserID: r.cfg.suppressedUserID,
			Email:  r.cfg.suppressedAddress,
		},
		Variables: map[string]string{
			"order_id": fmt.Sprintf("ORD-SUP-%d", r.nextID()),
		},
		Metadata: map[string]string{
			"scenario": "suppressed_email",
		},
		Priority: "low",
	}
	return r.submitAndMaybeCallback(ctx, "suppressed_email", req, false)
}

func runRetryDLQReplay(ctx context.Context, r *runner, workerID int) result {
	start := time.Now()
	bindingSet := fmt.Sprintf("loadtest-retry-%s-%d-%d", r.runID, workerID, r.nextID())

	badBinding := notification.ProviderBindingUpsertRequest{
		Channel:       notification.ChannelEmail,
		BindingSet:    bindingSet,
		ConnectorName: "connector-email-bad",
		EndpointURL:   "http://connector-email-bad:8099",
		Enabled:       true,
		Priority:      10,
	}
	status, body, err := r.postJSON(ctx, http.MethodPost, r.cfg.apiBaseURL+"/v1/provider-bindings", badBinding)
	if err != nil {
		return result{scenario: "retry_dlq_replay", outcome: "binding_request_error", statusCode: status, duration: time.Since(start), err: err}
	}
	if status != http.StatusOK {
		return result{scenario: "retry_dlq_replay", outcome: fmt.Sprintf("binding_status_%d", status), statusCode: status, duration: time.Since(start), err: fmt.Errorf("bad binding setup failed: %s", body)}
	}

	req := notification.NotificationRequest{
		IdempotencyKey: r.newKey("load-retry-dlq"),
		EventName:      "order.delayed",
		TemplateKey:    "order-delayed-v1",
		Channels:       []notification.Channel{notification.ChannelEmail},
		BindingSet:     bindingSet,
		Recipient: notification.Recipient{
			UserID: fmt.Sprintf("user-retry-%d-%d", workerID, r.nextID()),
			Email:  fmt.Sprintf("retry-%d@example.com", r.nextID()),
		},
		Variables: map[string]string{
			"order_id": fmt.Sprintf("ORD-RETRY-%d", r.nextID()),
			"reason":   "connector_down",
		},
		Metadata: map[string]string{
			"scenario": "retry_dlq_replay",
		},
		Priority: "high",
	}

	status, body, err = r.postJSON(ctx, http.MethodPost, r.cfg.apiBaseURL+"/v1/notification-requests", req)
	if err != nil {
		return result{scenario: "retry_dlq_replay", outcome: "request_error", statusCode: status, duration: time.Since(start), err: err}
	}
	if status != http.StatusAccepted {
		return result{scenario: "retry_dlq_replay", outcome: fmt.Sprintf("unexpected_submit_%d", status), statusCode: status, duration: time.Since(start), err: fmt.Errorf("submit failed: %s", body)}
	}

	var accepted notification.NotificationAccepted
	if err := json.Unmarshal(body, &accepted); err != nil {
		return result{scenario: "retry_dlq_replay", outcome: "decode_failed", statusCode: status, duration: time.Since(start), err: err}
	}

	details, err := r.waitForTerminalRequestDetails(ctx, accepted.RequestID, 80, 500*time.Millisecond)
	if err != nil {
		return result{scenario: "retry_dlq_replay", outcome: "wait_failed", statusCode: status, duration: time.Since(start), err: err}
	}
	if details.Request.Status != notification.RequestStatusFailed || len(details.DeadLetters) == 0 {
		return result{scenario: "retry_dlq_replay", outcome: "dlq_missing", statusCode: status, duration: time.Since(start), err: fmt.Errorf("expected failed request with dead letter, got status=%s dead_letters=%d", details.Request.Status, len(details.DeadLetters))}
	}

	goodBinding := notification.ProviderBindingUpsertRequest{
		Channel:       notification.ChannelEmail,
		BindingSet:    bindingSet,
		ConnectorName: "connector-email",
		EndpointURL:   "http://connector-email:8091",
		Enabled:       true,
		Priority:      10,
	}
	status, body, err = r.postJSON(ctx, http.MethodPost, r.cfg.apiBaseURL+"/v1/provider-bindings", goodBinding)
	if err != nil {
		return result{scenario: "retry_dlq_replay", outcome: "binding_request_error", statusCode: status, duration: time.Since(start), err: err}
	}
	if status != http.StatusOK {
		return result{scenario: "retry_dlq_replay", outcome: fmt.Sprintf("binding_fix_status_%d", status), statusCode: status, duration: time.Since(start), err: fmt.Errorf("good binding setup failed: %s", body)}
	}

	deadLetterID := details.DeadLetters[0].DeadLetterID
	status, body, err = r.postRaw(ctx, http.MethodPost, r.cfg.apiBaseURL+"/v1/dead-letters/"+deadLetterID+"/replay", []byte("{}"))
	if err != nil {
		return result{scenario: "retry_dlq_replay", outcome: "replay_request_error", statusCode: status, duration: time.Since(start), err: err}
	}
	if status != http.StatusAccepted {
		return result{scenario: "retry_dlq_replay", outcome: fmt.Sprintf("unexpected_replay_%d", status), statusCode: status, duration: time.Since(start), err: fmt.Errorf("replay failed: %s", body)}
	}

	var replayAccepted notification.NotificationAccepted
	if err := json.Unmarshal(body, &replayAccepted); err != nil {
		return result{scenario: "retry_dlq_replay", outcome: "replay_decode_failed", statusCode: status, duration: time.Since(start), err: err}
	}

	replayDetails, err := r.waitForRequestDetails(ctx, replayAccepted.RequestID)
	if err != nil {
		return result{scenario: "retry_dlq_replay", outcome: "replay_wait_failed", statusCode: status, duration: time.Since(start), err: err}
	}
	if len(replayDetails.DeliveryAttempts) == 0 {
		return result{scenario: "retry_dlq_replay", outcome: "replay_attempt_missing", statusCode: status, duration: time.Since(start), err: fmt.Errorf("replay request produced no delivery attempts")}
	}
	r.maybeCallbackFromAccepted(ctx, replayAccepted, true)

	return result{scenario: "retry_dlq_replay", outcome: "replayed", statusCode: http.StatusAccepted, duration: time.Since(start)}
}

func runMissingVariable(ctx context.Context, r *runner, workerID int) result {
	req := notification.NotificationRequest{
		IdempotencyKey: r.newKey("load-missing-var"),
		EventName:      "order.delayed",
		TemplateKey:    "order-delayed-v1",
		Channels:       []notification.Channel{notification.ChannelEmail},
		Recipient: notification.Recipient{
			UserID: fmt.Sprintf("user-missing-%d-%d", workerID, r.nextID()),
			Email:  fmt.Sprintf("missing-%d@example.com", r.nextID()),
		},
		Variables: map[string]string{},
		Metadata: map[string]string{
			"scenario": "missing_variable",
		},
		Priority: "medium",
	}
	return r.submitAndMaybeCallback(ctx, "missing_variable", req, false)
}

func runUnsupportedChannel(ctx context.Context, r *runner, workerID int) result {
	req := notification.NotificationRequest{
		IdempotencyKey: r.newKey("load-unsupported"),
		EventName:      "order.delayed",
		TemplateKey:    "order-delayed-v1",
		Channels:       []notification.Channel{"push"},
		Recipient: notification.Recipient{
			UserID: fmt.Sprintf("user-push-%d-%d", workerID, r.nextID()),
		},
		Variables: map[string]string{
			"order_id": fmt.Sprintf("ORD-PUSH-%d", r.nextID()),
		},
		Metadata: map[string]string{
			"scenario": "unsupported_channel",
		},
		Priority: "low",
	}
	return r.submitAndMaybeCallback(ctx, "unsupported_channel", req, false)
}

func runIdempotentReplay(ctx context.Context, r *runner, workerID int) result {
	start := time.Now()
	key := r.newKey("load-replay")
	req := notification.NotificationRequest{
		IdempotencyKey: key,
		EventName:      "order.delayed",
		TemplateKey:    "order-delayed-v1",
		Channels:       []notification.Channel{notification.ChannelEmail},
		Recipient: notification.Recipient{
			UserID: fmt.Sprintf("user-replay-%d-%d", workerID, r.nextID()),
			Email:  fmt.Sprintf("replay-%d@example.com", r.nextID()),
		},
		Variables: map[string]string{
			"order_id": fmt.Sprintf("ORD-REPLAY-%d", r.nextID()),
			"reason":   "warehouse_delay",
		},
		Metadata: map[string]string{
			"scenario": "idempotent_replay",
		},
		Priority: "medium",
	}

	status1, body1, err := r.postJSON(ctx, http.MethodPost, r.cfg.apiBaseURL+"/v1/notification-requests", req)
	if err != nil {
		return result{scenario: "idempotent_replay", outcome: "request_error", statusCode: status1, duration: time.Since(start), err: err}
	}
	if status1 != http.StatusAccepted {
		return result{scenario: "idempotent_replay", outcome: fmt.Sprintf("unexpected_first_%d", status1), statusCode: status1, duration: time.Since(start), err: fmt.Errorf("unexpected first status %d: %s", status1, body1)}
	}

	status2, body2, err := r.postJSON(ctx, http.MethodPost, r.cfg.apiBaseURL+"/v1/notification-requests", req)
	if err != nil {
		return result{scenario: "idempotent_replay", outcome: "request_error", statusCode: status2, duration: time.Since(start), err: err}
	}

	var accepted notification.NotificationAccepted
	if err := json.Unmarshal(body2, &accepted); err != nil {
		return result{scenario: "idempotent_replay", outcome: "decode_failed", statusCode: status2, duration: time.Since(start), err: err}
	}
	if status2 == http.StatusOK && accepted.IdempotentReplay {
		r.maybeCallbackFromAccepted(ctx, accepted, true)
		return result{scenario: "idempotent_replay", outcome: "idempotent_replay", statusCode: status2, duration: time.Since(start)}
	}
	return result{scenario: "idempotent_replay", outcome: fmt.Sprintf("unexpected_second_%d", status2), statusCode: status2, duration: time.Since(start), err: fmt.Errorf("unexpected second response: %s", body2)}
}

func runConflictReplay(ctx context.Context, r *runner, workerID int) result {
	start := time.Now()
	key := r.newKey("load-conflict")
	base := notification.NotificationRequest{
		IdempotencyKey: key,
		EventName:      "order.delayed",
		TemplateKey:    "order-delayed-v1",
		Channels:       []notification.Channel{notification.ChannelEmail},
		Recipient: notification.Recipient{
			UserID: fmt.Sprintf("user-conflict-%d-%d", workerID, r.nextID()),
			Email:  fmt.Sprintf("conflict-%d@example.com", r.nextID()),
		},
		Variables: map[string]string{
			"order_id": fmt.Sprintf("ORD-CONFLICT-%d", r.nextID()),
			"reason":   "inventory_delay",
		},
		Metadata: map[string]string{
			"scenario": "conflict_replay",
		},
		Priority: "medium",
	}

	status1, _, err := r.postJSON(ctx, http.MethodPost, r.cfg.apiBaseURL+"/v1/notification-requests", base)
	if err != nil {
		return result{scenario: "conflict_replay", outcome: "request_error", statusCode: status1, duration: time.Since(start), err: err}
	}
	if status1 != http.StatusAccepted {
		return result{scenario: "conflict_replay", outcome: fmt.Sprintf("unexpected_first_%d", status1), statusCode: status1, duration: time.Since(start), err: fmt.Errorf("unexpected first status %d", status1)}
	}

	conflict := base
	conflict.Metadata = map[string]string{"scenario": "conflict_replay", "variant": "changed"}
	status2, body2, err := r.postJSON(ctx, http.MethodPost, r.cfg.apiBaseURL+"/v1/notification-requests", conflict)
	if err != nil {
		return result{scenario: "conflict_replay", outcome: "request_error", statusCode: status2, duration: time.Since(start), err: err}
	}
	if status2 == http.StatusConflict {
		return result{scenario: "conflict_replay", outcome: "conflict", statusCode: status2, duration: time.Since(start)}
	}
	return result{scenario: "conflict_replay", outcome: fmt.Sprintf("unexpected_second_%d", status2), statusCode: status2, duration: time.Since(start), err: fmt.Errorf("unexpected second response: %s", body2)}
}

func runValidationFailed(ctx context.Context, r *runner, _ int) result {
	start := time.Now()
	payload := map[string]any{
		"event_name":   "order.delayed",
		"template_key": "order-delayed-v1",
	}
	status, body, err := r.postJSON(ctx, http.MethodPost, r.cfg.apiBaseURL+"/v1/notification-requests", payload)
	if err != nil {
		return result{scenario: "validation_failed", outcome: "request_error", statusCode: status, duration: time.Since(start), err: err}
	}
	if status == http.StatusBadRequest {
		return result{scenario: "validation_failed", outcome: "validation_failed", statusCode: status, duration: time.Since(start)}
	}
	return result{scenario: "validation_failed", outcome: fmt.Sprintf("unexpected_%d", status), statusCode: status, duration: time.Since(start), err: fmt.Errorf("unexpected response: %s", body)}
}

func runInvalidJSON(ctx context.Context, r *runner, _ int) result {
	start := time.Now()
	status, body, err := r.postRaw(ctx, http.MethodPost, r.cfg.apiBaseURL+"/v1/notification-requests", []byte(`{"idempotency_key":`))
	if err != nil {
		return result{scenario: "invalid_json", outcome: "request_error", statusCode: status, duration: time.Since(start), err: err}
	}
	if status == http.StatusBadRequest {
		return result{scenario: "invalid_json", outcome: "invalid_payload", statusCode: status, duration: time.Since(start)}
	}
	return result{scenario: "invalid_json", outcome: fmt.Sprintf("unexpected_%d", status), statusCode: status, duration: time.Since(start), err: fmt.Errorf("unexpected response: %s", body)}
}

func (r *runner) submitAndMaybeCallback(ctx context.Context, scenarioName string, req notification.NotificationRequest, withCallback bool) result {
	start := time.Now()
	status, body, err := r.postJSON(ctx, http.MethodPost, r.cfg.apiBaseURL+"/v1/notification-requests", req)
	if err != nil {
		return result{scenario: scenarioName, outcome: "request_error", statusCode: status, duration: time.Since(start), err: err}
	}

	var accepted notification.NotificationAccepted
	if err := json.Unmarshal(body, &accepted); err != nil {
		return result{scenario: scenarioName, outcome: "decode_failed", statusCode: status, duration: time.Since(start), err: err}
	}

	if withCallback && accepted.RequestID != "" {
		r.maybeCallbackFromAccepted(ctx, accepted, true)
	}

	outcome := fmt.Sprintf("status_%d", status)
	switch status {
	case http.StatusAccepted:
		outcome = "accepted"
	case http.StatusOK:
		if accepted.IdempotentReplay {
			outcome = "idempotent_replay"
		}
	case http.StatusConflict:
		outcome = "conflict"
	case http.StatusBadRequest:
		outcome = "bad_request"
	}

	return result{
		scenario:   scenarioName,
		outcome:    outcome,
		statusCode: status,
		duration:   time.Since(start),
	}
}

func (r *runner) maybeCallbackFromAccepted(ctx context.Context, accepted notification.NotificationAccepted, enabled bool) {
	if !enabled || accepted.RequestID == "" || r.randomFloat() > r.cfg.callbackFraction {
		return
	}

	details, err := r.waitForRequestDetails(ctx, accepted.RequestID)
	if err != nil {
		return
	}

	for _, attempt := range details.DeliveryAttempts {
		if attempt.ProviderMessageID == "" {
			continue
		}
		if attempt.Status != notification.DeliveryAttemptAccepted {
			continue
		}

		payload := notification.ProviderCallback{
			ProviderMessageID: attempt.ProviderMessageID,
			Status:            "delivered",
		}
		_, _, _ = r.postJSON(ctx, http.MethodPost, fmt.Sprintf("%s/v1/providers/%s/callbacks", r.cfg.callbackBaseURL, attempt.Channel), payload)
		return
	}
}

func (r *runner) waitForRequestDetails(ctx context.Context, requestID string) (requestDetailsResponse, error) {
	var details requestDetailsResponse
	var err error
	for attempt := 0; attempt < r.cfg.pollAttempts; attempt++ {
		details, err = r.fetchRequestDetails(ctx, requestID)
		if err == nil && len(details.DeliveryAttempts) > 0 {
			return details, nil
		}
		select {
		case <-ctx.Done():
			return requestDetailsResponse{}, ctx.Err()
		case <-time.After(r.cfg.pollInterval):
		}
	}
	if err != nil {
		return requestDetailsResponse{}, err
	}
	return details, fmt.Errorf("request %s did not produce delivery attempts within polling window", requestID)
}

func (r *runner) waitForTerminalRequestDetails(ctx context.Context, requestID string, attempts int, interval time.Duration) (requestDetailsResponse, error) {
	var details requestDetailsResponse
	var err error
	for attempt := 0; attempt < attempts; attempt++ {
		details, err = r.fetchRequestDetails(ctx, requestID)
		if err == nil && details.Request.Status != notification.RequestStatusAccepted && details.Request.Status != notification.RequestStatusProcessing {
			return details, nil
		}
		select {
		case <-ctx.Done():
			return requestDetailsResponse{}, ctx.Err()
		case <-time.After(interval):
		}
	}
	if err != nil {
		return requestDetailsResponse{}, err
	}
	return details, fmt.Errorf("request %s did not reach terminal status within polling window", requestID)
}

func (r *runner) fetchRequestDetails(ctx context.Context, requestID string) (requestDetailsResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.cfg.apiBaseURL+"/v1/notification-requests/"+requestID, nil)
	if err != nil {
		return requestDetailsResponse{}, err
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return requestDetailsResponse{}, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return requestDetailsResponse{}, err
	}
	if resp.StatusCode >= http.StatusBadRequest {
		return requestDetailsResponse{}, fmt.Errorf("request details returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var details requestDetailsResponse
	if err := json.Unmarshal(body, &details); err != nil {
		return requestDetailsResponse{}, err
	}
	return details, nil
}

func (r *runner) postJSON(ctx context.Context, method string, url string, payload any) (int, []byte, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, nil, err
	}
	return r.postRaw(ctx, method, url, body)
}

func (r *runner) postRaw(ctx context.Context, method string, url string, payload []byte) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(payload))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, body, nil
}

func (r *runner) nextID() uint64 {
	return r.sequence.Add(1)
}

func (r *runner) newKey(prefix string) string {
	return fmt.Sprintf("%s-%s-%d", prefix, r.runID, r.nextID())
}

func (r *runner) randomFloat() float64 {
	r.randomMu.Lock()
	defer r.randomMu.Unlock()
	return r.random.Float64()
}

func (s *summary) add(res result) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.total++
	if res.err != nil {
		s.failures++
	}
	s.scenarioCounts[res.scenario]++
	s.outcomeCounts[res.outcome]++
	s.statusCodeCounts[res.statusCode]++
	s.scenarioDurations[res.scenario] += res.duration

	if res.err != nil {
		log.Printf("scenario=%s outcome=%s status=%d err=%v", res.scenario, res.outcome, res.statusCode, res.err)
	}
}

func (s *summary) print() {
	fmt.Printf("total results: %d\n", s.total)
	fmt.Printf("results with local errors: %d\n", s.failures)

	fmt.Println("\noutcomes:")
	printStringCounts(s.outcomeCounts)

	fmt.Println("\nHTTP status codes:")
	intKeys := make([]int, 0, len(s.statusCodeCounts))
	for key := range s.statusCodeCounts {
		intKeys = append(intKeys, key)
	}
	sort.Ints(intKeys)
	for _, key := range intKeys {
		fmt.Printf("  %d -> %d\n", key, s.statusCodeCounts[key])
	}

	fmt.Println("\nscenarios:")
	scenarioNames := make([]string, 0, len(s.scenarioCounts))
	for name := range s.scenarioCounts {
		scenarioNames = append(scenarioNames, name)
	}
	sort.Strings(scenarioNames)
	for _, name := range scenarioNames {
		count := s.scenarioCounts[name]
		avg := time.Duration(0)
		if count > 0 {
			avg = s.scenarioDurations[name] / time.Duration(count)
		}
		fmt.Printf("  %s -> count=%d avg_duration=%s\n", name, count, avg.Round(time.Millisecond))
	}
}

func printStringCounts(values map[string]int) {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		fmt.Printf("  %s -> %d\n", key, values[key])
	}
}

func envOrDefault(key string, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if value := os.Getenv(key); value != "" {
		var parsed int
		if _, err := fmt.Sscanf(value, "%d", &parsed); err == nil {
			return parsed
		}
	}
	return fallback
}

func envFloat(key string, fallback float64) float64 {
	if value := os.Getenv(key); value != "" {
		var parsed float64
		if _, err := fmt.Sscanf(value, "%f", &parsed); err == nil {
			return parsed
		}
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if parsed, err := time.ParseDuration(value); err == nil {
			return parsed
		}
	}
	return fallback
}
