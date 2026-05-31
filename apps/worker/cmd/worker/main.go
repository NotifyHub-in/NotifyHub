package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sony/gobreaker"
	"github.com/your-org/notification-control-plane/libs/contracts/notification"
	"github.com/your-org/notification-control-plane/libs/core/app"
	"github.com/your-org/notification-control-plane/libs/core/config"
	"github.com/your-org/notification-control-plane/libs/core/httpx"
	"github.com/your-org/notification-control-plane/libs/core/id"
	"github.com/your-org/notification-control-plane/libs/core/render"
	"github.com/your-org/notification-control-plane/libs/core/secrets"
	"github.com/your-org/notification-control-plane/libs/core/serviceinfo"
	"github.com/your-org/notification-control-plane/libs/core/webhooks"
	kafkamq "github.com/your-org/notification-control-plane/libs/messaging/kafka"
	"github.com/your-org/notification-control-plane/libs/observability/logging"
	"github.com/your-org/notification-control-plane/libs/observability/metrics"
	"github.com/your-org/notification-control-plane/libs/storage/postgres"
)

type connectorClient struct {
	baseURL string
	client  *http.Client
}

type connectorSendError struct {
	StatusCode     int
	Message        string
	Code           string
	Classification notification.FailureClass
	Retryable      bool
}

type deliveryStats struct {
	accepted    int
	failed      int
	suppressed  int
	unsupported int
}

type resolvedRoute struct {
	Channels   []notification.Channel
	BindingSet string
}

type channelResult struct {
	Accepted       bool
	ScheduledRetry bool
	TerminalFailed bool
	Unsupported    bool
}

var (
	providerCircuitFailureThreshold = 2
	providerCircuitCooldown         = 15 * time.Second
)

type providerBreakerManager struct {
	mu       sync.Mutex
	breakers map[string]*gobreaker.CircuitBreaker
	store    *postgres.Store
	logger   *slog.Logger
}

func (e connectorSendError) Error() string {
	classification := e.Classification
	if classification == "" {
		classification = notification.FailureClassPermanent
	}
	if e.Code != "" {
		return fmt.Sprintf("connector returned %d (%s/%s): %s", e.StatusCode, classification, e.Code, e.Message)
	}
	return fmt.Sprintf("connector returned %d (%s): %s", e.StatusCode, classification, e.Message)
}

func (c connectorClient) send(ctx context.Context, req notification.ConnectorSendRequest) (notification.ConnectorSendResponse, error) {
	var response notification.ConnectorSendResponse

	payload, err := json.Marshal(req)
	if err != nil {
		return response, fmt.Errorf("marshal connector request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/send", bytes.NewReader(payload))
	if err != nil {
		return response, fmt.Errorf("build connector request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := c.client.Do(httpReq)
	if err != nil {
		return response, connectorSendError{
			Message:        fmt.Sprintf("call connector: %v", err),
			Classification: notification.FailureClassTransient,
			Retryable:      true,
		}
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode >= http.StatusBadRequest {
		body, _ := io.ReadAll(httpResp.Body)
		var failure notification.ConnectorErrorResponse
		if err := json.Unmarshal(body, &failure); err != nil {
			return response, connectorSendError{
				StatusCode:     httpResp.StatusCode,
				Message:        strings.TrimSpace(string(body)),
				Classification: classifyHTTPStatus(httpResp.StatusCode),
				Retryable:      isRetryableHTTPStatus(httpResp.StatusCode),
			}
		}
		if failure.Error == "" {
			failure.Error = strings.TrimSpace(string(body))
		}
		if failure.Classification == "" {
			failure.Classification = classifyHTTPStatus(httpResp.StatusCode)
		}
		if !failure.Retryable {
			failure.Retryable = isRetryableFailureClass(failure.Classification)
		}
		return response, connectorSendError{
			StatusCode:     httpResp.StatusCode,
			Message:        failure.Error,
			Code:           failure.Code,
			Classification: failure.Classification,
			Retryable:      failure.Retryable,
		}
	}

	if err := json.NewDecoder(httpResp.Body).Decode(&response); err != nil {
		return response, fmt.Errorf("decode connector response: %w", err)
	}

	return response, nil
}

func main() {
	cfg, err := config.LoadHTTPServiceConfig("worker", 8081)
	if err != nil {
		panic(err)
	}

	logger := logging.New(cfg.ServiceName)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registry := metrics.NewRegistry(cfg.ServiceName)
	providerCircuitFailureThreshold = parsePositiveInt(config.GetEnv("PROVIDER_CIRCUIT_BREAKER_FAILURE_THRESHOLD", "2"), 2)
	providerCircuitCooldown = time.Duration(parsePositiveInt(config.GetEnv("PROVIDER_CIRCUIT_BREAKER_COOLDOWN_SECONDS", "15"), 15)) * time.Second

	store, err := postgres.Open(ctx, config.MustGetEnv("DATABASE_URL"))
	if err != nil {
		panic(err)
	}
	defer store.Close()
	postgres.AttachMetrics(registry)
	notifier := webhooks.NewNotifier(store)
	breakers := newProviderBreakerManager(store, logger)

	brokers := config.MustGetEnv("KAFKA_BROKERS")
	topic := config.GetEnv("KAFKA_NOTIFICATION_TOPIC", "notification.requests")
	groupID := config.GetEnv("KAFKA_WORKER_GROUP_ID", "notification-worker")
	if err := kafkamq.EnsureTopic(ctx, brokers, topic, 1, 1); err != nil {
		panic(err)
	}

	consumer := kafkamq.NewConsumer(brokers, topic, groupID)
	defer consumer.Close()
	publisher := kafkamq.NewPublisher(brokers, topic)
	defer publisher.Close()

	var status sync.Map
	status.Store("state", "starting")
	status.Store("last_heartbeat", "")
	status.Store("last_request_id", "")
	status.Store("last_error", "")

	go func() {
		err := consumer.Consume(ctx, func(messageCtx context.Context, payload []byte) error {
			var plan notification.DeliveryPlan
			if err := json.Unmarshal(payload, &plan); err != nil {
				status.Store("last_error", err.Error())
				logger.Error("unmarshal delivery plan failed", "error", err)
				return nil
			}

			processDeliveryPlan(messageCtx, logger, registry, store, notifier, breakers, &status, plan)
			return nil
		})
		if err != nil {
			status.Store("state", "stopped")
			status.Store("last_error", err.Error())
			logger.Error("worker consume loop stopped", "error", err)
		}
	}()

	go pollScheduledRetries(ctx, logger, registry, store, publisher, &status)
	go pollQueueMetrics(ctx, logger, registry, store)

	err = app.RunHTTPService(cfg, logger, registry, func(mux *http.ServeMux, info serviceinfo.Info, _ *metrics.Registry) {
		mux.HandleFunc("GET /v1/status", func(w http.ResponseWriter, _ *http.Request) {
			httpx.WriteJSON(w, http.StatusOK, map[string]any{
				"service":         info.Name,
				"phase":           "provider-circuit-breakers",
				"state":           loadString(&status, "state"),
				"last_request_id": loadString(&status, "last_request_id"),
				"last_heartbeat":  loadString(&status, "last_heartbeat"),
				"last_error":      loadString(&status, "last_error"),
				"topic":           topic,
				"provider_circuit_breaker_failure_threshold": providerCircuitFailureThreshold,
				"provider_circuit_breaker_cooldown_seconds":  int(providerCircuitCooldown.Seconds()),
			})
		})
	})
	if err != nil {
		panic(err)
	}
}

func processDeliveryPlan(
	ctx context.Context,
	logger *slog.Logger,
	registry *metrics.Registry,
	store *postgres.Store,
	notifier *webhooks.Notifier,
	breakers *providerBreakerManager,
	status *sync.Map,
	plan notification.DeliveryPlan,
) {
	status.Store("state", "processing")
	status.Store("last_request_id", plan.Request.RequestID)
	status.Store("last_heartbeat", time.Now().UTC().Format(time.RFC3339))

	effectiveRoute, err := resolveRouteForPlan(ctx, store, plan)
	if err != nil {
		status.Store("last_error", err.Error())
		logger.Error("resolve routing policy channels failed", "error", err, "request_id", plan.Request.RequestID)
		_ = store.UpdateNotificationRequestStatus(ctx, plan.Request.RequestID, notification.RequestStatusFailed)
		return
	}

	if isExpired(plan.Request, time.Now().UTC()) {
		markExpired(ctx, logger, registry, store, notifier, status, plan.Request, effectiveRoute.Channels)
		return
	}

	if err := store.UpdateNotificationRequestStatus(ctx, plan.Request.RequestID, notification.RequestStatusProcessing); err != nil {
		status.Store("last_error", err.Error())
		logger.Error("mark notification request processing failed", "error", err, "request_id", plan.Request.RequestID)
		return
	}

	allowedChannels, suppressedChannels, err := resolvePreferredChannels(ctx, store, plan.Request, effectiveRoute.Channels)
	if err != nil {
		status.Store("last_error", err.Error())
		logger.Error("resolve preference policy channels failed", "error", err, "request_id", plan.Request.RequestID)
		_ = store.UpdateNotificationRequestStatus(ctx, plan.Request.RequestID, notification.RequestStatusFailed)
		return
	}

	attemptStats := deliveryStats{}
	for _, channel := range suppressedChannels {
		attempt := notification.DeliveryAttempt{
			AttemptID:     id.New(12),
			RequestID:     plan.Request.RequestID,
			Channel:       channel,
			ConnectorName: connectorName(channel),
			Status:        notification.DeliveryAttemptSuppressed,
			Destination:   destination(plan.Request, channel),
			ErrorMessage:  "suppressed by preference policy",
		}
		if err := store.CreateDeliveryAttempt(ctx, attempt); err != nil {
			logger.Error("create suppressed delivery attempt failed", "error", err, "request_id", plan.Request.RequestID, "channel", channel)
			attemptStats.failed++
			continue
		}
		recordDeliveryAttemptMetric(registry, attempt)
		attemptStats.suppressed++
	}

	for _, channel := range allowedChannels {
		result := processChannelDelivery(ctx, logger, registry, store, breakers, plan.Request, channel, effectiveRoute.BindingSet)
		switch {
		case result.Accepted:
			attemptStats.accepted++
		case result.ScheduledRetry:
		case result.Unsupported:
			attemptStats.unsupported++
		case result.TerminalFailed:
			attemptStats.failed++
		}
	}

	finalStatus, hasPendingRetry, err := deriveRequestStatus(ctx, store, plan.Request.RequestID)
	if err != nil {
		status.Store("last_error", err.Error())
		logger.Error("derive final notification request status failed", "error", err, "request_id", plan.Request.RequestID)
		return
	}
	if attemptStats.accepted > 0 {
		finalStatus = notification.RequestStatusDispatched
	}
	if !hasPendingRetry && attemptStats.accepted == 0 && finalStatus == notification.RequestStatusUnsupported && attemptStats.suppressed > 0 && attemptStats.failed == 0 {
		finalStatus = notification.RequestStatusSuppressed
	}

	if err := store.UpdateNotificationRequestStatus(ctx, plan.Request.RequestID, finalStatus); err != nil {
		status.Store("last_error", err.Error())
		logger.Error("update final notification request status failed", "error", err, "request_id", plan.Request.RequestID)
		return
	}

	if finalStatus != notification.RequestStatusProcessing {
		registry.IncCounter("notification_request_final_status_total", "Final notification request statuses produced by the worker.", map[string]string{
			"status": string(finalStatus),
		})
		registry.ObserveHistogram("notification_end_to_end_seconds", "End-to-end notification latency in seconds from API acceptance to observed terminal or dispatched state.", map[string]string{
			"stage":        "worker",
			"final_status": string(finalStatus),
		}, metrics.DefaultLatencyBuckets(), time.Since(plan.Request.RequestedAt).Seconds())
	}
	if err := notifier.NotifyRequestUpdated(ctx, plan.Request.RequestID, map[string]interface{}{"source": "worker"}); err != nil {
		logger.Error("notify lifecycle webhook failed", "error", err, "request_id", plan.Request.RequestID)
	}

	status.Store("state", "idle")
	status.Store("last_heartbeat", time.Now().UTC().Format(time.RFC3339))
	status.Store("last_error", "")
	logger.Info("processed delivery plan", "request_id", plan.Request.RequestID, "status", finalStatus)
}

func pollScheduledRetries(
	ctx context.Context,
	logger *slog.Logger,
	registry *metrics.Registry,
	store *postgres.Store,
	publisher *kafkamq.Publisher,
	status *sync.Map,
) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			retry, err := store.ClaimNextScheduledRetry(ctx)
			if errors.Is(err, postgres.ErrNotFound) {
				continue
			}
			if err != nil {
				status.Store("last_error", err.Error())
				logger.Error("claim scheduled retry failed", "error", err)
				continue
			}
			recordScheduledRetryEventMetric(registry, "claimed", retry.Channel)

			record, err := store.GetNotificationRequest(ctx, retry.RequestID)
			if errors.Is(err, postgres.ErrNotFound) {
				logger.Error("scheduled retry request not found", "retry_id", retry.RetryID, "request_id", retry.RequestID)
				_ = store.DeleteScheduledRetry(ctx, retry.RetryID)
				continue
			}
			if err != nil {
				status.Store("last_error", err.Error())
				logger.Error("load scheduled retry request failed", "error", err, "retry_id", retry.RetryID, "request_id", retry.RequestID)
				_ = store.ReleaseScheduledRetryClaim(ctx, retry.RetryID)
				continue
			}
			recordScheduledRetryPickupDelayMetric(registry, retry.Channel, time.Since(retry.AvailableAt))

			plan := notification.DeliveryPlan{
				Request: record,
				Retry: &notification.RetryDirective{
					RetryID:    retry.RetryID,
					Channel:    retry.Channel,
					BindingSet: retry.BindingSet,
				},
			}
			if err := publisher.PublishJSON(ctx, retry.RetryID, plan); err != nil {
				status.Store("last_error", err.Error())
				logger.Error("publish scheduled retry failed", "error", err, "retry_id", retry.RetryID, "request_id", retry.RequestID)
				_ = store.ReleaseScheduledRetryClaim(ctx, retry.RetryID)
				recordScheduledRetryEventMetric(registry, "released", retry.Channel)
				continue
			}
			recordScheduledRetryEventMetric(registry, "republished", retry.Channel)

			if err := store.DeleteScheduledRetry(ctx, retry.RetryID); err != nil {
				logger.Error("delete scheduled retry after publish failed", "error", err, "retry_id", retry.RetryID)
			}
			recordScheduledRetryEventMetric(registry, "deleted", retry.Channel)
		}
	}
}

func pollQueueMetrics(
	ctx context.Context,
	logger *slog.Logger,
	registry *metrics.Registry,
	store *postgres.Store,
) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	refreshQueueMetrics(ctx, logger, registry, store)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			refreshQueueMetrics(ctx, logger, registry, store)
		}
	}
}

func processChannelDelivery(
	ctx context.Context,
	logger *slog.Logger,
	registry *metrics.Registry,
	store *postgres.Store,
	breakers *providerBreakerManager,
	record notification.NotificationRecord,
	channel notification.Channel,
	bindingSet string,
) channelResult {
	deliveryPolicy, err := resolveDeliveryPolicy(ctx, store, channel)
	if err != nil {
		logger.Error("load delivery policy failed", "error", err, "request_id", record.RequestID, "channel", channel)
		createTerminalFailedAttempt(ctx, logger, registry, store, record, channel, 1, 1, connectorName(channel), err.Error())
		return channelResult{TerminalFailed: true}
	}

	tmpl, err := store.GetTemplateByKeyAndChannel(ctx, record.TemplateKey, channel)
	if errors.Is(err, postgres.ErrNotFound) {
		attempt := notification.DeliveryAttempt{
			AttemptID:     id.New(12),
			RequestID:     record.RequestID,
			AttemptNumber: 1,
			MaxAttempts:   deliveryPolicy.MaxAttempts,
			Channel:       channel,
			ConnectorName: connectorName(channel),
			Status:        notification.DeliveryAttemptUnsupported,
			Destination:   destination(record, channel),
			ErrorMessage:  "no enabled template for template key and channel",
		}
		if err := store.CreateDeliveryAttempt(ctx, attempt); err != nil {
			logger.Error("create unsupported template delivery attempt failed", "error", err, "request_id", record.RequestID, "channel", channel)
		}
		recordDeliveryAttemptMetric(registry, attempt)
		return channelResult{Unsupported: true}
	}
	if err != nil {
		logger.Error("load template failed", "error", err, "request_id", record.RequestID, "channel", channel)
		createTerminalFailedAttempt(ctx, logger, registry, store, record, channel, 1, deliveryPolicy.MaxAttempts, connectorName(channel), err.Error())
		return channelResult{TerminalFailed: true}
	}

	subject, err := render.Subject(tmpl, record)
	if err != nil {
		createTerminalFailedAttempt(ctx, logger, registry, store, record, channel, 1, deliveryPolicy.MaxAttempts, connectorName(channel), err.Error())
		return channelResult{TerminalFailed: true}
	}
	body, err := render.Body(tmpl, record)
	if err != nil {
		createTerminalFailedAttempt(ctx, logger, registry, store, record, channel, 1, deliveryPolicy.MaxAttempts, connectorName(channel), err.Error())
		return channelResult{TerminalFailed: true}
	}

	if !supportedChannel(channel) {
		attempt := notification.DeliveryAttempt{
			AttemptID:     id.New(12),
			RequestID:     record.RequestID,
			AttemptNumber: 1,
			MaxAttempts:   1,
			Channel:       channel,
			ConnectorName: connectorName(channel),
			Status:        notification.DeliveryAttemptUnsupported,
			Destination:   destination(record, channel),
			ErrorMessage:  "connector not implemented in worker happy path",
		}
		if err := store.CreateDeliveryAttempt(ctx, attempt); err != nil {
			logger.Error("create unsupported delivery attempt failed", "error", err, "request_id", record.RequestID, "channel", channel)
		}
		recordDeliveryAttemptMetric(registry, attempt)
		return channelResult{Unsupported: true}
	}

	bindings, err := store.ListProviderBindingsByChannel(ctx, channel, bindingSet)
	if errors.Is(err, postgres.ErrNotFound) {
		attempt := notification.DeliveryAttempt{
			AttemptID:     id.New(12),
			RequestID:     record.RequestID,
			AttemptNumber: 1,
			MaxAttempts:   deliveryPolicy.MaxAttempts,
			Channel:       channel,
			ConnectorName: connectorName(channel),
			Status:        notification.DeliveryAttemptUnsupported,
			Destination:   destination(record, channel),
			ErrorMessage:  "no enabled provider binding for channel",
		}
		if err := store.CreateDeliveryAttempt(ctx, attempt); err != nil {
			logger.Error("create unsupported binding delivery attempt failed", "error", err, "request_id", record.RequestID, "channel", channel)
		}
		recordDeliveryAttemptMetric(registry, attempt)
		return channelResult{Unsupported: true}
	}
	if err != nil {
		logger.Error("load provider bindings failed", "error", err, "request_id", record.RequestID, "channel", channel)
		createTerminalFailedAttempt(ctx, logger, registry, store, record, channel, 1, deliveryPolicy.MaxAttempts, connectorName(channel), err.Error())
		return channelResult{TerminalFailed: true}
	}

	existingAttempts, err := store.CountDeliveryAttemptsByRequestAndChannel(ctx, record.RequestID, channel)
	if err != nil {
		logger.Error("count delivery attempts failed", "error", err, "request_id", record.RequestID, "channel", channel)
		createTerminalFailedAttempt(ctx, logger, registry, store, record, channel, 1, deliveryPolicy.MaxAttempts*len(bindings), connectorName(channel), err.Error())
		return channelResult{TerminalFailed: true}
	}

	totalMaxAttempts := deliveryPolicy.MaxAttempts * len(bindings)
	if totalMaxAttempts < 1 {
		totalMaxAttempts = 1
	}
	nextAttemptNumber := existingAttempts + 1
	sendReq := notification.ConnectorSendRequest{
		RequestID:   record.RequestID,
		Channel:     channel,
		Destination: destination(record, channel),
		Subject:     subject,
		Body:        body,
		Metadata:    record.Metadata,
	}

	lastError := ""
	lastConnector := ""
	lastFailureClass := notification.FailureClassPermanent
	retryableFailureSeen := false
	for bindingIndex, binding := range bindings {
		if nextAttemptNumber > totalMaxAttempts {
			break
		}
		lastConnector = binding.ConnectorName
		now := time.Now().UTC()

		health, err := loadProviderBindingHealth(ctx, store, binding)
		if err != nil {
			lastError = err.Error()
			logger.Error("load provider binding health failed", "error", err, "request_id", record.RequestID, "channel", channel, "connector", binding.ConnectorName, "binding_id", binding.BindingID)
			break
		}
		if shouldSkipBindingForCircuit(health, now) {
			lastError = fmt.Sprintf("provider circuit open until %s", health.CooldownUntil.UTC().Format(time.RFC3339))
			lastFailureClass = notification.FailureClassTransient
			retryableFailureSeen = true
			recordProviderCircuitEventMetric(registry, "skipped", channel, binding.ConnectorName)
			logger.Info("skipping open provider circuit", "request_id", record.RequestID, "channel", channel, "connector", binding.ConnectorName, "binding_id", binding.BindingID, "cooldown_until", health.CooldownUntil)
			if bindingIndex+1 < len(bindings) {
				recordFailoverMetric(registry, channel, binding.ConnectorName, bindings[bindingIndex+1].ConnectorName)
			}
			continue
		}
		breaker := breakers.breakerFor(binding, health)

		providerKey, providerConfig, err := resolveBindingProviderConfig(ctx, store, binding)
		if err != nil {
			lastError = err.Error()
			lastFailureClass = notification.FailureClassMisconfigured
			logger.Error("resolve provider config failed", "error", err, "request_id", record.RequestID, "channel", channel, "connector", binding.ConnectorName, "binding_set", binding.BindingSet)
			attempt := notification.DeliveryAttempt{
				AttemptID:     id.New(12),
				RequestID:     record.RequestID,
				AttemptNumber: nextAttemptNumber,
				MaxAttempts:   totalMaxAttempts,
				Channel:       channel,
				ConnectorName: binding.ConnectorName,
				Status:        notification.DeliveryAttemptFailed,
				Destination:   destination(record, channel),
				ErrorMessage:  err.Error(),
			}
			if createErr := store.CreateDeliveryAttempt(ctx, attempt); createErr != nil {
				logger.Error("create provider config failure attempt failed", "error", createErr, "request_id", record.RequestID, "channel", channel, "connector", binding.ConnectorName)
			}
			recordDeliveryAttemptMetric(registry, attempt)
			recordFailureClassificationMetric(registry, channel, binding.ConnectorName, lastFailureClass)
			nextAttemptNumber++
			if bindingIndex+1 < len(bindings) {
				recordFailoverMetric(registry, channel, binding.ConnectorName, bindings[bindingIndex+1].ConnectorName)
			}
			continue
		}

		bindingSendReq := sendReq
		bindingSendReq.ProviderKey = providerKey
		bindingSendReq.ProviderAccountID = binding.ProviderAccountID
		bindingSendReq.ProviderConfig = providerConfig
		attempt := notification.DeliveryAttempt{
			AttemptID:     id.New(12),
			RequestID:     record.RequestID,
			AttemptNumber: nextAttemptNumber,
			MaxAttempts:   totalMaxAttempts,
			Channel:       channel,
			ConnectorName: binding.ConnectorName,
			Status:        notification.DeliveryAttemptPending,
			Destination:   destination(record, channel),
		}
		if err := store.CreateDeliveryAttempt(ctx, attempt); err != nil {
			logger.Error("create delivery attempt failed", "error", err, "request_id", record.RequestID, "channel", channel, "attempt_number", nextAttemptNumber, "connector", binding.ConnectorName)
			lastError = err.Error()
			break
		}

		connector := connectorClient{
			baseURL: binding.EndpointURL,
			client:  &http.Client{Timeout: 10 * time.Second},
		}
		rawResp, err := breaker.Execute(func() (interface{}, error) {
			return connector.send(ctx, bindingSendReq)
		})
		if err == nil {
			sendResp, ok := rawResp.(notification.ConnectorSendResponse)
			if !ok {
				lastError = "unexpected circuit breaker response type"
				logger.Error("unexpected circuit breaker response type", "request_id", record.RequestID, "channel", channel, "connector", binding.ConnectorName)
				break
			}
			attempt.Status = notification.DeliveryAttemptAccepted
			attempt.ProviderMessageID = sendResp.ProviderMessageID
			if err := store.UpdateDeliveryAttempt(ctx, attempt); err != nil {
				logger.Error("update accepted delivery attempt failed", "error", err, "attempt_id", attempt.AttemptID)
				lastError = err.Error()
				break
			}
			recordDeliveryAttemptMetric(registry, attempt)
			if err := breakers.recordSuccess(ctx, binding); err != nil {
				logger.Error("record provider binding success failed", "error", err, "request_id", record.RequestID, "channel", channel, "connector", binding.ConnectorName, "binding_id", binding.BindingID)
			} else if health.ConsecutiveFailures > 0 || health.CircuitState == notification.ProviderCircuitStateOpen {
				recordProviderCircuitEventMetric(registry, "closed", channel, binding.ConnectorName)
			}
			return channelResult{Accepted: true}
		}

		if errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests) {
			lastError = fmt.Sprintf("provider circuit open until %s", time.Now().UTC().Add(providerCircuitCooldown).Format(time.RFC3339))
			lastFailureClass = notification.FailureClassTransient
			retryableFailureSeen = true
			recordProviderCircuitEventMetric(registry, "skipped", channel, binding.ConnectorName)
			if bindingIndex+1 < len(bindings) {
				recordFailoverMetric(registry, channel, binding.ConnectorName, bindings[bindingIndex+1].ConnectorName)
			}
			continue
		}

		lastError = err.Error()
		failureClass, retryable := classifyConnectorFailure(err)
		lastFailureClass = failureClass
		if retryable {
			retryableFailureSeen = true
		}
		logger.Error("connector send failed", "error", err, "request_id", record.RequestID, "channel", channel, "attempt_number", nextAttemptNumber, "connector", binding.ConnectorName, "binding_priority", binding.Priority)
		attempt.Status = notification.DeliveryAttemptFailed
		attempt.ErrorMessage = err.Error()
		if err := store.UpdateDeliveryAttempt(ctx, attempt); err != nil {
			logger.Error("update failed delivery attempt failed", "error", err, "attempt_id", attempt.AttemptID)
		}
		recordDeliveryAttemptMetric(registry, attempt)
		recordFailureClassificationMetric(registry, channel, binding.ConnectorName, failureClass)
		if retryable {
			updatedHealth, healthErr := breakers.recordFailure(ctx, binding, failureClass, err.Error())
			if healthErr != nil {
				logger.Error("update provider binding circuit failed", "error", healthErr, "request_id", record.RequestID, "channel", channel, "connector", binding.ConnectorName, "binding_id", binding.BindingID)
			} else if updatedHealth.CircuitState == notification.ProviderCircuitStateOpen && health.CircuitState != notification.ProviderCircuitStateOpen {
				recordProviderCircuitEventMetric(registry, "opened", channel, binding.ConnectorName)
			}
		}
		nextAttemptNumber++

		if bindingIndex+1 < len(bindings) {
			recordFailoverMetric(registry, channel, binding.ConnectorName, bindings[bindingIndex+1].ConnectorName)
			logger.Info("provider binding exhausted, failing over", "request_id", record.RequestID, "channel", channel, "connector", binding.ConnectorName, "binding_priority", binding.Priority, "next_binding_index", bindingIndex+1)
		}
	}

	if retryableFailureSeen && nextAttemptNumber <= totalMaxAttempts {
		availableAt := time.Now().UTC().Add(time.Duration(deliveryPolicy.BackoffSeconds) * time.Second)
		retry := notification.ScheduledRetry{
			RetryID:                  id.New(12),
			RequestID:                record.RequestID,
			Channel:                  channel,
			BindingSet:               bindingSet,
			AvailableAt:              availableAt,
			LastError:                lastError,
			TriggeredByAttemptNumber: nextAttemptNumber - 1,
		}
		if err := store.CreateScheduledRetry(ctx, retry); err != nil {
			logger.Error("create scheduled retry failed", "error", err, "request_id", record.RequestID, "channel", channel)
			createDeadLetter(ctx, logger, registry, store, record, channel, bindingSet, lastConnector, nextAttemptNumber-1, lastError, "failed to schedule retry")
			return channelResult{TerminalFailed: true}
		}
		recordRetryMetric(registry, channel, lastConnector)
		recordScheduledRetryEventMetric(registry, "created", channel)
		return channelResult{ScheduledRetry: true}
	}

	deadLetterReason := "max attempts exhausted"
	if !retryableFailureSeen {
		deadLetterReason = fmt.Sprintf("non-retryable %s failure", lastFailureClass)
	}
	createDeadLetter(ctx, logger, registry, store, record, channel, bindingSet, lastConnector, nextAttemptNumber-1, lastError, deadLetterReason)
	return channelResult{TerminalFailed: true}
}

func markExpired(
	ctx context.Context,
	logger *slog.Logger,
	registry *metrics.Registry,
	store *postgres.Store,
	notifier *webhooks.Notifier,
	status *sync.Map,
	record notification.NotificationRecord,
	channels []notification.Channel,
) {
	for _, channel := range channels {
		attempt := notification.DeliveryAttempt{
			AttemptID:     id.New(12),
			RequestID:     record.RequestID,
			AttemptNumber: 1,
			MaxAttempts:   1,
			Channel:       channel,
			ConnectorName: connectorName(channel),
			Status:        notification.DeliveryAttemptExpired,
			Destination:   destination(record, channel),
			ErrorMessage:  "notification request expired before delivery",
		}
		if err := store.CreateDeliveryAttempt(ctx, attempt); err != nil {
			logger.Error("create expired delivery attempt failed", "error", err, "request_id", record.RequestID, "channel", channel)
			continue
		}
		recordDeliveryAttemptMetric(registry, attempt)
	}

	if err := store.UpdateNotificationRequestStatus(ctx, record.RequestID, notification.RequestStatusExpired); err != nil {
		status.Store("last_error", err.Error())
		logger.Error("update expired notification request status failed", "error", err, "request_id", record.RequestID)
		return
	}
	registry.IncCounter("notification_request_final_status_total", "Final notification request statuses produced by the worker.", map[string]string{
		"status": string(notification.RequestStatusExpired),
	})
	registry.ObserveHistogram("notification_end_to_end_seconds", "End-to-end notification latency in seconds from API acceptance to observed terminal or dispatched state.", map[string]string{
		"stage":        "worker",
		"final_status": string(notification.RequestStatusExpired),
	}, metrics.DefaultLatencyBuckets(), time.Since(record.RequestedAt).Seconds())
	if err := notifier.NotifyRequestUpdated(ctx, record.RequestID, map[string]interface{}{"source": "worker"}); err != nil {
		logger.Error("notify lifecycle webhook failed", "error", err, "request_id", record.RequestID)
	}

	status.Store("state", "idle")
	status.Store("last_heartbeat", time.Now().UTC().Format(time.RFC3339))
	status.Store("last_error", "")
	logger.Info("skipped expired delivery plan", "request_id", record.RequestID)
}

func createTerminalFailedAttempt(
	ctx context.Context,
	logger *slog.Logger,
	registry *metrics.Registry,
	store *postgres.Store,
	record notification.NotificationRecord,
	channel notification.Channel,
	attemptNumber int,
	maxAttempts int,
	connector string,
	errorMessage string,
) {
	attempt := notification.DeliveryAttempt{
		AttemptID:     id.New(12),
		RequestID:     record.RequestID,
		AttemptNumber: attemptNumber,
		MaxAttempts:   maxAttempts,
		Channel:       channel,
		ConnectorName: connector,
		Status:        notification.DeliveryAttemptFailed,
		Destination:   destination(record, channel),
		ErrorMessage:  errorMessage,
	}
	if err := store.CreateDeliveryAttempt(ctx, attempt); err != nil {
		logger.Error("create failed delivery attempt failed", "error", err, "request_id", record.RequestID, "channel", channel)
	}
	recordDeliveryAttemptMetric(registry, attempt)
}

func createDeadLetter(
	ctx context.Context,
	logger *slog.Logger,
	registry *metrics.Registry,
	store *postgres.Store,
	record notification.NotificationRecord,
	channel notification.Channel,
	bindingSet string,
	connector string,
	attemptCount int,
	lastError string,
	reason string,
) {
	deadLetter := notification.DeadLetterNotification{
		DeadLetterID:  id.New(12),
		RequestID:     record.RequestID,
		Channel:       channel,
		BindingSet:    bindingSet,
		ConnectorName: connector,
		Reason:        reason,
		AttemptCount:  attemptCount,
		LastError:     lastError,
		PayloadSnapshot: map[string]any{
			"request":     record,
			"channel":     channel,
			"binding_set": bindingSet,
		},
	}
	if err := store.CreateDeadLetterNotification(ctx, deadLetter); err != nil {
		logger.Error("create dead letter notification failed", "error", err, "request_id", record.RequestID, "channel", channel)
		return
	}
	recordDeadLetterMetric(registry, channel, connector)
}

func deriveRequestStatus(ctx context.Context, store *postgres.Store, requestID string) (notification.RequestStatus, bool, error) {
	attempts, err := store.ListDeliveryAttempts(ctx, requestID)
	if err != nil {
		return "", false, err
	}
	retries, err := store.ListScheduledRetries(ctx, requestID)
	if err != nil {
		return "", false, err
	}

	hasAccepted := false
	hasFailed := false
	hasUnsupported := false
	hasSuppressed := false
	for _, attempt := range attempts {
		switch attempt.Status {
		case notification.DeliveryAttemptAccepted, notification.DeliveryAttemptDelivered:
			hasAccepted = true
		case notification.DeliveryAttemptFailed, notification.DeliveryAttemptExpired:
			hasFailed = true
		case notification.DeliveryAttemptUnsupported:
			hasUnsupported = true
		case notification.DeliveryAttemptSuppressed:
			hasSuppressed = true
		}
	}

	hasPendingRetry := len(retries) > 0
	switch {
	case hasAccepted:
		return notification.RequestStatusDispatched, hasPendingRetry, nil
	case hasPendingRetry:
		return notification.RequestStatusProcessing, true, nil
	case hasFailed:
		return notification.RequestStatusFailed, false, nil
	case hasUnsupported:
		return notification.RequestStatusUnsupported, false, nil
	case hasSuppressed:
		return notification.RequestStatusSuppressed, false, nil
	default:
		return notification.RequestStatusUnsupported, false, nil
	}
}

func connectorName(channel notification.Channel) string {
	switch channel {
	case notification.ChannelWebhook:
		return "webhook"
	case notification.ChannelEmail:
		return "email"
	case notification.ChannelSMS:
		return "sms"
	case notification.ChannelPush:
		return "push"
	default:
		return "unsupported"
	}
}

func supportedChannel(channel notification.Channel) bool {
	switch channel {
	case notification.ChannelWebhook, notification.ChannelEmail, notification.ChannelSMS, notification.ChannelPush:
		return true
	default:
		return false
	}
}

func destination(record notification.NotificationRecord, channel notification.Channel) string {
	switch channel {
	case notification.ChannelWebhook:
		return record.Recipient.Webhook
	case notification.ChannelEmail:
		return record.Recipient.Email
	case notification.ChannelSMS:
		return record.Recipient.Phone
	case notification.ChannelPush:
		if record.Recipient.Topic != "" {
			return "/topics/" + record.Recipient.Topic
		}
		return record.Recipient.UserID
	default:
		return record.Recipient.UserID
	}
}

func loadString(status *sync.Map, key string) string {
	value, _ := status.Load(key)
	if text, ok := value.(string); ok {
		return text
	}
	return ""
}

func recordDeliveryAttemptMetric(registry *metrics.Registry, attempt notification.DeliveryAttempt) {
	if registry == nil {
		return
	}
	registry.IncCounter("delivery_attempts_total", "Delivery attempt outcomes recorded by the worker.", map[string]string{
		"channel":   string(attempt.Channel),
		"connector": attempt.ConnectorName,
		"status":    string(attempt.Status),
	})
}

func recordRetryMetric(registry *metrics.Registry, channel notification.Channel, connector string) {
	if registry == nil {
		return
	}
	registry.IncCounter("delivery_retries_total", "Delivery retry attempts scheduled by the worker.", map[string]string{
		"channel":   string(channel),
		"connector": connector,
	})
}

func recordFailoverMetric(registry *metrics.Registry, channel notification.Channel, fromConnector string, toConnector string) {
	if registry == nil {
		return
	}
	registry.IncCounter("provider_failovers_total", "Provider failover transitions executed by the worker.", map[string]string{
		"channel":        string(channel),
		"from_connector": fromConnector,
		"to_connector":   toConnector,
	})
}

func recordDeadLetterMetric(registry *metrics.Registry, channel notification.Channel, connector string) {
	if registry == nil {
		return
	}
	registry.IncCounter("dead_letters_total", "Dead-letter notifications recorded by the worker.", map[string]string{
		"channel":   string(channel),
		"connector": connector,
	})
}

func recordProviderCircuitEventMetric(registry *metrics.Registry, action string, channel notification.Channel, connector string) {
	if registry == nil {
		return
	}
	registry.IncCounter("provider_circuit_events_total", "Provider circuit-breaker lifecycle events recorded by the worker.", map[string]string{
		"action":    action,
		"channel":   string(channel),
		"connector": connector,
	})
}

func recordFailureClassificationMetric(registry *metrics.Registry, channel notification.Channel, connector string, classification notification.FailureClass) {
	if registry == nil {
		return
	}
	if classification == "" {
		classification = notification.FailureClassPermanent
	}
	registry.IncCounter("connector_failures_total", "Connector send failures grouped by worker-side failure classification.", map[string]string{
		"channel":        string(channel),
		"connector":      connector,
		"classification": string(classification),
	})
}

func recordScheduledRetryEventMetric(registry *metrics.Registry, action string, channel notification.Channel) {
	if registry == nil {
		return
	}
	registry.IncCounter("scheduled_retry_events_total", "Scheduled retry lifecycle events recorded by the worker.", map[string]string{
		"action":  action,
		"channel": string(channel),
	})
}

func recordScheduledRetryPickupDelayMetric(registry *metrics.Registry, channel notification.Channel, delay time.Duration) {
	if registry == nil {
		return
	}
	registry.ObserveHistogram("scheduled_retry_pickup_delay_seconds", "Delay between a scheduled retry becoming available and being republished.", map[string]string{
		"channel": string(channel),
	}, metrics.DefaultLatencyBuckets(), delay.Seconds())
}

func refreshQueueMetrics(
	ctx context.Context,
	logger *slog.Logger,
	registry *metrics.Registry,
	store *postgres.Store,
) {
	if registry == nil {
		return
	}

	retries, err := store.ListScheduledRetries(ctx, "")
	if err != nil {
		logger.Error("list scheduled retries for metrics failed", "error", err)
		return
	}
	now := time.Now().UTC()
	ready := 0
	oldestRetryAge := 0.0
	for _, retry := range retries {
		age := now.Sub(retry.CreatedAt).Seconds()
		if age > oldestRetryAge {
			oldestRetryAge = age
		}
		if !retry.AvailableAt.After(now) {
			ready++
		}
	}
	registry.SetGauge("scheduled_retry_backlog_total", "Current number of scheduled retries awaiting processing.", nil, float64(len(retries)))
	registry.SetGauge("scheduled_retry_ready_total", "Current number of scheduled retries ready to be republished.", nil, float64(ready))
	registry.SetGauge("scheduled_retry_oldest_age_seconds", "Age in seconds of the oldest scheduled retry record.", nil, oldestRetryAge)

	deadLetters, err := store.ListDeadLetterNotifications(ctx, "")
	if err != nil {
		logger.Error("list dead letters for metrics failed", "error", err)
		return
	}
	unreplayed := 0
	oldestDeadLetterAge := 0.0
	for _, deadLetter := range deadLetters {
		age := now.Sub(deadLetter.CreatedAt).Seconds()
		if age > oldestDeadLetterAge {
			oldestDeadLetterAge = age
		}
		if deadLetter.ReplayedAt == nil {
			unreplayed++
		}
	}
	registry.SetGauge("dead_letter_backlog_total", "Current number of dead-letter notifications retained for operator review.", nil, float64(len(deadLetters)))
	registry.SetGauge("dead_letter_unreplayed_total", "Current number of dead-letter notifications that have not been replayed.", nil, float64(unreplayed))
	registry.SetGauge("dead_letter_oldest_age_seconds", "Age in seconds of the oldest dead-letter notification.", nil, oldestDeadLetterAge)

	healthRecords, err := store.ListProviderBindingHealth(ctx)
	if err != nil {
		logger.Error("list provider binding health for metrics failed", "error", err)
		return
	}
	openCircuits := 0.0
	for _, health := range healthRecords {
		isOpen := 0.0
		if health.CircuitState == notification.ProviderCircuitStateOpen {
			isOpen = 1.0
			openCircuits++
		}
		labels := map[string]string{
			"binding_id":  health.BindingID,
			"channel":     string(health.Channel),
			"binding_set": health.BindingSet,
			"connector":   health.ConnectorName,
		}
		registry.SetGauge("provider_circuit_open_total", "Whether a provider binding circuit is currently open.", labels, isOpen)
		registry.SetGauge("provider_circuit_consecutive_failures", "Current consecutive retryable failures tracked for a provider binding.", labels, float64(health.ConsecutiveFailures))
	}
	registry.SetGauge("provider_circuit_open_bindings_total", "Current number of provider bindings with an open circuit.", nil, openCircuits)
}

func resolveRoute(ctx context.Context, store *postgres.Store, record notification.NotificationRecord) (resolvedRoute, error) {
	if len(record.Channels) > 0 {
		return resolvedRoute{
			Channels:   record.Channels,
			BindingSet: record.BindingSet,
		}, nil
	}

	policy, err := store.GetRoutingPolicyByEventName(ctx, record.EventName)
	if errors.Is(err, postgres.ErrNotFound) {
		return resolvedRoute{}, fmt.Errorf("no channels requested and no routing policy for event %s", record.EventName)
	}
	if err != nil {
		return resolvedRoute{}, err
	}
	return resolvedRoute{
		Channels:   policy.Channels,
		BindingSet: policy.BindingSet,
	}, nil
}

func resolveRouteForPlan(ctx context.Context, store *postgres.Store, plan notification.DeliveryPlan) (resolvedRoute, error) {
	if plan.Retry != nil {
		bindingSet := plan.Retry.BindingSet
		if bindingSet == "" {
			bindingSet = plan.Request.BindingSet
		}
		return resolvedRoute{
			Channels:   []notification.Channel{plan.Retry.Channel},
			BindingSet: bindingSet,
		}, nil
	}
	return resolveRoute(ctx, store, plan.Request)
}

func resolvePreferredChannels(ctx context.Context, store *postgres.Store, record notification.NotificationRecord, channels []notification.Channel) ([]notification.Channel, []notification.Channel, error) {
	if record.Recipient.UserID == "" {
		return channels, nil, nil
	}

	preferences, err := store.ListPreferencePolicies(ctx, record.Recipient.UserID)
	if err != nil {
		return nil, nil, err
	}
	if len(preferences) == 0 {
		return channels, nil, nil
	}

	disabled := make(map[notification.Channel]bool, len(preferences))
	for _, preference := range preferences {
		if !preference.IsEnabled {
			disabled[preference.Channel] = true
		}
	}

	allowed := make([]notification.Channel, 0, len(channels))
	suppressed := make([]notification.Channel, 0, len(channels))
	for _, channel := range channels {
		if disabled[channel] {
			suppressed = append(suppressed, channel)
			continue
		}
		allowed = append(allowed, channel)
	}

	return allowed, suppressed, nil
}

func resolveDeliveryPolicy(ctx context.Context, store *postgres.Store, channel notification.Channel) (notification.DeliveryPolicy, error) {
	policy, err := store.GetDeliveryPolicyByChannel(ctx, channel)
	if errors.Is(err, postgres.ErrNotFound) {
		return notification.DeliveryPolicy{
			Channel:        channel,
			MaxAttempts:    1,
			BackoffSeconds: 0,
			Enabled:        true,
		}, nil
	}
	if err != nil {
		return notification.DeliveryPolicy{}, err
	}
	if policy.MaxAttempts < 1 {
		policy.MaxAttempts = 1
	}
	if policy.BackoffSeconds < 0 {
		policy.BackoffSeconds = 0
	}
	return policy, nil
}

func isExpired(record notification.NotificationRecord, now time.Time) bool {
	return record.ExpiresAt != nil && !record.ExpiresAt.After(now)
}

func resolveProviderConfig(binding notification.ProviderBinding) (map[string]string, error) {
	if len(binding.ConfigRefs) == 0 {
		return nil, nil
	}

	resolved := make(map[string]string, len(binding.ConfigRefs))
	for key, envVar := range binding.ConfigRefs {
		if key == "" {
			return nil, fmt.Errorf("provider config ref key cannot be empty for connector %s", binding.ConnectorName)
		}
		if envVar == "" {
			return nil, fmt.Errorf("provider config ref %q has an empty env var name for connector %s", key, binding.ConnectorName)
		}

		value, ok := os.LookupEnv(envVar)
		if !ok || value == "" {
			return nil, fmt.Errorf("missing provider config env var %q for connector %s", envVar, binding.ConnectorName)
		}
		resolved[key] = value
	}

	return resolved, nil
}

func resolveBindingProviderConfig(ctx context.Context, store *postgres.Store, binding notification.ProviderBinding) (string, map[string]string, error) {
	if binding.ProviderAccountID != "" {
		account, err := store.GetProviderAccount(ctx, binding.ProviderAccountID)
		if err != nil {
			return "", nil, fmt.Errorf("load provider account %s: %w", binding.ProviderAccountID, err)
		}
		if !account.Enabled {
			return "", nil, fmt.Errorf("provider account %s is disabled", binding.ProviderAccountID)
		}

		resolved := make(map[string]string, len(account.Config)+len(account.SecretRefs))
		for key, value := range account.Config {
			if key == "" {
				return "", nil, fmt.Errorf("provider account %s has an empty config key", binding.ProviderAccountID)
			}
			resolved[key] = value
		}
		for key, ref := range account.SecretRefs {
			if key == "" {
				return "", nil, fmt.Errorf("provider account %s has an empty secret ref key", binding.ProviderAccountID)
			}
			value, err := secrets.Resolve(ref)
			if err != nil {
				return "", nil, fmt.Errorf("resolve secret ref %q for provider account %s: %w", key, binding.ProviderAccountID, err)
			}
			resolved[key] = value
		}
		return account.ProviderKey, resolved, nil
	}

	resolved, err := resolveProviderConfig(binding)
	return "", resolved, err
}

func newProviderBreakerManager(store *postgres.Store, logger *slog.Logger) *providerBreakerManager {
	return &providerBreakerManager{
		breakers: make(map[string]*gobreaker.CircuitBreaker),
		store:    store,
		logger:   logger,
	}
}

func (m *providerBreakerManager) breakerFor(binding notification.ProviderBinding, health notification.ProviderBindingHealth) *gobreaker.CircuitBreaker {
	m.mu.Lock()
	defer m.mu.Unlock()

	if existing, ok := m.breakers[binding.BindingID]; ok {
		if health.CircuitState == notification.ProviderCircuitStateClosed && existing.State() == gobreaker.StateOpen {
			delete(m.breakers, binding.BindingID)
		} else {
			return existing
		}
	}

	settings := gobreaker.Settings{
		Name:        binding.BindingID,
		Timeout:     providerCircuitCooldown,
		MaxRequests: 1,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return int(counts.ConsecutiveFailures) >= providerCircuitFailureThreshold
		},
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			if err := m.syncState(context.Background(), binding, to); err != nil {
				m.logger.Error("sync circuit breaker state failed", "error", err, "binding_id", binding.BindingID, "connector", binding.ConnectorName, "from", from.String(), "to", to.String())
			}
		},
	}
	breaker := gobreaker.NewCircuitBreaker(settings)
	m.breakers[binding.BindingID] = breaker
	return breaker
}

func loadProviderBindingHealth(ctx context.Context, store *postgres.Store, binding notification.ProviderBinding) (notification.ProviderBindingHealth, error) {
	health, err := store.GetProviderBindingHealth(ctx, binding.BindingID)
	if errors.Is(err, postgres.ErrNotFound) {
		return notification.ProviderBindingHealth{
			BindingID:     binding.BindingID,
			Channel:       binding.Channel,
			BindingSet:    binding.BindingSet,
			ConnectorName: binding.ConnectorName,
			CircuitState:  notification.ProviderCircuitStateClosed,
		}, nil
	}
	if err != nil {
		return notification.ProviderBindingHealth{}, err
	}
	return health, nil
}

func shouldSkipBindingForCircuit(health notification.ProviderBindingHealth, now time.Time) bool {
	if health.CircuitState != notification.ProviderCircuitStateOpen {
		return false
	}
	return health.CooldownUntil != nil && health.CooldownUntil.After(now)
}

func (m *providerBreakerManager) recordFailure(ctx context.Context, binding notification.ProviderBinding, failureClass notification.FailureClass, errorMessage string) (notification.ProviderBindingHealth, error) {
	health, err := loadProviderBindingHealth(ctx, m.store, binding)
	if err != nil {
		return notification.ProviderBindingHealth{}, err
	}
	breaker := m.breakerFor(binding, health)
	counts := breaker.Counts()
	now := time.Now().UTC()

	health.BindingID = binding.BindingID
	health.Channel = binding.Channel
	health.BindingSet = binding.BindingSet
	health.ConnectorName = binding.ConnectorName
	health.LastFailureClass = failureClass
	health.LastError = errorMessage
	health.LastFailureAt = &now
	health.ConsecutiveFailures = int(counts.ConsecutiveFailures)
	health.CircuitState = providerCircuitStateFromGoBreaker(breaker.State())
	if health.CircuitState == notification.ProviderCircuitStateOpen {
		if health.ConsecutiveFailures < providerCircuitFailureThreshold {
			health.ConsecutiveFailures = providerCircuitFailureThreshold
		}
		health.OpenedAt = &now
		cooldownUntil := now.Add(providerCircuitCooldown)
		health.CooldownUntil = &cooldownUntil
	}

	return health, m.store.UpsertProviderBindingHealth(ctx, health)
}

func (m *providerBreakerManager) recordSuccess(ctx context.Context, binding notification.ProviderBinding) error {
	return m.store.UpsertProviderBindingHealth(ctx, notification.ProviderBindingHealth{
		BindingID:           binding.BindingID,
		Channel:             binding.Channel,
		BindingSet:          binding.BindingSet,
		ConnectorName:       binding.ConnectorName,
		CircuitState:        notification.ProviderCircuitStateClosed,
		ConsecutiveFailures: 0,
	})
}

func (m *providerBreakerManager) syncState(ctx context.Context, binding notification.ProviderBinding, state gobreaker.State) error {
	health, err := loadProviderBindingHealth(ctx, m.store, binding)
	if err != nil && !errors.Is(err, postgres.ErrNotFound) {
		return err
	}

	now := time.Now().UTC()
	health.BindingID = binding.BindingID
	health.Channel = binding.Channel
	health.BindingSet = binding.BindingSet
	health.ConnectorName = binding.ConnectorName
	health.CircuitState = providerCircuitStateFromGoBreaker(state)

	switch state {
	case gobreaker.StateOpen:
		health.OpenedAt = &now
		cooldownUntil := now.Add(providerCircuitCooldown)
		health.CooldownUntil = &cooldownUntil
	case gobreaker.StateClosed:
		health.OpenedAt = nil
		health.CooldownUntil = nil
		health.ConsecutiveFailures = 0
	}

	return m.store.UpsertProviderBindingHealth(ctx, health)
}

func parsePositiveInt(raw string, fallback int) int {
	value, err := strconv.Atoi(raw)
	if err != nil || value < 1 {
		return fallback
	}
	return value
}

func classifyConnectorFailure(err error) (notification.FailureClass, bool) {
	if err == nil {
		return notification.FailureClassPermanent, false
	}

	var sendErr connectorSendError
	if errors.As(err, &sendErr) {
		classification := sendErr.Classification
		if classification == "" {
			classification = notification.FailureClassPermanent
		}
		return classification, sendErr.Retryable || isRetryableFailureClass(classification)
	}

	if _, ok := errors.AsType[net.Error](err); ok {
		return notification.FailureClassTransient, true
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return notification.FailureClassTransient, true
	}

	message := strings.ToLower(err.Error())
	switch {
	case strings.Contains(message, "missing provider config env var"),
		strings.Contains(message, "provider config ref"),
		strings.Contains(message, "empty env var name"):
		return notification.FailureClassMisconfigured, false
	case strings.Contains(message, "connection refused"),
		strings.Contains(message, "no such host"),
		strings.Contains(message, "timeout"),
		strings.Contains(message, "temporarily unavailable"),
		strings.Contains(message, "eof"):
		return notification.FailureClassTransient, true
	default:
		return notification.FailureClassPermanent, false
	}
}

func classifyHTTPStatus(statusCode int) notification.FailureClass {
	switch statusCode {
	case http.StatusTooManyRequests:
		return notification.FailureClassRateLimited
	case http.StatusUnauthorized, http.StatusForbidden:
		return notification.FailureClassUnauthorized
	case http.StatusBadRequest, http.StatusUnprocessableEntity:
		return notification.FailureClassInvalidRequest
	case http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return notification.FailureClassTransient
	default:
		if statusCode >= 500 {
			return notification.FailureClassTransient
		}
		if statusCode >= 400 {
			return notification.FailureClassPermanent
		}
		return notification.FailureClassPermanent
	}
}

func isRetryableHTTPStatus(statusCode int) bool {
	if statusCode == http.StatusTooManyRequests {
		return true
	}
	return statusCode >= 500
}

func isRetryableFailureClass(classification notification.FailureClass) bool {
	switch classification {
	case notification.FailureClassTransient, notification.FailureClassRateLimited:
		return true
	default:
		return false
	}
}

func providerCircuitStateFromGoBreaker(state gobreaker.State) notification.ProviderCircuitState {
	switch state {
	case gobreaker.StateOpen:
		return notification.ProviderCircuitStateOpen
	default:
		return notification.ProviderCircuitStateClosed
	}
}
